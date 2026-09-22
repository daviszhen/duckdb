//===----------------------------------------------------------------------===//
// Git4Data -- `git4data_apply`: ordinary SQL written against a delta branch
//===----------------------------------------------------------------------===//
// `PragmaGit4DataApply` translates INSERT, UPDATE and DELETE into edits of the two tables that hold a delta
// branch's line, and the helpers it needs are here with it: the checks that the statement names the branch's
// table directly, and the walkers that point a parsed query at a reference. Increment 3fo moved it out of the
// file that keeps the merge, which 3gb renamed to `git4data_pragma_merge.cpp` for what it holds.
//===----------------------------------------------------------------------===//

#include "duckdb/function/pragma/pragma_functions.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>

#ifdef __APPLE__
#include <sys/clonefile.h>
#endif
#ifdef __linux__
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <unistd.h>
#endif

#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/git4data_registry.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/tableref/at_clause.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/literal.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <cctype>

#include "duckdb/function/pragma/git4data_pragmas.hpp"

#include "duckdb/function/pragma/git4data_pragmas.hpp"

namespace duckdb {

//! Give a branch a table of its own for a source table, which is what lets it be written to: a
//! branch read as the pinned image cannot diverge, because the image is shared with everything else
//! on the timeline. The copy is made by the statement this returns; the mapping is recorded first so
//! a read of the branch can see it, and a mapping whose table never appears falls back to the pinned
//! image instead of failing.
static void Git4DataPointExprAtBranch(ParsedExpression &expr, const string &table_name, const string &branch_name);

static void Git4DataPointRefAtBranch(TableRef &ref, const string &table_name, const string &branch_name) {
	if (ref.type != TableReferenceType::BASE_TABLE) {
		// the iterator has already walked into joins, subqueries and table functions
		return;
	}
	auto &base = ref.Cast<BaseTableRef>();
	if (base.at_clause || base.GetQualifiedName().Name().GetIdentifierName() != table_name) {
		return;
	}
	// a ConstantExpression is built from a Literal, and a string constant is a string literal
	base.at_clause =
	    make_uniq<AtClause>(Identifier("BRANCH"), make_uniq<ConstantExpression>(Literal::String(branch_name)));
}

static void Git4DataPointQueryAtBranch(QueryNode &node, const string &table_name, const string &branch_name) {
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node, [&](unique_ptr<ParsedExpression> &child) { Git4DataPointExprAtBranch(*child, table_name, branch_name); },
	    [&](TableRef &ref) { Git4DataPointRefAtBranch(ref, table_name, branch_name); });
}

static void Git4DataPointExprAtBranch(ParsedExpression &expr, const string &table_name, const string &branch_name) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expr.Cast<SubqueryExpression>();
		if (subquery.Subquery()) {
			Git4DataPointQueryAtBranch(*subquery.SubqueryMutable()->node, table_name, branch_name);
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<ParsedExpression> &child) { Git4DataPointExprAtBranch(*child, table_name, branch_name); });
}

//! The table a write statement names has to be the table the delta is for, resolved and compared - the
//! name alone would match a same-named table in another schema.
static void Git4DataCheckApplyTargetTable(ClientContext &context, const QualifiedName &written_name,
                                          TableCatalogEntry &source) {
	auto &table_name = source.name.GetIdentifierName();
	// the name is not the table: a statement can reach a same-named table in another schema, and the write
	// is recorded against the name the pragma resolved. Both are resolved here, so the table is compared
	// rather than its name.
	auto by_name = Catalog::GetEntry<TableCatalogEntry>(context, written_name, OnEntryNotFound::RETURN_NULL);
	if (by_name && by_name->oid == source.oid) {
		return;
	}
	// The clause has to be true of the table the statement actually named: "a different table of that name"
	// describes the same-named table in another schema, and it was said about a table with a *different* name
	// too - measured (increment 3cj), `apply('a', 'br', 'INSERT INTO other ...')` answered "the statement writes
	// "other", which is a different table of that name, in schema "main"", where the name is the thing that is
	// not the same.
	auto same_name = by_name && StringUtil::CIEquals(by_name->name.GetIdentifierName(), table_name);
	throw InvalidInputException(
	    "git4data_apply was given \"%s\", but the statement writes \"%s\", which is %s: the write would be "
	    "recorded in the delta of \"%s\". Write the table the branch's delta is for.",
	    table_name, written_name.ToString(),
	    by_name ? (same_name ? "a different table of that name, in schema \"" +
	                               by_name->ParentSchema().name.GetIdentifierName() + "\""
	                         : "a different table")
	            : "not a table",
	    table_name);
}

//! The statement a delta branch is written through has to name the table the delta is for, and name it
//! directly: a write through another name would be recorded against the wrong line, and a join has no
//! single row of that table to record.
static void Git4DataCheckApplyTarget(ClientContext &context, TableRef &ref, TableCatalogEntry &source) {
	auto &table_name = source.name.GetIdentifierName();
	if (ref.type != TableReferenceType::BASE_TABLE) {
		throw InvalidInputException("git4data_apply takes a statement that names \"%s\" directly, not one that "
		                            "writes through another table or a subquery",
		                            table_name);
	}
	auto &base = ref.Cast<BaseTableRef>();
	auto written = base.GetQualifiedName().Name().GetIdentifierName();
	// a name in SQL is not case-sensitive, and neither is this: measured, "Mixed" and mixed are the same
	// table, and comparing them as strings refused a statement that wrote the right one
	if (!StringUtil::CIEquals(written, table_name)) {
		throw InvalidInputException("git4data_apply was given \"%s\", but the statement writes \"%s\": the statement "
		                            "has to write the table the branch's delta is for",
		                            table_name, written);
	}
	Git4DataCheckApplyTargetTable(context, base.GetQualifiedName(), source);
}

//! Apply one statement of ordinary SQL to a branch that inherits by delta. The statement is translated
//! rather than executed: what holds a delta branch's line is the two tables that say which base keys it
//! hides and which rows it adds, so a write is an edit to those two - and the caller writes the SQL it
//! would have written anyway, at a cost set by the change rather than by the table.
//!
//! The branch's own image is read for every shape, never the live table, and it is read *before* the
//! delta is edited, so the rows a statement touches are the rows the branch shows:
//!
//!   DELETE   the affected keys go into `removed` and the branch's own added rows for those keys go
//!   UPDATE   the keys the statement touched go into `removed` and the new rows into `added`. A key the
//!            statement rewrites is therefore a removal and an addition, which is what an update is in
//!            a delta's terms - the pairing a read needs, kept by the feature rather than by the caller
//!   INSERT   the rows go into `added`. A key the branch was hiding stays hidden: the branch does not
//!            show that row, so inserting it adds one, and the base row stays where the delta put it
//! What every translated statement of `git4data_apply` is written against: the delta's two tables, the branch's
//! own image of the source, and the temporary names a translation may use. Built once per call, so the three
//! statement shapes below read it rather than rebuilding it.
struct Git4DataApplyTranslation {
	TableCatalogEntry &table;
	string at_clause;
	string key_list;
	vector<string> key_columns;
	string quoted_added;
	string quoted_removed;
	string rows_name;
	string keys_name;
	string alias_rows;
	string alias_keys;
	string alias_added;
	string alias_removed;
	string alias_branch;
	string branch_image;

	//! The condition that says two aliases name the same row
	string Match(const string &left_alias, const string &right_alias) const {
		string result;
		for (auto &key : key_columns) {
			if (!result.empty()) {
				result += " AND ";
			}
			auto quoted = KeywordHelper::WriteOptionallyQuoted(key);
			result += StringUtil::Format("%s.%s = %s.%s", left_alias, quoted, right_alias, quoted);
		}
		return result;
	}

	//! The statement that records the rows - or the keys - `from_name` holds as removed
	string AddRemovals(const string &from_name, const string &from_alias) const {
		return StringUtil::Format("INSERT INTO %s (%s) SELECT %s FROM %s %s WHERE NOT EXISTS (SELECT 1 FROM %s %s "
		                          "WHERE %s);\n",
		                          quoted_removed, key_list, key_list, from_name, from_alias, quoted_removed,
		                          alias_removed, Match(alias_removed, from_alias));
	}
};

//! The rows a DELETE takes out of the branch: their keys are what the delta hides.
static string Git4DataApplyDelete(ClientContext &context, Git4DataApplyTranslation &t, SQLStatement &statement) {
			auto &node = statement.Cast<DeleteStatement>().node;
			if (!node->using_clauses.empty()) {
				throw InvalidInputException("git4data_apply does not take DELETE ... USING: the rows to remove have to "
				                            "be the branch's own, which a second table is not");
			}
			if (!node->returning_list.empty()) {
				throw InvalidInputException("git4data_apply does not take RETURNING: nothing is written where the rows "
				                            "would come from");
			}
			Git4DataCheckApplyTarget(context, *node->table, t.table);
			auto condition = node->condition ? node->condition->ToString() : string();
			string result =
			    StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM %s%s;\n", t.rows_name, t.key_list,
			                       t.at_clause, condition.empty() ? string() : " WHERE " + condition);
			result +=
			    StringUtil::Format("DELETE FROM %s %s WHERE EXISTS (SELECT 1 FROM %s %s WHERE %s);\n", t.quoted_added,
			                       t.alias_added, t.rows_name, t.alias_rows, t.Match(t.alias_added, t.alias_rows));
			result += t.AddRemovals(t.rows_name, t.alias_rows);
			result += StringUtil::Format("DROP TABLE %s;", t.rows_name);
			return result;
}

//! An UPDATE is a removal and an addition in a delta's terms: the keys the statement rewrote go into
//! `removed`, and the branch's own rows for them, with the assignments applied, go into `added`.
static string Git4DataApplyUpdate(ClientContext &context, Git4DataApplyTranslation &t, SQLStatement &statement) {
			auto &node = statement.Cast<UpdateStatement>().node;
			if (node->from_table) {
				throw InvalidInputException("git4data_apply does not take UPDATE ... FROM: the rows to change have to "
				                            "be the branch's own, which a joined table is not");
			}
			if (!node->returning_list.empty()) {
				throw InvalidInputException("git4data_apply does not take RETURNING: nothing is written where the rows "
				                            "would come from");
			}
			Git4DataCheckApplyTarget(context, *node->table, t.table);
			auto &set_info = *node->set_info;
			auto condition = set_info.condition ? set_info.condition->ToString() : string();
			// the new rows: the branch's own columns, with the assigned ones replaced by what they are
			// assigned. A row's other columns are the row's own, which is what a SET list means.
			vector<pair<string, string>> assignments;
			for (idx_t i = 0; i < set_info.columns.size(); i++) {
				assignments.emplace_back(set_info.columns[i].GetIdentifierName(), set_info.expressions[i]->ToString());
			}
			string new_columns;
			for (auto &column : t.table.GetColumns().Logical()) {
				auto name = column.Name().GetIdentifierName();
				if (!new_columns.empty()) {
					new_columns += ", ";
				}
				string value;
				for (auto &assignment : assignments) {
					if (assignment.first == name) {
						value = assignment.second;
						break;
					}
				}
				// every column is named: a SET expression that is a literal or an expression takes its
				// name from the expression otherwise, and the rows table is joined on its keys -
				// measured, `SET k = 5` produced a column called "5" and the join could not find the key
				new_columns += value.empty() ? KeywordHelper::WriteOptionallyQuoted(name)
				                             : (value + " AS " + KeywordHelper::WriteOptionallyQuoted(name));
			}
			string result =
			    StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM %s%s;\n", t.keys_name, t.key_list,
			                       t.at_clause, condition.empty() ? string() : " WHERE " + condition);
			result += StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM %s%s;\n", t.rows_name,
			                             new_columns, t.at_clause, condition.empty() ? string() : " WHERE " + condition);
			result +=
			    StringUtil::Format("DELETE FROM %s %s WHERE EXISTS (SELECT 1 FROM %s %s WHERE %s);\n", t.quoted_added,
			                       t.alias_added, t.keys_name, t.alias_keys, t.Match(t.alias_added, t.alias_keys));
			result += t.AddRemovals(t.keys_name, t.alias_keys);
			// A key the statement rewrites onto a key the branch already shows is the collision the same
			// statement against the table itself raises, and here it was a silent wrong answer: measured,
			// `UPDATE t SET k = 5 WHERE k = 3` with both keys present left the branch showing *two* rows
			// for k = 5, where the table itself refuses with a duplicate key. Keys the statement is
			// replacing are not collisions - a row keeping its key, or two rows swapping theirs, is an
			// ordinary update - so the check asks whether a new key is one the branch shows and the
			// statement is not rewriting.
			result += StringUtil::Format(
			    "INSERT INTO %s SELECT * FROM %s %s WHERE (SELECT CASE WHEN EXISTS (SELECT 1 FROM %s JOIN %s %s ON "
			    "(%s) WHERE NOT EXISTS (SELECT 1 FROM %s %s WHERE %s)) THEN error('update collides with a row the "
			    "branch already shows: two rows would carry the same key, which the table itself refuses. Delete "
			    "the row that key belongs to, or rewrite it to a key the branch does not have') ELSE 1 END) = 1;\n",
			    t.quoted_added, t.rows_name, t.alias_rows, t.branch_image, t.rows_name, t.alias_rows,
			    t.Match(t.alias_branch, t.alias_rows), t.keys_name, t.alias_keys, t.Match(t.alias_keys, t.alias_branch));
			result += StringUtil::Format("DROP TABLE %s;\nDROP TABLE %s;", t.keys_name, t.rows_name);
			return result;
}

//! The rows an INSERT adds, landed in the branch's own `added` table. A key the branch was hiding stays
//! hidden, because the branch does not show that row.
static string Git4DataApplyInsert(ClientContext &context, Git4DataApplyTranslation &t, SQLStatement &statement) {
			auto &node = statement.Cast<InsertStatement>().node;
			if (node->on_conflict_info) {
				throw InvalidInputException("git4data_apply does not take ON CONFLICT: a branch that keeps its own "
				                            "changes has no conflict for the base to resolve");
			}
			if (!node->returning_list.empty()) {
				throw InvalidInputException("git4data_apply does not take RETURNING: nothing is written where the rows "
				                            "would come from");
			}
			if (node->default_values) {
				throw InvalidInputException("git4data_apply does not take DEFAULT VALUES: the defaults are the source "
				                            "table's, and the table this branch adds rows to is not it");
			}
			if (node->column_order != InsertColumnOrder::INSERT_BY_POSITION) {
				throw InvalidInputException("git4data_apply does not take INSERT ... BY NAME");
			}
			if (!node->select_statement) {
				throw InvalidInputException("git4data_apply needs rows to insert");
			}
			// the rows are *for* the table the delta is for, and the statement is translated rather than
			// executed, so the name it writes is never read again. The target is a qualified name here; the
			// insert's table reference is not always there.
			Git4DataCheckApplyTargetTable(context, node->qualified_name, t.table);
			// the column list the insert names, or the table's own columns in order when it names none
			vector<string> insert_columns;
			for (auto &column : node->columns) {
				insert_columns.push_back(column.GetIdentifierName());
			}
			if (insert_columns.empty()) {
				for (auto &column : t.table.GetColumns().Logical()) {
					insert_columns.push_back(column.Name().GetIdentifierName());
				}
			}
			string insert_list;
			for (idx_t i = 0; i < insert_columns.size(); i++) {
				if (i > 0) {
					insert_list += ", ";
				}
				insert_list += KeywordHelper::WriteOptionallyQuoted(insert_columns[i]);
			}
			// The rows are landed in a table whose columns are the ones the insert names, in that order, so
			// the rows can be checked and then added by position. Naming them here rather than aliasing the
			// select avoids depending on what the parser calls the columns of a VALUES list - it calls them
			// `valueslist`, measured.
			string declared;
			for (auto &name : insert_columns) {
				auto &column = t.table.GetColumns().GetColumn(Identifier(name));
				if (!declared.empty()) {
					declared += ", ";
				}
				declared += KeywordHelper::WriteOptionallyQuoted(name) + " " + column.Type().ToString();
			}
			string result = StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s (%s);\n", t.rows_name, declared);
			result += StringUtil::Format("INSERT INTO %s (%s) %s;\n", t.rows_name, insert_list,
			                             node->select_statement->ToString());
			result += StringUtil::Format(
			    "INSERT INTO %s (%s) SELECT * FROM %s WHERE (SELECT CASE WHEN EXISTS (SELECT 1 FROM %s JOIN %s %s ON "
			    "(%s)) THEN error('insert collides with a row the branch already shows: use an update, which a delta "
			    "can record, or delete the row first') ELSE 1 END) = 1;\n",
			    t.quoted_added, insert_list, t.rows_name, t.branch_image, t.rows_name, t.alias_rows,
			    t.Match(t.alias_branch, t.alias_rows));
			result += StringUtil::Format("DROP TABLE %s;", t.rows_name);
			return result;
}

string PragmaGit4DataApply(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto dml = parameters.values[2].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	auto branch = Git4DataRequiredBranch(context, transaction_manager, branch_name);
	// the source table can be renamed under the mapping, and the mapping follows the table it was bound to:
	// the oid finds the delta a name no longer does, exactly as it does on a read
	auto already = Git4DataResolveTableOrNull(context, table_name);
	auto write_key = already ? Git4DataSourceKey(*already) : table_name;
	auto found_delta = Git4DataFindDeltaTable(context, branch, write_key, already ? already->oid : 0);

	if (!found_delta) {
		throw InvalidInputException(
		    "Branch \"%s\" does not differ from its base by a delta for \"%s\", so a write has nowhere to be "
		    "recorded. Give the branch a delta for that table first, or materialize the branch and write to the "
		    "table it then has of its own.",
		    branch_name, table_name);
	}
	auto delta = *found_delta;
	auto &table = Git4DataResolveTable(context, table_name);
	auto key_columns = Git4DataPrimaryKeyColumnNames(table);
	if (key_columns.empty()) {
		throw InvalidInputException("Table \"%s\" has no primary key, so a delta cannot say which rows a write "
		                            "removes",
		                            table_name);
	}
	auto added_table = Git4DataResolveTableName(context, delta.added, delta.added_oid);
	auto removed_table = Git4DataResolveTableName(context, delta.removed, delta.removed_oid);
	if (added_table.empty() || removed_table.empty()) {
		throw InvalidInputException("The delta branch \"%s\" differs by is gone: \"%s\" or \"%s\" is not there "
		                            "any more",
		                            branch_name, delta.added, delta.removed);
	}

	Parser parser(context.GetParserOptions());
	parser.ParseQuery(dml);
	if (parser.statements.empty()) {
		throw InvalidInputException("git4data_apply needs at least one statement to apply");
	}

	auto quoted_added = Git4DataTableSQLName(added_table);
	auto quoted_removed = Git4DataTableSQLName(removed_table);
	auto at_clause = FormatGit4DataAtClause(table_name, branch_name, "BRANCH");
	// the statement reads the source table, the branch's image of it and the two delta tables, so no
	// alias may be a column of any of them
	auto taken = Git4DataScopeColumnNames(context, transaction_manager, table_name, {branch_name});
	Git4DataAddColumnNames(context, added_table, taken);
	Git4DataAddColumnNames(context, removed_table, taken);
	string key_list;
	for (idx_t i = 0; i < key_columns.size(); i++) {
		if (i > 0) {
			key_list += ", ";
		}
		key_list += KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
	}
	// the names are handed to the translation in the order they were taken: a helper name is registered as
	// taken when it is asked for, so the order is what keeps them distinct
	Git4DataApplyTranslation t {table, at_clause, key_list, key_columns, quoted_added, quoted_removed,
	                            Git4DataFreeHelperName(context, "g4d_apply_rows", taken),
	                            Git4DataFreeHelperName(context, "g4d_apply_keys", taken),
	                            Git4DataFreeName("g4d_ar", taken), Git4DataFreeName("g4d_ak", taken),
	                            Git4DataFreeName("g4d_aa", taken), Git4DataFreeName("g4d_arr", taken),
	                            Git4DataFreeName("g4d_ab", taken), string()};
	t.branch_image = StringUtil::Format("(SELECT * FROM %s) %s", t.at_clause, t.alias_branch);

	// the statements read the branch, so their own references to the table are pointed at it before
	// anything is rendered
	for (auto &statement : parser.statements) {
		switch (statement->type) {
		case StatementType::INSERT_STATEMENT:
			Git4DataPointQueryAtBranch(*statement->Cast<InsertStatement>().node, table_name, branch_name);
			break;
		case StatementType::UPDATE_STATEMENT:
			Git4DataPointQueryAtBranch(*statement->Cast<UpdateStatement>().node, table_name, branch_name);
			break;
		case StatementType::DELETE_STATEMENT:
			Git4DataPointQueryAtBranch(*statement->Cast<DeleteStatement>().node, table_name, branch_name);
			break;
		default:
			break;
		}
	}
	// Every statement is translated against the same delta, and the whole group is handed back as one
	// string so the caller's statements land in one transaction: either all of them are recorded or
	// none is, which is what makes a group a unit of work rather than a sequence of writes.
	auto translate = [&](SQLStatement &statement) {
		switch (statement.type) {
		case StatementType::DELETE_STATEMENT:
			return Git4DataApplyDelete(context, t, statement);
		case StatementType::UPDATE_STATEMENT:
			return Git4DataApplyUpdate(context, t, statement);
		case StatementType::INSERT_STATEMENT:
			return Git4DataApplyInsert(context, t, statement);
		default:
			throw InvalidInputException("git4data_apply takes INSERT, UPDATE and DELETE statements, and this one is "
			                            "not one of them");
		}
	};
	string result;
	for (auto &statement : parser.statements) {
		result += translate(*statement);
		result += "\n";
	}
	return result;
}
} // namespace duckdb
