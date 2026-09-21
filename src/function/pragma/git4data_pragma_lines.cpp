//===----------------------------------------------------------------------===//
// Git4Data -- the pragmas that reason about two lines
//===----------------------------------------------------------------------===//
// pick, apply and merge compare one reference's line against another's, and the helpers they need for that: the
// scope a statement is pointed at, the comparison a pick builds, the internal reference a merge names its base
// with, and the shared ancestor a captured line forks from. They lived in git4data_pragmas.cpp with the rest of
// the feature's pragmas; increment 3et moved them here without changing a line of them, apart from the `static`
// that a header declaration cannot keep. What they share with that file is in git4data_pragmas.hpp.
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// Git4Data -- the feature's pragmas
//===----------------------------------------------------------------------===//
// Moved out of pragma_functions.cpp by increment 3er without changing a line of any of them, apart from taking
// the feature's `set.AddFunction` lines out of that file's registration and into RegisterGit4DataPragmas below.
// What they share with the rest of the feature is in git4data_registry.hpp and the read path's own header.
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

static case_insensitive_set_t Git4DataScopeColumnNames(ClientContext &context,
                                                       DuckTransactionManager &transaction_manager,
                                                       const string &table_name, const vector<string> &references) {
	case_insensitive_set_t names;
	Git4DataAddColumnNames(context, table_name, names);
	auto entry = Git4DataResolveTableOrNull(context, table_name);
	auto key = entry ? Git4DataSourceKey(*entry) : table_name;
	for (auto &reference : references) {
		Git4DataBranch branch;
		if (!transaction_manager.branches.Get(reference, branch)) {
			continue;
		}
		auto storage = Git4DataFindStorageTable(context, branch, key, entry ? entry->oid : 0);
		if (!storage) {
			continue;
		}
		auto resolved = Git4DataResolveTableName(context, storage->storage, storage->oid);
		if (!resolved.empty()) {
			Git4DataAddColumnNames(context, resolved, names);
		}
	}
	return names;
}

//! A name for one of the feature's own aliases or helper tables that no column in the statement carries.
//! An alias is in scope beside those columns, so one that a column happens to share makes the generated
//! SQL ambiguous - measured, with a column named g4d_b: "Ambiguous reference to column name \"g4d_b\""
//! from the merge. Names are lengthened until they are clear rather than refused, because the caller's
//! table is not at fault and the aliases are the feature's own. A column named like a name this function
//! is *asked* for simply moves the feature's name along.
static string Git4DataFreeName(const string &base, const case_insensitive_set_t &taken) {
	auto name = base;
	while (taken.find(name) != taken.end()) {
		name += "_";
	}
	return name;
}

//! The name of one of the feature's temporary helper tables, which has to clear a second thing as well:
//! the statement that creates it is `CREATE OR REPLACE TEMP TABLE`, so a table the caller already holds
//! under that name would be replaced and then dropped along with the helper. Measured: a caller's own
//! `CREATE TEMP TABLE g4d_merge_conflicts (x)` holding a row was gone after a merge, and the pick's
//! helper has the same shape. A permanent table of that name is not destroyed - the temporary one
//! shadows it for the length of the group, and the drop takes the temporary one - but it is left alone
//! rather than shadowed, which costs only a longer internal name.
static string Git4DataFreeHelperName(ClientContext &context, const string &base, const case_insensitive_set_t &taken) {
	auto name = base;
	while (
	    taken.find(name) != taken.end() ||
	    Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(name)), OnEntryNotFound::RETURN_NULL)) {
		name += "_";
	}
	return name;
}

string PragmaGit4DataPick(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto source = parameters.values[1].ToString();
	auto base = parameters.values[2].ToString();
	// Overwriting is what a pick did before there was a choice, so it stays the default - it answers
	// "make the table look like the source", and the caller named the direction. fail and skip are
	// there because the live table may have moved on since the base, and overwriting would throw away
	// a decision the pick never saw.
	auto policy = parameters.values.size() > 3 ? StringUtil::Lower(parameters.values[3].ToString()) : "overwrite";
	if (policy != "overwrite" && policy != "fail" && policy != "skip") {
		throw InvalidInputException("Pick policy has to be overwrite, fail or skip, not \"%s\"", policy);
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	// both names resolve before any statement is generated, so a typo cannot half-apply a pick
	auto source_unit = ResolveGit4DataReference(transaction_manager, source);
	auto base_unit = ResolveGit4DataReference(transaction_manager, base);

	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
	// a pick writes the live table through the feature's own statements, so it is refused the registry's tables
	// for the reason a restore is
	Git4DataCheckRegistryTarget(table.ParentSchema().name.GetIdentifierName(), table.name.GetIdentifierName());
	auto key_columns = Git4DataPrimaryKeyColumnNames(table);
	if (key_columns.empty()) {
		throw InvalidInputException("Table \"%s\" has no primary key, so a pick cannot tell which rows to "
		                            "apply the changes to",
		                            table_name);
	}
	string key_list;
	for (idx_t i = 0; i < key_columns.size(); i++) {
		if (i > 0) {
			key_list += ", ";
		}
		key_list += KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
	}
	auto quoted_table = Git4DataTableSQLName(table_name);
	auto diff = StringUtil::Format("git4data_diff('%s', '%s', '%s')", Git4DataSQLLiteral(table_name), Git4DataSQLLiteral(base), Git4DataSQLLiteral(source));

	// The key is what the two sides are matched on, not a row id: a reference scan does not expose
	// row ids, and the key is the identity the diff already pairs updates by.
	// A conflict is a key the source changed whose live row has also changed since the base, and where
	// the two sides did not end up with the same row. Both sides making the same change is agreement,
	// not a conflict - the same rule a merge applies.
	auto base_image = FormatGit4DataAtClause(table_name, base, base_unit);
	auto source_image = FormatGit4DataAtClause(table_name, source, source_unit);
	// the statement reads the live table and both images, so no alias may be a column of any of them
	auto taken = Git4DataScopeColumnNames(context, transaction_manager, table_name, {base, source});
	auto alias_live = Git4DataFreeName("g4d_l", taken);
	auto alias_base = Git4DataFreeName("g4d_b", taken);
	auto alias_source = Git4DataFreeName("g4d_s", taken);
	auto alias_keys = Git4DataFreeName("g4d_tk", taken);
	auto conflict_name = Git4DataFreeHelperName(context, "g4d_pick_conflicts", taken);
	string conflict_table;
	if (policy != "overwrite") {
		string key_list_joined;
		string join_live;
		string join_base;
		string join_source;
		string row_comparison_base;
		string row_comparison_source;
		string presence_base;
		string presence_source;
		for (idx_t i = 0; i < key_columns.size(); i++) {
			auto quoted = KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
			if (i > 0) {
				key_list_joined += ", ";
				join_live += " AND ";
				join_base += " AND ";
				join_source += " AND ";
				presence_base += " OR ";
				presence_source += " OR ";
			}
			key_list_joined += alias_keys + "." + quoted;
			join_live += StringUtil::Format("%s.%s = %s.%s", alias_live, quoted, alias_keys, quoted);
			join_base += StringUtil::Format("%s.%s = %s.%s", alias_base, quoted, alias_keys, quoted);
			join_source += StringUtil::Format("%s.%s = %s.%s", alias_source, quoted, alias_keys, quoted);
			// a key is never null, so a null one in an outer join means that side has no such row
			presence_base +=
			    StringUtil::Format("(%s.%s IS NULL) <> (%s.%s IS NULL)", alias_live, quoted, alias_base, quoted);
			presence_source +=
			    StringUtil::Format("(%s.%s IS NULL) <> (%s.%s IS NULL)", alias_live, quoted, alias_source, quoted);
		}
		auto live_key = alias_live + "." + KeywordHelper::WriteOptionallyQuoted(key_columns[0]);
		auto changed_from_base = StringUtil::Format("((%s) OR (%s IS NOT NULL AND NOT (%s)))", presence_base, live_key,
		                                            BuildGit4DataRowComparison(table, alias_live, alias_base));
		auto differs_from_source =
		    StringUtil::Format("((%s) OR (%s IS NOT NULL AND NOT (%s)))", presence_source, live_key,
		                       BuildGit4DataRowComparison(table, alias_live, alias_source));
		conflict_table = StringUtil::Format(
		    "CREATE OR REPLACE TEMP TABLE %s AS WITH %s AS (SELECT %s FROM %s WHERE g4d_change IN "
		    "('added', 'removed', 'changed')), %s AS (SELECT * FROM %s), %s AS (SELECT * FROM %s), %s AS "
		    "(SELECT * FROM %s) SELECT %s FROM %s LEFT JOIN %s ON (%s) LEFT JOIN %s ON (%s) LEFT JOIN "
		    "%s ON (%s) WHERE (%s) AND (%s);\n",
		    conflict_name, alias_keys, key_list, diff, alias_live, quoted_table, alias_base, base_image, alias_source,
		    source_image, key_list_joined, alias_keys, alias_live, join_live, alias_base, join_base, alias_source,
		    join_source, changed_from_base, differs_from_source);
	}

	// A changed row is applied as a delete followed by an insert: there is no in-place update to reach
	// for here, and the outcome is the same.
	string result = conflict_table;
	if (policy == "fail") {
		result += StringUtil::Format(
		    "SELECT CASE WHEN EXISTS (SELECT 1 FROM %s) THEN error('pick conflict: the live "
		    "table changed the same rows since the base - pass policy skip to leave them, or overwrite to "
		    "take the source') ELSE 0 END;\n",
		    conflict_name);
	}
	auto conflict_filter =
	    policy == "skip" ? StringUtil::Format(" AND (%s) NOT IN (SELECT %s FROM %s)", key_list, key_list, conflict_name)
	                     : string();
	result += StringUtil::Format("DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change IN ('removed', "
	                             "'changed')%s);\n",
	                             quoted_table, key_list, key_list, diff, conflict_filter);
	// Only the keys the live table is actually missing: a row the source added may well still be there,
	// because the live table is usually the source's own continuation, and inserting it again would
	// collide with its primary key instead of restoring anything. The keys of changed rows were removed
	// by the statement above, so they are missing by the time this runs and take the source's values.
	result += StringUtil::Format("INSERT INTO %s SELECT * FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change "
	                             "IN ('added', 'changed')%s) AND (%s) NOT IN (SELECT %s FROM %s);",
	                             quoted_table, source_image, key_list, key_list, diff, conflict_filter, key_list,
	                             key_list, quoted_table);
	if (!conflict_table.empty()) {
		result += StringUtil::Format("\nDROP TABLE %s;", conflict_name);
	}
	return result;
}

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

	auto quoted_source = Git4DataTableSQLName(table_name);
	auto quoted_added = Git4DataTableSQLName(added_table);
	auto quoted_removed = Git4DataTableSQLName(removed_table);
	auto at_clause = FormatGit4DataAtClause(table_name, branch_name, "BRANCH");
	// the statement reads the source table, the branch's image of it and the two delta tables, so no
	// alias may be a column of any of them
	auto taken = Git4DataScopeColumnNames(context, transaction_manager, table_name, {branch_name});
	Git4DataAddColumnNames(context, added_table, taken);
	Git4DataAddColumnNames(context, removed_table, taken);
	auto rows_name = Git4DataFreeHelperName(context, "g4d_apply_rows", taken);
	auto keys_name = Git4DataFreeHelperName(context, "g4d_apply_keys", taken);
	auto alias_rows = Git4DataFreeName("g4d_ar", taken);
	auto alias_keys = Git4DataFreeName("g4d_ak", taken);
	auto alias_added = Git4DataFreeName("g4d_aa", taken);
	auto alias_removed = Git4DataFreeName("g4d_arr", taken);
	auto alias_branch = Git4DataFreeName("g4d_ab", taken);

	string key_list;
	string column_list;
	for (idx_t i = 0; i < key_columns.size(); i++) {
		if (i > 0) {
			key_list += ", ";
		}
		key_list += KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
	}
	for (auto &column : table.GetColumns().Logical()) {
		if (!column_list.empty()) {
			column_list += ", ";
		}
		column_list += KeywordHelper::WriteOptionallyQuoted(column.Name().GetIdentifierName());
	}
	auto match = [&](const string &left_alias, const string &right_alias) {
		string result;
		for (auto &key : key_columns) {
			if (!result.empty()) {
				result += " AND ";
			}
			auto quoted = KeywordHelper::WriteOptionallyQuoted(key);
			result += StringUtil::Format("%s.%s = %s.%s", left_alias, quoted, right_alias, quoted);
		}
		return result;
	};
	auto branch_image = StringUtil::Format("(SELECT * FROM %s) %s", at_clause, alias_branch);
	// a hidden base row stays hidden, and an editing statement never names a key twice in the table that
	// hides rows: the table is the caller's and may well carry the primary key
	auto add_removals = [&](const string &from_name, const string &from_alias) {
		return StringUtil::Format("INSERT INTO %s (%s) SELECT %s FROM %s %s WHERE NOT EXISTS (SELECT 1 FROM %s %s "
		                          "WHERE %s);\n",
		                          quoted_removed, key_list, key_list, from_name, from_alias, quoted_removed,
		                          alias_removed, match(alias_removed, from_alias));
	};

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
		case StatementType::DELETE_STATEMENT: {
			auto &node = statement.Cast<DeleteStatement>().node;
			if (!node->using_clauses.empty()) {
				throw InvalidInputException("git4data_apply does not take DELETE ... USING: the rows to remove have to "
				                            "be the branch's own, which a second table is not");
			}
			if (!node->returning_list.empty()) {
				throw InvalidInputException("git4data_apply does not take RETURNING: nothing is written where the rows "
				                            "would come from");
			}
			Git4DataCheckApplyTarget(context, *node->table, table);
			auto condition = node->condition ? node->condition->ToString() : string();
			string result =
			    StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM %s%s;\n", rows_name, key_list,
			                       at_clause, condition.empty() ? string() : " WHERE " + condition);
			result +=
			    StringUtil::Format("DELETE FROM %s %s WHERE EXISTS (SELECT 1 FROM %s %s WHERE %s);\n", quoted_added,
			                       alias_added, rows_name, alias_rows, match(alias_added, alias_rows));
			result += add_removals(rows_name, alias_rows);
			result += StringUtil::Format("DROP TABLE %s;", rows_name);
			return result;
		}
		case StatementType::UPDATE_STATEMENT: {
			auto &node = statement.Cast<UpdateStatement>().node;
			if (node->from_table) {
				throw InvalidInputException("git4data_apply does not take UPDATE ... FROM: the rows to change have to "
				                            "be the branch's own, which a joined table is not");
			}
			if (!node->returning_list.empty()) {
				throw InvalidInputException("git4data_apply does not take RETURNING: nothing is written where the rows "
				                            "would come from");
			}
			Git4DataCheckApplyTarget(context, *node->table, table);
			auto &set_info = *node->set_info;
			auto condition = set_info.condition ? set_info.condition->ToString() : string();
			// the new rows: the branch's own columns, with the assigned ones replaced by what they are
			// assigned. A row's other columns are the row's own, which is what a SET list means.
			vector<pair<string, string>> assignments;
			for (idx_t i = 0; i < set_info.columns.size(); i++) {
				assignments.emplace_back(set_info.columns[i].GetIdentifierName(), set_info.expressions[i]->ToString());
			}
			string new_columns;
			for (auto &column : table.GetColumns().Logical()) {
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
			    StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM %s%s;\n", keys_name, key_list,
			                       at_clause, condition.empty() ? string() : " WHERE " + condition);
			result += StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM %s%s;\n", rows_name,
			                             new_columns, at_clause, condition.empty() ? string() : " WHERE " + condition);
			result +=
			    StringUtil::Format("DELETE FROM %s %s WHERE EXISTS (SELECT 1 FROM %s %s WHERE %s);\n", quoted_added,
			                       alias_added, keys_name, alias_keys, match(alias_added, alias_keys));
			result += add_removals(keys_name, alias_keys);
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
			    quoted_added, rows_name, alias_rows, branch_image, rows_name, alias_rows,
			    match(alias_branch, alias_rows), keys_name, alias_keys, match(alias_keys, alias_branch));
			result += StringUtil::Format("DROP TABLE %s;\nDROP TABLE %s;", keys_name, rows_name);
			return result;
		}
		case StatementType::INSERT_STATEMENT: {
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
			Git4DataCheckApplyTargetTable(context, node->qualified_name, table);
			// the column list the insert names, or the table's own columns in order when it names none
			vector<string> insert_columns;
			for (auto &column : node->columns) {
				insert_columns.push_back(column.GetIdentifierName());
			}
			if (insert_columns.empty()) {
				for (auto &column : table.GetColumns().Logical()) {
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
				auto &column = table.GetColumns().GetColumn(Identifier(name));
				if (!declared.empty()) {
					declared += ", ";
				}
				declared += KeywordHelper::WriteOptionallyQuoted(name) + " " + column.Type().ToString();
			}
			string result = StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s (%s);\n", rows_name, declared);
			result += StringUtil::Format("INSERT INTO %s (%s) %s;\n", rows_name, insert_list,
			                             node->select_statement->ToString());
			result += StringUtil::Format(
			    "INSERT INTO %s (%s) SELECT * FROM %s WHERE (SELECT CASE WHEN EXISTS (SELECT 1 FROM %s JOIN %s %s ON "
			    "(%s)) THEN error('insert collides with a row the branch already shows: use an update, which a delta "
			    "can record, or delete the row first') ELSE 1 END) = 1;\n",
			    quoted_added, insert_list, rows_name, branch_image, rows_name, alias_rows,
			    match(alias_branch, alias_rows));
			result += StringUtil::Format("DROP TABLE %s;", rows_name);
			return result;
		}
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

//! The columns of one image compared against the other's, for telling a real merge conflict from two
//! branches that made the same change.
string BuildGit4DataRowComparison(TableCatalogEntry &table, const string &left_alias,
                                         const string &right_alias) {
	string result;
	for (auto &column : table.GetColumns().Logical()) {
		if (!result.empty()) {
			result += " AND ";
		}
		auto name = KeywordHelper::WriteOptionallyQuoted(column.Name().GetIdentifierName());
		result += StringUtil::Format("%s.%s IS NOT DISTINCT FROM %s.%s", left_alias, name, right_alias, name);
	}
	return result;
}

//! The snapshot a merge between these two lines should use as its base, when one of them was forked from a
//! *captured line*. Such a branch's parent is a snapshot rather than a branch, so the branch-ancestor walk
//! stops at it and a merge that has to find an ancestor between it and the line's branch refuses: measured,
//! `Branches "C" and "B" share no ancestor to merge against`. The state the two parted at is exactly what that
//! snapshot holds - it captured the line at the commit the fork started from - and a diff takes a snapshot as
//! readily as a branch, so its name is the base.
//!
//! The snapshot is not an *ancestor* of the line's branch: it was taken from it, and the branch has moved on
//! since. So the question is not what both sides descend from but what they parted at: the snapshot this side
//! descends from, whose captured line is on the other side's branch chain.
static string Git4DataSharedSnapshotAncestor(DuckTransactionManager &transaction_manager, const string &a,
                                             const string &b) {
	auto all = transaction_manager.branches.List();
	auto parent_of = [&](const string &name) {
		for (auto &branch : all) {
			if (branch.name == name) {
				return branch.parent;
			}
		}
		return string();
	};
	// the branches a line descends from, nearest first: the walk ends where the parent is not a branch
	auto branch_chain = [&](const string &name) {
		vector<string> chain;
		auto current = name;
		for (idx_t depth = 0; !current.empty() && depth <= all.size(); depth++) {
			auto parent = parent_of(current);
			if (parent.empty() && current != name) {
				break;
			}
			chain.push_back(current);
			Git4DataSnapshot parent_snapshot;
			if (!parent.empty() && transaction_manager.snapshots.Get(parent, parent_snapshot)) {
				break;
			}
			current = parent;
		}
		return chain;
	};
	// the nearest snapshot a line descends from, and the line it captured: a fork from a captured line is the
	// only way a snapshot gets into a parent chain
	auto captured = [&](const string &name, string &snapshot_name, string &line) {
		auto current = name;
		for (idx_t depth = 0; !current.empty() && depth <= all.size(); depth++) {
			Git4DataSnapshot snapshot;
			if (transaction_manager.snapshots.Get(current, snapshot)) {
				snapshot_name = current;
				line = snapshot.line;
				return;
			}
			current = parent_of(current);
		}
	};
	auto chain_a = branch_chain(a);
	auto chain_b = branch_chain(b);
	for (idx_t side = 0; side < 2; side++) {
		string snapshot_name;
		string line;
		captured(side == 0 ? a : b, snapshot_name, line);
		auto &other = side == 0 ? chain_b : chain_a;
		if (snapshot_name.empty() || line.empty()) {
			continue;
		}
		if (std::find(other.begin(), other.end(), line) != other.end()) {
			return snapshot_name;
		}
	}
	return string();
}

//! Merge one branch into another, three-way, with their least common ancestor as the base. The left
//! branch is where the result lands, so it has to have storage of its own to be written to.
//!
//! A key is in conflict when both sides changed it relative to the ancestor. Two sides that ended up
//! holding the same row have not conflicted, which is what the row comparison is for - without it,
//! both branches adding the same row would be reported as a conflict and fail a policy of fail.
//! The internal reference a three-way merge names its base with: the source as of `commit`, read as the line
//! `line`. It replaces any reference of the same name, because a merge drops its own when it is done and a stale
//! one would answer the next merge with the state of the one before. Both ways of naming a base go through here
//! and differ only in what they pass - which is the point: this was ten lines written twice inside the merge.
static string Git4DataCreateMergeBase(ClientContext &context, DuckTransactionManager &transaction_manager,
                                      transaction_t commit, const string &line) {
	transaction_manager.snapshots.Drop(DuckTransactionManager::MERGE_BASE_REFERENCE);
	if (!transaction_manager.snapshots.Create(DuckTransactionManager::MERGE_BASE_REFERENCE, commit,
	                                          Timestamp::GetCurrentTimestamp(), line)) {
		throw InternalException("Could not create the internal merge base reference");
	}
	context.client_data->git4data_added_records.push_back("snapshot\t" +
	                                                      string(DuckTransactionManager::MERGE_BASE_REFERENCE));
	return DuckTransactionManager::MERGE_BASE_REFERENCE;
}

//! The commit the two sides parted at: the fork pin of whichever of them is not the ancestor. When one of the two
//! *is* the ancestor - merging a child back into its parent - the ancestor's own pin is where the *ancestor*
//! started rather than where they parted, so the other side's pin is what names the state. The measured account
//! of what that rule fixes is at the call site.
static transaction_t Git4DataMergeForkCommit(DuckTransactionManager &transaction_manager, const string &left_name,
                                             const string &right_name, const string &ancestor) {
	transaction_t fork = 0;
	Git4DataBranch fork_branch;
	for (auto &side : {left_name, right_name}) {
		if (side == ancestor) {
			continue;
		}
		if (transaction_manager.branches.Get(side, fork_branch)) {
			fork = fork_branch.pin;
			break;
		}
	}
	return fork;
}

string PragmaGit4DataMerge(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto left_name = parameters.values[1].ToString();
	auto right_name = parameters.values[2].ToString();
	auto policy = StringUtil::Lower(parameters.values[3].ToString());
	if (policy != "fail" && policy != "skip" && policy != "accept") {
		throw InvalidInputException("Merge policy has to be fail, skip or accept, not \"%s\"", policy);
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	auto left = Git4DataRequiredBranch(context, transaction_manager, left_name);
	auto right = Git4DataRequiredBranch(context, transaction_manager, right_name);
	string merge_into;
	auto &merge_source = Git4DataResolveTable(context, table_name);
	auto merge_key = Git4DataSourceKey(merge_source);
	for (auto &entry : left.storage_tables) {
		if (Git4DataSourceKeyMatches(context, entry.source, merge_key) ||
		    (entry.source_oid != 0 && entry.source_oid == merge_source.oid)) {
			// the table the branch was given can have been renamed, and the merge writes into it
			merge_into = Git4DataResolveTableName(context, entry.storage, entry.oid);
		}
	}
	// A branch does not have to have a table of its own to be merged into: one that differs by a delta has
	// somewhere to record the change too - the same two tables git4data_apply records in - so the landing
	// statements below are written as delta rows instead of as changes to a table, and the "live" side of the
	// conflict rule is read as the branch's own view rather than as a table.
	optional_ptr<const Git4DataBranchDelta> merge_delta;
	if (merge_into.empty()) {
		merge_delta = Git4DataFindDeltaTable(context, left, merge_key, merge_source.oid);
		if (merge_delta) {
			// The merge lands in this mapping, so it has to be the one the target was given. This is load-bearing
			// and not covered by the read path: measured, with it removed the refusal comes from the merge's own
			// ancestor lookup instead ("Branches ... share no ancestor to merge against"), which is a different
			// sentence about a different problem - the read of the target never happens.
			Git4DataCheckSourceBoundTable(context, left, merge_delta->source, merge_delta->source_oid,
			                              merge_source.oid);
		}
		if (!merge_delta) {
			throw InvalidInputException("Branch \"%s\" has neither a table of its own nor a delta for table "
			                            "\"%s\", so a merge has nowhere to land - materialize it, or give it a "
			                            "delta with git4data_bind_branch_delta",
			                            left_name, table_name);
		}
	}
	string ancestor;
	if (!transaction_manager.branches.FindLCA(left_name, right_name, ancestor)) {
		// A child forked from a captured line has no *branch* ancestor, so what it parted from is the snapshot
		// it was forked from - and that snapshot is a reference the diffs below take, exactly as an ancestor's
		// name is. Naming it here is what makes such a line mergeable.
		ancestor = Git4DataSharedSnapshotAncestor(transaction_manager, left_name, right_name);
		if (ancestor.empty()) {
			throw InvalidInputException("Branches \"%s\" and \"%s\" share no ancestor to merge against", left_name,
			                            right_name);
		}
	}

	// The base a three-way merge needs is the state the two lines parted at. An ancestor without a table
	// of its own already reads as that state: its name resolves to its pinned image. An ancestor with one
	// would read as the table instead, which is the pinned image plus everything it has done since, and a
	// merge based on that applies the ancestor's own changes backwards - measured, silently. So a branch
	// with storage is asked the questions below, which end in naming its pin directly rather than its
	// name. Materializing is a single INSERT .. SELECT, which is what makes the first question answerable:
	// every row of such a table carries one insert id and none of them is deleted.
	Git4DataBranch ancestor_branch;
	// A diff reads the base as the branch name; only the case below has to name a commit instead
	string base_reference = ancestor;
	bool drop_merge_base = false;
	if (transaction_manager.branches.Get(ancestor, ancestor_branch)) {
		// Naming the ancestor's pin as the base, which is what the cases below end in. The line it inherited
		// goes with the pin: a branch forked from a captured line reads *that* line, so its pin alone names
		// the source table at that commit - the state it parted at is that line as it stood then, which is
		// what the reference below has to name. Measured: without the line, a merge whose ancestor had a copy
		// of its own read the source's values as the base and reported a conflict on a row only one side had
		// touched.
		auto name_pin_as_base = [&]() {
			base_reference = Git4DataCreateMergeBase(context, transaction_manager, ancestor_branch.pin,
			                                         ancestor_branch.base_line);
			drop_merge_base = true;
		};
		// The state an ancestor's copy was written from is its own line, read at the commit the two sides
		// forked at: that is what each of them saw, and it is not the source's pinned image, because the
		// ancestor's own changes were already part of its line when they forked. Measured: an ancestor whose
		// copy held 111 for a row, with two sides that both read 111 and one of them changing it to 222, was
		// merged against the source's 1 - so both sides looked changed and the merge was refused as a conflict
		// although only one had touched the row. The commit is the one the *merge target* parted at, which is
		// the fork's own pin: `fork_state_merge.test` pins that choice, and it is what makes the merge bring
		// over what the ancestor changed after the target forked (222 in that test) as the other side's change
		// rather than as something the target already had.
		// The commit the two sides parted at: the fork pin of whichever of them is not the ancestor. When one of
		// the two *is* the ancestor - merging a child back into its parent - the ancestor's own pin is where the
		// *ancestor* started, not where they parted, so the other side's pin is the one that names the state.
		auto fork_commit = [&]() {
			return Git4DataMergeForkCommit(transaction_manager, left_name, right_name, ancestor);
		};
		// Naming the base as the ancestor's own *line* at that commit, which is what carries the ancestor's own
		// change: a delta lives in two tables and a copy is a table of its own, and neither is pointed at by a
		// commit. Naming it as the line it *inherited* instead, against the ancestor's own pin, reads the raw
		// source - measured below, in both shapes.
		auto name_line_as_base = [&]() {
			auto fork = fork_commit();
			if (fork == 0) {
				// Both sides are the ancestor (a branch merged with itself): there is no fork to name, and the
				// ancestor's own pin is what is left.
				fork = ancestor_branch.pin;
			}
			base_reference = Git4DataCreateMergeBase(context, transaction_manager, fork, ancestor);
			drop_merge_base = true;
		};
		// Which of the two is the state the two sides parted at depends on when they forked, and the pin says
		// which: a fork from a parent that has a line of its own pins the commit it forked at, so a merge
		// target whose pin is newer than the ancestor's own parted *inside* that line. Both the ancestor's own
		// copy below and its own delta go through this, because both are its line: a delta is a change it made
		// to the source, and naming the pin instead reads the source without it. **Measured, and corrected in
		// increment 3bp (`fb4e40e8a5`)**: the fork commit is the one the side that is *not* the ancestor forked
		// at, so the question is whether that side's pin is newer than the ancestor's - asking it of the merge
		// target alone sent "merge a child into its parent" down the pin path, where a parent whose line is a
		// delta read as the raw source and one whose line is a copy that was written to read as the source's
		// values, losing the parent's own change from the base.
		auto name_the_parting_state = [&]() {
			if (fork_commit() > ancestor_branch.pin) {
				name_line_as_base();
			} else {
				name_pin_as_base();
			}
		};
		// A delta is the ancestor's own change to the source, so its name reads as its line *now*; whether its
		// two tables are empty yet is not asked, because naming the pin is right either way.
		for (auto &delta_entry : ancestor_branch.deltas) {
			if (Git4DataSourceKeyMatches(context, delta_entry.source, merge_key) ||
			    (delta_entry.source_oid != 0 && delta_entry.source_oid == merge_source.oid)) {
				// A delta is the ancestor's own change to the source and lives in two tables, which no pin
				// points at, so the base has to name the ancestor's *line* at the commit the sides parted at -
				// not the ancestor's pin against the line it inherited. Measured: with that pair the base was
				// the raw source at the ancestor's own start commit, so the ancestor's own additions and
				// removals came back as the other side's changes - a merge into it reported "removed 3" (a key
				// the ancestor itself had removed) and never saw the key the other side had really removed,
				// which then stayed in the target while the merge reported success.
				name_line_as_base();
			}
		}
		for (auto &storage_entry : ancestor_branch.storage_tables) {
			if (!Git4DataSourceKeyMatches(context, storage_entry.source, merge_key) &&
			    (storage_entry.source_oid == 0 || storage_entry.source_oid != merge_source.oid)) {
				continue;
			}
			auto &storage_table = Git4DataResolveTable(context, storage_entry.storage);
			auto &storage_groups = *storage_table.GetStorage().GetRowGroupCollection();
			transaction_t copy_commit = 0;
			bool unwritten = true;
			// The rows the caller has appended to this copy and not committed are in its local storage,
			// and a copy with any of them has been written to. Measured: an uncommitted insert into an
			// ancestor's copy left the walk below saying it was still the copy it was given, so the merge
			// took the ancestor's current line as its base and both sides looked changed.
			auto &ancestor_transaction = DuckTransaction::Get(context, storage_table.GetStorage().GetAttached());
			if (LocalStorage::Get(ancestor_transaction).Find(storage_table.GetStorage())) {
				unwritten = false;
			}
			for (idx_t i = 0; i < storage_groups.GetRowGroupCount(); i++) {
				auto row_group = storage_groups.GetRowGroup(NumericCast<int64_t>(i));
				if (!row_group) {
					continue;
				}
				transaction_t insertion = 0;
				if (!row_group->OneUnchangedInsertion(insertion)) {
					unwritten = false;
					break;
				}
				if (i == 0) {
					copy_commit = insertion;
				} else if (insertion != copy_commit) {
					// written in more than one statement, so it is not the copy it was given
					unwritten = false;
					break;
				}
			}
			if (unwritten) {
				// One unchanged insertion cannot tell an empty copy from one that has had rows inserted
				// since, so ask the source instead: a branch's table is made from the branch's pinned
				// image, so if the source table held no rows at that commit then the copy held none
				// either - and a table that holds rows now has been written to.
				idx_t rows_at_copy = 0;
				if (storage_groups.GetTotalRows() > 0 || storage_groups.GetRowGroupCount() > 0) {
					auto &source_table = Git4DataResolveTable(context, table_name);
					auto &source_groups = *source_table.GetStorage().GetRowGroupCollection();
					ScanOptions pinned(
					    TransactionData(MAX_TRANSACTION_ID, VisibilityBound::Through(ancestor_branch.pin)));
					pinned.insert_type = InsertedScanType::ALL_ROWS;
					pinned.delete_type = DeletedScanType::OMIT_COMMITTED_DELETES;
					for (idx_t i = 0; i < source_groups.GetRowGroupCount(); i++) {
						auto row_group = source_groups.GetRowGroup(NumericCast<int64_t>(i));
						if (row_group) {
							rows_at_copy += row_group->GetRowCountFor(pinned);
						}
					}
					if (rows_at_copy == 0) {
						unwritten = false;
					}
				}
			}
			if (!unwritten) {
				// Written to since: what the table holds now is the line this branch started from plus its own
				// changes, and reading *it* as the base would apply those changes backwards. Where the two
				// sides parted depends on when they forked: a side forked *after* the ancestor had a line of
				// its own parted at that line as it stood then, and a side forked before it parted at the
				// ancestor's pinned image, which is what naming the pin reads. `merge_lineage.test` and
				// `storage_writes.test` are the second case (their sides were forked before the ancestor was
				// given a copy) and `fork_state_merge.test` the first; a pin newer than the ancestor's own is
				// what tells them apart, because a fork from a parent with a line pins the commit it forked at.
				name_the_parting_state();
			}
		}
	}

	auto &table = Git4DataResolveTable(context, table_name);
	auto key_columns = Git4DataPrimaryKeyColumnNames(table);
	if (key_columns.empty()) {
		throw InvalidInputException("Table \"%s\" has no primary key, so a merge cannot tell which rows are "
		                            "the same row",
		                            table_name);
	}
	// The statement reads the source table, the table being merged into and the right side's image, so
	// no alias may be a column of any of them - a column named g4d_b made this ambiguous, measured.
	auto taken = Git4DataScopeColumnNames(context, transaction_manager, table_name, {right_name});
	Git4DataAddColumnNames(context, merge_into, taken);
	auto alias_left = Git4DataFreeName("g4d_l", taken);
	auto alias_right = Git4DataFreeName("g4d_r", taken);
	auto alias_both = Git4DataFreeName("g4d_both", taken);
	auto alias_both_rows = Git4DataFreeName("g4d_b", taken);
	auto alias_x = Git4DataFreeName("g4d_x", taken);
	auto alias_y = Git4DataFreeName("g4d_y", taken);
	auto conflict_name = Git4DataFreeHelperName(context, "g4d_merge_conflicts", taken);
	string key_list;
	string key_list_both;
	string key_match_xy;
	string key_match_by;
	string key_match_x_by;
	for (idx_t i = 0; i < key_columns.size(); i++) {
		auto quoted = KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
		if (i > 0) {
			key_list += ", ";
			key_list_both += ", ";
			key_match_xy += " AND ";
			key_match_by += " AND ";
			key_match_x_by += " AND ";
		}
		key_list += quoted;
		key_list_both += alias_both_rows + "." + quoted;
		key_match_xy += alias_x + "." + quoted + " = " + alias_y + "." + quoted;
		key_match_by += alias_y + "." + quoted + " = " + alias_both_rows + "." + quoted;
		key_match_x_by += alias_x + "." + quoted + " = " + alias_both_rows + "." + quoted;
	}
	auto quoted_table = Git4DataTableSQLName(table_name);
	auto quoted_into = Git4DataTableSQLName(merge_into);
	auto read_target = merge_into.empty() ? FormatGit4DataAtClause(table_name, left_name, "BRANCH") : quoted_into;
	auto diff_left = StringUtil::Format("git4data_diff('%s', '%s', '%s')", Git4DataSQLLiteral(table_name), Git4DataSQLLiteral(base_reference),
	                                    Git4DataSQLLiteral(left_name));
	auto diff_right = StringUtil::Format("git4data_diff('%s', '%s', '%s')", Git4DataSQLLiteral(table_name), Git4DataSQLLiteral(base_reference),
	                                     Git4DataSQLLiteral(right_name));
	auto right_image =
	    FormatGit4DataAtClause(table_name, right_name, ResolveGit4DataReference(transaction_manager, right_name));

	string result = StringUtil::Format(
	    "CREATE OR REPLACE TEMP TABLE %s AS WITH %s AS (SELECT %s FROM %s), "
	    "%s AS (SELECT %s FROM %s), %s AS (SELECT %s FROM %s INTERSECT SELECT %s FROM "
	    "%s) SELECT %s FROM %s %s WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM %s) "
	    "%s JOIN (SELECT * FROM %s) %s ON (%s) WHERE %s AND (%s)) "
	    // A key that both sides *removed* is agreement, not a conflict: the comparison above cannot say so,
	    // because there is no row on either side to compare, and its NOT EXISTS is then true - measured, two
	    // sides that had deleted the same row were refused as a conflict although nothing was left to
	    // disagree about. A key both sides changed is only a conflict if a row survives on one of them.
	    "AND (EXISTS (SELECT 1 FROM (SELECT * FROM %s) %s WHERE %s) OR EXISTS (SELECT 1 FROM (SELECT * FROM %s) "
	    "%s WHERE %s));\n",
	    conflict_name, alias_left, key_list, diff_left, alias_right, key_list, diff_right, alias_both, key_list,
	    alias_left, key_list, alias_right, key_list_both, alias_both, alias_both_rows, read_target, alias_x,
	    right_image, alias_y, key_match_xy, key_match_by, BuildGit4DataRowComparison(table, alias_x, alias_y),
	    right_image, alias_x, key_match_x_by, read_target, alias_y, key_match_by);
	if (policy == "fail") {
		result += StringUtil::Format("SELECT CASE WHEN EXISTS (SELECT 1 FROM %s) THEN error('merge conflict: use "
		                             "policy skip or accept, or resolve it first') ELSE 0 END;\n",
		                             conflict_name);
	}
	// with accept the right side wins outright; otherwise a conflicting key is left as the left has it
	auto conflict_filter = policy == "accept" ? string()
	                                          : StringUtil::Format(" AND (%s) NOT IN (SELECT %s FROM %s)", key_list,
	                                                               key_list, conflict_name);
	// Accepting means the right side's row replaces whatever the left has for that key, including when
	// the right side added it and the left also has one - so every key the right touched is cleared
	// first, not only the ones it removed or changed.
	auto changed_filter = policy == "accept" ? "'added', 'removed', 'changed'" : "'removed', 'changed'";
	auto dropped_keys = StringUtil::Format("SELECT %s FROM %s WHERE g4d_change IN (%s)%s", key_list, diff_right,
	                                       changed_filter, conflict_filter);
	auto added_keys = StringUtil::Format(
	    "SELECT %s FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change IN ('added', 'changed')%s) AND (%s) "
	    "NOT IN (SELECT %s FROM %s)",
	    key_list, right_image, key_list, key_list, diff_right, conflict_filter, key_list, key_list, read_target);
	auto added_rows = StringUtil::Format("SELECT * FROM %s WHERE (%s) IN (%s)", right_image, key_list, added_keys);
	if (merge_delta) {
		// the delta is where the change is recorded: a key the merge removes loses the branch's own override
		// and is recorded removed, and a row it adds is recorded added and loses any removal - the two
		// statements git4data_apply renders for a delete and an insert, against the same two tables
		auto delta_added = Git4DataResolveTableName(context, merge_delta->added, merge_delta->added_oid);
		auto delta_removed = Git4DataResolveTableName(context, merge_delta->removed, merge_delta->removed_oid);
		if (delta_added.empty() || delta_removed.empty()) {
			throw InvalidInputException("The delta branch \"%s\" differs by is gone: \"%s\" or \"%s\" is not "
			                            "there any more",
			                            left_name, merge_delta->added, merge_delta->removed);
		}
		auto quoted_added = Git4DataTableSQLName(delta_added);
		auto quoted_removed = Git4DataTableSQLName(delta_removed);
		result += StringUtil::Format("DELETE FROM %s WHERE (%s) IN (%s);\n", quoted_added, key_list, dropped_keys);
		result += StringUtil::Format("INSERT INTO %s (%s) SELECT %s FROM (%s) g4d_dk WHERE (%s) NOT IN (SELECT %s FROM "
		                             "%s) AND (%s) IN (SELECT %s FROM %s);\n",
		                             quoted_removed, key_list, key_list, dropped_keys, key_list, key_list,
		                             quoted_removed, key_list, key_list, read_target);
		result += StringUtil::Format("INSERT INTO %s %s;\n", quoted_added, added_rows);
		result += StringUtil::Format("DELETE FROM %s WHERE (%s) IN (%s);\n", quoted_removed, key_list, added_keys);
	} else {
		result += StringUtil::Format("DELETE FROM %s WHERE (%s) IN (%s);\n", quoted_into, key_list, dropped_keys);
		result += StringUtil::Format(
		    "INSERT INTO %s SELECT * FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change IN ('added', "
		    "'changed')%s) AND (%s) NOT IN (SELECT %s FROM %s);\n",
		    quoted_into, right_image, key_list, key_list, diff_right, conflict_filter, key_list, key_list, quoted_into);
	}
	result += StringUtil::Format("DROP TABLE %s;", conflict_name);

	if (drop_merge_base) {
		// The reference was only there for the two diffs above, which have run by now, so it goes. Its row
		// goes with plain DML and the in-memory entry with the pragma below rather than through
		// git4data_drop_snapshot: that one *returns* the row's DELETE now, and SQL a pragma returns is
		// executed as statements, where a statement-returning pragma cannot be called - measured, and the
		// merge refused with "PRAGMA function does not have a function specified" until this was written
		// this way.
		if (Git4DataRegistryTablesThere(context)) {
			result += StringUtil::Format("\nDELETE FROM %s.%s WHERE name = '%s';", GIT4DATA_REGISTRY_SCHEMA,
			                             GIT4DATA_SNAPSHOTS_TABLE, DuckTransactionManager::MERGE_BASE_REFERENCE);
		}
		result += StringUtil::Format("\nPRAGMA git4data_forget_internal('%s');",
		                             DuckTransactionManager::MERGE_BASE_REFERENCE);
	}
	return result;
}

//! Refuse one more reference when the database is at its limit. An exceeded limit stops new
//! references without dropping any: what exists was paid for already.

} // namespace duckdb
