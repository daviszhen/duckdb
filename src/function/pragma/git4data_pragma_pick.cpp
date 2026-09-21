//===----------------------------------------------------------------------===//
// Git4Data -- the pragma that takes one side's change onto a line
//===----------------------------------------------------------------------===//
// `git4data_pick` reads the difference between two references and lands the rows one side has in
// the other's line, which is why it carries the helpers that point a statement at a scope and keep
// generated names free. Increment 3fo moved them out of git4data_pragma_lines.cpp, which keeps the
// merge; the declarations the registration and the merge need are in git4data_pragmas.hpp.
//===----------------------------------------------------------------------===//

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

case_insensitive_set_t Git4DataScopeColumnNames(ClientContext &context,
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
string Git4DataFreeName(const string &base, const case_insensitive_set_t &taken) {
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
string Git4DataFreeHelperName(ClientContext &context, const string &base, const case_insensitive_set_t &taken) {
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

} // namespace duckdb
