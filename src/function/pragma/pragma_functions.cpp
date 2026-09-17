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
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
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

namespace duckdb {

static void PragmaEnableProfilingStatement(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_profiler = true;
}

void RegisterEnableProfiling(BuiltinFunctions &set) {
	PragmaFunctionSet functions("");
	functions.AddFunction(PragmaFunction::PragmaStatement(Identifier(), PragmaEnableProfilingStatement));

	set.AddFunction("enable_profile", functions);
	set.AddFunction("enable_profiling", functions);
}

static void PragmaDisableProfiling(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_profiler = false;
}

static void PragmaEnableProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_progress_bar = true;
}

static void PragmaDisableProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_progress_bar = false;
}

static void PragmaEnablePrintProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).print_progress_bar = true;
}

static void PragmaDisablePrintProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).print_progress_bar = false;
}

//! Warn that a pragma that no longer does anything is on its way out
static void WarnDeprecatedPragma(ClientContext &context, const char *name) {
	DUCKDB_LOG_WARNING(
	    context,
	    StringUtil::Format("The '%s' pragma no longer has any effect; it is deprecated and will be removed in a "
	                       "future release.",
	                       name));
}

static void PragmaEnableVerification(ClientContext &context, const FunctionParameters &parameters) {
	WarnDeprecatedPragma(context, "enable_verification");
}

static void PragmaDisableVerification(ClientContext &context, const FunctionParameters &parameters) {
	WarnDeprecatedPragma(context, "disable_verification");
}

static void PragmaEnableForceParallelism(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_parallelism = true;
}

static void PragmaForceCheckpoint(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.force_checkpoint = true;
}

static void PragmaDisableForceParallelism(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_parallelism = false;
}

static void PragmaEnableObjectCache(ClientContext &context, const FunctionParameters &parameters) {
	WarnDeprecatedPragma(context, "enable_object_cache");
}

static void PragmaDisableObjectCache(ClientContext &context, const FunctionParameters &parameters) {
	WarnDeprecatedPragma(context, "disable_object_cache");
}

static void PragmaEnableCheckpointOnShutdown(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.checkpoint_on_shutdown = true;
}

static void PragmaDisableCheckpointOnShutdown(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.checkpoint_on_shutdown = false;
}

static void PragmaEnableOptimizer(ClientContext &context, const FunctionParameters &parameters) {
	Settings::Set<EnableOptimizerSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));
}

static void PragmaDisableOptimizer(ClientContext &context, const FunctionParameters &parameters) {
	Settings::Set<EnableOptimizerSetting>(context, SetScope::SESSION, Value::BOOLEAN(false));
}

//===----------------------------------------------------------------------===//
// Git4Data snapshots
//===----------------------------------------------------------------------===//

static DuckTransactionManager &GetGit4DataTransactionManager(ClientContext &context) {
	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	if (!attached) {
		throw InvalidInputException("Git4Data snapshots require an attached database");
	}
	return DuckTransactionManager::Get(*attached);
}

//! The AT unit a reference has to be read through, throwing when neither registry has the name
static const char *ResolveGit4DataReference(DuckTransactionManager &transaction_manager, const string &name) {
	if (transaction_manager.snapshots.GetPin(name).IsValid()) {
		return "SNAPSHOT";
	}
	Git4DataBranch branch;
	if (transaction_manager.branches.Get(name, branch)) {
		return "BRANCH";
	}
	throw InvalidInputException("Reference \"%s\" is neither a snapshot nor a branch", name);
}

//! A table read as of a reference, spelled the way the binder accepts it. The unit has to match
//! the registry the name lives in, or the read is looked up in the wrong one.
static string FormatGit4DataAtClause(const string &table_name, const string &reference, const char *unit) {
	return StringUtil::Format("%s AT (%s => '%s')", KeywordHelper::WriteOptionallyQuoted(table_name), unit,
	                          StringUtil::Replace(reference, "'", "''"));
}

//! The primary key's column names, empty when the table has none
static vector<string> Git4DataPrimaryKeyColumnNames(TableCatalogEntry &table) {
	vector<string> result;
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		if (!unique.IsPrimaryKey()) {
			continue;
		}
		for (auto &name : unique.GetColumnNames()) {
			result.push_back(name.GetIdentifierName());
		}
		break;
	}
	return result;
}

//! Restore a table's contents to what a reference saw. Delete every row the reference knows and put
//! the reference's rows back, rather than comparing them column by column: the table is being rewound,
//! so rewriting the rows it keeps is the honest reading of that, and it needs no comparison. The
//! primary key decides which rows those are, as it does for a pick.
//! The statements that rewind a table to a reference, shared by the by-name and by-time forms
static string BuildGit4DataRestore(ClientContext &context, const string &table_name, const string &reference,
                                   const char *unit) {
	// A restore does not need a key: every row goes and the reference's own image comes back, so there is
	// nothing to match row by row. It used to insist on one, which refused tables it could have served.
	Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
	auto quoted_table = KeywordHelper::WriteOptionallyQuoted(table_name);
	auto at_clause = FormatGit4DataAtClause(table_name, reference, unit);

	// Every row goes, including the ones the reference never had: a restore rewinds the table, so a row
	// added since the reference is as wrong as a row it changed. The delete runs first, and the reference
	// scan still reads its own image afterwards because the delete is newer than the reference's bound.
	string result;
	result += StringUtil::Format("DELETE FROM %s;\n", quoted_table);
	result += StringUtil::Format("INSERT INTO %s SELECT * FROM %s;", quoted_table, at_clause);
	return result;
}

//! Keep the last N days of history reachable without anyone taking a reference by hand: one
//! reference is moved forward as it falls out of the window, at each checkpoint. A window of zero
//! keeps nothing, which is what a test can rely on.
static void PragmaGit4DataPitr(ClientContext &context, const FunctionParameters &parameters) {
	auto days = parameters.values[0].GetValue<int64_t>();
	if (days < 0) {
		throw InvalidInputException("A point-in-time recovery window cannot be negative");
	}
	constexpr int64_t MICROS_PER_DAY = 24LL * 60 * 60 * 1000000;
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	transaction_manager.SetPitr(timestamp_t(days * MICROS_PER_DAY), Timestamp::GetCurrentTimestamp());
}

static void PragmaGit4DataPitrOff(ClientContext &context, const FunctionParameters &parameters) {
	GetGit4DataTransactionManager(context).ClearPitr();
}

static string PragmaGit4DataPitrState(ClientContext &context, const FunctionParameters &parameters) {
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	constexpr int64_t MICROS_PER_DAY = 24LL * 60 * 60 * 1000000;
	auto days = transaction_manager.PitrWindow().value / MICROS_PER_DAY;
	// when the reference was taken is deliberately not reported here: it is the wall clock, so a
	// test cannot assert it. It shows in git4data_snapshots, where it belongs.
	Git4DataSnapshot reference;
	auto held = transaction_manager.snapshots.Get(DuckTransactionManager::PITR_REFERENCE, reference);
	return StringUtil::Format("SELECT %s::BOOLEAN AS pitr_enabled, %lld::BIGINT AS window_days, "
	                          "%s::BOOLEAN AS reference_held",
	                          transaction_manager.PitrEnabled() ? "true" : "false", (long long)days,
	                          held ? "true" : "false");
}

static string PragmaGit4DataRestore(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto reference = parameters.values[1].ToString();
	auto unit = ResolveGit4DataReference(GetGit4DataTransactionManager(context), reference);
	return BuildGit4DataRestore(context, table_name, reference, unit);
}

//! Rewind a table to the newest reference taken at or before a wall-clock time. Commit ids are a
//! counter, so "as of 14:30" can only be answered from the timestamp each reference records; the
//! reach is therefore as far back as references are still held.
static string PragmaGit4DataRestoreAt(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto target = parameters.values[1].DefaultCastAs(LogicalType::TIMESTAMP);
	auto target_ts = TimestampValue::Get(target);
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	string best;
	timestamp_t best_ts;
	bool found = false;
	auto consider = [&](const string &name, timestamp_t created) {
		if (created > target_ts || (found && created <= best_ts)) {
			return;
		}
		best = name;
		best_ts = created;
		found = true;
	};
	for (auto &snapshot : transaction_manager.snapshots.List()) {
		consider(snapshot.name, snapshot.created);
	}
	for (auto &branch : transaction_manager.branches.List()) {
		consider(branch.name, branch.created);
	}
	if (!found) {
		throw InvalidInputException("No snapshot or branch was taken at or before %s, so there is nothing to "
		                            "restore \"%s\" to",
		                            target.ToString(), table_name);
	}
	// the name was chosen from the registries, so it resolves
	auto unit = ResolveGit4DataReference(transaction_manager, best);
	return BuildGit4DataRestore(context, table_name, best, unit);
}

//! Apply the changes one reference has relative to another onto the live table. Expressed as SQL
//! rather than as a new physical operator: the read side already exists (the reference scan), the
//! write side is ordinary DML, and the difference between the two references is what git4data_diff
//! computes. What this adds is the composition, and the decision that a pick needs a primary key -
//! without one there is nothing to match a current row against, and applying a change to the wrong
//! row would be worse than refusing to guess.
//! The predicate that says two aliased rows hold the same values, NULL-safe column by column
static string BuildGit4DataRowComparison(TableCatalogEntry &table, const string &left_alias, const string &right_alias);

//! The column names of a table, if it is there. Used to keep generated names clear of them.
static void Git4DataAddColumnNames(ClientContext &context, const string &table_name, case_insensitive_set_t &names) {
	auto entry = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)),
	                                                  OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		return;
	}
	for (auto &column : entry->GetColumns().Logical()) {
		names.insert(column.Name().GetIdentifierName());
	}
}

//! The columns every table a generated statement reads brings into scope: the source table, and for
//! each named reference the table that reference reads its rows from - a branch with a line of its own
//! reads that table rather than the source's columns.
static case_insensitive_set_t Git4DataScopeColumnNames(ClientContext &context,
                                                       DuckTransactionManager &transaction_manager,
                                                       const string &table_name, const vector<string> &references) {
	case_insensitive_set_t names;
	Git4DataAddColumnNames(context, table_name, names);
	for (auto &reference : references) {
		Git4DataStorageTable storage;
		if (!transaction_manager.branches.GetStorageTable(reference, table_name, storage)) {
			continue;
		}
		auto resolved = Git4DataResolveTableName(context, storage.storage, storage.oid);
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

static string PragmaGit4DataPick(ClientContext &context, const FunctionParameters &parameters) {
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
	auto quoted_table = KeywordHelper::WriteOptionallyQuoted(table_name);
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	auto diff = StringUtil::Format("git4data_diff('%s', '%s', '%s')", escape(table_name), escape(base), escape(source));

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
static string PragmaGit4DataMaterialize(ClientContext &context, const FunctionParameters &parameters) {
	auto source_table = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto storage_table = parameters.values[2].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	Git4DataBranch branch;
	if (!transaction_manager.branches.Get(branch_name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
	// the source has to be there, and the target must not be
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(source_table)));
	auto key_columns = Git4DataPrimaryKeyColumnNames(table);
	if (Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(storage_table)),
	                                         OnEntryNotFound::RETURN_NULL)) {
		throw InvalidInputException("Table \"%s\" already exists", storage_table);
	}
	// The mapping is registered by the last statement rather than here: the copy below reads the
	// branch, and a branch with a mapping already reads its own table - which at that moment is the
	// empty one just created, so the copy would bring nothing over.
	//
	// The columns are spelled out rather than taken with CREATE TABLE AS, because that does not carry
	// the primary key over - and the key is what tells two rows apart for a diff, a pick or a merge
	// afterwards. Nothing else of the source's definition is copied.
	string columns;
	for (auto &column : table.GetColumns().Logical()) {
		if (!columns.empty()) {
			columns += ", ";
		}
		columns +=
		    KeywordHelper::WriteOptionallyQuoted(column.Name().GetIdentifierName()) + " " + column.Type().ToString();
	}
	for (idx_t i = 0; i < key_columns.size(); i++) {
		columns += i == 0 ? ", PRIMARY KEY (" : ", ";
		columns += KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
		if (i + 1 == key_columns.size()) {
			columns += ")";
		}
	}
	auto quoted_storage = KeywordHelper::WriteOptionallyQuoted(storage_table);
	auto escaped_source = StringUtil::Replace(source_table, "'", "''");
	auto escaped_branch = StringUtil::Replace(branch_name, "'", "''");
	auto escaped_storage = StringUtil::Replace(storage_table, "'", "''");
	// A second materialize of one source replaces the mapping, and the copy the first one made is then a
	// table of the identical line with nothing pointing at it - measured: the branch read the new table
	// while the first was still there, and dropping the branch took only the second, so the first
	// survived that too. Materializing costs a table, so the one this feature created is dropped here.
	// The caller's own table is left alone, and the drop comes last so the copy above still reads the
	// branch as it was - the branch reads the table of its own that is about to go.
	string replaced;
	Git4DataStorageTable existing;
	if (transaction_manager.branches.GetStorageTable(branch_name, source_table, existing) && existing.owned &&
	    existing.storage != storage_table) {
		auto resolved = Git4DataResolveTableName(context, existing.storage, existing.oid);
		if (!resolved.empty()) {
			replaced = StringUtil::Format("\nDROP TABLE IF EXISTS %s;", KeywordHelper::WriteOptionallyQuoted(resolved));
		}
	}
	return StringUtil::Format("CREATE TABLE %s (%s);\nINSERT INTO %s SELECT * FROM %s AT (BRANCH => '%s');\n"
	                          "PRAGMA git4data_bind_branch_table('%s', '%s', '%s', 'true');%s",
	                          quoted_storage, columns, quoted_storage,
	                          KeywordHelper::WriteOptionallyQuoted(source_table), escaped_branch, escaped_source,
	                          escaped_branch, escaped_storage, replaced);
}

//! Take back what a branch was given for a source table: its table of its own, or the delta it differs
//! by. A mapping whose table is dropped falls back to the pinned image rather than failing - that rule is
//! deliberate, because the branch is still a valid reference - but it is silent, and until now there was
//! no way to say "stop reading that table, the branch reads its pin now". This is that way, and it is
//! also what turns the fallback from a state nobody can see into one a caller can see and clear.
static string PragmaGit4DataUnbind(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	Git4DataBranch branch;
	if (!transaction_manager.branches.Get(branch_name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	Git4DataStorageTable storage;
	if (transaction_manager.branches.GetStorageTable(branch_name, table_name, storage)) {
		// the mapping is found and cleared by the same comparison, so a name that differs only in case
		// cannot be reported as unbound while the mapping stays - measured, that is exactly what happened
		// before the clearing comparison was made case-insensitive with the finding one
		if (!transaction_manager.branches.ClearStorageTable(branch_name, table_name)) {
			throw InternalException("Could not unbind branch \"%s\" from \"%s\"", branch_name, table_name);
		}
		// the table itself is the caller's: it was created for the branch by materialize, and a caller
		// who gave one by hand would not expect this call to drop it either
		return StringUtil::Format("SELECT '%s'::VARCHAR AS branch, '%s'::VARCHAR AS table_name, 'storage' AS "
		                          "unbound, '%s'::VARCHAR AS was",
		                          escape(branch_name), escape(table_name), escape(storage.storage));
	}
	Git4DataBranchDelta delta;
	if (transaction_manager.branches.GetDeltaTable(branch_name, table_name, delta)) {
		if (!transaction_manager.branches.ClearDeltaTable(branch_name, table_name)) {
			throw InternalException("Could not unbind branch \"%s\" from \"%s\"", branch_name, table_name);
		}
		return StringUtil::Format("SELECT '%s'::VARCHAR AS branch, '%s'::VARCHAR AS table_name, 'delta' AS "
		                          "unbound, '%s'::VARCHAR AS was",
		                          escape(branch_name), escape(table_name), escape(delta.added));
	}
	throw InvalidInputException("Branch \"%s\" has neither a table of its own nor a delta for \"%s\", so "
	                            "there is nothing to unbind",
	                            branch_name, table_name);
}

//! Record that a branch reads a table of its own. Only meaningful as the last statement of a
//! materialize, once the copy has been made. The optional fourth argument says the feature created the
//! table rather than the caller handing one over, which decides whether a later materialize may drop it.
static void PragmaGit4DataBindBranchTable(ClientContext &context, const FunctionParameters &parameters) {
	auto source_table = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto storage_table = parameters.values[2].ToString();
	auto owned = false;
	if (parameters.values.size() > 3) {
		auto flag = StringUtil::Lower(parameters.values[3].ToString());
		if (flag != "true" && flag != "false") {
			throw InvalidInputException(
			    "The fourth argument of git4data_bind_branch_table says whether the table was created for the branch, "
			    "so it is true or false, not \"%s\"",
			    parameters.values[3].ToString());
		}
		owned = flag == "true";
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	Git4DataBranch branch;
	if (!transaction_manager.branches.Get(branch_name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
	// both tables have to be there, or the mapping would be a promise the reads then have to break
	auto &source_entry = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(source_table)));
	auto &storage_entry = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(storage_table)));
	// The read maps a branch's own table onto the source's columns *by position*, so the table has to have
	// the source's columns in the source's order. Measured: a table holding (v, k) where the source holds
	// (k, v) read back as k = 777 and v = 1 from a row that held k = 1 and v = 777. The delta path has
	// always checked this for its two tables; the storage path did not.
	if (storage_entry.GetColumns().GetColumnNames() != source_entry.GetColumns().GetColumnNames() ||
	    storage_entry.GetColumns().GetColumnTypes() != source_entry.GetColumns().GetColumnTypes()) {
		throw InvalidInputException("Table \"%s\" is read as the rows of \"%s\" for this branch, and a read maps "
		                            "its columns by position - so it needs that table's columns, in that order.",
		                            storage_table, source_table);
	}
	// A branch's own line has to be a table of its own. Pointed at the source itself the branch stops
	// being a branch: measured, it then read the *live* table - 11 rows after a row was inserted and 6
	// after five were deleted, where its pin held 10 either way.
	if (source_entry.oid == storage_entry.oid) {
		throw InvalidInputException("Table \"%s\" is \"%s\" itself, and a branch's own line has to be a table "
		                            "of its own - given the source, the branch reads the live table instead of the "
		                            "state it pins. Materialize the branch to get a copy.",
		                            storage_table, source_table);
	}
	// both oids go with the mapping: the storage table's so a read can tell it from another table that
	// takes its name after it is dropped, and the source's so the mapping follows the source if it is
	// renamed
	transaction_manager.branches.SetStorageTable(branch_name, source_table, storage_table, storage_entry.oid,
	                                             source_entry.oid, owned);
	// the copy is the branch's line from here on, so a delta beside it would be a second one that nothing
	// reads - the branch reads its own table the moment it has one
	transaction_manager.branches.ClearDeltaTable(branch_name, source_table);
}

//! Record that a branch differs from its base by a delta rather than by a copy of it. The two tables
//! are the caller's and are written with ordinary SQL: rows to add with the source table's columns,
//! keys to remove with the source table's key columns. Reads of the branch then serve the base minus
//! those keys plus those rows, which is what makes writing to a branch cost the change rather than the
//! table.
static void PragmaGit4DataBindBranchDelta(ClientContext &context, const FunctionParameters &parameters) {
	auto source_table = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto added_table = parameters.values[2].ToString();
	auto removed_table = parameters.values[3].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	Git4DataBranch branch;
	if (!transaction_manager.branches.Get(branch_name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
	auto &source = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(source_table)));
	auto &added = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(added_table)));
	auto &removed = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(removed_table)));
	// the added rows are read as rows of the source table, so the two have to be interchangeable
	auto &source_columns = source.GetColumns();
	// the added rows are read as rows of the source table, so the two have to be interchangeable
	if (added.GetColumns().GetColumnNames() != source_columns.GetColumnNames() ||
	    added.GetColumns().GetColumnTypes() != source_columns.GetColumnTypes()) {
		throw InvalidInputException("Table \"%s\" holds the rows added to \"%s\", so it needs that table's columns "
		                            "in that order",
		                            added_table, source_table);
	}
	// the removed table names the rows to hide from the base, so it carries exactly the key columns, in
	// the order the source table declares them
	auto key_names = Git4DataPrimaryKeyColumnNames(source);
	if (key_names.empty()) {
		throw InvalidInputException("Table \"%s\" has no primary key, so a delta cannot say which rows it removes",
		                            source_table);
	}
	vector<string> expected_key_names;
	vector<LogicalType> expected_key_types;
	for (idx_t i = 0; i < source_columns.LogicalColumnCount(); i++) {
		auto &column = source_columns.GetColumn(LogicalIndex(i));
		auto name = column.Name().GetIdentifierName();
		if (std::find(key_names.begin(), key_names.end(), name) == key_names.end()) {
			continue;
		}
		expected_key_names.push_back(name);
		expected_key_types.push_back(column.Type());
	}
	if (removed.GetColumns().GetColumnNames() != expected_key_names ||
	    removed.GetColumns().GetColumnTypes() != expected_key_types) {
		throw InvalidInputException("Table \"%s\" holds the keys removed from \"%s\", so it needs exactly those "
		                            "%llu key column(s), in the order the source declares them",
		                            removed_table, source_table, expected_key_names.size());
	}
	// Neither side of a delta can be the source itself: the added rows are read *as* rows of it, so the
	// same table there means every row is read twice and the branch follows the live table - measured, a
	// ten-row source with itself as the added table read twenty rows and then twenty-one; and the keys
	// removed from it are keys of the live table, which the branch would hide by.
	if (added.oid == source.oid) {
		throw InvalidInputException("Table \"%s\" cannot hold the rows added to it: a branch's delta adds rows "
		                            "to what it inherits, so the added table has to be one of its own. Every row "
		                            "would be read twice, and the branch would follow the live table.",
		                            added_table);
	}
	if (removed.oid == source.oid) {
		throw InvalidInputException("Table \"%s\" cannot hold the keys removed from it: those keys are the live "
		                            "table's own rows, which the branch would then hide. The removed table has to "
		                            "be one of the branch's.",
		                            removed_table);
	}
	// One table cannot be both sides of a delta: the added rows are rows the branch *has* and the removed
	// keys are keys it does *not*, so a key in the same table for both means a row that is hidden and
	// shown at once. A source table whose only column is its key satisfies both column checks, which is
	// how this was reachable at all - measured: a delete written through such a delta left the row
	// visible while the table quietly gained its key.
	if (added.oid == removed.oid) {
		throw InvalidInputException("Table \"%s\" cannot hold both the rows added to \"%s\" and the keys removed "
		                            "from it: the two sides mean opposite things. Name two tables.",
		                            added_table, source_table);
	}
	// A branch that inherited a line which differs by a delta would have to apply two of them, and a read
	// cannot. Refusing here rather than at the first read is the same rule the rest of this file follows:
	// nothing is promised that the reads cannot deliver.
	if (!branch.base_line.empty()) {
		Git4DataBranch line_branch;
		if (transaction_manager.branches.Get(branch.base_line, line_branch)) {
			for (auto &line_delta : line_branch.deltas) {
				if (line_delta.source != source_table) {
					continue;
				}
				throw InvalidInputException(
				    "Branch \"%s\" inherits a line that differs by a delta, so a delta of its own "
				    "would be a second one over the same source and a read would have to apply both. "
				    "Materialize one of the two lines first.",
				    branch_name);
			}
		}
	}
	for (auto &storage_entry : branch.storage_tables) {
		if (storage_entry.source != source_table) {
			continue;
		}
		// one line, one source: a branch that already reads a table of its own reads that, and a delta
		// beside it would be a second answer to the same question
		throw InvalidInputException("Branch \"%s\" already has a table of its own for \"%s\", so it reads that "
		                            "rather than a delta. A delta is for a branch that has no copy.",
		                            branch_name, source_table);
	}
	transaction_manager.branches.SetDeltaTable(branch_name, source_table, added_table, removed_table, added.oid,
	                                           removed.oid, source.oid);
}

//! A copy-on-write clone of one file, which is what "zero-copy" means at this level: the copy costs
//! nothing at the moment it is made and shares its blocks with the original until either side writes.
static void Git4DataCloneFile(const string &source, const string &target) {
#ifdef __APPLE__
	if (clonefile(source.c_str(), target.c_str(), 0) != 0) {
		throw InvalidInputException("Could not clone \"%s\" to \"%s\": %s", source, target, strerror(errno));
	}
#elif defined(__linux__)
	auto source_fd = open(source.c_str(), O_RDONLY);
	if (source_fd < 0) {
		throw InvalidInputException("Could not open \"%s\": %s", source, strerror(errno));
	}
	auto target_fd = open(target.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
	if (target_fd < 0) {
		close(source_fd);
		throw InvalidInputException("Could not create \"%s\": %s", target, strerror(errno));
	}
	auto result = ioctl(target_fd, FICLONE, source_fd);
	auto saved_errno = errno;
	close(target_fd);
	close(source_fd);
	if (result != 0) {
		throw InvalidInputException("Could not clone \"%s\" to \"%s\": %s", source, target, strerror(saved_errno));
	}
#else
	throw NotImplementedException("A zero-copy clone needs a file system that can do one, and this platform has no "
	                              "clone call - the database can still be copied with the file system's own tools");
#endif
}

//! Clone the database into a file of its own. The clone is a copy of the *file*, so it holds everything
//! committed; the write-ahead log comes with it when there is one, or the clone would be missing what
//! the log holds. Writing to the clone afterwards leaves the original alone and the other way round,
//! which is the file system's copy-on-write and not something this feature has to maintain.
static string PragmaGit4DataCloneDatabase(ClientContext &context, const FunctionParameters &parameters) {
	auto target = parameters.values[0].ToString();
	if (target.empty()) {
		throw InvalidInputException("A clone needs a path to write to");
	}
	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	auto source = attached->StoredPath();
	if (source.empty()) {
		throw InvalidInputException("This database has no file of its own, so there is nothing to clone");
	}
	auto &fs = FileSystem::GetFileSystem(context);
	if (fs.FileExists(target)) {
		throw InvalidInputException("\"%s\" already exists - a clone does not overwrite", target);
	}
	Git4DataCloneFile(source, target);
	// the log is part of the database until the next checkpoint merges it, so the clone needs it too or
	// it would be missing whatever the log holds
	auto wal = source + ".wal";
	bool cloned_log = false;
	if (fs.FileExists(wal)) {
		Git4DataCloneFile(wal, target + ".wal");
		cloned_log = true;
	}
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	return StringUtil::Format("SELECT '%s'::VARCHAR AS source, '%s'::VARCHAR AS clone, %s::BOOLEAN AS with_log",
	                          escape(source), escape(target), cloned_log ? "true" : "false");
}

//! Point every reference to the source table inside a parsed statement at the branch the statement is
//! being applied to. The call writes *the branch*, so a bare `t` in the statement means the branch's
//! line - reading the live table instead would be a silent wrong answer, and it was one: measured, an
//! insert whose rows came from the table itself copied the live table's value (v = 1) where the branch
//! held 101. A reference that already names a reference of its own is left alone, because naming one is
//! a decision rather than an omission.
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

//! The statement a delta branch is written through has to name the table the delta is for, and name it
//! directly: a write through another name would be recorded against the wrong line, and a join has no
//! single row of that table to record.
static void Git4DataCheckApplyTarget(TableRef &ref, const string &table_name) {
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
static string PragmaGit4DataApply(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto dml = parameters.values[2].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	Git4DataBranch branch;
	if (!transaction_manager.branches.Get(branch_name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
	Git4DataBranchDelta delta;
	if (!transaction_manager.branches.GetDeltaTable(branch_name, table_name, delta)) {
		throw InvalidInputException(
		    "Branch \"%s\" does not differ from its base by a delta for \"%s\", so a write has nowhere to be "
		    "recorded. Give the branch a delta for that table first, or materialize the branch and write to the "
		    "table it then has of its own.",
		    branch_name, table_name);
	}
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
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

	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	auto quoted_source = KeywordHelper::WriteOptionallyQuoted(table_name);
	auto quoted_added = KeywordHelper::WriteOptionallyQuoted(added_table);
	auto quoted_removed = KeywordHelper::WriteOptionallyQuoted(removed_table);
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
			Git4DataCheckApplyTarget(*node->table, table_name);
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
			Git4DataCheckApplyTarget(*node->table, table_name);
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
static string BuildGit4DataRowComparison(TableCatalogEntry &table, const string &left_alias,
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

//! Merge one branch into another, three-way, with their least common ancestor as the base. The left
//! branch is where the result lands, so it has to have storage of its own to be written to.
//!
//! A key is in conflict when both sides changed it relative to the ancestor. Two sides that ended up
//! holding the same row have not conflicted, which is what the row comparison is for - without it,
//! both branches adding the same row would be reported as a conflict and fail a policy of fail.
static string PragmaGit4DataMerge(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto left_name = parameters.values[1].ToString();
	auto right_name = parameters.values[2].ToString();
	auto policy = StringUtil::Lower(parameters.values[3].ToString());
	if (policy != "fail" && policy != "skip" && policy != "accept") {
		throw InvalidInputException("Merge policy has to be fail, skip or accept, not \"%s\"", policy);
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	Git4DataBranch left;
	if (!transaction_manager.branches.Get(left_name, left)) {
		throw InvalidInputException("Branch \"%s\" does not exist", left_name);
	}
	Git4DataBranch right;
	if (!transaction_manager.branches.Get(right_name, right)) {
		throw InvalidInputException("Branch \"%s\" does not exist", right_name);
	}
	string merge_into;
	for (auto &entry : left.storage_tables) {
		if (entry.source == table_name) {
			// the table the branch was given can have been renamed, and the merge writes into it
			merge_into = Git4DataResolveTableName(context, entry.storage, entry.oid);
		}
	}
	if (merge_into.empty()) {
		throw InvalidInputException("Branch \"%s\" has no storage of its own for table \"%s\", so a merge "
		                            "has nowhere to land - materialize it first",
		                            left_name, table_name);
	}
	string ancestor;
	if (!transaction_manager.branches.FindLCA(left_name, right_name, ancestor)) {
		throw InvalidInputException("Branches \"%s\" and \"%s\" share no ancestor to merge against", left_name,
		                            right_name);
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
		for (auto &storage_entry : ancestor_branch.storage_tables) {
			if (!StringUtil::CIEquals(storage_entry.source, table_name)) {
				continue;
			}
			auto &storage_table =
			    Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(storage_entry.storage)));
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
					auto &source_table =
					    Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
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
				// Written to since. The state a three-way merge needs is the one the two lines parted
				// at, and for a branch that has a table of its own that state is still namable: a
				// branch with storage cannot be forked from at all, so both lines here must have been
				// forked before this table was given - which makes the state they parted at exactly
				// this branch's pinned image, a commit a reference can name. What the table holds now
				// is that image plus this branch's own changes since, and reading it as the base is
				// what would apply those changes backwards; naming the pin instead is the fix.
				transaction_manager.snapshots.Drop(DuckTransactionManager::MERGE_BASE_REFERENCE);
				if (!transaction_manager.snapshots.Create(DuckTransactionManager::MERGE_BASE_REFERENCE,
				                                          ancestor_branch.pin, Timestamp::GetCurrentTimestamp())) {
					throw InternalException("Could not create the internal merge base reference");
				}
				base_reference = DuckTransactionManager::MERGE_BASE_REFERENCE;
				drop_merge_base = true;
			}
		}
	}

	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
	auto key_columns = Git4DataPrimaryKeyColumnNames(table);
	if (key_columns.empty()) {
		throw InvalidInputException("Table \"%s\" has no primary key, so a merge cannot tell which rows are "
		                            "the same row",
		                            table_name);
	}
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
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
	for (idx_t i = 0; i < key_columns.size(); i++) {
		auto quoted = KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
		if (i > 0) {
			key_list += ", ";
			key_list_both += ", ";
			key_match_xy += " AND ";
			key_match_by += " AND ";
		}
		key_list += quoted;
		key_list_both += alias_both_rows + "." + quoted;
		key_match_xy += alias_x + "." + quoted + " = " + alias_y + "." + quoted;
		key_match_by += alias_y + "." + quoted + " = " + alias_both_rows + "." + quoted;
	}
	auto quoted_table = KeywordHelper::WriteOptionallyQuoted(table_name);
	auto quoted_into = KeywordHelper::WriteOptionallyQuoted(merge_into);
	auto diff_left = StringUtil::Format("git4data_diff('%s', '%s', '%s')", escape(table_name), escape(base_reference),
	                                    escape(left_name));
	auto diff_right = StringUtil::Format("git4data_diff('%s', '%s', '%s')", escape(table_name), escape(base_reference),
	                                     escape(right_name));
	auto right_image =
	    FormatGit4DataAtClause(table_name, right_name, ResolveGit4DataReference(transaction_manager, right_name));

	string result = StringUtil::Format(
	    "CREATE OR REPLACE TEMP TABLE %s AS WITH %s AS (SELECT %s FROM %s), "
	    "%s AS (SELECT %s FROM %s), %s AS (SELECT %s FROM %s INTERSECT SELECT %s FROM "
	    "%s) SELECT %s FROM %s %s WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM %s) "
	    "%s JOIN (SELECT * FROM %s) %s ON (%s) WHERE %s AND (%s));\n",
	    conflict_name, alias_left, key_list, diff_left, alias_right, key_list, diff_right, alias_both, key_list,
	    alias_left, key_list, alias_right, key_list_both, alias_both, alias_both_rows, quoted_into, alias_x,
	    right_image, alias_y, key_match_xy, key_match_by, BuildGit4DataRowComparison(table, alias_x, alias_y));
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
	result += StringUtil::Format("DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change IN (%s)%s);\n",
	                             quoted_into, key_list, key_list, diff_right,
	                             policy == "accept" ? "'added', 'removed', 'changed'" : "'removed', 'changed'",
	                             conflict_filter);
	result += StringUtil::Format(
	    "INSERT INTO %s SELECT * FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change IN (%s)%s) AND (%s) NOT IN "
	    "(SELECT %s FROM %s);\n",
	    quoted_into, right_image, key_list, key_list, diff_right, "'added', 'changed'", conflict_filter, key_list,
	    key_list, quoted_into);
	result += StringUtil::Format("DROP TABLE %s;", conflict_name);

	if (drop_merge_base) {
		// the reference was only there for the two diffs above, which have run by now
		result +=
		    StringUtil::Format("\nPRAGMA git4data_drop_snapshot('%s');", DuckTransactionManager::MERGE_BASE_REFERENCE);
	}
	return result;
}

//! Refuse one more reference when the database is at its limit. An exceeded limit stops new
//! references without dropping any: what exists was paid for already.
static void CheckGit4DataReferenceLimit(const string &plural, idx_t count, int64_t limit) {
	if (limit < 0) {
		return;
	}
	if (limit == 0) {
		throw InvalidInputException("%s are disabled for this database", plural);
	}
	if (NumericCast<int64_t>(count) >= limit) {
		throw InvalidInputException("At most %lld %s are allowed for this database, and %llu already exist",
		                            (long long)limit, plural, (unsigned long long)count);
	}
}

//! Set how many snapshots or branches this database may hold: -1 unlimited, 0 disabled, a positive
//! number is the most. The recovery window's own reference is not counted.
static void PragmaGit4DataLimit(ClientContext &context, const FunctionParameters &parameters) {
	auto kind = StringUtil::Lower(parameters.values[0].ToString());
	auto limit = parameters.values[1].GetValue<int64_t>();
	if (limit < -1) {
		throw InvalidInputException("A limit is -1 for unlimited, 0 to disable, or a positive number");
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	if (kind == "snapshot") {
		transaction_manager.snapshot_limit = limit;
	} else if (kind == "branch") {
		transaction_manager.branch_limit = limit;
	} else {
		throw InvalidInputException("Limits are set for \"snapshot\" or \"branch\", not \"%s\"", kind);
	}
}

static string PragmaGit4DataLimits(ClientContext &context, const FunctionParameters &parameters) {
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	return StringUtil::Format(
	    "SELECT %lld::BIGINT AS snapshot_limit, %llu::UBIGINT AS snapshots, "
	    "%lld::BIGINT AS branch_limit, %llu::UBIGINT AS branches",
	    (long long)transaction_manager.snapshot_limit, (unsigned long long)transaction_manager.UserSnapshotCount(),
	    (long long)transaction_manager.branch_limit, (unsigned long long)transaction_manager.branches.Count());
}

//! A name beginning with two underscores is one of the feature's own internal references - the
//! recovery window, a merge's base. Those are looked up by exact name, so a user reference sharing one
//! would be dropped by the machinery that manages them.
static void CheckGit4DataReferenceName(const string &name) {
	if (StringUtil::StartsWith(name, "__")) {
		throw InvalidInputException(
		    "\"%s\" begins with two underscores, which this feature reserves for references it manages "
		    "itself: the point-in-time recovery window and the base a merge reads against. Choose another "
		    "name.",
		    name);
	}
}

static void PragmaGit4DataCreateBranch(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	auto parent = parameters.values[1].ToString();
	CheckGit4DataReferenceName(name);
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// the other half of the rule the snapshot side states: the two kinds of reference share a name
	// namespace wherever one is named without its kind, so they cannot share a name
	Git4DataSnapshot same_named_snapshot;
	if (transaction_manager.snapshots.Get(name, same_named_snapshot)) {
		throw InvalidInputException("A snapshot named \"%s\" exists, and a branch of that name would be "
		                            "indistinguishable from it wherever a reference is named without its kind",
		                            name);
	}
	// A branch descends from another branch when one is named; otherwise it starts at the newest
	// commit. Either way the branch holds its base down exactly as a pin does.
	//
	// The ancestor may also be a snapshot, which is how a line is started from a state that is no
	// longer the newest one - a snapshot is a commit, and a commit is what a branch is forked from.
	// Such a line has no branch ancestor, so a merge that has to find one between it and another
	// branch still refuses; a pick takes its base explicitly and works.
	transaction_t pin;
	bool parent_is_snapshot = false;
	string base_line;
	if (parent.empty()) {
		pin = transaction_manager.GetLastCommit();
	} else {
		Git4DataBranch parent_branch;
		if (transaction_manager.branches.Get(parent, parent_branch)) {
			// A fork takes the state the parent holds, not the commit the parent was pinned at, whenever
			// the two differ - which they do the moment the parent has a table of its own or a delta. The
			// child carries the parent as the line it started from and pins the commit it forked at, so
			// what it reads is that line as it stood then, and whatever the parent does afterwards is
			// outside the pin. Without this the child reads the parent's base: measured, a parent whose row
			// read 111 forked a child whose row read 1.
			if (!parent_branch.storage_tables.empty() || !parent_branch.deltas.empty()) {
				// A fork takes the state the parent holds, not the commit the parent was pinned at,
				// whenever the two differ - which they do the moment the parent has a table of its own or a
				// delta. The child carries the parent as the line it started from and pins the commit it
				// forked at, so what it reads is that line as it stood then, and whatever the parent does
				// afterwards is outside the pin. Without this the child reads the parent's base: measured, a
				// parent whose row read 111 forked a child whose row read 1.
				base_line = parent;
				pin = transaction_manager.GetLastCommit();
			} else {
				pin = parent_branch.pin;
			}
		} else {
			Git4DataSnapshot parent_snapshot;
			if (!transaction_manager.snapshots.Get(parent, parent_snapshot)) {
				throw InvalidInputException("Reference \"%s\" is neither a branch nor a snapshot, so a branch "
				                            "cannot descend from it",
				                            parent);
			}
			if (!parent_snapshot.line.empty()) {
				// What such a reference names is a branch's own state, so the child carries that line: it
				// reads the line's storage at this pin, which is the state the reference captured, and
				// changes the other branch makes later are outside the pin and stay out.
				base_line = parent_snapshot.line;
			}
			pin = parent_snapshot.pin;
			parent_is_snapshot = true;
		}
	}
	CheckGit4DataReferenceLimit("branches", transaction_manager.branches.Count(), transaction_manager.branch_limit);
	if (!transaction_manager.branches.Create(name, pin, parent, Timestamp::GetCurrentTimestamp(), parent_is_snapshot,
	                                         base_line)) {
		throw InvalidInputException("Branch \"%s\" already exists", name);
	}
}

//! Drop a branch and the storage it was materialized into: that table is the branch's own copy, so
//! leaving it behind would leak a table per branch ever dropped.
static string PragmaGit4DataDropBranch(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	Git4DataBranch existing;
	if (!transaction_manager.branches.Get(name, existing)) {
		throw InvalidInputException("Branch \"%s\" does not exist", name);
	}
	if (transaction_manager.branches.HasChildren(name)) {
		// a descendant still walks through this name to reach its ancestors
		throw InvalidInputException("Branch \"%s\" still has branches descending from it", name);
	}
	if (transaction_manager.branches.HasLineDependents(name)) {
		// those branches read this one's storage at their fork pin, which dropping it would take away
		throw InvalidInputException("Branch \"%s\" still has branches forked from a reference that captured its "
		                            "line. Drop those branches first.",
		                            name);
	}
	string result;
	for (auto &entry : existing.storage_tables) {
		// dropping the branch takes the tables it was given with it, and one of them may have been renamed
		// since - dropping the name it was given instead would leave the table behind, silently
		auto resolved = Git4DataResolveTableName(context, entry.storage, entry.oid);
		if (resolved.empty()) {
			continue;
		}
		// A table two branches were given is not this one's alone to take: dropping it would leave the
		// other reading its pinned image, silently - measured, a branch bound to another branch's copy
		// fell back from v = 111 to the pinned v = 1 when that other branch was dropped. Only the tables
		// no other branch names go with it.
		if (transaction_manager.branches.OtherBranchUsesTable(name, resolved, entry.oid)) {
			continue;
		}
		result += StringUtil::Format("DROP TABLE IF EXISTS %s;\n", KeywordHelper::WriteOptionallyQuoted(resolved));
	}
	transaction_manager.branches.Drop(name);
	if (result.empty()) {
		// a branch with no storage of its own still has to return something to execute
		return "SELECT 1 WHERE false";
	}
	return result;
}

static string PragmaGit4DataBranches(ClientContext &context, const FunctionParameters &parameters) {
	auto entries = GetGit4DataTransactionManager(context).branches.List();
	if (entries.empty()) {
		return "SELECT NULL::VARCHAR AS branch_name, NULL::UBIGINT AS pinned_commit_id, "
		       "NULL::VARCHAR AS parent, NULL::TIMESTAMP AS created, NULL::BOOLEAN AS has_storage, "
		       "NULL::BOOLEAN AS has_delta, NULL::BOOLEAN AS storage_missing, NULL::BOOLEAN AS delta_missing "
		       "WHERE false";
	}
	string query = "SELECT * FROM (VALUES ";
	for (idx_t i = 0; i < entries.size(); i++) {
		if (i > 0) {
			query += ", ";
		}
		// Branch names are user input, escape them before splicing into SQL
		auto name = StringUtil::Replace(entries[i].name, "'", "''");
		auto parent = StringUtil::Replace(entries[i].parent, "'", "''");
		// has_storage is the precondition both refusals in create_branch and merge key on: a branch with
		// a table of its own has moved on from the commit it was forked at, which is why a fork from it
		// and a merge into it are not the simple operations they look like. It is reported so that is
		// visible rather than something a caller has to infer from a conflict.
		// has_delta says the branch differs from its base through tables of its own rather than through a
		// copy of it, which is the cheaper shape to write to and not one the listing should hide
		//
		// storage_missing and delta_missing are the other half of that: a mapping whose table has been
		// dropped falls back to the pinned image rather than failing, so a listing that only says a branch
		// *has* storage would report a table it cannot read. Measured: after dropping a materialized
		// branch's storage table, the branch read its pin while has_storage still said true.
		auto storage_missing = false;
		for (auto &storage_entry : entries[i].storage_tables) {
			if (Git4DataResolveTableName(context, storage_entry.storage, storage_entry.oid).empty()) {
				storage_missing = true;
				break;
			}
		}
		auto delta_missing = false;
		for (auto &delta_entry : entries[i].deltas) {
			if (Git4DataResolveTableName(context, delta_entry.added, delta_entry.added_oid).empty() ||
			    Git4DataResolveTableName(context, delta_entry.removed, delta_entry.removed_oid).empty()) {
				delta_missing = true;
				break;
			}
		}
		query += StringUtil::Format(
		    "('%s'::VARCHAR, %llu::UBIGINT, '%s'::VARCHAR, '%s'::TIMESTAMP, %s::BOOLEAN, %s::BOOLEAN, %s::BOOLEAN, "
		    "%s::BOOLEAN)",
		    name, entries[i].pin, parent, Timestamp::ToString(entries[i].created),
		    entries[i].storage_tables.empty() ? "false" : "true", entries[i].deltas.empty() ? "false" : "true",
		    storage_missing ? "true" : "false", delta_missing ? "true" : "false");
	}
	query += ") t(branch_name, pinned_commit_id, parent, created, has_storage, has_delta, storage_missing, "
	         "delta_missing) ORDER BY pinned_commit_id, branch_name";
	return query;
}

static string PragmaGit4DataLCA(ClientContext &context, const FunctionParameters &parameters) {
	auto left = parameters.values[0].ToString();
	auto right = parameters.values[1].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	Git4DataBranch existing;
	if (!transaction_manager.branches.Get(left, existing)) {
		throw InvalidInputException("Branch \"%s\" does not exist", left);
	}
	if (!transaction_manager.branches.Get(right, existing)) {
		throw InvalidInputException("Branch \"%s\" does not exist", right);
	}
	string lca;
	if (!transaction_manager.branches.FindLCA(left, right, lca)) {
		// the two share no ancestor, which is what a broken lineage looks like
		return "SELECT NULL::VARCHAR AS lca WHERE false";
	}
	lca = StringUtil::Replace(lca, "'", "''");
	return StringUtil::Format("SELECT '%s'::VARCHAR AS lca", lca);
}

static void PragmaGit4DataCreateSnapshot(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	CheckGit4DataReferenceName(name);
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// A snapshot and a branch with one name would be indistinguishable wherever a reference is named
	// without saying which kind it is - a diff, a pick, a restore, and a merge's own image clauses all
	// resolve a bare name, and they look in the snapshot registry first. Measured: with a branch and a
	// snapshot both called x, a merge of x with x compared the branch against the snapshot and reported
	// a conflict that did not exist. The two names cannot collide.
	Git4DataBranch same_named_branch;
	if (transaction_manager.branches.Get(name, same_named_branch)) {
		throw InvalidInputException("A branch named \"%s\" exists, and a snapshot of that name would be "
		                            "indistinguishable from it wherever a reference is named without its kind",
		                            name);
	}
	CheckGit4DataReferenceLimit("snapshots", transaction_manager.UserSnapshotCount(),
	                            transaction_manager.snapshot_limit);
	// A second argument names a branch, and the reference then captures that branch's own line rather
	// than the table: a branch that has been written to keeps its state in a table of its own, which a
	// pin alone cannot point at. Without it the reference is the table as this commit saw it.
	string line;
	if (parameters.values.size() > 1) {
		line = parameters.values[1].ToString();
		Git4DataBranch branch;
		if (!transaction_manager.branches.Get(line, branch)) {
			throw InvalidInputException("Branch \"%s\" does not exist", line);
		}
		if (branch.storage_tables.empty() && branch.deltas.empty()) {
			throw InvalidInputException("Branch \"%s\" has no line of its own, so there is nothing to capture "
			                            "- it reads the table exactly as a snapshot does",
			                            line);
		}
	}
	// Pin at the newest commit, keep everything committed so far readable
	if (!transaction_manager.snapshots.Create(name, transaction_manager.GetLastCommit(),
	                                          Timestamp::GetCurrentTimestamp(), line)) {
		throw InvalidInputException("Snapshot \"%s\" already exists", name);
	}
}

static void PragmaGit4DataDropSnapshot(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	if (!transaction_manager.snapshots.Drop(name)) {
		throw InvalidInputException("Snapshot \"%s\" does not exist", name);
	}
}

static string PragmaGit4DataSnapshots(ClientContext &context, const FunctionParameters &parameters) {
	auto entries = GetGit4DataTransactionManager(context).snapshots.List();
	if (entries.empty()) {
		return "SELECT NULL::VARCHAR AS snapshot_name, NULL::UBIGINT AS pinned_commit_id, "
		       "NULL::TIMESTAMP AS created, NULL::VARCHAR AS line WHERE false";
	}
	vector<Git4DataSnapshot> visible;
	for (auto &entry : entries) {
		if (entry.name == DuckTransactionManager::MERGE_BASE_REFERENCE) {
			// an internal reference, alive only for the length of a merge - and left behind by one that
			// failed, which is why it is neither counted nor shown
			continue;
		}
		visible.push_back(entry);
	}
	string query = "SELECT * FROM (VALUES ";
	for (idx_t i = 0; i < visible.size(); i++) {
		if (i > 0) {
			query += ", ";
		}
		// Snapshot names are user input, escape them before splicing into SQL
		auto name = StringUtil::Replace(visible[i].name, "'", "''");
		// NULL rather than an empty string for the main line, so a listing says which references
		// capture a branch's own state and which are the table at a commit
		auto line = visible[i].line.empty()
		                ? string("NULL::VARCHAR")
		                : StringUtil::Format("'%s'::VARCHAR", StringUtil::Replace(visible[i].line, "'", "''"));
		query += StringUtil::Format("('%s'::VARCHAR, %llu::UBIGINT, '%s'::TIMESTAMP, %s)", name, visible[i].pin,
		                            Timestamp::ToString(visible[i].created), line);
	}
	query += ") t(snapshot_name, pinned_commit_id, created, line) ORDER BY pinned_commit_id";
	return query;
}

static string PragmaGit4DataState(ClientContext &context, const FunctionParameters &parameters) {
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	auto last_commit = transaction_manager.GetLastCommit();
	auto pinned = transaction_manager.snapshots.LowestPin();
	// Mirrors the decision made in DuckTransactionManager::Checkpoint
	auto retained = last_commit >= transaction_manager.LowestRetentionBound();
	return StringUtil::Format("SELECT %llu::UBIGINT AS last_commit_id, %llu::UBIGINT AS pinned_commit_id, "
	                          "%s::BOOLEAN AS history_retained, %llu::UBIGINT AS retention_bound_value, "
	                          "%llu::UBIGINT AS start_timestamp_counter",
	                          last_commit, pinned, retained ? "true" : "false",
	                          transaction_manager.LowestRetentionBound().Value(),
	                          transaction_manager.GetStartTimestampCounter());
}

//! Reads a table through the physical row groups, using the retention bound rather
//! than a transaction's own snapshot. This is a measurement hook: with a snapshot
//! pinned it reports the pre-image row count, so retention across a checkpoint can
//! be observed directly.
static string Git4DataProbe(ClientContext &context, const string &table_name, TableScanType scan_type) {
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
	auto &storage = table.GetStorage();
	auto &row_groups = *storage.GetRowGroupCollection();
	auto &types = row_groups.GetTypes();

	vector<StorageIndex> column_ids;
	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.emplace_back(i);
		scan_types.push_back(types[i]);
	}
	column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	scan_types.push_back(LogicalType::ROW_TYPE);

	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::Get(context), scan_types);

	TableScanState state;
	state.Initialize(column_ids, nullptr);
	QueryContext query_context;
	row_groups.InitializeScan(query_context, state.table_state, column_ids, nullptr);

	idx_t count = 0;
	int64_t min_value = 0;
	int64_t max_value = 0;
	bool have_value = false;
	while (true) {
		scan_chunk.Reset();
		state.table_state.Scan(scan_chunk, scan_type);
		if (scan_chunk.size() == 0) {
			break;
		}
		count += scan_chunk.size();
		// Track value extremes of the first column so tests can tell the pre-image apart from the
		// post-image, not just count rows. Any integer width, because a key is as likely to be an
		// INTEGER as a BIGINT and the extremes are only reported when the column is integral at all.
		if (scan_chunk.data.empty()) {
			continue;
		}
		auto &first = scan_chunk.data[0];
		switch (first.GetType().id()) {
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT: {
			// read one value at a time: the typed iterator insists on the column's own width, and this
			// is a diagnostic rather than a hot path
			for (idx_t row = 0; row < first.size(); row++) {
				auto entry = first.GetValue(row);
				if (entry.IsNull()) {
					continue;
				}
				auto value = entry.GetValue<int64_t>();
				if (!have_value) {
					min_value = max_value = value;
					have_value = true;
				} else {
					min_value = MinValue(min_value, value);
					max_value = MaxValue(max_value, value);
				}
			}
			break;
		}
		default:
			break;
		}
	}
	return StringUtil::Format("SELECT %llu::UBIGINT AS rows_at_retention_bound, %lld::BIGINT AS min_col0, "
	                          "%lld::BIGINT AS max_col0",
	                          count, min_value, max_value);
}

//! Base data with updates skipped: what a pinned snapshot needs kept
static string PragmaGit4DataProbe(ClientContext &context, const FunctionParameters &parameters) {
	return Git4DataProbe(context, parameters.values[0].ToString(), TableScanType::TABLE_SCAN_BASE_ROWS);
}

//! Base data with updates skipped, read at an explicitly given visibility bound. Sweeping
//! the bound reveals the effective delete and insert ids: the count changes where the bound
//! crosses an id, which is what tells the two runners apart.
static string PragmaGit4DataProbeAt(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto bound_value = parameters.values[1].GetValue<int64_t>();

	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
	auto &storage = table.GetStorage();
	auto &row_groups = *storage.GetRowGroupCollection();

	vector<StorageIndex> column_ids;
	vector<LogicalType> scan_types;
	for (auto &column : table.GetColumns().Logical()) {
		column_ids.emplace_back(column.Physical().index);
		scan_types.push_back(column.Type());
	}
	column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	scan_types.push_back(LogicalType::ROW_TYPE);

	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::Get(context), scan_types);

	TableScanState state;
	state.Initialize(column_ids, nullptr);
	QueryContext query_context;
	row_groups.InitializeScan(query_context, state.table_state, column_ids, nullptr);

	idx_t count = 0;
	while (true) {
		scan_chunk.Reset();
		ScanOptions options(
		    TransactionData(MAX_TRANSACTION_ID, VisibilityBound::Before(NumericCast<transaction_t>(bound_value))));
		options.insert_type = InsertedScanType::ALL_ROWS;
		options.delete_type = DeletedScanType::OMIT_COMMITTED_DELETES;
		options.update_type = UpdateScanType::SKIP_UPDATES;
		state.table_state.Scan(options, scan_chunk);
		if (scan_chunk.size() == 0) {
			break;
		}
		count += scan_chunk.size();
	}
	return StringUtil::Format("SELECT %llu::UBIGINT AS rows_visible_at_bound", count);
}

//! Every physical row including all deleted ones, to tell "the data is gone" apart from
//! "the data is there but filtered out"
static string PragmaGit4DataProbeAll(ClientContext &context, const FunctionParameters &parameters) {
	return Git4DataProbe(context, parameters.values[0].ToString(), TableScanType::TABLE_SCAN_ALL_ROWS);
}

void PragmaFunctions::RegisterFunction(BuiltinFunctions &set) {
	RegisterEnableProfiling(set);

	// NOTE: use of Pragma functions is discouraged. Instead, opt for adding a regular table function to be invoked with
	// CALL.
	//       see for example the "enable_logging" function

	set.AddFunction(PragmaFunction::PragmaStatement("disable_profile", PragmaDisableProfiling));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_profiling", PragmaDisableProfiling));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_verification", PragmaEnableVerification));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verification", PragmaDisableVerification));

	set.AddFunction(PragmaFunction::PragmaStatement("verify_parallelism", PragmaEnableForceParallelism));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verify_parallelism", PragmaDisableForceParallelism));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_object_cache", PragmaEnableObjectCache));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_object_cache", PragmaDisableObjectCache));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_optimizer", PragmaEnableOptimizer));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_optimizer", PragmaDisableOptimizer));

	set.AddFunction(PragmaFunction::PragmaStatement("force_checkpoint", PragmaForceCheckpoint));

	// the branch whose line to capture is optional, so a snapshot of the table as a commit saw it
	// keeps working with one argument
	set.AddFunction(PragmaFunction::PragmaCall("git4data_create_snapshot", PragmaGit4DataCreateSnapshot,
	                                           {LogicalType::VARCHAR}, LogicalType::VARCHAR));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_drop_snapshot", PragmaGit4DataDropSnapshot, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_snapshots", PragmaGit4DataSnapshots));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_create_branch", PragmaGit4DataCreateBranch,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_drop_branch", PragmaGit4DataDropBranch, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_branches", PragmaGit4DataBranches));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_materialize", PragmaGit4DataMaterialize,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_clone_database", PragmaGit4DataCloneDatabase, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall(
	    "git4data_bind_branch_delta", PragmaGit4DataBindBranchDelta,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_unbind", PragmaGit4DataUnbind,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	// a delta branch written through the SQL a caller would have written anyway
	set.AddFunction(PragmaFunction::PragmaCall("git4data_apply", PragmaGit4DataApply,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	// who created the table is optional, so a binding written by hand keeps the three-argument form and
	// its table is not the feature's to drop
	set.AddFunction(PragmaFunction::PragmaCall("git4data_bind_branch_table", PragmaGit4DataBindBranchTable,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                           LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaCall(
	    "git4data_merge", PragmaGit4DataMerge,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_lca", PragmaGit4DataLCA, {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	// the policy is optional, so a pick that does not name one keeps overwriting
	set.AddFunction(PragmaFunction::PragmaCall("git4data_pick", PragmaGit4DataPick,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                           LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_restore", PragmaGit4DataRestore,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_restore_at", PragmaGit4DataRestoreAt,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_pitr", PragmaGit4DataPitr, {LogicalType::BIGINT}));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_pitr_off", PragmaGit4DataPitrOff));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_pitr_state", PragmaGit4DataPitrState));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_limit", PragmaGit4DataLimit, {LogicalType::VARCHAR, LogicalType::BIGINT}));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_limits", PragmaGit4DataLimits));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_state", PragmaGit4DataState));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_probe", PragmaGit4DataProbe, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_probe_all", PragmaGit4DataProbeAll, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_probe_at", PragmaGit4DataProbeAt,
	                                           {LogicalType::VARCHAR, LogicalType::BIGINT}));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_progress_bar", PragmaEnableProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_progress_bar", PragmaDisableProgressBar));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_print_progress_bar", PragmaEnablePrintProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_print_progress_bar", PragmaDisablePrintProgressBar));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_checkpoint_on_shutdown", PragmaEnableCheckpointOnShutdown));
	set.AddFunction(
	    PragmaFunction::PragmaStatement("disable_checkpoint_on_shutdown", PragmaDisableCheckpointOnShutdown));
}

} // namespace duckdb
