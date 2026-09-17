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
			key_list_joined += "g4d_tk." + quoted;
			join_live += StringUtil::Format("g4d_l.%s = g4d_tk.%s", quoted, quoted);
			join_base += StringUtil::Format("g4d_b.%s = g4d_tk.%s", quoted, quoted);
			join_source += StringUtil::Format("g4d_s.%s = g4d_tk.%s", quoted, quoted);
			// a key is never null, so a null one in an outer join means that side has no such row
			presence_base += StringUtil::Format("(g4d_l.%s IS NULL) <> (g4d_b.%s IS NULL)", quoted, quoted);
			presence_source += StringUtil::Format("(g4d_l.%s IS NULL) <> (g4d_s.%s IS NULL)", quoted, quoted);
		}
		auto live_key = "g4d_l." + KeywordHelper::WriteOptionallyQuoted(key_columns[0]);
		auto changed_from_base = StringUtil::Format("((%s) OR (%s IS NOT NULL AND NOT (%s)))", presence_base, live_key,
		                                            BuildGit4DataRowComparison(table, "g4d_l", "g4d_b"));
		auto differs_from_source = StringUtil::Format("((%s) OR (%s IS NOT NULL AND NOT (%s)))", presence_source,
		                                              live_key, BuildGit4DataRowComparison(table, "g4d_l", "g4d_s"));
		conflict_table = StringUtil::Format(
		    "CREATE OR REPLACE TEMP TABLE g4d_pick_conflicts AS WITH g4d_tk AS (SELECT %s FROM %s WHERE g4d_change IN "
		    "('added', 'removed', 'changed')), g4d_l AS (SELECT * FROM %s), g4d_b AS (SELECT * FROM %s), g4d_s AS "
		    "(SELECT * FROM %s) SELECT %s FROM g4d_tk LEFT JOIN g4d_l ON (%s) LEFT JOIN g4d_b ON (%s) LEFT JOIN "
		    "g4d_s ON (%s) WHERE (%s) AND (%s);\n",
		    key_list, diff, quoted_table, base_image, source_image, key_list_joined, join_live, join_base, join_source,
		    changed_from_base, differs_from_source);
	}

	// A changed row is applied as a delete followed by an insert: there is no in-place update to reach
	// for here, and the outcome is the same.
	string result = conflict_table;
	if (policy == "fail") {
		result += "SELECT CASE WHEN EXISTS (SELECT 1 FROM g4d_pick_conflicts) THEN error('pick conflict: the live "
		          "table changed the same rows since the base - pass policy skip to leave them, or overwrite to "
		          "take the source') ELSE 0 END;\n";
	}
	auto conflict_filter = policy == "skip" ? StringUtil::Format(" AND (%s) NOT IN (SELECT %s FROM g4d_pick_conflicts)",
	                                                             key_list, key_list)
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
		result += "\nDROP TABLE g4d_pick_conflicts;";
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
	return StringUtil::Format("CREATE TABLE %s (%s);\nINSERT INTO %s SELECT * FROM %s AT (BRANCH => '%s');\n"
	                          "PRAGMA git4data_bind_branch_table('%s', '%s', '%s');",
	                          quoted_storage, columns, quoted_storage,
	                          KeywordHelper::WriteOptionallyQuoted(source_table), escaped_branch, escaped_source,
	                          escaped_branch, escaped_storage);
}

//! Record that a branch reads a table of its own. Only meaningful as the last statement of a
//! materialize, once the copy has been made.
static void PragmaGit4DataBindBranchTable(ClientContext &context, const FunctionParameters &parameters) {
	auto source_table = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto storage_table = parameters.values[2].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	Git4DataBranch branch;
	if (!transaction_manager.branches.Get(branch_name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
	// both tables have to be there, or the mapping would be a promise the reads then have to break
	auto &source_entry = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(source_table)));
	auto &storage_entry = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(storage_table)));
	// both oids go with the mapping: the storage table's so a read can tell it from another table that
	// takes its name after it is dropped, and the source's so the mapping follows the source if it is
	// renamed
	transaction_manager.branches.SetStorageTable(branch_name, source_table, storage_table, storage_entry.oid,
	                                             source_entry.oid);
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
				throw InvalidInputException("Branch \"%s\" inherits a line that differs by a delta, so a delta of its own "
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
			merge_into = entry.storage;
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
			if (storage_entry.source != table_name) {
				continue;
			}
			auto &storage_table =
			    Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(storage_entry.storage)));
			auto &storage_groups = *storage_table.GetStorage().GetRowGroupCollection();
			transaction_t copy_commit = 0;
			bool unwritten = true;
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
				transaction_manager.snapshots.Drop("__merge_base__");
				if (!transaction_manager.snapshots.Create("__merge_base__", ancestor_branch.pin,
				                                          Timestamp::GetCurrentTimestamp())) {
					throw InternalException("Could not create the internal merge base reference");
				}
				base_reference = "__merge_base__";
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
		key_list_both += "g4d_b." + quoted;
		key_match_xy += "g4d_x." + quoted + " = g4d_y." + quoted;
		key_match_by += "g4d_y." + quoted + " = g4d_b." + quoted;
	}
	auto quoted_table = KeywordHelper::WriteOptionallyQuoted(table_name);
	auto quoted_into = KeywordHelper::WriteOptionallyQuoted(merge_into);
	auto diff_left = StringUtil::Format("git4data_diff('%s', '%s', '%s')", escape(table_name), escape(base_reference),
	                                    escape(left_name));
	auto diff_right = StringUtil::Format("git4data_diff('%s', '%s', '%s')", escape(table_name), escape(base_reference),
	                                     escape(right_name));
	auto right_image =
	    FormatGit4DataAtClause(table_name, right_name, ResolveGit4DataReference(transaction_manager, right_name));

	string result =
	    StringUtil::Format("CREATE OR REPLACE TEMP TABLE g4d_merge_conflicts AS WITH g4d_l AS (SELECT %s FROM %s), "
	                       "g4d_r AS (SELECT %s FROM %s), g4d_both AS (SELECT %s FROM g4d_l INTERSECT SELECT %s FROM "
	                       "g4d_r) SELECT %s FROM g4d_both g4d_b WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM %s) "
	                       "g4d_x JOIN (SELECT * FROM %s) g4d_y ON (%s) WHERE %s AND (%s));\n",
	                       key_list, diff_left, key_list, diff_right, key_list, key_list, key_list_both, quoted_into,
	                       right_image, key_match_xy, key_match_by, BuildGit4DataRowComparison(table, "g4d_x", "g4d_y"));
	if (policy == "fail") {
		result += "SELECT CASE WHEN EXISTS (SELECT 1 FROM g4d_merge_conflicts) THEN error('merge conflict: use "
		          "policy skip or accept, or resolve it first') ELSE 0 END;\n";
	}
	// with accept the right side wins outright; otherwise a conflicting key is left as the left has it
	auto conflict_filter =
	    policy == "accept"
	        ? string()
	        : StringUtil::Format(" AND (%s) NOT IN (SELECT %s FROM g4d_merge_conflicts)", key_list, key_list);
	// Accepting means the right side's row replaces whatever the left has for that key, including when
	// the right side added it and the left also has one - so every key the right touched is cleared
	// first, not only the ones it removed or changed.
	result += StringUtil::Format(
	    "DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change IN (%s)%s);\n", quoted_into, key_list, key_list,
	    diff_right, policy == "accept" ? "'added', 'removed', 'changed'" : "'removed', 'changed'", conflict_filter);
	result += StringUtil::Format(
	    "INSERT INTO %s SELECT * FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change IN (%s)%s) AND (%s) NOT IN "
	    "(SELECT %s FROM %s);\n",
	    quoted_into, right_image, key_list, key_list, diff_right, "'added', 'changed'", conflict_filter, key_list,
	    key_list, quoted_into);
	result += "DROP TABLE g4d_merge_conflicts;";
	if (drop_merge_base) {
		// the reference was only there for the two diffs above, which have run by now
		result += "\nPRAGMA git4data_drop_snapshot('__merge_base__');";
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
		result += StringUtil::Format("DROP TABLE IF EXISTS %s;\n", KeywordHelper::WriteOptionallyQuoted(entry.storage));
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
		       "NULL::BOOLEAN AS has_delta WHERE false";
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
		query += StringUtil::Format(
		    "('%s'::VARCHAR, %llu::UBIGINT, '%s'::VARCHAR, '%s'::TIMESTAMP, %s::BOOLEAN, %s::BOOLEAN)", name,
		    entries[i].pin, parent, Timestamp::ToString(entries[i].created),
		    entries[i].storage_tables.empty() ? "false" : "true", entries[i].deltas.empty() ? "false" : "true");
	}
	query += ") t(branch_name, pinned_commit_id, parent, created, has_storage, has_delta) ORDER BY pinned_commit_id, "
	         "branch_name";
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
	string query = "SELECT * FROM (VALUES ";
	for (idx_t i = 0; i < entries.size(); i++) {
		if (i > 0) {
			query += ", ";
		}
		// Snapshot names are user input, escape them before splicing into SQL
		auto name = StringUtil::Replace(entries[i].name, "'", "''");
		// NULL rather than an empty string for the main line, so a listing says which references
		// capture a branch's own state and which are the table at a commit
		auto line = entries[i].line.empty()
		                ? string("NULL::VARCHAR")
		                : StringUtil::Format("'%s'::VARCHAR", StringUtil::Replace(entries[i].line, "'", "''"));
		query += StringUtil::Format("('%s'::VARCHAR, %llu::UBIGINT, '%s'::TIMESTAMP, %s)", name, entries[i].pin,
		                            Timestamp::ToString(entries[i].created), line);
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
	set.AddFunction(PragmaFunction::PragmaCall("git4data_bind_branch_table", PragmaGit4DataBindBranchTable,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
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
