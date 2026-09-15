#include "duckdb/function/pragma/pragma_functions.hpp"

#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
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
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
	auto key_columns = Git4DataPrimaryKeyColumnNames(table);
	if (key_columns.empty()) {
		throw InvalidInputException("Table \"%s\" has no primary key, so a restore cannot tell which rows to "
		                            "replace",
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
	auto at_clause = FormatGit4DataAtClause(table_name, reference, unit);

	// Every row goes, including the ones the reference never had: a restore rewinds the table, so a row
	// added since the reference is as wrong as a row it changed. The delete runs first, and the reference
	// scan still reads its own image afterwards because the delete is newer than the reference's bound.
	string result;
	result += StringUtil::Format("DELETE FROM %s;\n", quoted_table);
	result += StringUtil::Format("INSERT INTO %s SELECT * FROM %s;", quoted_table, at_clause);
	return result;
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
static string PragmaGit4DataPick(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto source = parameters.values[1].ToString();
	auto base = parameters.values[2].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	// both names resolve before any statement is generated, so a typo cannot half-apply a pick
	auto source_unit = ResolveGit4DataReference(transaction_manager, source);
	ResolveGit4DataReference(transaction_manager, base);

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
	auto removed_or_changed =
	    StringUtil::Format("SELECT %s FROM %s WHERE change IN ('removed', 'changed')", key_list, diff);
	auto added_or_changed =
	    StringUtil::Format("SELECT %s FROM %s WHERE change IN ('added', 'changed')", key_list, diff);

	// A changed row is applied as a delete followed by an insert: there is no in-place update to reach
	// for here, and the outcome is the same.
	string result;
	result += StringUtil::Format("DELETE FROM %s WHERE (%s) IN (%s);\n", quoted_table, key_list, removed_or_changed);
	// Only the keys the live table is actually missing: a row the source added may well still be there,
	// because the live table is usually the source's own continuation, and inserting it again would
	// collide with its primary key instead of restoring anything. The keys of changed rows were removed
	// by the statement above, so they are missing by the time this runs and take the source's values.
	result += StringUtil::Format("INSERT INTO %s SELECT * FROM %s WHERE (%s) IN (%s) AND (%s) NOT IN "
	                             "(SELECT %s FROM %s);",
	                             quoted_table, FormatGit4DataAtClause(table_name, source, source_unit), key_list,
	                             added_or_changed, key_list, key_list, quoted_table);
	return result;
}

static void PragmaGit4DataCreateBranch(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	auto parent = parameters.values[1].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// A branch descends from another branch when one is named; otherwise it starts at the
	// newest commit, which is the point a snapshot would pin. Either way the branch holds its
	// base down exactly as a pin does.
	transaction_t pin;
	if (parent.empty()) {
		pin = transaction_manager.GetLastCommit();
	} else {
		Git4DataBranch parent_branch;
		if (!transaction_manager.branches.Get(parent, parent_branch)) {
			throw InvalidInputException("Branch \"%s\" does not exist", parent);
		}
		pin = parent_branch.pin;
	}
	if (!transaction_manager.branches.Create(name, pin, parent, Timestamp::GetCurrentTimestamp())) {
		throw InvalidInputException("Branch \"%s\" already exists", name);
	}
}

static void PragmaGit4DataDropBranch(ClientContext &context, const FunctionParameters &parameters) {
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
	transaction_manager.branches.Drop(name);
}

static string PragmaGit4DataBranches(ClientContext &context, const FunctionParameters &parameters) {
	auto entries = GetGit4DataTransactionManager(context).branches.List();
	if (entries.empty()) {
		return "SELECT NULL::VARCHAR AS branch_name, NULL::UBIGINT AS pinned_commit_id, "
		       "NULL::VARCHAR AS parent_branch, NULL::TIMESTAMP AS created WHERE false";
	}
	string query = "SELECT * FROM (VALUES ";
	for (idx_t i = 0; i < entries.size(); i++) {
		if (i > 0) {
			query += ", ";
		}
		// Branch names are user input, escape them before splicing into SQL
		auto name = StringUtil::Replace(entries[i].name, "'", "''");
		auto parent = StringUtil::Replace(entries[i].parent, "'", "''");
		query += StringUtil::Format("('%s'::VARCHAR, %llu::UBIGINT, '%s'::VARCHAR, '%s'::TIMESTAMP)", name,
		                            entries[i].pin, parent, Timestamp::ToString(entries[i].created));
	}
	query += ") t(branch_name, pinned_commit_id, parent_branch, created) ORDER BY pinned_commit_id, branch_name";
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
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// Pin at the newest commit, keep everything committed so far readable
	if (!transaction_manager.snapshots.Create(name, transaction_manager.GetLastCommit(),
	                                          Timestamp::GetCurrentTimestamp())) {
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
		       "NULL::TIMESTAMP AS created WHERE false";
	}
	string query = "SELECT * FROM (VALUES ";
	for (idx_t i = 0; i < entries.size(); i++) {
		if (i > 0) {
			query += ", ";
		}
		// Snapshot names are user input, escape them before splicing into SQL
		auto name = StringUtil::Replace(entries[i].name, "'", "''");
		query += StringUtil::Format("('%s'::VARCHAR, %llu::UBIGINT, '%s'::TIMESTAMP)", name, entries[i].pin,
		                            Timestamp::ToString(entries[i].created));
	}
	query += ") t(snapshot_name, pinned_commit_id, created) ORDER BY pinned_commit_id";
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
		// Track value extremes of the first column so tests can tell the pre-image
		// apart from the post-image, not just count rows
		if (scan_chunk.data.empty() || scan_chunk.data[0].GetType().id() != LogicalTypeId::BIGINT) {
			continue;
		}
		for (auto entry : scan_chunk.data[0].Values<int64_t>()) {
			if (!entry.IsValid()) {
				continue;
			}
			auto value = entry.GetValue();
			if (!have_value) {
				min_value = max_value = value;
				have_value = true;
			} else {
				min_value = MinValue(min_value, value);
				max_value = MaxValue(max_value, value);
			}
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

	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_create_snapshot", PragmaGit4DataCreateSnapshot, {LogicalType::VARCHAR}));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_drop_snapshot", PragmaGit4DataDropSnapshot, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_snapshots", PragmaGit4DataSnapshots));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_create_branch", PragmaGit4DataCreateBranch,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_drop_branch", PragmaGit4DataDropBranch, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_branches", PragmaGit4DataBranches));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_lca", PragmaGit4DataLCA, {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_pick", PragmaGit4DataPick,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_restore", PragmaGit4DataRestore,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_restore_at", PragmaGit4DataRestoreAt,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR}));
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
