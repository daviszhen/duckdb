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

static void PragmaGit4DataCreateSnapshot(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// Pin at the newest commit, keep everything committed so far readable
	if (!transaction_manager.snapshots.Create(name, transaction_manager.GetLastCommit())) {
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
		return "SELECT NULL::VARCHAR AS snapshot_name, NULL::UBIGINT AS pinned_commit_id WHERE false";
	}
	string query = "SELECT * FROM (VALUES ";
	for (idx_t i = 0; i < entries.size(); i++) {
		if (i > 0) {
			query += ", ";
		}
		// Snapshot names are user input, escape them before splicing into SQL
		auto name = StringUtil::Replace(entries[i].name, "'", "''");
		query += StringUtil::Format("('%s'::VARCHAR, %llu::UBIGINT)", name, entries[i].pin);
	}
	query += ") t(snapshot_name, pinned_commit_id) ORDER BY pinned_commit_id";
	return query;
}

static string PragmaGit4DataState(ClientContext &context, const FunctionParameters &parameters) {
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	auto last_commit = transaction_manager.GetLastCommit();
	auto pinned = transaction_manager.snapshots.LowestPin();
	// Mirrors the decision made in DuckTransactionManager::Checkpoint
	auto retained = last_commit >= transaction_manager.LowestRetentionBound();
	return StringUtil::Format("SELECT %llu::UBIGINT AS last_commit_id, %llu::UBIGINT AS pinned_commit_id, "
	                          "%s::BOOLEAN AS history_retained",
	                          last_commit, pinned, retained ? "true" : "false");
}

//! Reads a table through the physical row groups, using the retention bound rather
//! than a transaction's own snapshot. This is a measurement hook: with a snapshot
//! pinned it reports the pre-image row count, so retention across a checkpoint can
//! be observed directly.
static string PragmaGit4DataProbe(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
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

	CreateIndexScanState state;
	state.Initialize(column_ids, nullptr);
	QueryContext query_context;
	row_groups.InitializeScan(query_context, state.table_state, column_ids, nullptr);
	row_groups.InitializeCreateIndexScan(state);

	idx_t count = 0;
	int64_t min_value = 0;
	int64_t max_value = 0;
	bool have_value = false;
	while (true) {
		scan_chunk.Reset();
		// The segment lock taken by InitializeCreateIndexScan has to be handed to the
		// scan, otherwise lazily loading row groups from disk deadlocks
		state.table_state.Scan(scan_chunk, TableScanType::TABLE_SCAN_BASE_ROWS, state.segment_lock);
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
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_state", PragmaGit4DataState));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_probe", PragmaGit4DataProbe, {LogicalType::VARCHAR}));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_progress_bar", PragmaEnableProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_progress_bar", PragmaDisableProgressBar));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_print_progress_bar", PragmaEnablePrintProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_print_progress_bar", PragmaDisablePrintProgressBar));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_checkpoint_on_shutdown", PragmaEnableCheckpointOnShutdown));
	set.AddFunction(
	    PragmaFunction::PragmaStatement("disable_checkpoint_on_shutdown", PragmaDisableCheckpointOnShutdown));
}

} // namespace duckdb
