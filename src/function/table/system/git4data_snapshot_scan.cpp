#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

//! Reads a table as of a pinned reference: the rows a reader at that commit id would have seen.
//! The retention bound keeps those rows and their delete ids alive, so the scan is an ordinary
//! visibility-filtered scan with the pinned bound substituted for "now".
struct Git4DataSnapshotScanBindData : public TableFunctionData {
	string table_name;
	string reference_name;
	Git4DataRefKind kind;
	VisibilityBound bound;
};

struct Git4DataSnapshotScanState : public GlobalTableFunctionState {
	TableScanState scan_state;
	DataChunk scan_chunk;
	vector<StorageIndex> column_ids;
	VisibilityBound bound;
	bool finished = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

unique_ptr<FunctionData> Git4DataSnapshotScanFun::CreateBindData(ClientContext &context, const string &table_name,
                                                                 const string &reference_name, Git4DataRefKind kind) {
	auto result = make_uniq<Git4DataSnapshotScanBindData>();
	result->table_name = table_name;
	result->reference_name = reference_name;
	result->kind = kind;

	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	auto &transaction_manager = DuckTransactionManager::Get(*attached);
	if (kind == Git4DataRefKind::BRANCH) {
		Git4DataBranch branch;
		if (!transaction_manager.branches.Get(reference_name, branch)) {
			throw InvalidInputException("Branch \"%s\" does not exist", reference_name);
		}
		result->bound = VisibilityBound::Through(branch.pin);
		return std::move(result);
	}
	auto pin = transaction_manager.snapshots.GetPin(reference_name);
	if (!pin.IsValid()) {
		throw InvalidInputException("Snapshot \"%s\" does not exist", reference_name);
	}
	result->bound = VisibilityBound::Through(pin.GetIndex());
	return std::move(result);
}

static unique_ptr<FunctionData> Git4DataSnapshotScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = Git4DataSnapshotScanFun::CreateBindData(
	    context, input.inputs[0].GetValue<string>(), input.inputs[1].GetValue<string>(), Git4DataRefKind::SNAPSHOT);

	auto &bind_data = result->Cast<Git4DataSnapshotScanBindData>();
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(bind_data.table_name)));
	for (auto &column : table.GetColumns().Logical()) {
		names.emplace_back(column.Name());
		return_types.emplace_back(column.Type());
	}
	return result;
}

static unique_ptr<GlobalTableFunctionState> Git4DataSnapshotScanInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<Git4DataSnapshotScanBindData>();
	auto result = make_uniq<Git4DataSnapshotScanState>();
	result->bound = bind_data.bound;

	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(bind_data.table_name)));
	auto &storage = table.GetStorage();
	auto &row_groups = *storage.GetRowGroupCollection();

	vector<LogicalType> scan_types;
	for (auto &column : table.GetColumns().Logical()) {
		result->column_ids.emplace_back(column.Physical().index);
		scan_types.push_back(column.Type());
	}
	// the row id is needed to drive the scan, but is not part of the output
	result->column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	scan_types.push_back(LogicalType::ROW_TYPE);

	result->scan_chunk.Initialize(Allocator::Get(context), scan_types);

	// the segment lock taken here has to be handed to every Scan call below
	result->scan_state.Initialize(result->column_ids, nullptr);
	QueryContext query_context;
	row_groups.InitializeScan(query_context, result->scan_state.table_state, result->column_ids, nullptr);
	return std::move(result);
}

static void Git4DataSnapshotScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<Git4DataSnapshotScanState>();
	if (state.finished) {
		return;
	}
	state.scan_chunk.Reset();
	// no transaction of our own writes to see, and the pinned bound in place of "now"
	ScanOptions options(TransactionData(MAX_TRANSACTION_ID, state.bound));
	state.scan_state.table_state.Scan(options, state.scan_chunk);
	if (state.scan_chunk.size() == 0) {
		state.finished = true;
		return;
	}
	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		output.data[i].Reference(state.scan_chunk.data[i]);
	}
	output.SetCardinality(state.scan_chunk.size());
}

//! The rows that differ between two references. A row is visible at a bound when its insert id is
//! below that bound and its delete id is not, which is exactly what a scan at the bound computes -
//! so the difference between two references is the difference between the row id sets the two
//! bounds produce. Only row ids are compared here; aligning a removed row with the added row that
//! replaced it is row identity work, which is a later module.
struct Git4DataDiffBindData : public TableFunctionData {
	string table_name;
	VisibilityBound left_bound;
	VisibilityBound right_bound;
};

struct Git4DataDiffState : public GlobalTableFunctionState {
	//! LEFT walks the first reference, RIGHT the second and works out the difference, EMIT drains
	//! it. The two sets are the whole diff: a row is visible at a bound when its insert id is below
	//! that bound and its delete id is not, which is what a scan at the bound already computes.
	enum class Phase : uint8_t { LEFT, RIGHT, EMIT, DONE };

	// one scan per side: a scan state is consumed as it walks, so the second pass needs its own
	TableScanState left_scan;
	TableScanState right_scan;
	DataChunk scan_chunk;
	vector<StorageIndex> column_ids;
	VisibilityBound left_bound;
	VisibilityBound right_bound;
	unordered_set<row_t> left_rows;
	unordered_set<row_t> right_rows;
	vector<row_t> added;
	vector<row_t> removed;
	idx_t added_cursor = 0;
	idx_t removed_cursor = 0;
	Phase phase = Phase::LEFT;

	idx_t MaxThreads() const override {
		return 1;
	}
};

//! A reference name may be a snapshot or a branch; both are the same kind of thing, so a diff does
//! not need to be told which and resolves in either registry.
static VisibilityBound ResolveReferenceBound(DuckTransactionManager &transaction_manager, const string &name) {
	auto pin = transaction_manager.snapshots.GetPin(name);
	if (pin.IsValid()) {
		return VisibilityBound::Through(pin.GetIndex());
	}
	Git4DataBranch branch;
	if (transaction_manager.branches.Get(name, branch)) {
		return VisibilityBound::Through(branch.pin);
	}
	throw InvalidInputException("Reference \"%s\" is neither a snapshot nor a branch", name);
}

unique_ptr<FunctionData> Git4DataDiffFun::CreateBindData(ClientContext &context, const string &table_name,
                                                         const string &left_name, const string &right_name) {
	auto result = make_uniq<Git4DataDiffBindData>();
	result->table_name = table_name;

	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	auto &transaction_manager = DuckTransactionManager::Get(*attached);
	result->left_bound = ResolveReferenceBound(transaction_manager, left_name);
	result->right_bound = ResolveReferenceBound(transaction_manager, right_name);
	return std::move(result);
}

static unique_ptr<FunctionData> Git4DataDiffBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result =
	    Git4DataDiffFun::CreateBindData(context, input.inputs[0].GetValue<string>(), input.inputs[1].GetValue<string>(),
	                                    input.inputs[2].GetValue<string>());
	names.emplace_back("row_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("change");
	return_types.emplace_back(LogicalType::VARCHAR);
	return result;
}

static unique_ptr<GlobalTableFunctionState> Git4DataDiffInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<Git4DataDiffBindData>();
	auto result = make_uniq<Git4DataDiffState>();
	result->left_bound = bind_data.left_bound;
	result->right_bound = bind_data.right_bound;

	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(bind_data.table_name)));
	auto &row_groups = *table.GetStorage().GetRowGroupCollection();

	// only the row id is read: the diff is a comparison of which rows each bound can see
	vector<LogicalType> scan_types {LogicalType::ROW_TYPE};
	result->column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	result->scan_chunk.Initialize(Allocator::Get(context), scan_types);

	QueryContext query_context;
	result->left_scan.Initialize(result->column_ids, nullptr);
	row_groups.InitializeScan(query_context, result->left_scan.table_state, result->column_ids, nullptr);
	result->right_scan.Initialize(result->column_ids, nullptr);
	row_groups.InitializeScan(query_context, result->right_scan.table_state, result->column_ids, nullptr);
	return std::move(result);
}

//! Walk the whole table at one bound, handing every visible row id to the callback.
static void Git4DataDiffCollect(Git4DataDiffState &state, TableScanState &scan, VisibilityBound bound,
                                const std::function<void(row_t)> &callback) {
	ScanOptions options(TransactionData(MAX_TRANSACTION_ID, bound));
	while (true) {
		state.scan_chunk.Reset();
		scan.table_state.Scan(options, state.scan_chunk);
		auto count = state.scan_chunk.size();
		if (count == 0) {
			break;
		}
		// the row id column is not necessarily flat - a scan may hand back a dictionary
		auto row_ids = state.scan_chunk.data[0].Values<row_t>();
		for (idx_t i = 0; i < count; i++) {
			callback(row_ids[i].GetValue());
		}
	}
}

//! Write up to one vector's worth of rows from a list, as (row_id, change).
static idx_t Git4DataDiffEmit(DataChunk &output, const vector<row_t> &rows, idx_t &cursor, const char *change) {
	auto remaining = rows.size() - cursor;
	if (remaining == 0) {
		return 0;
	}
	auto count = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
	auto row_writer = FlatVector::Writer<uint64_t>(output.data[0], count);
	auto change_writer = FlatVector::Writer<string_t>(output.data[1], count);
	for (idx_t i = 0; i < count; i++) {
		row_writer.WriteValue(NumericCast<uint64_t>(rows[cursor + i]));
		change_writer.WriteStringRef(change);
	}
	cursor += count;
	return count;
}

static void Git4DataDiffFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<Git4DataDiffState>();
	if (state.phase == Git4DataDiffState::Phase::DONE) {
		return;
	}
	if (state.phase == Git4DataDiffState::Phase::LEFT) {
		Git4DataDiffCollect(state, state.left_scan, state.left_bound,
		                    [&](row_t row_id) { state.left_rows.insert(row_id); });
		state.phase = Git4DataDiffState::Phase::RIGHT;
	}
	if (state.phase == Git4DataDiffState::Phase::RIGHT) {
		Git4DataDiffCollect(state, state.right_scan, state.right_bound,
		                    [&](row_t row_id) { state.right_rows.insert(row_id); });
		for (auto row_id : state.right_rows) {
			if (state.left_rows.find(row_id) == state.left_rows.end()) {
				state.added.push_back(row_id);
			}
		}
		for (auto row_id : state.left_rows) {
			if (state.right_rows.find(row_id) == state.right_rows.end()) {
				state.removed.push_back(row_id);
			}
		}
		// the sets are only needed to work out the two lists
		state.left_rows.clear();
		state.right_rows.clear();
		state.phase = Git4DataDiffState::Phase::EMIT;
	}
	auto emitted = Git4DataDiffEmit(output, state.added, state.added_cursor, "added");
	if (emitted > 0) {
		return;
	}
	emitted = Git4DataDiffEmit(output, state.removed, state.removed_cursor, "removed");
	if (emitted > 0) {
		return;
	}
	state.phase = Git4DataDiffState::Phase::DONE;
	output.SetCardinality(0);
}

TableFunction Git4DataDiffFun::GetFunction() {
	TableFunction fun("git4data_diff", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                  Git4DataDiffFunction, Git4DataDiffBind);
	fun.init_global = Git4DataDiffInit;
	fun.projection_pushdown = false;
	return fun;
}

void Git4DataDiffFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

TableFunction Git4DataSnapshotScanFun::GetFunction() {
	TableFunction fun("git4data_snapshot_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                  Git4DataSnapshotScanFunction, Git4DataSnapshotScanBind, Git4DataSnapshotScanInit);
	// always produce every column and let the projection happen above, so a LogicalGet built
	// from this function by the binder does not have to translate column ids
	fun.projection_pushdown = false;
	return fun;
}

void Git4DataSnapshotScanFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

} // namespace duckdb
