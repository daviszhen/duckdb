#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

//! Reads a table as of a pinned snapshot: the rows a reader at that snapshot's commit id would
//! have seen. The retention bound keeps those rows and their delete ids alive, so the scan is
//! an ordinary visibility-filtered scan with the pinned bound substituted for "now".
struct Git4DataSnapshotScanBindData : public TableFunctionData {
	string table_name;
	string snapshot_name;
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
                                                                 const string &snapshot_name) {
	auto result = make_uniq<Git4DataSnapshotScanBindData>();
	result->table_name = table_name;
	result->snapshot_name = snapshot_name;

	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	auto &transaction_manager = DuckTransactionManager::Get(*attached);
	auto pin = transaction_manager.snapshots.GetPin(snapshot_name);
	if (!pin.IsValid()) {
		throw InvalidInputException("Snapshot \"%s\" does not exist", snapshot_name);
	}
	result->bound = VisibilityBound::Through(pin.GetIndex());
	return std::move(result);
}

static unique_ptr<FunctionData> Git4DataSnapshotScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types,
                                                         vector<Identifier> &names) {
	auto result = Git4DataSnapshotScanFun::CreateBindData(context, input.inputs[0].GetValue<string>(),
	                                                      input.inputs[1].GetValue<string>());

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
