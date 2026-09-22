//===----------------------------------------------------------------------===//
// Git4Data -- the measurement hooks, as rows a statement can select
//===----------------------------------------------------------------------===//
// The probes read a table's physical row groups rather than any reference: what a checkpoint kept, which is
// what the retention instrument asks them. Increment 3fx moved them out of git4data_listings.cpp.
//===----------------------------------------------------------------------===//

#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/function/table/git4data_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// The probes, as rows a statement can select
//===----------------------------------------------------------------------===//
//! Reads a table through the physical row groups, using the retention bound rather than a transaction's own
//! snapshot. This is a measurement hook: with a snapshot pinned it reports the pre-image row count, so
//! retention across a checkpoint can be observed directly. It was reachable only through `PRAGMA
//! git4data_probe`, whose result cannot be selected *from*, so `diagnostics.test` could assert that it ran and
//! nothing about the numbers - which is the whole point of a measurement hook. The rows are a table function
//! now and the pragmas return it.
static void Git4DataProbeRows(ClientContext &context, const string &table_name, TableScanType scan_type,
                              vector<vector<Value>> &rows) {
	auto &table = Git4DataResolveTable(context, table_name);
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
		// Track value extremes of the first column so tests can tell the pre-image apart from the post-image,
		// not just count rows. Any integer width, because a key is as likely to be an INTEGER as a BIGINT and
		// the extremes are only reported when the column is integral at all.
		if (scan_chunk.data.empty()) {
			continue;
		}
		auto &first = scan_chunk.data[0];
		switch (first.GetType().id()) {
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT: {
			// read one value at a time: the typed iterator insists on the column's own width, and this is a
			// diagnostic rather than a hot path
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
	rows.push_back({Value::UBIGINT(count), Value::BIGINT(min_value), Value::BIGINT(max_value)});
}

//! The same scan at an explicitly given commit, sweeping which rows the bound lets through
static void Git4DataProbeAtRows(ClientContext &context, const string &table_name, int64_t bound_value,
                                vector<vector<Value>> &rows) {
	auto &table = Git4DataResolveTable(context, table_name);
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
	rows.push_back({Value::UBIGINT(count)});
}

static void Git4DataProbeNames(vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("rows_at_retention_bound");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("min_col0");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("max_col0");
	return_types.emplace_back(LogicalType::BIGINT);
}

static unique_ptr<FunctionData> Git4DataProbeImpl(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names,
                                                  bool all_rows) {
	auto result = make_uniq<Git4DataListingBindData>();
	Git4DataProbeRows(context, input.inputs[0].GetValue<string>(),
	                  all_rows ? TableScanType::TABLE_SCAN_ALL_ROWS : TableScanType::TABLE_SCAN_BASE_ROWS,
	                  result->rows);
	Git4DataProbeNames(return_types, names);
	return std::move(result);
}

static unique_ptr<FunctionData> Git4DataProbeBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	return Git4DataProbeImpl(context, input, return_types, names, false);
}

static unique_ptr<FunctionData> Git4DataProbeAllBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	return Git4DataProbeImpl(context, input, return_types, names, true);
}

static unique_ptr<FunctionData> Git4DataProbeAtBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<Git4DataListingBindData>();
	Git4DataProbeAtRows(context, input.inputs[0].GetValue<string>(), input.inputs[1].GetValue<int64_t>(), result->rows);
	names.emplace_back("rows_visible_at_bound");
	return_types.emplace_back(LogicalType::UBIGINT);
	return std::move(result);
}

TableFunction Git4DataProbeFun::GetFunction() {
	TableFunction fun("git4data_probe", {LogicalType::VARCHAR}, Git4DataListingScan, Git4DataProbeBind);
	fun.init_global = Git4DataListingInit;
	fun.projection_pushdown = false;
	// A plan that carries this function is not round-tripped by `debug_verify_serializer`, and claiming it can be
	// is worse than useless: a table function's arguments are not part of the serialized plan, so the
	// deserializer re-binds the function with an **empty** input list - measured under that mode,
	// `INTERNAL Error: Attempted to access index 0 within vector of size 0` inside `Git4DataSnapshotScanBind`,
	// reached from `LogicalGet::Deserialize`. The bind data is not serialized either, so the honest declaration is
	// that this function does not claim serialization support (the flag exists for exactly this:
	// `LogicalGet::SupportSerialization` consults it).
	fun.verify_serialization = false;
	return fun;
}

void Git4DataProbeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

TableFunction Git4DataProbeAllFun::GetFunction() {
	TableFunction fun("git4data_probe_all", {LogicalType::VARCHAR}, Git4DataListingScan, Git4DataProbeAllBind);
	fun.init_global = Git4DataListingInit;
	fun.projection_pushdown = false;
	// A plan that carries this function is not round-tripped by `debug_verify_serializer`, and claiming it can be
	// is worse than useless: a table function's arguments are not part of the serialized plan, so the
	// deserializer re-binds the function with an **empty** input list - measured under that mode,
	// `INTERNAL Error: Attempted to access index 0 within vector of size 0` inside `Git4DataSnapshotScanBind`,
	// reached from `LogicalGet::Deserialize`. The bind data is not serialized either, so the honest declaration is
	// that this function does not claim serialization support (the flag exists for exactly this:
	// `LogicalGet::SupportSerialization` consults it).
	fun.verify_serialization = false;
	return fun;
}

void Git4DataProbeAllFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

TableFunction Git4DataProbeAtFun::GetFunction() {
	TableFunction fun("git4data_probe_at", {LogicalType::VARCHAR, LogicalType::BIGINT}, Git4DataListingScan,
	                  Git4DataProbeAtBind);
	fun.init_global = Git4DataListingInit;
	fun.projection_pushdown = false;
	// A plan that carries this function is not round-tripped by `debug_verify_serializer`, and claiming it can be
	// is worse than useless: a table function's arguments are not part of the serialized plan, so the
	// deserializer re-binds the function with an **empty** input list - measured under that mode,
	// `INTERNAL Error: Attempted to access index 0 within vector of size 0` inside `Git4DataSnapshotScanBind`,
	// reached from `LogicalGet::Deserialize`. The bind data is not serialized either, so the honest declaration is
	// that this function does not claim serialization support (the flag exists for exactly this:
	// `LogicalGet::SupportSerialization` consults it).
	fun.verify_serialization = false;
	return fun;
}

void Git4DataProbeAtFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}
} // namespace duckdb
