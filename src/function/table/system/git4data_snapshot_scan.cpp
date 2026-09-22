//===----------------------------------------------------------------------===//
// Git4Data -- the scan operator behind a reference read
//===----------------------------------------------------------------------===//
// The bind data and the global state of a read, the overlays it applies in order, and the physical scan
// itself. The resolution it runs on lives in git4data_read_plan.cpp.
//===----------------------------------------------------------------------===//

#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/function/table/git4data_internal.hpp"
#include "duckdb/storage/git4data_registry.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {
struct Git4DataSnapshotScanBindData : public TableFunctionData {
	string table_name;
	string reference_name;
	Git4DataRefKind kind;
	VisibilityBound bound;
	//! The overlays standing between the base and what this reference holds, in the order they apply -
	//! empty when it holds a copy of its base, or a pin of the table itself
	vector<Git4DataDeltaLayer> deltas;
	//! Whether the table this read scans is a branch's own copy rather than the source table
	bool own_storage = false;
};

struct Git4DataSnapshotScanState : public GlobalTableFunctionState {
	TableScanState scan_state;
	DataChunk scan_chunk;
	vector<StorageIndex> column_ids;
	//! Where the key columns sit in the scanned columns, so a row's key can be built from a chunk
	vector<idx_t> key_positions;
	//! The overlays this read applies, in the order they apply, with the keys each removed
	vector<Git4DataDeltaRead> deltas;
	//! The removals of every overlay, one bucket each, because the base is filtered by all of them. The sets
	//! are held here once and pointed at by the filters, so no read keeps two copies of a half-million-key set
	vector<Git4DataRemovedKeys> removals;
	//! The same sets, in the same order, as the filters walk them
	vector<Git4DataRemovedKeys *> removal_views;
	vector<unique_ptr<DataChunk>> removal_hashes;
	//! The overlay whose added rows are being read, and once the base is behind this read
	idx_t delta_index = 0;
	bool base_done = false;
	bool finished = false;
	//! Whether the base's collection has no row groups at all, in which case there is nothing to scan and
	//! nothing was initialized to scan: `RowGroupCollection::InitializeScan` asserts on the root segment, and
	//! an empty table has none (measured - a table with no rows reports no row groups in `pragma_storage_info`
	//! both in session and after a reopen). An empty base is what the caller's own uncommitted rows are read
	//! on top of, so the scan is skipped rather than the branch being cut off.
	bool base_groups_empty = false;
	//! The database the delta's tables live in, so a scan of them can run under the caller's transaction
	optional_ptr<AttachedDatabase> delta_database;
	//! The table this read scans, when it is a branch's own copy, and whether its local rows have been
	//! read yet: a copy is the caller's table, so its uncommitted rows are part of what it holds
	optional_ptr<DataTable> base_storage;
	bool own_storage = false;
	bool reading_local_base = false;
	//! The commit the base is read at: everything committed for a branch read as of now, and the commit
	//! a reference captured
	VisibilityBound bound;

	idx_t MaxThreads() const override {
		return 1;
	}
};

unique_ptr<FunctionData> Git4DataSnapshotScanFun::CreateBindData(ClientContext &context, const string &table_name,
                                                                 const string &reference_name, Git4DataRefKind kind) {
	auto result = make_uniq<Git4DataSnapshotScanBindData>();
	Git4DataCheckNoGeneratedColumns(context, table_name);
	result->table_name = table_name;
	result->reference_name = reference_name;
	result->kind = kind;

	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	auto &transaction_manager = DuckTransactionManager::Get(*attached);
	// a rollback may have removed a reference this read is about to resolve, whoever made it
	Git4DataReconcileIfNeeded(context);
	Git4DataReadPlan plan;
	ResolveGit4DataRead(context, transaction_manager, table_name, reference_name, kind, plan);
	result->table_name = plan.table_name;
	result->own_storage = plan.own_storage;
	result->bound = plan.bound;
	result->deltas = plan.deltas;
	return std::move(result);
}

static unique_ptr<FunctionData> Git4DataSnapshotScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = Git4DataSnapshotScanFun::CreateBindData(
	    context, input.inputs[0].GetValue<string>(), input.inputs[1].GetValue<string>(), Git4DataRefKind::SNAPSHOT);

	auto &bind_data = result->Cast<Git4DataSnapshotScanBindData>();
	auto &table = Git4DataResolveTable(context, bind_data.table_name);
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

	auto &table = Git4DataResolveTable(context, bind_data.table_name);
	auto &storage = table.GetStorage();
	auto &row_groups = *storage.GetRowGroupCollection();
	result->own_storage = bind_data.own_storage;
	result->base_storage = storage;
	result->delta_database = storage.GetAttached();

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
	if (row_groups.GetTotalRows() == 0) {
		// an empty table: there is no row group to scan, and `InitializeScan` is not written for that
		result->base_groups_empty = true;
	} else {
		row_groups.InitializeScan(query_context, result->scan_state.table_state, result->column_ids, nullptr);
	}

	if (bind_data.deltas.empty()) {
		return std::move(result);
	}
	// Overlays: the base is read minus the keys they removed, and the rows they added follow, in the
	// order the overlays apply. All the tables carry the source's columns in the same order, so the
	// same column list serves for all of them.
	vector<StorageIndex> key_columns;
	vector<LogicalType> key_types;
	vector<Identifier> key_names;
	Git4DataDiffPrimaryKey(table, key_columns, key_types, key_names);
	// The key columns in the order the source declares them: that is the order the removed table holds
	// them in, and it is also the order the bundled scan lays the columns out in.
	vector<LogicalType> ordered_key_types;
	for (idx_t i = 0; i < table.GetColumns().LogicalColumnCount(); i++) {
		auto &column = table.GetColumns().GetColumn(LogicalIndex(i));
		for (auto &key : key_columns) {
			if (key.GetPrimaryIndex() != column.Physical().index) {
				continue;
			}
			result->key_positions.push_back(i);
			ordered_key_types.push_back(column.Type());
		}
	}
	// one hash column per key column per overlay, filled per chunk so the filter below needs no
	// allocation per row
	vector<LogicalType> hash_types;
	for (idx_t i = 0; i < result->key_positions.size(); i++) {
		hash_types.push_back(LogicalType::HASH);
	}
	result->removals.resize(bind_data.deltas.size());
	result->removal_hashes.resize(bind_data.deltas.size());
	for (idx_t i = 0; i < bind_data.deltas.size(); i++) {
		auto &layer = bind_data.deltas[i];
		Git4DataDeltaRead read;
		read.layer = layer;
		// the chunk exists whether or not there is a table to fill it: an overlay may name only removals
		read.chunk = make_uniq<DataChunk>();
		read.chunk->Initialize(Allocator::Get(context), scan_types);
		if (!layer.added.empty()) {
			auto &added_table = Git4DataResolveTable(context, layer.added);
			auto &added_groups = *added_table.GetStorage().GetRowGroupCollection();
			read.scan = make_uniq<TableScanState>();
			read.scan->Initialize(result->column_ids, nullptr);
			if (added_groups.GetTotalRows() == 0) {
				read.groups_empty = true;
			} else {
				added_groups.InitializeScan(query_context, read.scan->table_state, result->column_ids, nullptr);
			}
			read.storage = added_table.GetStorage();
		}
		Git4DataCollectRemovedKeys(context, layer.removed, ordered_key_types, key_columns.size(), layer.bound,
		                           read.removed);
		// the overlays' own tables are read as the caller has them, so the database each lives in is
		// remembered here; a read of a removed side is what decides it when there is one, which is where
		// the caller's transaction check runs
		if (!layer.removed.empty()) {
			auto &removed_entry = Git4DataResolveTable(context, layer.removed);
			read.database = removed_entry.GetStorage().GetAttached();
		} else if (read.storage) {
			read.database = read.storage->GetAttached();
		}
		result->delta_database = read.database;
		result->removals[i] = std::move(read.removed);
		result->removal_hashes[i] = make_uniq<DataChunk>();
		result->removal_hashes[i]->Initialize(Allocator::Get(context), hash_types);
		result->deltas.push_back(std::move(read));
	}
	// the sets are all in place, so the filters point at them rather than hold copies of them
	for (auto &entry : result->removals) {
		result->removal_views.push_back(&entry);
	}
	return std::move(result);
}

static void Git4DataSnapshotScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<Git4DataSnapshotScanState>();
	if (state.finished) {
		return;
	}
	// the base first, with the keys the overlays removed left out
	while (!state.base_done) {
		state.scan_chunk.Reset();
		if (state.reading_local_base) {
			// the rows the caller has written to the copy it holds but not committed, which are in its
			// local storage rather than in the row groups above
			auto &local_transaction = DuckTransaction::Get(context, *state.delta_database);
			LocalStorage::Get(local_transaction).Scan(state.scan_state.local_state, state.column_ids, state.scan_chunk);
		} else {
			// A copy is the caller's own table, so the caller's transaction reads it and sees its own
			// writes; the source table at a pin is not, and keeps the synthetic bound-only transaction
			// below - a reference read must not see uncommitted writes to the *live* table. Measured
			// before this: an update to a copy inside a transaction was invisible to a read of the
			// branch that holds it (1 where 999 had been written), and a copy created inside one read
			// as no rows at all.
			ScanOptions options(state.own_storage && state.delta_database
			                        ? Git4DataDeltaTransactionData(context, state.delta_database, state.bound)
			                        : TransactionData(MAX_TRANSACTION_ID, state.bound));
			if (!state.base_groups_empty) {
				state.scan_state.table_state.Scan(options, state.scan_chunk);
			}
			// an empty base leaves the chunk at zero rows, exactly as a scan of one would, and the branch
			// below is what reads the caller's own uncommitted rows on top of it
			if (state.scan_chunk.size() == 0 && state.own_storage && state.base_storage && state.delta_database) {
				auto &local_transaction = DuckTransaction::Get(context, *state.delta_database);
				LocalStorage::Get(local_transaction)
				    .InitializeScan(*state.base_storage, state.scan_state.local_state, nullptr);
				state.reading_local_base = true;
				continue;
			}
		}
		auto count = state.scan_chunk.size();
		if (count == 0) {
			// the base is exhausted; what follows is what the overlays added, in their own order
			state.base_done = true;
			break;
		}
		// every overlay's removals apply to the base: a key one of them took away is not part of the
		// line, whichever overlay took it
		SelectionVector kept(count);
		auto kept_count = Git4DataKeepRows(context, state.removal_views, state.removal_hashes, 0, state.scan_chunk,
		                                   state.key_positions, kept);
		if (kept_count == 0) {
			// every row of this chunk was removed, so the next one is what to look at
			continue;
		}
		if (kept_count == count) {
			for (idx_t i = 0; i < output.ColumnCount(); i++) {
				output.data[i].Reference(state.scan_chunk.data[i]);
			}
			output.SetChildCardinality(count);
			return;
		}
		for (idx_t i = 0; i < output.ColumnCount(); i++) {
			output.data[i].Slice(state.scan_chunk.data[i], kept, kept_count);
		}
		output.SetChildCardinality(kept_count);
		return;
	}
	if (!state.base_done) {
		state.finished = true;
		return;
	}
	// then the rows the overlays added, in the order the overlays apply. They are the caller's own
	// tables and so are read as the caller has them, each at the commit its own line pinned. The
	// committed rows come from the row groups and the caller's own uncommitted ones from its local
	// storage, in that order: measured, reading only the row groups left a statement group unable to
	// see what its own earlier statements had added - `INSERT INTO t VALUES (50,50); UPDATE t SET v = 51
	// WHERE k = 50` left 50.
	while (state.delta_index < state.deltas.size()) {
		auto &delta = state.deltas[state.delta_index];
		if (!delta.Next(context, state.column_ids, *delta.chunk)) {
			state.delta_index++;
			continue;
		}
		auto count = delta.chunk->size();
		// An overlay's own rows are filtered by the removals of the overlays *after* it, and by no
		// others: a row it added is there whatever the line below it had removed, and an overlay above
		// it that removed the same key takes it away again.
		if (state.delta_index + 1 >= state.removals.size()) {
			for (idx_t i = 0; i < output.ColumnCount(); i++) {
				output.data[i].Reference(delta.chunk->data[i]);
			}
			output.SetChildCardinality(count);
			return;
		}
		SelectionVector kept(count);
		auto kept_count = Git4DataKeepRows(context, state.removal_views, state.removal_hashes, state.delta_index + 1,
		                                   *delta.chunk, state.key_positions, kept);
		if (kept_count == 0) {
			continue;
		}
		for (idx_t i = 0; i < output.ColumnCount(); i++) {
			output.data[i].Slice(delta.chunk->data[i], kept, kept_count);
		}
		output.SetChildCardinality(kept_count);
		return;
	}
	state.finished = true;
}


//! Marks a Change that stands alone rather than being paired with a counterpart

//! One side's rows for a diff that compares row ids, and the key pattern of a left row that has one.
//!
//! A row-id diff reads two references of **one table**, so the row ids are that table's own and run from zero
//! - which is what lets a row be a *bit* here rather than a hash entry. A transaction's own rows are the
//! exception: their ids start at `MAX_ROW_ID`, far above any table's, so they go into a small map instead of
//! stretching the bits to the top of the range. Measured at a million rows, the two `unordered_set<row_t>`
//! and the `unordered_map<row_t, uint64_t>` this replaces held about 35 MiB each; this holds a quarter of a
//! megabyte of bits and eight bytes a row for the patterns. It is also what makes a diff's removals come out
//! in ascending row-id order rather than in whatever order hash buckets iterate in.

TableFunction Git4DataSnapshotScanFun::GetFunction() {
	TableFunction fun("git4data_snapshot_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                  Git4DataSnapshotScanFunction, Git4DataSnapshotScanBind, Git4DataSnapshotScanInit);
	// always produce every column and let the projection happen above, so a LogicalGet built
	// from this function by the binder does not have to translate column ids
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

void Git4DataSnapshotScanFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

} // namespace duckdb
