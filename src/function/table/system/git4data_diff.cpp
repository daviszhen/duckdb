//===----------------------------------------------------------------------===//
// Git4Data -- the diff of two references
//===----------------------------------------------------------------------===//
// Which rows one reference has that another does not: the diff's bind and its state, the walk that compares the
// two, and the function that emits the rows. It lived in git4data_snapshot_scan.cpp; increment 3eq moved it here
// without changing a line of it. What it shares with the read path is declared in git4data_internal.hpp.
//===----------------------------------------------------------------------===//

#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/storage/git4data_registry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

#include "duckdb/function/table/git4data_internal.hpp"

namespace duckdb {

struct Git4DataDiffState : public GlobalTableFunctionState {
	//! LEFT walks the first reference, RIGHT the second and works out the difference, EMIT drains it
	enum class Phase : uint8_t { LEFT, RIGHT, EMIT, DONE };

	// one scan per side: a scan state is consumed as it walks, so the second pass needs its own
	TableScanState left_scan;
	TableScanState right_scan;
	//! Whether a side's collection has no row groups at all, as for the read's base: nothing to scan, and
	//! the side's own uncommitted rows are what is read instead
	bool left_groups_empty = false;
	bool right_groups_empty = false;
	DataChunk scan_chunk;
	//! One hash column per key column, filled per chunk while a side is walked
	DataChunk key_hashes;
	//! The database the delta's tables live in, so a scan of them can run under the caller's transaction
	optional_ptr<AttachedDatabase> delta_database;
	//! The table each side is read from, for the scan of the caller's own uncommitted rows, and whether
	//! that scan has been set up for the side
	optional_ptr<DataTable> left_storage;
	optional_ptr<DataTable> right_storage;
	bool left_local_open = false;
	bool right_local_open = false;
	bool left_own_storage = false;
	bool right_own_storage = false;
	vector<StorageIndex> column_ids;
	idx_t key_count = 0;
	//! Whether the key is a single integer column, which is what the bits path above needs
	bool key_is_integer = false;
	VisibilityBound left_bound;
	VisibilityBound right_bound;
	//! Which row ids each side holds. A row-id diff compares physical rows of one table, so the ids are the
	//! table's own - and those run from zero, which is what makes a *bit* per row enough: the hash sets this
	//! replaces cost about 35 MiB each at a million rows, and a diff's removals came out in whatever order
	//! their buckets happened to hold.
	Git4DataRowSet left_rows;
	Git4DataRowSet right_rows;
	//! Key values of the rows on the left, so a removed row can be paired with the added row that
	//! replaced it. Only filled when the table has a primary key - unless the key is one integer column, in
	//! which case `left_key_bits` holds it and this map stays empty: a key vector per left row was one
	//! allocation per row over the whole side, which is what made two reads of one table cost 2.630 s for a
	//! million rows (increment 3s).
	//! The left rows' own keys, in blocks of the key columns, for a row-id diff whose key is composite. The
	//! map this replaces held one `vector<Value>` per row - about 190 MiB for a million composite-key rows,
	//! measured - and a key is only ever read back for the rows that turn out to be *removed*.
	vector<unique_ptr<DataChunk>> left_key_blocks;
	idx_t left_key_block_rows = 0;
	//! The key's type, so a key can be built back from its bits for the rows that are reported
	LogicalType integer_key_type;
	//! Whether the two sides are two different tables, in which case the comparison is made on the
	//! primary key and the scan carries every column so two rows can be told apart by value.
	bool compare_by_key = false;
	//! The overlays each side applies, with the keys each of them removed, and the scan over each
	//! overlay's added rows. Every side needs its own: a scan is consumed as it walks, and the phases
	//! run one after the other over the same state - a side that found a scan already open would read no
	//! added rows at all, which is what a diff of a reference against itself did, reporting its own added
	//! row as a removed one.
	vector<Git4DataDeltaRead> left_deltas;
	vector<Git4DataDeltaRead> right_deltas;
	//! The types of the bundled scan, so an overlay's chunk can be built to match it
	vector<LogicalType> scan_types;
	//! The number of columns after the keys, contiguous in the scan chunk, when comparing by key
	idx_t value_count = 0;
	//! Where a stored left row is: which block of rows holds it and which row inside that block
	struct StoredRow {
		idx_t block;
		idx_t row;
	};
	//! A row of the left side, held under its primary key
	struct KeyedRow {
		//! Where this row's key and values are. The left side is held as the *values themselves* in flat
		//! columns, a block of `STANDARD_VECTOR_SIZE` rows at a time, rather than as a vector of `Value`
		//! objects per row: the peak of a million-row diff measured 286 MiB that way and 168 MiB this way
		//! (increment 3u). A diff is what merge, pick and restore are built on, so this is the memory a
		//! caller pays to compare two references at all.
		StoredRow stored;
		row_t row_id;
		//! Whether the right side accounted for this row. It used to be a *set* of matched keys - a string
		//! per matched row on the composite path and a 64-bit pattern per row on the integer one - which cost
		//! a hash entry per row and, on the composite path, a formatted key string per row: measured, a
		//! half-million-row diff under a composite key took 5.6 s that way (increment 3w).
		bool matched = false;
		//! The key's 64-bit pattern when the key is a single integer column: the map is then bucketed by it,
		//! which is exact, and matching a right row against it allocates nothing at all
		uint64_t bits = 0;
		//! The next row sharing this one's key, as a position in `left_candidates`: the rows that share a key
		//! are one chain, so a key costs a map entry and nothing else. A `vector` per key - the shape this
		//! replaces - cost a header and a heap block per distinct key on top of the map entry, measured at
		//! about 60 MiB over a million rows.
		idx_t next = DConstants::INVALID_INDEX;
	};
	//! The left side, in blocks: every block holds up to `STANDARD_VECTOR_SIZE` rows of the key and value
	//! columns, and `stored_rows` is how many rows of the last block are used
	vector<unique_ptr<DataChunk>> left_blocks;
	idx_t stored_rows = 0;
	//! The keys the change lists name, in blocks of key columns, with `change_key_rows` rows used in the last
	vector<unique_ptr<DataChunk>> change_keys;
	idx_t change_key_rows = 0;
	//! Every left row, in the order it was read, with `left_heads` saying where each key's chain starts
	vector<KeyedRow> left_candidates;
	unordered_map<hash_t, idx_t> left_heads;
	struct Change {
		row_t row_id;
		row_t counterpart;
		//! Where the row's primary key is, carried so a caller can match it without knowing row ids. The key
		//! is in blocks of the same shape the left side is held in, so a change costs a row id and a pair of
		//! indexes rather than a vector of Values of its own: measured on a diff that changes every row of a
		//! million, this took the peak from 312 MiB to 249 (increment 3v).
		StoredRow key;
	};
	vector<Change> added;
	vector<Change> removed;
	vector<Change> changed;
	idx_t added_cursor = 0;
	idx_t removed_cursor = 0;
	idx_t changed_cursor = 0;
	Phase phase = Phase::LEFT;

	idx_t MaxThreads() const override {
		return 1;
	}
};

//! A reference name may be a snapshot or a branch; both are the same kind of thing, so a diff does
//! not need to be told which and resolves in either registry.
//! Which table a reference is read from, and at what bound. A snapshot is always the pinned image of
//! the table; a branch is that too unless it has a table of its own for it, in which case it is read
//! as that table with everything it holds, because that line can have been written to.

//! The primary key of a table as physical column indexes plus their types, both empty when it has
//! none. A key is what makes two physical rows recognisably the same row.
static void Git4DataDiffPrimaryKey(TableCatalogEntry &table, vector<StorageIndex> &columns, vector<LogicalType> &types,
                                   vector<Identifier> &names) {
	auto &table_columns = table.GetColumns();
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		if (!unique.IsPrimaryKey()) {
			continue;
		}
		for (auto index : unique.GetLogicalIndexes(table_columns)) {
			auto &column = table_columns.GetColumn(index);
			columns.emplace_back(column.Physical().index);
			types.push_back(column.Type());
			names.push_back(column.Name());
		}
		return;
	}
}

unique_ptr<FunctionData> Git4DataDiffFun::CreateBindData(ClientContext &context, const string &table_name,
                                                         const string &left_name, const string &right_name) {
	auto result = make_uniq<Git4DataDiffBindData>();
	// keyed by the table the caller named rather than by the text, so the two sides' mappings are looked up for
	// that table and the base is read as it - see the reference read's own key
	result->table_name = Git4DataSourceKey(context, table_name);

	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	auto &transaction_manager = DuckTransactionManager::Get(*attached);
	// a rollback may have removed a reference this read is about to resolve, whoever made it
	Git4DataReconcileIfNeeded(context);
	auto left_plan = ResolveGit4DataReferencePlan(context, transaction_manager, table_name, left_name);
	auto right_plan = ResolveGit4DataReferencePlan(context, transaction_manager, table_name, right_name);
	result->left_table = left_plan.table_name;
	result->right_table = right_plan.table_name;
	result->left_own_storage = left_plan.own_storage;
	result->right_own_storage = right_plan.own_storage;
	result->left_bound = left_plan.bound;
	result->right_bound = right_plan.bound;
	result->left_deltas = left_plan.deltas;
	result->right_deltas = right_plan.deltas;

	auto &table = Git4DataResolveTable(context, table_name);
	// a generated column cannot be read out of a row's stored columns, and a diff reads the same table the scan
	// would (see Git4DataCheckNoGeneratedColumns - the diff alone would work, and refusing it anyway is this
	// file's choice)
	Git4DataCheckNoGeneratedColumns(context, table_name);
	Git4DataDiffPrimaryKey(table, result->key_columns, result->key_types, result->key_names);
	// A branch with a table of its own is read as that table, and its row ids are its own. Comparing
	// those against another table's row ids would compare numbers that mean different rows, so the
	// comparison moves to the primary key - and without one there is nothing left to compare on.
	// A side that differs from its base through a delta is not the table either: its added rows carry
	// row ids of their own table, so the two sides have to be matched on the key.
	result->compare_by_key = result->left_table != result->right_table || left_plan.HasDelta() || right_plan.HasDelta();
	if (result->compare_by_key && result->key_columns.empty()) {
		throw InvalidInputException("Comparing references \"%s\" and \"%s\" reads two different tables (\"%s\" and "
		                            "\"%s\"), which needs a primary key to tell one row from another",
		                            left_name, right_name, result->left_table, result->right_table);
	}
	return std::move(result);
}

static unique_ptr<FunctionData> Git4DataDiffBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	// the two names this resolves are references, so a rollback anywhere has to be reconciled first
	Git4DataReconcileIfNeeded(context);
	auto result =
	    Git4DataDiffFun::CreateBindData(context, input.inputs[0].GetValue<string>(), input.inputs[1].GetValue<string>(),
	                                    input.inputs[2].GetValue<string>());
	auto &bind_data = result->Cast<Git4DataDiffBindData>();
	// The three columns below are this function's own, and the key columns follow them, carrying names
	// that belong to the table. A key column with one of those three names would leave the result with
	// two columns of that name, which the binder refuses - measured, with a key column named
	// g4d_change: Binder Error: table "git4data_diff" has duplicate column name "g4d_change". Renaming
	// either side is worse than refusing: a caller asking for g4d_change would silently be handed the
	// table's column rather than the change marker. The table is not at fault, but it is the side that
	// can be renamed, so the sentence says so.
	for (auto &key_name : bind_data.key_names) {
		auto raw_name = key_name.GetIdentifierName();
		if (raw_name == "g4d_change" || raw_name == "g4d_row_id" || raw_name == "g4d_counterpart_row_id") {
			throw InvalidInputException(
			    "Table \"%s\" has a key column named \"%s\", which is one of the columns a diff reports, so the result "
			    "would carry that name twice. Rename the column and ask again.",
			    bind_data.table_name, raw_name);
		}
	}
	names.emplace_back("g4d_change");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("g4d_row_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("g4d_counterpart_row_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	// the key follows, so a caller can match a changed row against the live table by key rather
	// than by a physical row id that the reference scan does not expose
	for (idx_t i = 0; i < bind_data.key_columns.size(); i++) {
		names.push_back(bind_data.key_names[i]);
		return_types.push_back(bind_data.key_types[i]);
	}
	return result;
}

//! The hash of a scanned row's key, read out of the chunk rather than out of a key vector built for it: the
//! combination is the one `Git4DataDiffKeyHash` makes, so a left row and a right row of the same key land in
//! the same bucket however they were hashed.
static hash_t Git4DataDiffChunkKeyHash(DataChunk &chunk, idx_t key_count, idx_t row) {
	hash_t result = 0;
	for (idx_t i = 0; i < key_count; i++) {
		result = CombineHash(result, chunk.GetValue(i, row).Hash());
	}
	return result;
}

//! Whether a left row's key is the key of this row of a chunk
static bool Git4DataDiffSameStoredKeyChunk(const Git4DataDiffState &state, const Git4DataDiffState::StoredRow &stored,
                                           DataChunk &chunk, idx_t row) {
	auto &block = *state.left_blocks[stored.block];
	for (idx_t i = 0; i < state.key_count; i++) {
		if (!Value::NotDistinctFrom(block.GetValue(i, stored.row), chunk.GetValue(i, row))) {
			return false;
		}
	}
	return true;
}

//! The key of the row at the given position, or an empty vector when the table has no primary key
static vector<Value> Git4DataDiffRowKey(DataChunk &chunk, idx_t key_count, idx_t row) {
	vector<Value> key;
	key.reserve(key_count);
	for (idx_t i = 0; i < key_count; i++) {
		key.push_back(chunk.data[i].GetValue(row));
	}
	return key;
}

//! The key a 64-bit pattern stands for, in the key's own type
static Value Git4DataDiffKeyFromBits(const LogicalType &type, uint64_t bits) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(NumericCast<int8_t>(static_cast<int64_t>(bits)));
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(NumericCast<int16_t>(static_cast<int64_t>(bits)));
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(NumericCast<int32_t>(static_cast<int64_t>(bits)));
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(static_cast<int64_t>(bits));
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(NumericCast<uint8_t>(bits));
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(NumericCast<uint16_t>(bits));
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(NumericCast<uint32_t>(bits));
	default:
		return Value::UBIGINT(bits);
	}
}

//! The key bits of one row of a chunk, for a diff whose key is a single integer column
static bool Git4DataDiffChunkKeyBits(DataChunk &chunk, idx_t row, uint64_t &bits) {
	auto key = chunk.GetValue(0, row);
	if (key.IsNull()) {
		return false;
	}
	return Git4DataIntegerKeyBits(key, bits);
}

//! Append one scanned row's key and value columns to the left side's blocks, and say where it landed. The
//! columns are written one at a time into a block of `STANDARD_VECTOR_SIZE` rows, so a stored row costs the
//! values themselves - four bytes for an INTEGER - with a row id and a key pattern beside them.
static Git4DataDiffState::StoredRow Git4DataDiffStoreRow(Git4DataDiffState &state, ClientContext &context,
                                                         DataChunk &chunk, idx_t row) {
	if (state.left_blocks.empty() || state.stored_rows == STANDARD_VECTOR_SIZE) {
		auto block = make_uniq<DataChunk>();
		vector<LogicalType> stored_types;
		for (idx_t i = 0; i < state.key_count + state.value_count; i++) {
			stored_types.push_back(state.scan_types[i]);
		}
		block->Initialize(Allocator::Get(context), stored_types);
		block->SetCardinality(STANDARD_VECTOR_SIZE);
		state.left_blocks.push_back(std::move(block));
		state.stored_rows = 0;
	}
	auto stored = Git4DataDiffState::StoredRow {state.left_blocks.size() - 1, state.stored_rows++};
	auto &block = *state.left_blocks[stored.block];
	for (idx_t i = 0; i < state.key_count + state.value_count; i++) {
		block.data[i].SetValue(stored.row, chunk.GetValue(i, row));
	}
	return stored;
}

//! Whether the values a left row stored are the ones this row of the chunk carries. The comparison is made
//! against the chunk rather than against a materialized row, so a matched row costs no allocation, and the
//! stored side is read out of the block the row is in.
static bool Git4DataDiffSameStoredValues(const Git4DataDiffState &state, const Git4DataDiffState::StoredRow &stored,
                                         DataChunk &chunk, idx_t first_value, idx_t row) {
	auto &block = *state.left_blocks[stored.block];
	for (idx_t i = 0; i < state.value_count; i++) {
		auto value = chunk.GetValue(first_value + i, row);
		if (!Value::NotDistinctFrom(block.GetValue(state.key_count + i, stored.row), value)) {
			return false;
		}
	}
	return true;
}

//! Keep a left row's own key for a row-id diff, and say where it landed. Every block but the last is full, so
//! a position is enough to find it again: the block is the position divided by `STANDARD_VECTOR_SIZE` and the
//! row inside it is the remainder.
static idx_t Git4DataDiffStoreLeftKey(Git4DataDiffState &state, ClientContext &context, DataChunk &chunk, idx_t row) {
	if (state.left_key_blocks.empty() || state.left_key_block_rows == STANDARD_VECTOR_SIZE) {
		auto block = make_uniq<DataChunk>();
		vector<LogicalType> key_types;
		for (idx_t i = 0; i < state.key_count; i++) {
			key_types.push_back(state.scan_types[i]);
		}
		block->Initialize(Allocator::Get(context), key_types);
		block->SetCardinality(STANDARD_VECTOR_SIZE);
		state.left_key_blocks.push_back(std::move(block));
		state.left_key_block_rows = 0;
	}
	auto position = (state.left_key_blocks.size() - 1) * STANDARD_VECTOR_SIZE + state.left_key_block_rows;
	auto &block = *state.left_key_blocks.back();
	for (idx_t i = 0; i < state.key_count; i++) {
		block.data[i].SetValue(state.left_key_block_rows, chunk.GetValue(i, row));
	}
	state.left_key_block_rows++;
	return position;
}

//! One of those keys back, as the vector a change carries out
static vector<Value> Git4DataDiffStoredLeftKey(const Git4DataDiffState &state, idx_t position) {
	auto &block = *state.left_key_blocks[position / STANDARD_VECTOR_SIZE];
	auto row = position % STANDARD_VECTOR_SIZE;
	vector<Value> key;
	key.reserve(state.key_count);
	for (idx_t i = 0; i < state.key_count; i++) {
		key.push_back(block.GetValue(i, row));
	}
	return key;
}

//! Put a left row at the head of its key's chain, and answer with the link the caller's row should carry. The
//! rows live in one arena and the chain is a position in it, so a key costs a map entry - not a map entry and
//! a container of its own.
//! Which left row a right row pairs with: the position of the first row of the key's chain that is still
//! unpaired, or - when one of them carries the same values as this right row - that one.
//!
//! A chain holds more than one row only when a key is repeated, and a delta's added table is the caller's, so
//! a line can hold the same key twenty times. Pairing like with like is what keeps a repeated key from being
//! reported as a *change* of one of the rows: with a value-equal partner the two are the same row and nothing
//! is reported, and only a right row with no equal partner is a change of the first one. It also makes the
//! answer independent of the order the rows came back in, which the arena's order had made it depend on -
//! measured, reversing the chain turned a removal into a change in `delta_duplicate_key.test`.
static idx_t Git4DataDiffPair(Git4DataDiffState &state, idx_t head, DataChunk &chunk, idx_t row, bool key_is_bits) {
	idx_t first = DConstants::INVALID_INDEX;
	for (auto index = head; index != DConstants::INVALID_INDEX; index = state.left_candidates[index].next) {
		auto &candidate = state.left_candidates[index];
		if (candidate.matched) {
			continue;
		}
		if (key_is_bits) {
			if (!Git4DataDiffSameStoredValues(state, candidate.stored, chunk, state.key_count, row)) {
				if (first == DConstants::INVALID_INDEX) {
					first = index;
				}
				continue;
			}
			return index;
		}
		if (!Git4DataDiffSameStoredKeyChunk(state, candidate.stored, chunk, row)) {
			continue;
		}
		if (Git4DataDiffSameStoredValues(state, candidate.stored, chunk, state.key_count, row)) {
			return index;
		}
		if (first == DConstants::INVALID_INDEX) {
			first = index;
		}
	}
	return first;
}

static void Git4DataDiffChainPush(Git4DataDiffState &state, hash_t key, Git4DataDiffState::KeyedRow entry) {
	auto head = state.left_heads.find(key);
	entry.next = head == state.left_heads.end() ? DConstants::INVALID_INDEX : head->second;
	state.left_heads[key] = state.left_candidates.size();
	state.left_candidates.push_back(entry);
}

//! A fresh block for the change keys, when the last one is full. A table with no primary key has no key
//! columns, so there is nothing to keep and the stored row it hands back is never read.
static Git4DataDiffState::StoredRow Git4DataDiffChangeKeyBlock(Git4DataDiffState &state, ClientContext &context) {
	if (state.key_count == 0) {
		return Git4DataDiffState::StoredRow {0, 0};
	}
	if (state.change_keys.empty() || state.change_key_rows == STANDARD_VECTOR_SIZE) {
		auto block = make_uniq<DataChunk>();
		vector<LogicalType> key_types;
		for (idx_t i = 0; i < state.key_count; i++) {
			key_types.push_back(state.scan_types[i]);
		}
		block->Initialize(Allocator::Get(context), key_types);
		block->SetCardinality(STANDARD_VECTOR_SIZE);
		state.change_keys.push_back(std::move(block));
		state.change_key_rows = 0;
	}
	return Git4DataDiffState::StoredRow {state.change_keys.size() - 1, state.change_key_rows++};
}

//! Keep a change's key, read straight out of the row the change came from: the key is written column by
//! column into the change's own block, so no vector is built for it at all.
static Git4DataDiffState::StoredRow Git4DataDiffStoreChangeKey(Git4DataDiffState &state, ClientContext &context,
                                                               DataChunk &chunk, idx_t row) {
	auto stored = Git4DataDiffChangeKeyBlock(state, context);
	if (state.key_count == 0) {
		return stored;
	}
	auto &block = *state.change_keys[stored.block];
	for (idx_t i = 0; i < state.key_count; i++) {
		block.data[i].SetValue(stored.row, chunk.GetValue(i, row));
	}
	return stored;
}

//! The same for a change whose key is a row of the left side, which is already held in a block
static Git4DataDiffState::StoredRow Git4DataDiffStoreChangeKeyFromLeft(Git4DataDiffState &state, ClientContext &context,
                                                                       const Git4DataDiffState::StoredRow &source) {
	auto stored = Git4DataDiffChangeKeyBlock(state, context);
	if (state.key_count == 0) {
		return stored;
	}
	auto &from = *state.left_blocks[source.block];
	auto &block = *state.change_keys[stored.block];
	for (idx_t i = 0; i < state.key_count; i++) {
		block.data[i].SetValue(stored.row, from.GetValue(i, source.row));
	}
	return stored;
}

//! The same for a key that is already a vector, which is how the row-id path carries the keys it pairs up.
//! A key shorter than the table's is filled with nulls rather than left as whatever the block held.
static Git4DataDiffState::StoredRow Git4DataDiffStoreChangeKeyValues(Git4DataDiffState &state, ClientContext &context,
                                                                     const vector<Value> &key) {
	auto stored = Git4DataDiffChangeKeyBlock(state, context);
	if (state.key_count == 0) {
		return stored;
	}
	auto &block = *state.change_keys[stored.block];
	for (idx_t i = 0; i < state.key_count; i++) {
		if (i < key.size()) {
			block.data[i].SetValue(stored.row, key[i]);
			continue;
		}
		block.data[i].SetValue(stored.row, Value(state.scan_types[i]));
	}
	return stored;
}

static bool Git4DataDiffSameKey(const vector<Value> &left, const vector<Value> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (Value::NotDistinctFrom(left[i], right[i])) {
			continue;
		}
		return false;
	}
	return true;
}

static hash_t Git4DataDiffKeyHash(const vector<Value> &key) {
	hash_t result = 0;
	for (auto &value : key) {
		result = CombineHash(result, value.Hash());
	}
	return result;
}

//! Set up one side's overlays for a diff, in the order they apply. The same bundled column list the
//! side's base is read with serves for every overlay's added table: they carry the source's columns in
//! the same order. Each overlay's removed keys are read once here, because a diff walks its base once.
static void Git4DataDiffSetupDeltas(ClientContext &context, const vector<Git4DataDeltaLayer> &layers,
                                    const vector<StorageIndex> &column_ids, const vector<LogicalType> &scan_types,
                                    const vector<LogicalType> &key_types, idx_t key_count, QueryContext &query_context,
                                    vector<Git4DataDeltaRead> &result, optional_ptr<AttachedDatabase> &database) {
	for (auto &layer : layers) {
		Git4DataDeltaRead read;
		read.layer = layer;
		// the chunk exists whether or not there is a table to fill it: an overlay may name only removals
		read.chunk = make_uniq<DataChunk>();
		read.chunk->Initialize(Allocator::Get(context), scan_types);
		if (!layer.added.empty()) {
			auto &added_table = Git4DataResolveTable(context, layer.added);
			auto &added_groups = *added_table.GetStorage().GetRowGroupCollection();
			read.scan = make_uniq<TableScanState>();
			read.scan->Initialize(column_ids, nullptr);
			if (added_groups.GetTotalRows() == 0) {
				read.groups_empty = true;
			} else {
				added_groups.InitializeScan(query_context, read.scan->table_state, column_ids, nullptr);
			}
			read.storage = added_table.GetStorage();
		}
		Git4DataCollectRemovedKeys(context, layer.removed, key_types, key_count, layer.bound, read.removed);
		// the overlays' own tables are read as the caller has them, so the database each lives in is
		// remembered here
		if (!layer.removed.empty()) {
			auto &removed_entry = Git4DataResolveTable(context, layer.removed);
			read.database = removed_entry.GetStorage().GetAttached();
		} else if (read.storage) {
			read.database = read.storage->GetAttached();
		}
		database = read.database;
		result.push_back(std::move(read));
	}
}

static unique_ptr<GlobalTableFunctionState> Git4DataDiffInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<Git4DataDiffBindData>();
	auto result = make_uniq<Git4DataDiffState>();
	result->left_bound = bind_data.left_bound;
	result->right_bound = bind_data.right_bound;
	result->key_count = bind_data.key_columns.size();

	auto &table = Git4DataResolveTable(context, bind_data.table_name);

	result->compare_by_key = bind_data.compare_by_key;
	// the key columns first, then the row id, so a chunk carries everything the diff compares. When
	// the two sides are different tables the values come along too, in one run after the keys.
	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < bind_data.key_columns.size(); i++) {
		result->column_ids.push_back(bind_data.key_columns[i]);
		scan_types.push_back(bind_data.key_types[i]);
	}
	if (bind_data.compare_by_key) {
		auto &columns = table.GetColumns();
		for (idx_t i = 0; i < columns.PhysicalColumnCount(); i++) {
			auto &column = columns.GetColumn(PhysicalIndex(i));
			if (column.Generated()) {
				continue;
			}
			bool is_key = false;
			for (auto &key_column : bind_data.key_columns) {
				if (key_column.GetPrimaryIndex() == i) {
					is_key = true;
					break;
				}
			}
			if (is_key) {
				continue;
			}
			result->column_ids.emplace_back(i);
			scan_types.push_back(column.Type());
			result->value_count++;
		}
	}
	result->column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	scan_types.push_back(LogicalType::ROW_TYPE);
	result->scan_chunk.Initialize(Allocator::Get(context), scan_types);
	result->scan_types = scan_types;
	vector<LogicalType> hash_types;
	for (idx_t i = 0; i < bind_data.key_columns.size(); i++) {
		hash_types.push_back(LogicalType::HASH);
	}
	result->key_hashes.Initialize(Allocator::Get(context), hash_types);
	// what each side removed from its base, and the tables it added rows with, read once: a side that
	// differs by an overlay is not the table
	// A single integer key gives the diff an exact bucket and removes the per-row key and string allocations
	result->key_is_integer = bind_data.key_columns.size() == 1 && Git4DataIsIntegerKeyType(bind_data.key_types[0]);
	if (result->key_is_integer) {
		result->integer_key_type = bind_data.key_types[0];
	}
	QueryContext query_context;
	Git4DataDiffSetupDeltas(context, bind_data.left_deltas, result->column_ids, scan_types, bind_data.key_types,
	                        bind_data.key_columns.size(), query_context, result->left_deltas, result->delta_database);
	Git4DataDiffSetupDeltas(context, bind_data.right_deltas, result->column_ids, scan_types, bind_data.key_types,
	                        bind_data.key_columns.size(), query_context, result->right_deltas, result->delta_database);
	result->left_own_storage = bind_data.left_own_storage;
	result->right_own_storage = bind_data.right_own_storage;
	result->left_storage = &Git4DataResolveTable(context, bind_data.left_table).GetStorage();
	result->right_storage = &Git4DataResolveTable(context, bind_data.right_table).GetStorage();

	// each side is read from its own table, which is the same table unless a side is a branch with a
	// line of its own
	auto &left_table = Git4DataResolveTable(context, bind_data.left_table);
	auto &left_groups = *left_table.GetStorage().GetRowGroupCollection();
	result->left_scan.Initialize(result->column_ids, nullptr);
	if (left_groups.GetTotalRows() == 0) {
		result->left_groups_empty = true;
	} else {
		left_groups.InitializeScan(query_context, result->left_scan.table_state, result->column_ids, nullptr);
	}
	auto &right_table = Git4DataResolveTable(context, bind_data.right_table);
	auto &right_groups = *right_table.GetStorage().GetRowGroupCollection();
	result->right_scan.Initialize(result->column_ids, nullptr);
	if (right_groups.GetTotalRows() == 0) {
		result->right_groups_empty = true;
	} else {
		right_groups.InitializeScan(query_context, result->right_scan.table_state, result->column_ids, nullptr);
	}
	return std::move(result);
}

//! Walk one side at one bound - its base, then the rows each of its overlays added - handing every
//! visible row and its row id to the callback.
static void Git4DataDiffCollect(ClientContext &context, Git4DataDiffState &state, TableScanState &scan,
                                VisibilityBound bound, vector<Git4DataDeltaRead> &deltas,
                                optional_ptr<DataTable> side_storage, bool side_own_storage, bool &side_local_open,
                                const std::function<void(DataChunk &, row_t, idx_t)> &callback) {
	// the key columns come first in the scan, so their positions are the first key_count columns
	vector<idx_t> key_positions;
	for (idx_t i = 0; i < state.key_count; i++) {
		key_positions.push_back(i);
	}
	// A side read from a branch's own copy is the caller's table, so it is read as the caller has it -
	// uncommitted rows included, which live in local storage rather than in the row groups. The source
	// table at a pin keeps the synthetic bound-only transaction. Measured before this: inside the
	// transaction that had built a branch's copy, a diff of that branch reported every base row as
	// removed, and the merge built on it deleted those rows from the other side.
	auto &side_database = side_storage ? side_storage->GetAttached() : *state.delta_database;
	auto &side_transaction = DuckTransaction::Get(context, side_database);
	auto side_view = TransactionData(side_transaction);
	ScanOptions options(side_own_storage ? TransactionData(side_view.GetTransactionId(), bound)
	                                     : TransactionData(MAX_TRANSACTION_ID, bound));
	// The base is filtered by every overlay's removals: a key one of them took away is not part of the
	// side, whichever overlay took it. The sets are *pointed at*, not copied: they belong to the side and
	// live for the whole walk, and a copy of a half-million-key set is tens of megabytes that a diff was
	// paying twice - measured, a diff that changes every row of a million peaked at 298 MiB with the copies
	// and 255 MiB without them.
	vector<Git4DataRemovedKeys *> removals;
	vector<unique_ptr<DataChunk>> removal_hashes;
	vector<LogicalType> hash_types;
	for (idx_t i = 0; i < key_positions.size(); i++) {
		hash_types.push_back(LogicalType::HASH);
	}
	for (auto &delta : deltas) {
		auto hashes = make_uniq<DataChunk>();
		hashes->Initialize(Allocator::Get(context), hash_types);
		removal_hashes.push_back(std::move(hashes));
		removals.push_back(&delta.removed);
	}
	while (true) {
		state.scan_chunk.Reset();
		if (side_local_open) {
			LocalStorage::Get(side_transaction).Scan(scan.local_state, state.column_ids, state.scan_chunk);
		} else {
			const bool side_groups_empty =
			    &scan == &state.left_scan ? state.left_groups_empty : state.right_groups_empty;
			if (!side_groups_empty) {
				scan.table_state.Scan(options, state.scan_chunk);
			}
			if (state.scan_chunk.size() == 0 && side_own_storage && side_storage) {
				LocalStorage::Get(side_transaction).InitializeScan(*side_storage, scan.local_state, nullptr);
				side_local_open = true;
				continue;
			}
		}
		auto count = state.scan_chunk.size();
		if (count == 0) {
			break;
		}
		if (!removals.empty() && state.key_hashes.ColumnCount() > 0) {
			Git4DataHashForRemovals(removals, removal_hashes, 0, state.scan_chunk, key_positions);
		}
		// the row id column is not necessarily flat - a scan may hand back a dictionary
		// the row id sits after the keys, and after the values when those are carried too
		auto row_ids = state.scan_chunk.data[state.key_count + state.value_count].Values<row_t>();
		// the same prepared key column the scan path uses, for the same reason
		Git4DataKeyColumn typed_keys;
		bool typed_only = key_positions.size() == 1;
		for (idx_t s = 0; s < removals.size() && typed_only; s++) {
			typed_only = removals[s]->typed;
		}
		if (typed_only) {
			typed_keys.Init(state.scan_chunk.data[key_positions[0]]);
		}
		for (idx_t i = 0; i < count; i++) {
			bool removed = false;
			for (idx_t s = 0; s < removals.size(); s++) {
				auto was_removed = typed_only ? Git4DataTypedRowWasRemoved(typed_keys, *removals[s], i)
				                              : Git4DataRowWasRemoved(*removals[s], state.scan_chunk, key_positions,
				                                                      *removal_hashes[s], i);
				if (was_removed) {
					removed = true;
					break;
				}
			}
			if (removed) {
				// this side removed the row from its base, so it is not part of what the side holds
				continue;
			}
			callback(state.scan_chunk, row_ids[i].GetValue(), i);
		}
	}
	// then the rows each overlay added, in the order the overlays apply. An overlay's own rows are
	// filtered by the removals of the overlays *after* it, and by no others. Each overlay is read from
	// its own table, through its own scan - a scan is consumed as it walks, and every side of the diff
	// walks its own.
	for (idx_t index = 0; index < deltas.size(); index++) {
		auto &delta = deltas[index];
		while (delta.Next(context, state.column_ids, *delta.chunk)) {
			auto count = delta.chunk->size();
			auto row_ids = delta.chunk->data[state.key_count + state.value_count].Values<row_t>();
			if (index + 1 >= removals.size()) {
				for (idx_t i = 0; i < count; i++) {
					callback(*delta.chunk, row_ids[i].GetValue(), i);
				}
				continue;
			}
			if (state.key_hashes.ColumnCount() > 0) {
				Git4DataHashForRemovals(removals, removal_hashes, index + 1, *delta.chunk, key_positions);
			}
			Git4DataKeyColumn typed_keys;
			bool typed_only = key_positions.size() == 1;
			for (idx_t s = index + 1; s < removals.size() && typed_only; s++) {
				typed_only = removals[s]->typed;
			}
			if (typed_only) {
				typed_keys.Init(delta.chunk->data[key_positions[0]]);
			}
			for (idx_t i = 0; i < count; i++) {
				bool removed = false;
				for (idx_t s = index + 1; s < removals.size(); s++) {
					auto was_removed = typed_only ? Git4DataTypedRowWasRemoved(typed_keys, *removals[s], i)
					                              : Git4DataRowWasRemoved(*removals[s], *delta.chunk, key_positions,
					                                                      *removal_hashes[s], i);
					if (was_removed) {
						removed = true;
						break;
					}
				}
				if (removed) {
					continue;
				}
				callback(*delta.chunk, row_ids[i].GetValue(), i);
			}
		}
	}
}

//! Write up to one vector's worth of rows from a list.
static idx_t Git4DataDiffEmit(DataChunk &output, const Git4DataDiffState &state,
                              const vector<Git4DataDiffState::Change> &rows, idx_t &cursor, const char *change) {
	auto remaining = rows.size() - cursor;
	if (remaining == 0) {
		return 0;
	}
	auto count = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
	// The first three columns are written with a writer, which sizes the vector; the key columns are written
	// value by value, which does not - `DataChunk::SetValue` only sets the value. A chunk whose vectors
	// disagree about their size is what `verify_vectors` refuses ("DataChunk::Verify - size mismatch: vector 3
	// (INTEGER) has size 0 but chunk has size 2", measured on a merge, which diffs two lines), so the sizes are
	// set once the values are in.
	auto change_writer = FlatVector::Writer<string_t>(output.data[0], count);
	auto row_writer = FlatVector::Writer<uint64_t>(output.data[1], count);
	auto counterpart_writer = FlatVector::Writer<uint64_t>(output.data[2], count);
	for (idx_t i = 0; i < count; i++) {
		auto &entry = rows[cursor + i];
		change_writer.WriteStringRef(change);
		row_writer.WriteValue(NumericCast<uint64_t>(entry.row_id));
		if (entry.counterpart == NO_COUNTERPART) {
			counterpart_writer.WriteNull();
		} else {
			counterpart_writer.WriteValue(NumericCast<uint64_t>(entry.counterpart));
		}
		if (state.key_count == 0) {
			continue;
		}
		auto &key_block = *state.change_keys[entry.key.block];
		for (idx_t k = 0; k < state.key_count; k++) {
			output.data[3 + k].SetValue(i, key_block.GetValue(k, entry.key.row));
		}
	}
	output.SetChildCardinality(count);
	cursor += count;
	return count;
}

//! The values of a row after the keys, in the order the scan carries them
static void Git4DataDiffFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<Git4DataDiffState>();
	if (state.phase == Git4DataDiffState::Phase::DONE) {
		return;
	}
	if (state.compare_by_key) {
		// The two sides are two different tables, so their row ids are not comparable: the branch's
		// storage numbers its own rows from zero again and the numbers drift apart as soon as the two
		// tables append a different number of rows. The primary key is the identity that survives
		// that, and the values are read so two rows sharing a key can be told apart from one row.
		if (state.phase == Git4DataDiffState::Phase::LEFT) {
			Git4DataDiffCollect(
			    context, state, state.left_scan, state.left_bound, state.left_deltas, state.left_storage,
			    state.left_own_storage, state.left_local_open, [&](DataChunk &chunk, row_t row_id, idx_t row) {
				    Git4DataDiffState::KeyedRow entry;
				    entry.stored = Git4DataDiffStoreRow(state, context, chunk, row);
				    entry.row_id = row_id;
				    if (state.key_is_integer) {
					    uint64_t bits = 0;
					    if (Git4DataDiffChunkKeyBits(chunk, row, bits)) {
						    // the bucket is the key itself, so a lookup in it needs no comparison at all
						    entry.bits = bits;
						    Git4DataDiffChainPush(state, bits, entry);
						    return;
					    }
				    }
				    Git4DataDiffChainPush(state, Git4DataDiffChunkKeyHash(chunk, state.key_count, row), entry);
			    });
			state.phase = Git4DataDiffState::Phase::RIGHT;
		}
		if (state.phase == Git4DataDiffState::Phase::RIGHT) {
			Git4DataDiffCollect(
			    context, state, state.right_scan, state.right_bound, state.right_deltas, state.right_storage,
			    state.right_own_storage, state.right_local_open, [&](DataChunk &chunk, row_t row_id, idx_t row) {
				    uint64_t bits = 0;
				    if (state.key_is_integer && Git4DataDiffChunkKeyBits(chunk, row, bits)) {
					    // the chain's key is the key itself, so a hit is exact and needs no comparison of keys
					    auto bits_entry = state.left_heads.find(bits);
					    if (bits_entry != state.left_heads.end()) {
						    auto partner = Git4DataDiffPair(state, bits_entry->second, chunk, row, true);
						    if (partner != DConstants::INVALID_INDEX) {
							    auto &candidate = state.left_candidates[partner];
							    candidate.matched = true;
							    if (!Git4DataDiffSameStoredValues(state, candidate.stored, chunk, state.key_count,
							                                      row)) {
								    state.changed.push_back(Git4DataDiffState::Change {
								        candidate.row_id, row_id,
								        Git4DataDiffStoreChangeKey(state, context, chunk, row)});
							    }
							    return;
						    }
					    }
					    state.added.push_back(Git4DataDiffState::Change {
					        row_id, NO_COUNTERPART, Git4DataDiffStoreChangeKey(state, context, chunk, row)});
					    return;
				    }
				    auto hash = Git4DataDiffChunkKeyHash(chunk, state.key_count, row);
				    auto entry = state.left_heads.find(hash);
				    if (entry != state.left_heads.end()) {
					    auto partner = Git4DataDiffPair(state, entry->second, chunk, row, false);
					    if (partner != DConstants::INVALID_INDEX) {
						    auto &candidate = state.left_candidates[partner];
						    candidate.matched = true;
						    if (!Git4DataDiffSameStoredValues(state, candidate.stored, chunk, state.key_count, row)) {
							    state.changed.push_back(Git4DataDiffState::Change {
							        candidate.row_id, row_id, Git4DataDiffStoreChangeKey(state, context, chunk, row)});
						    }
						    return;
					    }
				    }
				    state.added.push_back(Git4DataDiffState::Change {
				        row_id, NO_COUNTERPART, Git4DataDiffStoreChangeKey(state, context, chunk, row)});
			    });
			// in the order the rows were read, which the arena kept: the map's order was whatever its buckets
			// held, so the removals of a diff came out in an order no caller could predict
			for (auto &candidate : state.left_candidates) {
				if (candidate.matched) {
					continue;
				}
				state.removed.push_back(
				    Git4DataDiffState::Change {candidate.row_id, NO_COUNTERPART,
				                               Git4DataDiffStoreChangeKeyFromLeft(state, context, candidate.stored)});
			}
			state.left_candidates.clear();
			state.left_heads.clear();
			// the left side is in the changes that came out of it now, so the blocks go back
			state.left_blocks.clear();
			state.stored_rows = 0;
			state.phase = Git4DataDiffState::Phase::EMIT;
		}
		auto keyed = Git4DataDiffEmit(output, state, state.changed, state.changed_cursor, "changed");
		if (keyed > 0) {
			return;
		}
		keyed = Git4DataDiffEmit(output, state, state.removed, state.removed_cursor, "removed");
		if (keyed > 0) {
			return;
		}
		keyed = Git4DataDiffEmit(output, state, state.added, state.added_cursor, "added");
		if (keyed > 0) {
			return;
		}
		state.phase = Git4DataDiffState::Phase::DONE;
		return;
	}
	if (state.phase == Git4DataDiffState::Phase::LEFT) {
		Git4DataDiffCollect(
		    context, state, state.left_scan, state.left_bound, state.left_deltas, state.left_storage,
		    state.left_own_storage, state.left_local_open, [&](DataChunk &chunk, row_t row_id, idx_t row) {
			    state.left_rows.Insert(row_id);
			    if (state.key_count > 0) {
				    uint64_t key_bits = 0;
				    if (state.key_is_integer && Git4DataDiffChunkKeyBits(chunk, row, key_bits)) {
					    state.left_rows.SetKeyBits(row_id, key_bits);
				    } else {
					    state.left_rows.SetKeyIndex(row_id, Git4DataDiffStoreLeftKey(state, context, chunk, row));
				    }
			    }
		    });
		state.phase = Git4DataDiffState::Phase::RIGHT;
	}
	if (state.phase == Git4DataDiffState::Phase::RIGHT) {
		vector<pair<row_t, vector<Value>>> added_rows;
		Git4DataDiffCollect(context, state, state.right_scan, state.right_bound, state.right_deltas,
		                    state.right_storage, state.right_own_storage, state.right_local_open,
		                    [&](DataChunk &chunk, row_t row_id, idx_t row) {
			                    state.right_rows.Insert(row_id);
			                    if (!state.left_rows.Contains(row_id)) {
				                    added_rows.emplace_back(row_id, Git4DataDiffRowKey(chunk, state.key_count, row));
			                    }
		                    });
		vector<pair<row_t, vector<Value>>> removed_rows;
		state.left_rows.ForEach([&](row_t row_id) {
			if (state.right_rows.Contains(row_id)) {
				return;
			}
			uint64_t bits = 0;
			if (state.left_rows.GetKeyBits(row_id, bits)) {
				removed_rows.emplace_back(row_id,
				                          vector<Value> {Git4DataDiffKeyFromBits(state.integer_key_type, bits)});
				return;
			}
			idx_t position = 0;
			if (state.left_rows.GetKeyIndex(row_id, position)) {
				removed_rows.emplace_back(row_id, Git4DataDiffStoredLeftKey(state, position));
				return;
			}
			removed_rows.emplace_back(row_id, vector<Value>());
		});
		// With a primary key, an addition that carries the key of a removal is that same row after an
		// update rather than two unrelated changes.
		vector<bool> removed_taken(removed_rows.size(), false);
		if (state.key_count > 0) {
			unordered_map<hash_t, vector<idx_t>> removed_by_key;
			for (idx_t i = 0; i < removed_rows.size(); i++) {
				removed_by_key[Git4DataDiffKeyHash(removed_rows[i].second)].push_back(i);
			}
			for (auto &added : added_rows) {
				idx_t match = DConstants::INVALID_INDEX;
				auto bucket = removed_by_key.find(Git4DataDiffKeyHash(added.second));
				if (bucket != removed_by_key.end()) {
					for (auto candidate : bucket->second) {
						if (!removed_taken[candidate] &&
						    Git4DataDiffSameKey(removed_rows[candidate].second, added.second)) {
							match = candidate;
							break;
						}
					}
				}
				if (match == DConstants::INVALID_INDEX) {
					state.added.push_back(Git4DataDiffState::Change {
					    added.first, NO_COUNTERPART, Git4DataDiffStoreChangeKeyValues(state, context, added.second)});
				} else {
					removed_taken[match] = true;
					state.changed.push_back(
					    Git4DataDiffState::Change {removed_rows[match].first, added.first,
					                               Git4DataDiffStoreChangeKeyValues(state, context, added.second)});
				}
			}
		} else {
			for (auto &added : added_rows) {
				state.added.push_back(Git4DataDiffState::Change {
				    added.first, NO_COUNTERPART, Git4DataDiffStoreChangeKeyValues(state, context, added.second)});
			}
		}
		for (idx_t i = 0; i < removed_rows.size(); i++) {
			if (!removed_taken[i]) {
				state.removed.push_back(Git4DataDiffState::Change {
				    removed_rows[i].first, NO_COUNTERPART,
				    Git4DataDiffStoreChangeKeyValues(state, context, removed_rows[i].second)});
			}
		}
		// the row sets and the keys are only needed to work out the three lists
		state.left_rows.Clear();
		state.right_rows.Clear();
		state.left_key_blocks.clear();
		state.left_key_block_rows = 0;
		state.phase = Git4DataDiffState::Phase::EMIT;
	}
	auto emitted = Git4DataDiffEmit(output, state, state.changed, state.changed_cursor, "changed");
	if (emitted > 0) {
		return;
	}
	emitted = Git4DataDiffEmit(output, state, state.added, state.added_cursor, "added");
	if (emitted > 0) {
		return;
	}
	emitted = Git4DataDiffEmit(output, state, state.removed, state.removed_cursor, "removed");
	if (emitted > 0) {
		return;
	}
	state.phase = Git4DataDiffState::Phase::DONE;
	// the children too: a chunk that says zero rows while a vector still holds the last batch's size is the
	// same inconsistency from the other end
	output.SetChildCardinality(0);
}

TableFunction Git4DataDiffFun::GetFunction() {
	TableFunction fun("git4data_diff", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                  Git4DataDiffFunction, Git4DataDiffBind);
	fun.init_global = Git4DataDiffInit;
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

void Git4DataDiffFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

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


} // namespace duckdb
