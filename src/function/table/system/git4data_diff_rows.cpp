//===----------------------------------------------------------------------===//
// Git4Data -- the diff's row helpers
//===----------------------------------------------------------------------===//
// The primary-key collection, the row comparison and the hash containers the diff walks with. Increment 3fm
// moved them out of git4data_diff.cpp so that file holds the operator.
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

void Git4DataDiffPrimaryKey(TableCatalogEntry &table, vector<StorageIndex> &columns, vector<LogicalType> &types,
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
bool Git4DataDiffSameStoredKeyChunk(const Git4DataDiffState &state, const Git4DataDiffState::StoredRow &stored,
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
Git4DataDiffState::StoredRow Git4DataDiffStoreRow(Git4DataDiffState &state, ClientContext &context,
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
bool Git4DataDiffSameStoredValues(const Git4DataDiffState &state, const Git4DataDiffState::StoredRow &stored,
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
idx_t Git4DataDiffStoreLeftKey(Git4DataDiffState &state, ClientContext &context, DataChunk &chunk, idx_t row) {
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
vector<Value> Git4DataDiffStoredLeftKey(const Git4DataDiffState &state, idx_t position) {
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
idx_t Git4DataDiffPair(Git4DataDiffState &state, idx_t head, DataChunk &chunk, idx_t row, bool key_is_bits) {
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

void Git4DataDiffChainPush(Git4DataDiffState &state, hash_t key, Git4DataDiffState::KeyedRow entry) {
	auto head = state.left_heads.find(key);
	entry.next = head == state.left_heads.end() ? DConstants::INVALID_INDEX : head->second;
	state.left_heads[key] = state.left_candidates.size();
	state.left_candidates.push_back(entry);
}

//! A fresh block for the change keys, when the last one is full. A table with no primary key has no key
//! columns, so there is nothing to keep and the stored row it hands back is never read.
Git4DataDiffState::StoredRow Git4DataDiffChangeKeyBlock(Git4DataDiffState &state, ClientContext &context) {
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
Git4DataDiffState::StoredRow Git4DataDiffStoreChangeKey(Git4DataDiffState &state, ClientContext &context,
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

hash_t Git4DataDiffKeyHash(const vector<Value> &key) {
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

} // namespace duckdb
