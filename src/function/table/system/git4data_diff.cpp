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


//! A reference name may be a snapshot or a branch; both are the same kind of thing, so a diff does
//! not need to be told which and resolves in either registry.
//! Which table a reference is read from, and at what bound. A snapshot is always the pinned image of
//! the table; a branch is that too unless it has a table of its own for it, in which case it is read
//! as that table with everything it holds, because that line can have been written to.

//! The primary key of a table as physical column indexes plus their types, both empty when it has
//! none. A key is what makes two physical rows recognisably the same row.

//! The rows that differ between two references. A row is visible at a bound when its insert id is
//! below that bound and its delete id is not, which is exactly what a scan at the bound computes -
//! so the difference between two references is the difference between the row id sets the two
//! bounds produce. Only row ids are compared here; aligning a removed row with the added row that
//! replaced it is row identity work, which is a later module.
//! The rows that differ between two references. A row is visible at a bound when its insert id is
//! below that bound and its delete id is not, which is exactly what a scan at the bound computes -
//! so the difference between two references is the difference between the row id sets the two
//! bounds produce.
//!
//! An update is a delete plus an insert of a *different* physical row, so it arrives here as one
//! removal and one addition. When the table has a primary key those two are the same row and are
//! reported together as "changed"; without one there is nothing in the storage that links them, so
//! they stay separate. That distinction is deliberate - guessing an identity would be worse than
//! reporting the two halves.


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



} // namespace duckdb
