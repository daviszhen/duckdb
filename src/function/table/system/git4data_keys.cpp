//===----------------------------------------------------------------------===//
// Git4Data -- keys, and the rows a removal set hides
//===----------------------------------------------------------------------===//
// How a key is built, hashed and compared when it is an integer and when it is not, the limits that decide which
// of the two paths a key takes, and the filter that keeps the rows no overlay removed. These are the pieces the
// scan and the diff share, and they lived in the read path's file until increment 3fe moved them here - the
// split 3fc attempted and 3fd unblocked, now that each half can be compiled on its own before the build.
//===----------------------------------------------------------------------===//

#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/function/table/git4data_internal.hpp"
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

namespace duckdb {

//! How wide a key range a bitmap may cover: 2^26 bits is eight megabytes, and past that the bits would cost
//! more than the entries they replace.
static idx_t Git4DataBitmapSpanLimit() {
	return idx_t(1) << 26;
}

//! How many removed keys may be collected as a vector before the container is built as they arrive: 4,000,000
//! patterns are 32 MB, where the hash set they would become costs about 200.
static idx_t Git4DataTypedKeysLimit() {
	return 4000000;
}

//! Build a typed removal set's container from the keys it holds. A dense range becomes a bitmap; anything else
//! - keys spread over a wide range, or a key that is not small - keeps the hash set, because the bits would
//! then be the larger structure.
static void Git4DataBuildTypedContainer(const vector<uint64_t> &patterns, Git4DataRemovedKeys &result) {
	if (patterns.empty()) {
		return;
	}
	auto lowest = patterns[0];
	auto highest = patterns[0];
	for (auto pattern : patterns) {
		lowest = MinValue(lowest, pattern);
		highest = MaxValue(highest, pattern);
	}
	auto span = highest - lowest;
	if (span < Git4DataBitmapSpanLimit() && span <= patterns.size() * 64) {
		result.bitmap_base = lowest;
		result.bitmap.assign(NumericCast<idx_t>((span + 64) / 64), 0);
		for (auto pattern : patterns) {
			auto offset = pattern - lowest;
			result.bitmap[offset / 64] |= uint64_t(1) << (offset % 64);
		}
		result.bitmap_usable = true;
		return;
	}
	for (auto pattern : patterns) {
		result.bits.insert(pattern);
	}
}

//! One key column as the filters read it: a row's key is taken out of the vector as its own type rather than
//! through a `Value` built for the row. The type is known only at run time, which is the case the unified
//! format is for - a dictionary or a constant column reads through it too, so nothing has to be flattened.

//! One overlay on a line: the table holding the rows the reference has that its base does not, and the
//! table holding the keys it took away from the base. Both are the caller's tables, read at the commit
//! the line that carries them was pinned at - which is why the commit travels with the overlay rather
//! than with the read: a branch that inherited a line reads the inherited overlay at its own pin and
//! its own overlay as of now, and the two are different commits.



//! A row's key as one string, for telling a row of the base from one a delta removed. Length-prefixed,
//! so two different key tuples cannot produce the same string.
static string Git4DataKeyString(DataChunk &chunk, const vector<idx_t> &positions, idx_t row) {
	string key;
	for (auto position : positions) {
		auto value = chunk.data[position].GetValue(row).ToString();
		key += to_string(value.size());
		key += ':';
		key += value;
		key += '|';
	}
	return key;
}

//! How many removed keys a read will build hashes for. Past this the strings decide alone, which keeps a
//! read's memory at what the strings cost: 77 bytes a key measured, against 77 plus the hash set's own
//! footprint. The figure is where the trade stops paying - the hashes saved 0.754 s at 500,000 keys and
//! cost 25.2 MB there, against 0.549 s saved and 1 MB at 400,000 rows with one key.
constexpr idx_t Git4DataHashKeysLimit() {
	return 100000;
}

//! The hash of a row's key tuple, computed from the chunk's vectors rather than from the key string.
//! It is never the answer - the string above is - but building a string for every row of a table to
//! find the few a delta removed is what made a delta read cost about 1.4 us a row (measured, 0.554 s
//! for 400,000 rows against 0.001 s for the same rows as a pin with no delta). A row whose hash is not
//! in the removed set cannot be in that set either, so the string is built only for the rows that could
//! be, and the exact comparison still decides.
static void Git4DataHashKeyColumns(DataChunk &chunk, const vector<idx_t> &positions, DataChunk &hashes) {
	for (idx_t i = 0; i < positions.size(); i++) {
		VectorOperations::Hash(chunk.data[positions[i]], hashes.data[i], chunk.size());
	}
}

static hash_t Git4DataRowKeyHash(DataChunk &hashes, idx_t row) {
	auto hash = hashes.data[0].GetValue(row).GetValue<hash_t>();
	for (idx_t i = 1; i < hashes.ColumnCount(); i++) {
		hash = CombineHash(hash, hashes.data[i].GetValue(row).GetValue<hash_t>());
	}
	return hash;
}

//! The transaction the scans of a delta's *own* tables run under: the caller's, because those two tables
//! are the branch's storage and what the caller has written to them - committed or not - is the state it
//! is asking about. The base table beside them is a different matter and keeps the synthetic bound-only
//! transaction below, since a reference read must not see the caller's uncommitted writes to the live
//! table. Measured before this: with the synthetic context on the delta tables too, a statement group
//! could not read its own writes - `INSERT INTO t VALUES (50,50); UPDATE t SET v = 51 WHERE k = 50` left
//! 50 - and a group that deleted a key and re-inserted it was refused as a collision.
TransactionData Git4DataDeltaTransactionData(ClientContext &context, optional_ptr<AttachedDatabase> db,
                                                    VisibilityBound bound) {
	if (!db) {
		// no delta table was resolved, so there is nothing the caller could have written to
		return TransactionData(MAX_TRANSACTION_ID, bound);
	}
	auto transaction = TransactionData(DuckTransaction::Get(context, *db));
	return TransactionData(transaction.GetTransactionId(), bound);
}

//! The keys a delta removed from its base, read once while a read is set up. A key is a tuple of
//! arbitrary columns and this is the only place the tuple is compared, so it is held as one string.

//! Whether a key type is one the typed set can hold: an integer of at most 64 bits
bool Git4DataIsIntegerKeyType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return true;
	default:
		return false;
	}
}

//! The 64-bit pattern of a single integer key, or false when the key is not one
bool Git4DataIntegerKeyBits(const Value &key, uint64_t &result) {
	switch (key.type().id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		result = static_cast<uint64_t>(key.GetValue<int64_t>());
		return true;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		result = key.GetValue<uint64_t>();
		return true;
	default:
		return false;
	}
}

//! Whether the removed side holds this row's key. The typed set decides when the key is a single integer, and
//! otherwise the hash spares the rows that cannot be in the string set and the string decides for the rest.
bool Git4DataRowWasRemoved(const Git4DataRemovedKeys &removed, DataChunk &chunk,
                                  const vector<idx_t> &key_positions, DataChunk &key_hashes, idx_t row) {
	// Nothing was removed, which is the common case: a reference with no delta reads its base as it stands.
	// **Without this the string below is built for every row of every side even when the set is empty** -
	// measured, two million key strings for a diff of a million-row copy against itself, which is what its
	// 2.370 s was almost entirely made of.
	if (removed.Empty()) {
		return false;
	}
	if (removed.typed) {
		auto key = chunk.GetValue(key_positions[0], row);
		if (key.IsNull()) {
			return false;
		}
		uint64_t bits = 0;
		if (!Git4DataIntegerKeyBits(key, bits)) {
			return false;
		}
		return removed.HoldsPattern(bits);
	}
	if (removed.hashes_usable && key_hashes.ColumnCount() > 0 &&
	    removed.hashes.find(Git4DataRowKeyHash(key_hashes, row)) == removed.hashes.end()) {
		return false;
	}
	return removed.strings.find(Git4DataKeyString(chunk, key_positions, row)) != removed.strings.end();
}

//! Whether a typed set holds this row's key, with the key column already prepared for the chunk. This is the
//! filter a delta read and a diff spend their extra time in: measured on the script's own shape, 4,000,000 base
//! rows against 100,000 removed keys cost 50 ms over a plain read of the same rows, about 13 ns a row, and the
//! `Value` the generic path builds per row is most of it.
bool Git4DataTypedRowWasRemoved(const Git4DataKeyColumn &keys, const Git4DataRemovedKeys &removed, idx_t row) {
	uint64_t pattern = 0;
	if (!keys.Pattern(row, pattern)) {
		return false;
	}
	return removed.HoldsPattern(pattern);
}

void Git4DataCollectRemovedKeys(ClientContext &context, const string &removed_table,
                                       const vector<LogicalType> &key_types, idx_t key_count, VisibilityBound bound,
                                       Git4DataRemovedKeys &result) {
	if (removed_table.empty()) {
		return;
	}
	auto &table = Git4DataResolveTable(context, removed_table);
	auto &groups = *table.GetStorage().GetRowGroupCollection();
	// the removed table holds exactly the source's key columns, so they are its first columns
	vector<StorageIndex> removed_ids;
	vector<idx_t> positions;
	for (idx_t i = 0; i < key_count; i++) {
		removed_ids.emplace_back(i);
		positions.push_back(i);
	}
	TableScanState scan;
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), key_types);
	vector<LogicalType> hash_types;
	for (idx_t i = 0; i < key_count; i++) {
		hash_types.push_back(LogicalType::HASH);
	}
	DataChunk hashes;
	hashes.Initialize(Allocator::Get(context), hash_types);
	scan.Initialize(removed_ids, nullptr);
	// A removed table with no rows has nothing to read and no row group to scan (`InitializeScan` asserts on
	// the root segment); the caller's own uncommitted rows in it are read below, from its local storage, so
	// this is the on-disk half only.
	const bool groups_empty = groups.GetTotalRows() == 0;
	QueryContext query_context;
	if (!groups_empty) {
		groups.InitializeScan(query_context, scan.table_state, removed_ids, nullptr);
	}
	ScanOptions options(Git4DataDeltaTransactionData(context, table.GetStorage().GetAttached(), bound));
	// The hashes are a fast path for the common delta - a handful of keys against a large base, where
	// they turn a per-row string build into one hash lookup and measured 0.554 s into 0.005 s on 400,000
	// rows. They are also a second set, and one that costs memory the strings already cost: at 500,000
	// removed keys a read peaks at 84.6 MB with them against 59.4 MB without, and the win at that size is
	// only 1.968 s into 1.214 s. So they are built only for a removed side that is small, and whether they
	// will be is decided here, from the size of that table before anything is read: a set that was
	// *dropped* part-way would answer every lookup with "not in it" and be read as a complete answer -
	// which is what the first version of this did, and what delta_large.test caught (301,001 rows where
	// the branch held 201,000).
	// The decision is made up front from the size of that table, and the flag says whether what ends up in
	// the set is the *whole* removed side: the set is cleared, and the flag with it, the moment it passes
	// the limit - a set dropped part-way would answer every lookup with "not in it" and be read as a
	// complete answer, which is what delta_large.test caught (301,001 rows where the branch held 201,000).
	// A removed side that is not on disk yet - rows the caller wrote and has not committed - reports no
	// rows here, so the running size is what turns the hashes off for those.
	result.hashes_usable = groups.GetTotalRows() <= Git4DataHashKeysLimit();
	// One integer key needs neither the strings nor the hashes: its value is the set's key
	result.typed = key_count == 1 && Git4DataIsIntegerKeyType(key_types[0]);
	// A typed set's keys are collected first and given a container at the end, because which container is right
	// depends on the range they cover and that is only known once they are all in. The collection itself is a
	// vector of patterns, which is 8 bytes a key against the hash set's ~54 - so the collected form is the cheap
	// one - and past a cap that vector would be the larger structure too, which is when the set is built as the
	// keys arrive instead.
	vector<uint64_t> typed_patterns;
	bool typed_streamed = false;
	if (result.typed) {
		typed_streamed = groups.GetTotalRows() > Git4DataTypedKeysLimit();
	}
	auto collect = [&]() {
		if (result.typed) {
			// The same reading the filter does, on the other side of the read: the keys are taken out of the
			// column as their own type, so a removed key costs an insert rather than a `Value` and an insert.
			// **This is where the rest of a delta read's extra time is**: measured with the filter already
			// reading its column, 500,000 removed keys cost 20 ms over a plain read whatever the base's size,
			// and 1,000,000 base rows against 100,000 of them cost 10 ms.
			Git4DataKeyColumn keys;
			keys.Init(chunk.data[0]);
			for (idx_t row = 0; row < chunk.size(); row++) {
				uint64_t pattern = 0;
				if (!keys.Pattern(row, pattern)) {
					continue;
				}
				if (typed_streamed) {
					result.bits.insert(pattern);
				} else {
					typed_patterns.push_back(pattern);
				}
			}
			return;
		}
		if (result.hashes_usable) {
			Git4DataHashKeyColumns(chunk, positions, hashes);
		}
		for (idx_t row = 0; row < chunk.size(); row++) {
			if (result.hashes_usable) {
				result.hashes.insert(Git4DataRowKeyHash(hashes, row));
				if (result.strings.size() > Git4DataHashKeysLimit()) {
					result.hashes.clear();
					result.hashes_usable = false;
				}
			}
			result.strings.insert(Git4DataKeyString(chunk, positions, row));
		}
	};
	while (!groups_empty) {
		chunk.Reset();
		scan.table_state.Scan(options, chunk);
		if (chunk.size() == 0) {
			break;
		}
		collect();
	}
	// The rows the caller has written to this table but not committed are in its local storage rather
	// than in the row groups above, and they are part of the state being read: measured, a statement
	// group could not read its own writes - `INSERT INTO t VALUES (50,50); UPDATE t SET v = 51 WHERE
	// k = 50` left 50 - and a group that deleted a key and re-inserted it was refused as a collision.
	auto &delta_transaction = DuckTransaction::Get(context, table.GetStorage().GetAttached());
	auto &local_storage = LocalStorage::Get(delta_transaction);
	local_storage.InitializeScan(table.GetStorage(), scan.local_state, nullptr);
	while (true) {
		chunk.Reset();
		local_storage.Scan(scan.local_state, removed_ids, chunk);
		if (chunk.size() == 0) {
			break;
		}
		collect();
	}
	// Every key is in now, including the caller's uncommitted ones, so the container can be chosen: a bitmap
	// for a dense range, the hash set otherwise.
	if (result.typed && !typed_streamed) {
		Git4DataBuildTypedContainer(typed_patterns, result);
		vector<uint64_t>().swap(typed_patterns);
	}
}

//! One overlay as a read has it set up: the table holding its added rows, the commit it is read at, the
//! keys it removed (read in full while the read is set up - a delta removes what it removes, which is the
//! smaller side by construction), and the scan over its added rows. A scan is consumed as it walks, so
//! every side of a diff needs its own - which is why the scan lives here rather than in the read.

//! The hash of a row's key for every removal set that uses hashes, so the per-row loop below can look a
//! row up in each of them. It is one column per key column per set, computed for the chunk rather than
//! for a row, which is what keeps a filter over two sets from costing two string builds a row. The sets are
//! *pointed at* rather than held: a set of half a million keys is tens of megabytes, and a diff that copied
//! its sides' sets to filter its base held two of each - measured, a diff that changes every row of a
//! million peaked at 298 MiB that way and 255 MiB without the copies.
void Git4DataHashForRemovals(const vector<Git4DataRemovedKeys *> &removals,
                                    vector<unique_ptr<DataChunk>> &hashes, idx_t from, DataChunk &chunk,
                                    const vector<idx_t> &key_positions) {
	for (idx_t i = from; i < removals.size(); i++) {
		if (!removals[i]->Empty() && !removals[i]->typed && removals[i]->hashes_usable) {
			Git4DataHashKeyColumns(chunk, key_positions, *hashes[i]);
		}
	}
}

//! The rows of `chunk` that none of the removal sets from `from` on holds, as a selection. A read keeps a
//! row when every overlay that comes after the one it came from leaves it alone: the base is filtered by
//! all of them, and an overlay's own added rows only by the overlays that come after it - a key an overlay
//! removed is gone from everything below it, and a row added on top of that is there again.
idx_t Git4DataKeepRows(ClientContext &context, const vector<Git4DataRemovedKeys *> &removals,
                              vector<unique_ptr<DataChunk>> &hashes, idx_t from, DataChunk &chunk,
                              const vector<idx_t> &key_positions, SelectionVector &kept) {
	auto count = chunk.size();
	bool any = false;
	for (idx_t i = from; i < removals.size(); i++) {
		if (!removals[i]->Empty()) {
			any = true;
			break;
		}
	}
	if (!any) {
		return count;
	}
	Git4DataHashForRemovals(removals, hashes, from, chunk, key_positions);
	// A single integer key is the shape most tables have, and the filter over it is where a delta read's extra
	// time is spent, so the key column is prepared once for the chunk and read as its own type rather than
	// through a `Value` built per row. The sets in the window all have to be typed for that to be the *same*
	// question the generic path asks.
	Git4DataKeyColumn typed_keys;
	bool typed_only = key_positions.size() == 1;
	for (idx_t i = from; i < removals.size() && typed_only; i++) {
		typed_only = removals[i]->typed;
	}
	if (typed_only) {
		typed_keys.Init(chunk.data[key_positions[0]]);
	}
	idx_t kept_count = 0;
	for (idx_t row = 0; row < count; row++) {
		bool removed = false;
		for (idx_t i = from; i < removals.size(); i++) {
			auto was_removed = typed_only ? Git4DataTypedRowWasRemoved(typed_keys, *removals[i], row)
			                              : Git4DataRowWasRemoved(*removals[i], chunk, key_positions, *hashes[i], row);
			if (was_removed) {
				removed = true;
				break;
			}
		}
		if (!removed) {
			kept.set_index(kept_count++, row);
		}
	}
	return kept_count;
}

} // namespace duckdb
