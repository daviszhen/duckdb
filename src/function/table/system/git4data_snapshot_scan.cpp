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

namespace duckdb {

//! Reads a table as of a pinned reference: the rows a reader at that commit id would have seen.
//! The retention bound keeps those rows and their delete ids alive, so the scan is an ordinary
//! visibility-filtered scan with the pinned bound substituted for "now".
struct Git4DataRemovedKeys {
	//! The keys, as strings. Empty when the typed set below is the whole answer.
	unordered_set<string> strings;
	//! The hashes of those keys, so a row that cannot be one of them is not turned into a string, and
	//! whether that set is the whole removed side - past a limit it is not built at all
	unordered_set<hash_t> hashes;
	bool hashes_usable = false;
	//! The whole removed side as the 64-bit pattern of a single integer key: the shape a table keyed by
	//! one number has, which is the common one. **Measured: a 500,000-key delta's read cost 1.890 s
	//! against 0.010 s for the same rows copied**, and building one string per base row was almost all
	//! of it - the strings' memory was on top, 84.6 MB against 59.4 MB. The same cast is applied on both
	//! sides, so every <= 64-bit integer type keeps its values apart without the set knowing which type
	//! it holds, and no allocation happens per row.
	bool typed = false;
	unordered_set<uint64_t> bits;
	//! The same keys as a bitmap over their range, which is what a dense typed set gets: one bit a key
	//! instead of a hash entry, so both the lookup and the memory are smaller - measured, a branch read of a
	//! million rows against a delta that removed half of them peaked at 54 MiB with the set and 27 with the
	//! bits. Only chosen when the keys are dense enough for the bits to cost less than the entries would.
	vector<uint64_t> bitmap;
	uint64_t bitmap_base = 0;
	bool bitmap_usable = false;

	bool Empty() const {
		return strings.empty() && bits.empty() && !bitmap_usable;
	}

	//! Whether the typed set holds this 64-bit key pattern. The bitmap answers for the range it covers, and a
	//! key outside it is one the removed side cannot have - the range was taken from the keys it holds.
	bool HoldsPattern(uint64_t pattern) const {
		if (bitmap_usable) {
			if (pattern < bitmap_base) {
				return false;
			}
			auto offset = pattern - bitmap_base;
			if (offset >= bitmap.size() * 64) {
				return false;
			}
			return ((bitmap[offset / 64] >> (offset % 64)) & 1) != 0;
		}
		return bits.find(pattern) != bits.end();
	}
};

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
struct Git4DataKeyColumn {
	UnifiedVectorFormat format;
	LogicalTypeId type = LogicalTypeId::INVALID;

	void Init(const Vector &keys) {
		keys.ToUnifiedFormat(format);
		type = keys.GetType().id();
	}

	//! The key's 64-bit pattern, cast exactly as `Git4DataIntegerKeyBits` casts it, or false for a NULL key or
	//! a type that is not a single integer
	bool Pattern(idx_t row, uint64_t &result) const {
		auto index = format.sel->get_index(row);
		if (!format.validity.RowIsValid(index)) {
			return false;
		}
		switch (type) {
		case LogicalTypeId::TINYINT:
			result = static_cast<uint64_t>(reinterpret_cast<const int8_t *>(format.data)[index]);
			return true;
		case LogicalTypeId::SMALLINT:
			result = static_cast<uint64_t>(reinterpret_cast<const int16_t *>(format.data)[index]);
			return true;
		case LogicalTypeId::INTEGER:
			result = static_cast<uint64_t>(reinterpret_cast<const int32_t *>(format.data)[index]);
			return true;
		case LogicalTypeId::BIGINT:
			result = static_cast<uint64_t>(reinterpret_cast<const int64_t *>(format.data)[index]);
			return true;
		case LogicalTypeId::UTINYINT:
			result = reinterpret_cast<const uint8_t *>(format.data)[index];
			return true;
		case LogicalTypeId::USMALLINT:
			result = reinterpret_cast<const uint16_t *>(format.data)[index];
			return true;
		case LogicalTypeId::UINTEGER:
			result = reinterpret_cast<const uint32_t *>(format.data)[index];
			return true;
		case LogicalTypeId::UBIGINT:
			result = reinterpret_cast<const uint64_t *>(format.data)[index];
			return true;
		default:
			return false;
		}
	}
};

//! One overlay on a line: the table holding the rows the reference has that its base does not, and the
//! table holding the keys it took away from the base. Both are the caller's tables, read at the commit
//! the line that carries them was pinned at - which is why the commit travels with the overlay rather
//! than with the read: a branch that inherited a line reads the inherited overlay at its own pin and
//! its own overlay as of now, and the two are different commits.
struct Git4DataDeltaLayer {
	string added;
	string removed;
	VisibilityBound bound;
};

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

static void Git4DataDiffPrimaryKey(TableCatalogEntry &table, vector<StorageIndex> &columns, vector<LogicalType> &types,
                                   vector<Identifier> &names);

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
static TransactionData Git4DataDeltaTransactionData(ClientContext &context, optional_ptr<AttachedDatabase> db,
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
static bool Git4DataIsIntegerKeyType(const LogicalType &type) {
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
static bool Git4DataIntegerKeyBits(const Value &key, uint64_t &result) {
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
static bool Git4DataRowWasRemoved(const Git4DataRemovedKeys &removed, DataChunk &chunk,
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
static bool Git4DataTypedRowWasRemoved(const Git4DataKeyColumn &keys, const Git4DataRemovedKeys &removed, idx_t row) {
	uint64_t pattern = 0;
	if (!keys.Pattern(row, pattern)) {
		return false;
	}
	return removed.HoldsPattern(pattern);
}

static void Git4DataCollectRemovedKeys(ClientContext &context, const string &removed_table,
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
struct Git4DataDeltaRead {
	Git4DataDeltaLayer layer;
	Git4DataRemovedKeys removed;
	unique_ptr<TableScanState> scan;
	//! A chunk cannot be moved, so it is held behind a pointer like the scan
	unique_ptr<DataChunk> chunk;
	//! The added table itself, for the scan of the caller's own uncommitted rows, and the database it
	//! lives in
	optional_ptr<DataTable> storage;
	optional_ptr<AttachedDatabase> database;
	bool rows_done = false;
	bool local_open = false;
	//! Whether the added table has no row groups at all: there is then nothing to scan (and nothing was
	//! initialized to scan), and only the caller's own uncommitted rows in it are left to read
	bool groups_empty = false;

	//! Read this overlay's next chunk of added rows: the committed ones out of its row groups first, then
	//! the rows the caller has written to that table and not committed, which are in its local storage.
	//! False once the overlay has no rows left.
	bool Next(ClientContext &context, const vector<StorageIndex> &column_ids, DataChunk &target) {
		// An overlay that names no added table holds no rows to read: what it says is which keys it took
		// away, and that is a removal set the read filters with.
		if (!scan) {
			return false;
		}
		while (true) {
			target.Reset();
			if (local_open) {
				auto &transaction = DuckTransaction::Get(context, *database);
				LocalStorage::Get(transaction).Scan(scan->local_state, column_ids, target);
				return target.size() != 0;
			}
			if (rows_done) {
				return false;
			}
			ScanOptions options(Git4DataDeltaTransactionData(context, database, layer.bound));
			if (!groups_empty) {
				scan->table_state.Scan(options, target);
			}
			if (target.size() != 0) {
				return true;
			}
			rows_done = true;
			if (!storage || !database) {
				return false;
			}
			auto &transaction = DuckTransaction::Get(context, *database);
			LocalStorage::Get(transaction).InitializeScan(*storage, scan->local_state, nullptr);
			local_open = true;
		}
	}
};

//! The hash of a row's key for every removal set that uses hashes, so the per-row loop below can look a
//! row up in each of them. It is one column per key column per set, computed for the chunk rather than
//! for a row, which is what keeps a filter over two sets from costing two string builds a row. The sets are
//! *pointed at* rather than held: a set of half a million keys is tens of megabytes, and a diff that copied
//! its sides' sets to filter its base held two of each - measured, a diff that changes every row of a
//! million peaked at 298 MiB that way and 255 MiB without the copies.
static void Git4DataHashForRemovals(const vector<Git4DataRemovedKeys *> &removals,
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
static idx_t Git4DataKeepRows(ClientContext &context, const vector<Git4DataRemovedKeys *> &removals,
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

//! The table carrying this oid in the default schema, or nothing. A mapping is written in *names*, and a
//! name can be dropped, recreated or renamed under it; the oid is what survives a rename, so it is what
//! lets a read find the table a mapping was made against when its name no longer resolves.

static optional_ptr<TableCatalogEntry> Git4DataTableByOid(ClientContext &context, idx_t oid) {
	if (oid == 0) {
		return nullptr;
	}
	auto &schema = Catalog::GetCatalog(context, INVALID_CATALOG).GetSchema(context, DEFAULT_SCHEMA);
	optional_ptr<TableCatalogEntry> result;
	schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		if (result || entry.oid != oid) {
			return;
		}
		result = &entry.Cast<TableCatalogEntry>();
	});
	return result;
}

string Git4DataResolveTableName(ClientContext &context, const string &name, idx_t oid) {
	auto named = Git4DataResolveTableOrNull(context, name);
	if (named) {
		return name;
	}
	auto by_oid = Git4DataTableByOid(context, oid);
	if (!by_oid) {
		return string();
	}
	return Git4DataTableName(*by_oid);
}

//! A reference is keyed by the table a statement named, and the key is resolved again behind the read. The
//! binder is the last place where that table is still known, so a key that does not resolve back to it is
//! refused here rather than answered from whatever it does reach: the key carries a schema and a name and no
//! catalog, so a read of a table in another attached database is keyed by what names the default database's
//! table - measured, the read would be answered with that table's line.
void Git4DataSnapshotScanFun::CheckReferenceSource(ClientContext &context, TableCatalogEntry &table,
                                                   const string &key) {
	auto by_key = Git4DataResolveTableOrNull(context, key);
	if (by_key && by_key->oid == table.oid) {
		return;
	}
	throw InvalidInputException(
	    "Table \"%s\" is reached as \"%s\" in catalog %s, and a reference is keyed by the table a read names: "
	    "the key \"%s\" reaches %s, so the read would be answered from that table's line. A reference is kept "
	    "in the database it belongs to, and a table of another database cannot be its source.",
	    table.name.GetIdentifierName(), table.ParentSchema().name.GetIdentifierName(), table.ParentCatalog().GetName(),
	    key,
	    by_key ? "\"" + by_key->name.GetIdentifierName() + "\" in schema \"" +
	                 by_key->ParentSchema().name.GetIdentifierName() + "\""
		       : "no table here");
}

//! The table backing a branch for a source table, or empty when the branch has no line of its own
//! there. A mapping whose table has since been dropped falls back to the pinned image rather than
//! failing, because the branch itself is still a valid reference.
//!
//! A table with the *name* of one that was dropped is another matter: it is a different table, and
//! reading it would give the branch a line that has nothing to do with what the branch was given. The
//! oid recorded when the mapping was made is what tells them apart - within a session, since the
//! catalog's oids do not survive a reopen.
static string Git4DataBranchStorageTable(ClientContext &context, const Git4DataBranch &branch,
                                         const string &table_name) {
	// The mapping is keyed by the source table, and a source table can be renamed. The name it was given
	// under then matches nothing, and the branch would silently read its pinned image instead of the line it
	// holds - measured. The source's oid survives a rename, so a name that matches nothing is looked up by
	// oid as well, and the mapping follows the table it was made against. A mapping written before the key
	// carried a schema is matched by the table its stored name resolves to, which is what its name meant.
	auto requested = Git4DataResolveTableOrNull(context, table_name);
	for (auto &entry : branch.storage_tables) {
		if (!Git4DataSourceKeyMatches(context, entry.source, table_name)) {
			if (!requested || entry.source_oid == 0 || entry.source_oid != requested->oid) {
				continue;
			}
		} else {
			// the name matched, which is not the same as the mapping having been made against *this* table: a
			// table dropped and another created with its name reaches here, and a read of it would be answered
			// from the line this branch was given for the table that is gone
			Git4DataCheckSourceBoundTable(context, branch, entry.source, entry.source_oid,
			                              requested ? requested->oid : 0);
		}
		auto storage = Git4DataResolveTableOrNull(context, entry.storage);
		if (!storage) {
			// the name it was given no longer resolves, which is what a rename looks like: the oid is
			// what tells the table it was given from any other
			storage = Git4DataTableByOid(context, entry.oid);
		}
		if (!storage) {
			continue;
		}
		// the name it has now, not the one the mapping was written with
		auto resolved = Git4DataTableName(*storage);
		if (entry.oid != 0 && entry.oid != storage->oid) {
			throw InvalidInputException(
			    "Branch \"%s\" was given table \"%s\" for \"%s\", and the table carrying that name now is not it - "
			    "the table it was given is no longer reachable under that name. Point the branch at a table of its "
			    "own again with git4data_bind_branch_table.",
			    branch.name, entry.storage, table_name);
		}
		return resolved;
	}
	return string();
}

//! How a reference is read: the table to scan, the bound to scan it at, and the overlays that stand
//! between the two. Usually none (a pin of a table, or a copy of it), one for a branch that differs by a
//! delta - and **two** for a branch that inherited a line which differs by a delta and has one of its
//! own, which apply in order: the inherited line first, then what the branch has done since.
struct Git4DataReadPlan {
	string table_name;
	VisibilityBound bound;
	vector<Git4DataDeltaLayer> deltas;
	//! Whether the table being read is a branch's *own* copy rather than the source. A copy is the
	//! caller's table like any other, so a read of it sees what that transaction has written to it -
	//! including rows it has not committed, which live in its local storage rather than in row groups.
	bool own_storage = false;

	bool HasDelta() const {
		return !deltas.empty();
	}
};

//! Where a branch's line lives at a commit, and what it differs by. A branch is a pin plus what it
//! inherits, so its state at a commit is one table read at a bound - the base, which does not move -
//! plus the overlays it carries, each read at the commit its own line pinned. AllCommitted as the bound
//! means the state now.
struct Git4DataLineState {
	string base_table;
	//! Whether `base_table` is a branch's own copy rather than the source table
	bool own_storage = false;
	VisibilityBound base_bound;
	vector<Git4DataDeltaLayer> deltas;
};

//! A delta's two tables have to be able to stand in for the source: the added rows are read as rows of
//! it, and the removed table names its rows by key. Checked when the delta is named and again on every
//! read, because DDL afterwards can leave either of them unable to - which the read would otherwise
//! discover as an index out of range rather than as a sentence.
static void Git4DataCheckDeltaColumns(TableCatalogEntry &source, TableCatalogEntry &added, TableCatalogEntry &removed) {
	auto &source_columns = source.GetColumns();
	if (added.GetColumns().GetColumnNames() != source_columns.GetColumnNames() ||
	    added.GetColumns().GetColumnTypes() != source_columns.GetColumnTypes()) {
		throw InvalidInputException("Table \"%s\" holds the rows added to \"%s\", and it no longer has that table's "
		                            "columns",
		                            added.name.GetIdentifierName(), source.name.GetIdentifierName());
	}
	vector<StorageIndex> key_columns;
	vector<LogicalType> key_types;
	vector<Identifier> key_names;
	Git4DataDiffPrimaryKey(source, key_columns, key_types, key_names);
	vector<string> expected_names;
	vector<LogicalType> expected_types;
	for (idx_t i = 0; i < source_columns.LogicalColumnCount(); i++) {
		auto &column = source_columns.GetColumn(LogicalIndex(i));
		auto name = column.Name().GetIdentifierName();
		bool is_key = false;
		for (auto &key_name : key_names) {
			if (key_name.GetIdentifierName() == name) {
				is_key = true;
				break;
			}
		}
		if (!is_key) {
			continue;
		}
		expected_names.push_back(name);
		expected_types.push_back(column.Type());
	}
	if (removed.GetColumns().GetColumnNames() != expected_names ||
	    removed.GetColumns().GetColumnTypes() != expected_types) {
		throw InvalidInputException("Table \"%s\" holds the keys removed from \"%s\", and it no longer has that "
		                            "table's key columns",
		                            removed.name.GetIdentifierName(), source.name.GetIdentifierName());
	}
}

//! Whether one table's columns are the first columns of another's: the same names and types, in the same order,
//! and fewer of them. That is the shape a copy has when the table it was made from gained a column afterwards,
//! and it is a different thing from a table of a different shape - there the columns a read maps by position
//! really would be someone else's.
static bool Git4DataDiffColumnsArePrefix(const vector<string> &names, const vector<LogicalType> &types,
                                         const vector<string> &other_names, const vector<LogicalType> &other_types) {
	if (names.size() >= other_names.size() || names.size() != types.size()) {
		return false;
	}
	for (idx_t i = 0; i < names.size(); i++) {
		if (names[i] != other_names[i] || types[i] != other_types[i]) {
			return false;
		}
	}
	return true;
}

static Git4DataLineState ResolveGit4DataLineState(ClientContext &context, DuckTransactionManager &transaction_manager,
                                                  const string &table_name, const Git4DataBranch &branch,
                                                  VisibilityBound at) {
	Git4DataLineState state;
	state.base_bound = at;
	auto storage = Git4DataBranchStorageTable(context, branch, table_name);
	if (!storage.empty()) {
		// the branch's own line is a table: read as it is now, or as it stood at the commit asked for.
		// Its columns are mapped onto the source's by position, so the shape is checked on every read as
		// well as when it was bound: DDL afterwards can leave the table unable to stand in, and the read
		// would otherwise swap or rename what a caller sees - measured, a table holding (v, k) read back
		// as k = 777 and v = 1 from a row that held k = 1 and v = 777.
		{
			auto &source_entry = Git4DataResolveTable(context, table_name);
			auto &storage_entry = Git4DataResolveTable(context, storage);
			auto storage_names = storage_entry.GetColumns().GetColumnNames();
			auto source_names = source_entry.GetColumns().GetColumnNames();
			auto storage_types = storage_entry.GetColumns().GetColumnTypes();
			auto source_types = source_entry.GetColumns().GetColumnTypes();
			if (storage_names != source_names || storage_types != source_types) {
				if (Git4DataDiffColumnsArePrefix(storage_names, storage_types, source_names, source_types)) {
					// The table has the source's columns and not the ones added since: nothing is swapped,
					// the read is simply short of them. What it needs is the columns themselves, which the
					// caller can add to it and keep the branch's own line - the table is theirs.
					throw InvalidInputException(
					    "Branch \"%s\" reads table \"%s\" as its own line for \"%s\", which has columns that "
					    "table does not, so a read of the branch would be short of them. Add them to it (ALTER "
					    "TABLE \"%s\" ADD COLUMN ...), or copy the source's current shape with another "
					    "git4data_materialize.",
					    branch.name, storage, table_name, storage);
				}
				throw InvalidInputException(
				    "Branch \"%s\" reads table \"%s\" as its own line for \"%s\", and that table no longer has "
				    "that table's columns in that order - a read maps them by position, so the values would be "
				    "someone else's. Point the branch at a table of its own again with git4data_bind_branch_table.",
				    branch.name, storage, table_name);
			}
		}
		state.base_table = storage;
		state.own_storage = true;
	} else if (!branch.base_line.empty()) {
		// it inherited a line, and that inheritance is fixed at this branch's pin - whatever commit is
		// being asked about, the state it started from is the one the line had when it forked
		Git4DataBranch line_branch;
		if (transaction_manager.branches.Get(branch.base_line, line_branch)) {
			auto inherited = ResolveGit4DataLineState(context, transaction_manager, table_name, line_branch,
			                                          VisibilityBound::Through(branch.pin));
			state.base_table = inherited.base_table;
			state.own_storage = inherited.own_storage;
			state.base_bound = inherited.base_bound;
			// the line it inherited carries its own overlays, at the commits *it* pinned them - the
			// branch's own delta is appended after them below, so they are applied first
			state.deltas = inherited.deltas;
		}
	}
	if (state.base_table.empty()) {
		// no line of its own and nothing inherited: the base is the source table at this branch's pin
		state.base_table = table_name;
		state.base_bound = VisibilityBound::Through(branch.pin);
	}
	// the branch's own delta, read at the commit whose state was asked for
	Git4DataDeltaLayer own;
	own.bound = at;
	for (auto &delta : branch.deltas) {
		// the source's name can change under the mapping, and its oid is what survives that
		auto &requested = Git4DataResolveTable(context, table_name);
		if (!Git4DataSourceKeyMatches(context, delta.source, table_name)) {
			if (delta.source_oid == 0 || delta.source_oid != requested.oid) {
				continue;
			}
		} else {
			// the same refusal as the storage mapping's: a name that now reaches another table is not the one
			// this mapping was made against
			Git4DataCheckSourceBoundTable(context, branch, delta.source, delta.source_oid, requested.oid);
		}
		// the delta's tables are the caller's: one that has since been dropped falls back to the base, and
		// one that was *renamed* is found by oid, because the caller meant that table
		auto added_table = Git4DataResolveTableOrNull(context, delta.added);
		if (!added_table) {
			added_table = Git4DataTableByOid(context, delta.added_oid);
		}
		auto removed_table = Git4DataResolveTableOrNull(context, delta.removed);
		if (!removed_table) {
			removed_table = Git4DataTableByOid(context, delta.removed_oid);
		}
		if (added_table && removed_table) {
			// a table that took the name of one of these after it was dropped is a different table, and
			// reading it would apply a delta the caller never made
			if ((delta.added_oid != 0 && delta.added_oid != added_table->oid) ||
			    (delta.removed_oid != 0 && delta.removed_oid != removed_table->oid)) {
				throw InvalidInputException(
				    "Branch \"%s\" was given tables \"%s\" and \"%s\" for \"%s\", and the tables carrying those names "
				    "now are not them - the tables it was given are no longer reachable under those names. Bind the "
				    "delta "
				    "again with git4data_bind_branch_delta.",
				    branch.name, delta.added, delta.removed, table_name);
			}
			auto &source = Git4DataResolveTable(context, table_name);
			Git4DataCheckDeltaColumns(source, *added_table, *removed_table);
			own.added = Git4DataTableName(*added_table);
			own.removed = Git4DataTableName(*removed_table);
		}
	}
	if (!own.added.empty() || !own.removed.empty()) {
		// The overlays apply in order, the inherited ones first: what the line it took looks like, then
		// what the branch has done since. Each is read at the commit its own line pinned, which is why
		// they cannot be collapsed into one - the inherited one does not move with the branch.
		state.deltas.push_back(own);
	}
	return state;
}

//! The plan for one reference. The scan and the diff both come here, so the two cannot disagree about
//! which table a reference means - a disagreement that compares row ids of different tables and
//! reports no change at all.
static void ResolveGit4DataRead(ClientContext &context, DuckTransactionManager &transaction_manager,
                                const string &table_name, const string &name, Git4DataRefKind kind,
                                Git4DataReadPlan &plan) {
	if (kind == Git4DataRefKind::SNAPSHOT) {
		Git4DataSnapshot snapshot;
		// A snapshot another transaction added and has not committed is not there yet, the same way its row in
		// the registry table is not: the registries are shared memory and the table is the caller's view
		if (!Git4DataReferenceVisible(context, "snapshot", name) ||
		    !transaction_manager.snapshots.Get(name, snapshot)) {
			throw InvalidInputException("Snapshot \"%s\" does not exist", name);
		}
		plan.table_name = table_name;
		plan.bound = VisibilityBound::Through(snapshot.pin);
		if (!snapshot.line.empty()) {
			// It captured a branch's own line, so what it names is that line as it stood at the pin -
			// which is a base that does not move plus that branch's overlays read at the pin. A line whose
			// branch is gone falls back to the pinned image, the way a branch whose storage was dropped
			// does.
			Git4DataBranch line_branch;
			if (transaction_manager.branches.Get(snapshot.line, line_branch)) {
				auto state = ResolveGit4DataLineState(context, transaction_manager, table_name, line_branch,
				                                      VisibilityBound::Through(snapshot.pin));
				plan.table_name = state.base_table;
				plan.own_storage = state.own_storage;
				plan.bound = state.base_bound;
				plan.deltas = state.deltas;
			}
		}
		return;
	}
	Git4DataBranch branch;
	if (!Git4DataReferenceVisible(context, "branch", name) ||
	    !transaction_manager.branches.Get(name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", name);
	}
	// what the branch holds now: its base, and its own delta if it has one
	auto state =
	    ResolveGit4DataLineState(context, transaction_manager, table_name, branch, VisibilityBound::AllCommitted());
	plan.table_name = state.base_table;
	plan.own_storage = state.own_storage;
	plan.bound = state.base_bound;
	plan.deltas = state.deltas;
}

static Git4DataReadPlan ResolveGit4DataReferencePlan(ClientContext &context,
                                                     DuckTransactionManager &transaction_manager,
                                                     const string &table_name, const string &name) {
	if (name == DuckTransactionManager::LIVE_REFERENCE) {
		// The table as it stands now, read through the caller's own transaction - including rows it has not
		// committed, which is what "the live table" means everywhere else in this feature. A caller asking
		// what a branch has that the table does not would otherwise have to take a reference first, and a
		// reference pins the timeline: the answer is about *now*, so keeping history alive for it would be a
		// cost paid for a question that needs none. Its bound is everything committed, and the side is read
		// as the caller's own table so that its uncommitted rows are part of it.
		Git4DataReadPlan live;
		live.table_name = table_name;
		live.own_storage = true;
		live.bound = VisibilityBound::AllCommitted();
		return live;
	}
	Git4DataRefKind kind;
	if (transaction_manager.snapshots.GetPin(name).IsValid()) {
		kind = Git4DataRefKind::SNAPSHOT;
	} else {
		// the name has to be in one of the two registries, and a caller that passed neither wants to be
		// told that rather than which of the two was looked in
		Git4DataBranch branch;
		if (!transaction_manager.branches.Get(name, branch)) {
			throw InvalidInputException("Reference \"%s\" is neither a snapshot nor a branch (the table as it "
			                            "stands now is named \"%s\")",
			                            name, DuckTransactionManager::LIVE_REFERENCE);
		}
		kind = Git4DataRefKind::BRANCH;
	}
	Git4DataReadPlan plan;
	ResolveGit4DataRead(context, transaction_manager, table_name, name, kind, plan);
	return plan;
}
//! Refuse a table that has a generated column, which a reference read cannot produce.
//!
//! **Measured, and it was a wrong answer rather than a refusal.** The engine does not store a generated column
//! (`pragma_storage_info` lists no segment for a `g INTEGER GENERATED ALWAYS AS (v * 2)` column), so the value a
//! caller sees is computed by the binder on top of an ordinary scan - and a reference reads a table's *stored*
//! columns through its own row-group scan, which is not that. It used to take the column's physical index anyway
//! and return whatever sat in that slot: the `g` above read back as the row's own index (0 and 1) while reading
//! the table gave 20 and 40.
//!
//! Every surface reads through here or through the diff's bind, and **the diff alone would work**: it skips
//! generated columns by design (`Git4DataDiffInit`), because it compares keys and compares values only to tell a
//! changed row from one that is the same - so it never needs a column it cannot read. Refusing it anyway is the
//! choice this file makes, because a table with a generated column that can be diffed but not read is a rule a
//! caller has to hold in their head. What would make the whole feature work is **materializing the column**: a
//! copy spells out its columns, and one written as an ordinary column rather than as the expression would hold
//! real values, so a *materialized* branch could be read. That is a design decision (the copy's column is then
//! writable, where the source's is not) and it is recorded rather than taken.
static void Git4DataCheckNoGeneratedColumns(ClientContext &context, const string &table_name) {
	auto &source = Git4DataResolveTable(context, table_name);
	for (auto &column : source.GetColumns().Logical()) {
		if (!column.Generated()) {
			continue;
		}
		throw InvalidInputException(
		    "Table \"%s\" has a generated column (%s), and a reference reads a table's stored columns through "
		    "its own scan - a generated column is not stored, so the read would have to compute it, which is what "
		    "the binder does and this scan does not. Make the column an ordinary one, or read the table without a "
		    "reference.",
		    table_name, column.Name());
	}
}

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
		auto kept_count = Git4DataKeepRows(context, state.removal_views, state.removal_hashes,
		                                   state.delta_index + 1, *delta.chunk, state.key_positions, kept);
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
struct Git4DataDiffBindData : public TableFunctionData {
	string table_name;
	VisibilityBound left_bound;
	VisibilityBound right_bound;
	//! The tables the two sides are read from. They are the same table unless a side is a branch
	//! that has been given a line of its own, which is then read as itself - and a diff that did not
	//! follow that would compare the pinned image while a pick read the branch's table.
	string left_table;
	string right_table;
	//! Whether each side is read from a branch's *own* copy rather than the source table. A copy is the
	//! caller's table, so a diff of it has to see what that transaction has written to it.
	bool left_own_storage = false;
	bool right_own_storage = false;
	//! The primary key's physical column indexes, empty when the table has no primary key
	vector<StorageIndex> key_columns;
	//! The types of those columns, in the same order
	vector<LogicalType> key_types;
	//! The names of those columns, in the same order
	vector<Identifier> key_names;
	//! Whether the two sides are two different tables, which is what a branch with a table of its own
	//! makes them. Different tables do not share row ids - the branch's storage numbers its own rows
	//! from zero again - so the comparison has to be made on the primary key instead.
	bool compare_by_key = false;
	//! A side that differs from its base through overlays is read through them here too: the rows they
	//! added are part of that side, and the keys they removed are not
	vector<Git4DataDeltaLayer> left_deltas;
	vector<Git4DataDeltaLayer> right_deltas;
};

//! Marks a Change that stands alone rather than being paired with a counterpart
static constexpr row_t NO_COUNTERPART = -1;

//! One side's rows for a diff that compares row ids, and the key pattern of a left row that has one.
//!
//! A row-id diff reads two references of **one table**, so the row ids are that table's own and run from zero
//! - which is what lets a row be a *bit* here rather than a hash entry. A transaction's own rows are the
//! exception: their ids start at `MAX_ROW_ID`, far above any table's, so they go into a small map instead of
//! stretching the bits to the top of the range. Measured at a million rows, the two `unordered_set<row_t>`
//! and the `unordered_map<row_t, uint64_t>` this replaces held about 35 MiB each; this holds a quarter of a
//! megabyte of bits and eight bytes a row for the patterns. It is also what makes a diff's removals come out
//! in ascending row-id order rather than in whatever order hash buckets iterate in.
class Git4DataRowSet {
public:
	void Insert(row_t row_id) {
		if (row_id < 0 || row_id >= MAX_ROW_ID) {
			local_present.insert(row_id);
			return;
		}
		Reserve(NumericCast<idx_t>(row_id) + 1);
		present[NumericCast<idx_t>(row_id)] = true;
	}

	bool Contains(row_t row_id) const {
		if (row_id < 0 || row_id >= MAX_ROW_ID) {
			return local_present.find(row_id) != local_present.end();
		}
		auto index = NumericCast<idx_t>(row_id);
		return index < present.size() && present[index];
	}

	//! Where this row's key is, for the two shapes a key can have: with one integer key column the value *is*
	//! the key's 64-bit pattern, and with a composite key it is the row's position in the left key blocks. A
	//! row has one or the other and never both, which is what the two setters say.
	void SetKeyBits(row_t row_id, uint64_t bits) {
		SetKeyRef(row_id, bits);
	}

	bool GetKeyBits(row_t row_id, uint64_t &bits) const {
		return GetKeyRef(row_id, bits);
	}

	void SetKeyIndex(row_t row_id, idx_t index) {
		SetKeyRef(row_id, NumericCast<uint64_t>(index));
	}

	bool GetKeyIndex(row_t row_id, idx_t &index) const {
		uint64_t value = 0;
		if (!GetKeyRef(row_id, value)) {
			return false;
		}
		index = NumericCast<idx_t>(value);
		return true;
	}

	//! Every row in the set, in ascending id order: the table's rows first, then the transaction's own
	template <class FUNC>
	void ForEach(FUNC &&callback) const {
		for (idx_t i = 0; i < present.size(); i++) {
			if (present[i]) {
				callback(NumericCast<row_t>(i));
			}
		}
		vector<row_t> local_rows(local_present.begin(), local_present.end());
		std::sort(local_rows.begin(), local_rows.end());
		for (auto row_id : local_rows) {
			callback(row_id);
		}
	}

	void Clear() {
		present.clear();
		key_ref.clear();
		key_known.clear();
		local_present.clear();
		local_key_ref.clear();
	}

private:
	void SetKeyRef(row_t row_id, uint64_t value) {
		if (row_id < 0 || row_id >= MAX_ROW_ID) {
			local_key_ref[row_id] = value;
			return;
		}
		Reserve(NumericCast<idx_t>(row_id) + 1);
		auto index = NumericCast<idx_t>(row_id);
		key_ref[index] = value;
		key_known[index] = true;
	}

	bool GetKeyRef(row_t row_id, uint64_t &value) const {
		if (row_id < 0 || row_id >= MAX_ROW_ID) {
			auto entry = local_key_ref.find(row_id);
			if (entry == local_key_ref.end()) {
				return false;
			}
			value = entry->second;
			return true;
		}
		auto index = NumericCast<idx_t>(row_id);
		if (index >= key_ref.size() || !key_known[index]) {
			return false;
		}
		value = key_ref[index];
		return true;
	}

	void Reserve(idx_t rows) {
		if (rows <= present.size()) {
			return;
		}
		// doubling: the ids arrive in ascending order, so growing by one would copy the bits a million times
		auto capacity = MaxValue<idx_t>(rows, present.size() * 2 + 1024);
		present.resize(capacity, false);
		key_ref.resize(capacity, 0);
		key_known.resize(capacity, false);
	}

private:
	//! One bit a row, for the ids a table's own scan hands back
	vector<bool> present;
	//! Where a row's key is, and whether it has one - there is no spare value to mean "unset", a key pattern
	//! being as likely to be zero as anything else
	vector<uint64_t> key_ref;
	vector<bool> key_known;
	//! The rows a transaction added to the table itself, which carry ids near the top of the range
	unordered_set<row_t> local_present;
	unordered_map<row_t, uint64_t> local_key_ref;
};

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

//===----------------------------------------------------------------------===//
// The listings, as rows rather than as a pragma's printout
//===----------------------------------------------------------------------===//
//! The transaction manager of the database this client is attached to, which is where the registries live.
//! A listing is a read like any other, so a rollback that took a reference takes it out of the listing too.
static DuckTransactionManager &GetGit4DataTransactionManagerForListing(ClientContext &context) {
	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	// a rollback may have removed a reference this listing is about to read, and it may have been another
	// connection's - the registries are shared, so the flag that says so is on the transaction manager too
	Git4DataReconcileIfNeeded(context);
	return DuckTransactionManager::Get(*attached);
}

//! Every value in these listings - which tables a branch reads, whether those tables are still there, the
//! line a capture holds - used to be readable only by a person: `PRAGMA git4data_branches` returns a result
//! that cannot be selected *from*, so a test could assert that it ran and nothing about what it said. The
//! same rows are table functions now, which makes them data to filter, join and assert; the pragmas return
//! these functions so the two surfaces cannot drift apart.
struct Git4DataListingBindData : public TableFunctionData {
	vector<vector<Value>> rows;
};

struct Git4DataListingState : public GlobalTableFunctionState {
	idx_t cursor = 0;
};

static unique_ptr<GlobalTableFunctionState> Git4DataListingInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<Git4DataListingState>();
}

static void Git4DataListingScan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<Git4DataListingBindData>();
	auto &state = data_p.global_state->Cast<Git4DataListingState>();
	idx_t count = 0;
	while (state.cursor < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = bind_data.rows[state.cursor];
		for (idx_t column = 0; column < row.size(); column++) {
			output.SetValue(column, count, row[column]);
		}
		state.cursor++;
		count++;
	}
	// every column of a listing is written with `SetValue`, so every one of them has to be sized here
	output.SetChildCardinality(count);
}

//! The tables a branch reads for its line, comma-joined, resolved to the names they have now
static string Git4DataJoinedStorageTables(ClientContext &context, const Git4DataBranch &branch) {
	string result;
	for (auto &entry : branch.storage_tables) {
		if (!result.empty()) {
			result += ", ";
		}
		auto resolved = Git4DataResolveTableName(context, entry.storage, entry.oid);
		result += resolved.empty() ? entry.storage : resolved;
	}
	return result;
}

//! Whether a table any of *this* branch's own mappings names cannot be resolved. A read falls back to the
//! pinned image when one cannot - whole, for a delta, which needs both of its tables - so this is the
//! difference between what a branch reads and what its row says.
static bool Git4DataOwnMappingsMissing(ClientContext &context, const Git4DataBranch &branch) {
	for (auto &entry : branch.storage_tables) {
		if (Git4DataResolveTableName(context, entry.storage, entry.oid).empty()) {
			return true;
		}
	}
	for (auto &entry : branch.deltas) {
		if (Git4DataResolveTableName(context, entry.added, entry.added_oid).empty() ||
		    Git4DataResolveTableName(context, entry.removed, entry.removed_oid).empty()) {
			return true;
		}
	}
	return false;
}

//! Whether a table the *line* of this branch names cannot be resolved: the branch's own mappings and every
//! line it inherited, walked through `base_line` (which is always a branch name) and stopping if a name
//! repeats. A reference that reads that line - the branch itself, or a snapshot that captured it - reads the
//! pinned image where a table is gone, so this is the signal that says a read has changed under it.
static bool Git4DataLineMissing(ClientContext &context, DuckTransactionManager &transaction_manager,
                                const Git4DataBranch &branch) {
	if (Git4DataOwnMappingsMissing(context, branch)) {
		return true;
	}
	vector<string> visited;
	for (auto line = branch.base_line; !line.empty();) {
		if (std::find(visited.begin(), visited.end(), line) != visited.end()) {
			break;
		}
		visited.push_back(line);
		Git4DataBranch ancestor;
		if (!transaction_manager.branches.Get(line, ancestor)) {
			break;
		}
		if (Git4DataOwnMappingsMissing(context, ancestor)) {
			return true;
		}
		line = ancestor.base_line;
	}
	return false;
}

//! Whether the ids of the tables *this* branch's own mappings name are known in this session. A mapping read
//! back from disk has no oid - the catalog's ids are a process's and are not written to the file - so it is
//! compared by name and shape, which cannot tell a table from another table of the same name and shape created
//! while the database was closed.
static bool Git4DataOwnMappingsNotIdentified(const Git4DataBranch &branch) {
	for (auto &entry : branch.storage_tables) {
		if (entry.oid == 0) {
			return true;
		}
	}
	for (auto &entry : branch.deltas) {
		if (entry.added_oid == 0 || entry.removed_oid == 0) {
			return true;
		}
	}
	return false;
}

//! Whether any of the tables this branch reads were not identified in this session: its own mappings and every
//! line it inherited, since a branch reads through its parent's mappings as much as through its own and those
//! are read the same way.
static bool Git4DataLineNotIdentified(DuckTransactionManager &transaction_manager, const Git4DataBranch &branch) {
	if (Git4DataOwnMappingsNotIdentified(branch)) {
		return true;
	}
	vector<string> visited;
	for (auto line = branch.base_line; !line.empty();) {
		if (std::find(visited.begin(), visited.end(), line) != visited.end()) {
			break;
		}
		visited.push_back(line);
		Git4DataBranch ancestor;
		if (!transaction_manager.branches.Get(line, ancestor)) {
			break;
		}
		if (Git4DataOwnMappingsNotIdentified(ancestor)) {
			return true;
		}
		line = ancestor.base_line;
	}
	return false;
}

static unique_ptr<FunctionData> Git4DataBranchesBind(ClientContext &context, TableFunctionBindInput &,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<Git4DataListingBindData>();
	auto &transaction_manager = GetGit4DataTransactionManagerForListing(context);
	auto entries = transaction_manager.branches.List();
	// the pragma this replaces ordered by the commit and then the name, so the rows do too
	std::sort(entries.begin(), entries.end(), [](const Git4DataBranch &a, const Git4DataBranch &b) {
		if (a.pin != b.pin) {
			return a.pin < b.pin;
		}
		return a.name < b.name;
	});
	for (auto &branch : entries) {
		// A branch another transaction has added and has not committed is not there yet: the listing reads the
		// shared registry, and the registry *table* says the same thing by holding no row for it
		if (!Git4DataReferenceVisible(context, "branch", branch.name)) {
			continue;
		}
		// Both refusals in create_branch and merge key on whether a branch has moved on from its pin, and a
		// mapping whose table has been dropped falls back to the pinned image rather than failing: a listing
		// that said only "has_storage" would report a table the branch cannot read. Both are columns.
		bool storage_missing = false;
		for (auto &entry : branch.storage_tables) {
			if (Git4DataResolveTableName(context, entry.storage, entry.oid).empty()) {
				storage_missing = true;
				break;
			}
		}
		bool delta_missing = false;
		string delta_added;
		string delta_removed;
		for (auto &entry : branch.deltas) {
			if (!delta_added.empty()) {
				delta_added += ", ";
				delta_removed += ", ";
			}
			delta_added += entry.added;
			delta_removed += entry.removed;
			if (Git4DataResolveTableName(context, entry.added, entry.added_oid).empty() ||
			    Git4DataResolveTableName(context, entry.removed, entry.removed_oid).empty()) {
				delta_missing = true;
			}
		}
		// Whether a table the line this branch *inherited* names is gone. A branch reads its parent's line plus
		// its own mapping, and the read falls back to the pinned image when one of those tables cannot be
		// resolved - so a dropped table of the parent's changes what the child reads while every one of the
		// child's own columns stays intact. Measured (increment 3bq): after `DROP TABLE p_add`, the child of
		// `p` read six rows without the inherited added one, `delta_missing` said true for `p` and **false**
		// for the child, and nothing in either row said the child's read had changed. The walk follows
		// `base_line`, which is always a branch name, and stops if a name repeats.
		bool inherited_missing = false;
		vector<string> visited;
		for (auto line = branch.base_line; !line.empty();) {
			if (std::find(visited.begin(), visited.end(), line) != visited.end()) {
				break;
			}
			visited.push_back(line);
			Git4DataBranch ancestor;
			if (!transaction_manager.branches.Get(line, ancestor)) {
				break;
			}
			if (Git4DataOwnMappingsMissing(context, ancestor)) {
				inherited_missing = true;
				break;
			}
			line = ancestor.base_line;
		}
		// Whether the tables this branch reads were identified in this session. The ids that follow a renamed
		// table and refuse a replaced one belong to a *catalog*, and a catalog's ids do not survive a reopen
		// (`catalog_entry.cpp:18` takes them from a process-wide counter, and the catalog is written without
		// them): a mapping read back from disk is compared by name and shape instead, and that cannot tell a
		// table from another table of the same name and shape created while the database was closed. It is a
		// column rather than a footnote because it is the difference between a wrong answer and a refusal - a
		// branch with no mappings has nothing to identify and says true. The tables it reads through its
		// inherited line are read the same way, so they count here too.
		bool identified = !Git4DataLineNotIdentified(transaction_manager, branch);
		// The source each mapping is *for*. A branch is a line for every source it is given, so the tables are
		// two comma-joined lists without their sources - and a caller with a branch over two tables could not
		// tell which added/removed pair belonged to which, nor which table a copy stood for. Measured
		// (increment 3bs): a branch bound to two sources reported `delta_added = "s1_add, s2_add"` with the
		// source named nowhere in the listing. Each list is in the order of the mapping list it belongs to,
		// and the two new columns are placed beside the tables they pair with.
		string storage_source;
		for (auto &entry : branch.storage_tables) {
			if (!storage_source.empty()) {
				storage_source += ", ";
			}
			auto resolved = Git4DataResolveTableName(context, entry.source, entry.source_oid);
			storage_source += resolved.empty() ? entry.source : resolved;
		}
		string delta_source;
		for (auto &entry : branch.deltas) {
			if (!delta_source.empty()) {
				delta_source += ", ";
			}
			auto resolved = Git4DataResolveTableName(context, entry.source, entry.source_oid);
			delta_source += resolved.empty() ? entry.source : resolved;
		}
		// The line this branch **inherited**, which is not the reference it was created from: a fork from a
		// *snapshot* of a branch carries the snapshot as `parent` and that branch as its line. Measured (increment
		// 3bt): a child forked from a snapshot of `b` reported `parent = cap`, `has_delta = false` and no delta
		// tables, while it read five rows through `b`'s delta - so the row named neither the line nor the tables,
		// and the `inherited_missing` column beside it referred to a line the row did not mention. NULL when there
		// is no such line, as the snapshots listing leaves its own `line` NULL.
		//
		// **What it is not, measured in increment 3bw**: it is not a claim that every read of this branch comes
		// from that line. A branch's own mapping for a source is read *instead of* the line for that source (the
		// resolver takes the copy or the delta first), and which sources a branch is read through is the caller's
		// argument at read time, so no branch-wide column can say "for which sources the line is still live". The
		// columns beside this one are what answer that: `storage_source`, `storage_table`, `delta_source`,
		// `delta_added` and `delta_removed` name the sources it holds something of its own for. Measured: `b1`,
		// forked from a delta branch, read 5 rows through the line; after `git4data_bind_branch_table('t','b1',
		// 'b1_own')` it read **2 rows - only its own copy** - while this column still said `b0`.
		Value line_value(LogicalType::VARCHAR);
		if (!branch.base_line.empty()) {
			line_value = Value(branch.base_line);
		}
		// NULL rather than an empty string for a branch made with no parent, which is the rule the `line` column
		// beside it follows: a caller asking which branches were forked from something can ask one question, and
		// a row says "nothing here" the same way in both columns.
		auto parent_value = branch.parent.empty() ? Value(LogicalType::VARCHAR) : Value(branch.parent);
		result->rows.push_back({Value(branch.name), Value::UBIGINT(branch.pin), parent_value, Value(line_value),
		                        Value::TIMESTAMP(branch.created), Value::BOOLEAN(!branch.storage_tables.empty()),
		                        Value::BOOLEAN(!branch.deltas.empty()), Value::BOOLEAN(storage_missing),
		                        Value::BOOLEAN(delta_missing), Value::BOOLEAN(identified), Value(storage_source),
		                        Value(Git4DataJoinedStorageTables(context, branch)), Value(delta_source),
		                        Value(delta_added), Value(delta_removed), Value::BOOLEAN(inherited_missing)});
	}
	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("pinned_commit_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("parent");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("line");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("created");
	return_types.emplace_back(LogicalType::TIMESTAMP);
	names.emplace_back("has_storage");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("has_delta");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("storage_missing");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("delta_missing");
	return_types.emplace_back(LogicalType::BOOLEAN);
	//! Whether the ids of the tables it reads are known in this session, which is what a replacement is caught
	//! by; after a reopen they are not, and only names and shapes are compared
	names.emplace_back("identified");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("storage_source");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("storage_table");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("delta_source");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("delta_added");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("delta_removed");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("inherited_missing");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return std::move(result);
}

static unique_ptr<FunctionData> Git4DataSnapshotsBind(ClientContext &context, TableFunctionBindInput &,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<Git4DataListingBindData>();
	auto &transaction_manager = GetGit4DataTransactionManagerForListing(context);
	auto entries = transaction_manager.snapshots.List();
	// The registry is a hash map, so the order it hands out is not a promise and a listing whose rows moved
	// between runs would be a surface nothing could be asserted against: the rows are ordered by the commit
	// and then the name, as the branches listing is.
	std::sort(entries.begin(), entries.end(), [](const Git4DataSnapshot &a, const Git4DataSnapshot &b) {
		if (a.pin != b.pin) {
			return a.pin < b.pin;
		}
		return a.name < b.name;
	});
	for (auto &snapshot : entries) {
		// the same rule the branches listing follows
		if (!Git4DataReferenceVisible(context, "snapshot", snapshot.name)) {
			continue;
		}
		if (snapshot.name == DuckTransactionManager::MERGE_BASE_REFERENCE) {
			// an internal reference, alive for the length of a merge - and left behind by one that failed,
			// which is why it is neither counted nor shown
			continue;
		}
		// Whether a table of the line this capture names is gone, which is the same signal the branches listing
		// carries for a branch that inherited one: a capture reads that line, so a dropped table of it changes
		// what the capture reads. Measured (increment 3br): `cap` captured the line of a delta branch and read
		// six rows with the branch's added key; after `DROP TABLE` of that branch's added table it read the
		// same six rows without the key, and the row here named the line and nothing else. A line whose branch
		// cannot be found counts as missing, because the read then falls back to the pinned image - which is
		// what the resolution does when the name is not a branch.
		Value line_missing(LogicalType::BOOLEAN);
		Value identified(LogicalType::BOOLEAN);
		// **An internal reference's `line` is not a line** (increment 3ca). The point-in-time window rides its
		// own length on that field - `window_micros=604800000000` - because a reference is what survives a
		// crash, so the two columns below, which ask what the named line's tables are doing, have nothing to
		// answer about. Measured: with a window open, `__pitr__` was reported with `line_missing = true` while
		// nothing was missing - the window's reference holds no line and no mapping at all - which is a false
		// alarm in the one column that exists to say a read has changed. A caller cannot make a `__`-prefixed
		// reference (the reserved prefix refuses it), so the prefix is the test for "this is the engine's".
		auto internal = StringUtil::StartsWith(snapshot.name, "__");
		if (!internal && !snapshot.line.empty()) {
			Git4DataBranch captured;
			auto found = transaction_manager.branches.Get(snapshot.line, captured);
			line_missing = Value::BOOLEAN(!found || Git4DataLineMissing(context, transaction_manager, captured));
			// The same column the branches listing carries, for the same reason: a capture of a line reads that
			// line's mappings, and after a reopen they are the oid-less ones. NULL when the line names no branch,
			// because the read then falls back to the pinned image and there is no mapping left to identify.
			identified = found ? Value::BOOLEAN(!Git4DataLineNotIdentified(transaction_manager, captured))
			                   : Value(LogicalType::BOOLEAN);
		}
		// NULL rather than an empty string for the main line, so a listing says which references capture a
		// branch's own state and which are the table at a commit - and NULL for this column with it, since a
		// capture of the table itself has no line whose tables could be gone
		result->rows.push_back({Value(snapshot.name), Value::UBIGINT(snapshot.pin), Value::TIMESTAMP(snapshot.created),
		                        snapshot.line.empty() ? Value(LogicalType::VARCHAR) : Value(snapshot.line),
		                        line_missing, identified});
	}
	names.emplace_back("snapshot_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("pinned_commit_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("created");
	return_types.emplace_back(LogicalType::TIMESTAMP);
	names.emplace_back("line");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("line_missing");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("identified");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return std::move(result);
}

TableFunction Git4DataBranchesFun::GetFunction() {
	TableFunction fun("git4data_branches", {}, Git4DataListingScan, Git4DataBranchesBind);
	fun.init_global = Git4DataListingInit;
	fun.projection_pushdown = false;
	return fun;
}

void Git4DataBranchesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

TableFunction Git4DataSnapshotsFun::GetFunction() {
	TableFunction fun("git4data_snapshots", {}, Git4DataListingScan, Git4DataSnapshotsBind);
	fun.init_global = Git4DataListingInit;
	fun.projection_pushdown = false;
	return fun;
}

void Git4DataSnapshotsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

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
	return fun;
}

void Git4DataProbeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

TableFunction Git4DataProbeAllFun::GetFunction() {
	TableFunction fun("git4data_probe_all", {LogicalType::VARCHAR}, Git4DataListingScan, Git4DataProbeAllBind);
	fun.init_global = Git4DataListingInit;
	fun.projection_pushdown = false;
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
	return fun;
}

void Git4DataProbeAtFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

//===----------------------------------------------------------------------===//
// The state a database is in, as one row a statement can select
//===----------------------------------------------------------------------===//
//! The same argument as the listings above, for the three surfaces that report a *value*: the commit counter
//! and the bound a checkpoint must respect, the quotas on references, and the recovery window. Each was a
//! pragma returning a single row, so its numbers were readable and never asserted.
static void Git4DataRowNames(vector<Identifier> &names, vector<LogicalType> &types, const char *name,
                             LogicalType type) {
	names.emplace_back(name);
	types.push_back(std::move(type));
}

static unique_ptr<FunctionData> Git4DataStateBind(ClientContext &context, TableFunctionBindInput &,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<Git4DataListingBindData>();
	auto &transaction_manager = GetGit4DataTransactionManagerForListing(context);
	auto last_commit = transaction_manager.GetLastCommit();
	auto pinned = transaction_manager.snapshots.LowestPin();
	// Mirrors the decision made in DuckTransactionManager::Checkpoint
	auto retained = last_commit >= transaction_manager.LowestRetentionBound();
	result->rows.push_back({Value::UBIGINT(last_commit), Value::UBIGINT(pinned), Value::BOOLEAN(retained),
	                        Value::UBIGINT(transaction_manager.LowestRetentionBound().Value()),
	                        Value::UBIGINT(transaction_manager.GetStartTimestampCounter())});
	Git4DataRowNames(names, return_types, "last_commit_id", LogicalType::UBIGINT);
	Git4DataRowNames(names, return_types, "pinned_commit_id", LogicalType::UBIGINT);
	Git4DataRowNames(names, return_types, "history_retained", LogicalType::BOOLEAN);
	Git4DataRowNames(names, return_types, "retention_bound_value", LogicalType::UBIGINT);
	Git4DataRowNames(names, return_types, "start_timestamp_counter", LogicalType::UBIGINT);
	return std::move(result);
}

static unique_ptr<FunctionData> Git4DataLimitsBind(ClientContext &context, TableFunctionBindInput &,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<Git4DataListingBindData>();
	auto &transaction_manager = GetGit4DataTransactionManagerForListing(context);
	result->rows.push_back(
	    {Value::BIGINT(transaction_manager.snapshot_limit), Value::UBIGINT(transaction_manager.UserSnapshotCount()),
		 Value::BIGINT(transaction_manager.branch_limit), Value::UBIGINT(transaction_manager.branches.Count())});
	Git4DataRowNames(names, return_types, "snapshot_limit", LogicalType::BIGINT);
	Git4DataRowNames(names, return_types, "snapshots", LogicalType::UBIGINT);
	Git4DataRowNames(names, return_types, "branch_limit", LogicalType::BIGINT);
	Git4DataRowNames(names, return_types, "branches", LogicalType::UBIGINT);
	return std::move(result);
}

static unique_ptr<FunctionData> Git4DataPitrStateBind(ClientContext &context, TableFunctionBindInput &,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<Git4DataListingBindData>();
	auto &transaction_manager = GetGit4DataTransactionManagerForListing(context);
	constexpr int64_t MICROS_PER_DAY = 24LL * 60 * 60 * 1000000;
	auto days = transaction_manager.PitrWindow().value / MICROS_PER_DAY;
	// when the reference was taken is deliberately not reported here: it is the wall clock, so a test cannot
	// assert it. It shows in git4data_snapshots, where it belongs.
	Git4DataSnapshot reference;
	auto held = transaction_manager.snapshots.Get(DuckTransactionManager::PITR_REFERENCE, reference);
	result->rows.push_back(
	    {Value::BOOLEAN(transaction_manager.PitrEnabled()), Value::BIGINT(days), Value::BOOLEAN(held)});
	Git4DataRowNames(names, return_types, "pitr_enabled", LogicalType::BOOLEAN);
	Git4DataRowNames(names, return_types, "window_days", LogicalType::BIGINT);
	Git4DataRowNames(names, return_types, "reference_held", LogicalType::BOOLEAN);
	return std::move(result);
}

TableFunction Git4DataStateFun::GetFunction() {
	TableFunction fun("git4data_state", {}, Git4DataListingScan, Git4DataStateBind);
	fun.init_global = Git4DataListingInit;
	fun.projection_pushdown = false;
	return fun;
}

void Git4DataStateFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

TableFunction Git4DataLimitsFun::GetFunction() {
	TableFunction fun("git4data_limits", {}, Git4DataListingScan, Git4DataLimitsBind);
	fun.init_global = Git4DataListingInit;
	fun.projection_pushdown = false;
	return fun;
}

void Git4DataLimitsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

TableFunction Git4DataPitrStateFun::GetFunction() {
	TableFunction fun("git4data_pitr_state", {}, Git4DataListingScan, Git4DataPitrStateBind);
	fun.init_global = Git4DataListingInit;
	fun.projection_pushdown = false;
	return fun;
}

void Git4DataPitrStateFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

} // namespace duckdb
