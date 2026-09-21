//===----------------------------------------------------------------------===//
// Git4Data -- what the read path and the diff share
//===----------------------------------------------------------------------===//
// The diff reads the same source table as the scan, under the same reference plan, and compares rows by the same
// key columns, so it needs the key types, the removed-row tests and the reference plan that the read path's file
// defined for itself. Increment 3eq moved the diff into a file of its own and declared what it shares here. The
// definitions of the functions stay where they were - only the types and the declarations are in this file.
//===----------------------------------------------------------------------===//

#ifndef GIT4DATA_INTERNAL_HPP
#define GIT4DATA_INTERNAL_HPP

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

// The diff's overlay scan builds its own transaction data for a layer, which the types below call while they are
// being read: declared here because their definitions are inline in this file.
TransactionData Git4DataDeltaTransactionData(ClientContext &context, optional_ptr<AttachedDatabase> db, VisibilityBound bound);

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
struct Git4DataDeltaLayer {
	string added;
	string removed;
	VisibilityBound bound;
};
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

void Git4DataCheckNoGeneratedColumns(ClientContext &context, const string &table_name);
void Git4DataCollectRemovedKeys(ClientContext &context, const string &removed_table, const vector<LogicalType> &key_types, idx_t key_count, VisibilityBound bound, Git4DataRemovedKeys &result);
void Git4DataHashForRemovals(const vector<Git4DataRemovedKeys *> &removals, vector<unique_ptr<DataChunk>> &hashes, idx_t from, DataChunk &chunk, const vector<idx_t> &key_positions);
bool Git4DataIntegerKeyBits(const Value &key, uint64_t &result);
bool Git4DataIsIntegerKeyType(const LogicalType &type);
void Git4DataDiffPrimaryKey(TableCatalogEntry &table, vector<StorageIndex> &columns, vector<LogicalType> &types, vector<Identifier> &names);
//! The rows of a chunk that no overlay removed, as a selection the scan keeps.
idx_t Git4DataKeepRows(ClientContext &context, const vector<Git4DataRemovedKeys *> &removals, vector<unique_ptr<DataChunk>> &hashes, idx_t from, DataChunk &chunk, const vector<idx_t> &key_positions, SelectionVector &kept);
bool Git4DataRowWasRemoved(const Git4DataRemovedKeys &removed, DataChunk &chunk, const vector<idx_t> &key_positions, DataChunk &key_hashes, idx_t row);
bool Git4DataTypedRowWasRemoved(const Git4DataKeyColumn &keys, const Git4DataRemovedKeys &removed, idx_t row);
Git4DataReadPlan ResolveGit4DataReferencePlan(ClientContext &context, DuckTransactionManager &transaction_manager, const string &table_name, const string &name);
constexpr idx_t Git4DataHashKeysLimit();
//! A row that has no counterpart on the other side of a diff: the sentinel the comparison walks with.
constexpr row_t NO_COUNTERPART = -1;

// The diff's own state, which its helpers and its operator both work on, and the bind data the
// operator is built with. Moved here by increment 3fm so that the helper half of the diff can be a
// file of its own.
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


// The keyed-row helpers 3fm moved into git4data_diff_rows.cpp: every function that file defines,
// because the operator calls most of them and a partial list is a compile error.
void Git4DataDiffPrimaryKey(TableCatalogEntry &table, vector<StorageIndex> &columns, vector<LogicalType> &types, vector<Identifier> &names);
static unique_ptr<FunctionData> Git4DataDiffBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<Identifier> &names);
static hash_t Git4DataDiffChunkKeyHash(DataChunk &chunk, idx_t key_count, idx_t row);
bool Git4DataDiffSameStoredKeyChunk(const Git4DataDiffState &state, const Git4DataDiffState::StoredRow &stored, DataChunk &chunk, idx_t row);
static vector<Value> Git4DataDiffRowKey(DataChunk &chunk, idx_t key_count, idx_t row);
static Value Git4DataDiffKeyFromBits(const LogicalType &type, uint64_t bits);
static bool Git4DataDiffChunkKeyBits(DataChunk &chunk, idx_t row, uint64_t &bits);
Git4DataDiffState::StoredRow Git4DataDiffStoreRow(Git4DataDiffState &state, ClientContext &context, DataChunk &chunk, idx_t row);
bool Git4DataDiffSameStoredValues(const Git4DataDiffState &state, const Git4DataDiffState::StoredRow &stored, DataChunk &chunk, idx_t first_value, idx_t row);
idx_t Git4DataDiffStoreLeftKey(Git4DataDiffState &state, ClientContext &context, DataChunk &chunk, idx_t row);
vector<Value> Git4DataDiffStoredLeftKey(const Git4DataDiffState &state, idx_t position);
idx_t Git4DataDiffPair(Git4DataDiffState &state, idx_t head, DataChunk &chunk, idx_t row, bool key_is_bits);
void Git4DataDiffChainPush(Git4DataDiffState &state, hash_t key, Git4DataDiffState::KeyedRow entry);
Git4DataDiffState::StoredRow Git4DataDiffChangeKeyBlock(Git4DataDiffState &state, ClientContext &context);
Git4DataDiffState::StoredRow Git4DataDiffStoreChangeKey(Git4DataDiffState &state, ClientContext &context, DataChunk &chunk, idx_t row);
static Git4DataDiffState::StoredRow Git4DataDiffStoreChangeKeyFromLeft(Git4DataDiffState &state, ClientContext &context, const Git4DataDiffState::StoredRow &source);
static Git4DataDiffState::StoredRow Git4DataDiffStoreChangeKeyValues(Git4DataDiffState &state, ClientContext &context, const vector<Value> &key);
static bool Git4DataDiffSameKey(const vector<Value> &left, const vector<Value> &right);
hash_t Git4DataDiffKeyHash(const vector<Value> &key);
static void Git4DataDiffSetupDeltas(ClientContext &context, const vector<Git4DataDeltaLayer> &layers, const vector<StorageIndex> &column_ids, const vector<LogicalType> &scan_types, const vector<LogicalType> &key_types, idx_t key_count, QueryContext &query_context, vector<Git4DataDeltaRead> &result, optional_ptr<AttachedDatabase> &database);

} // namespace duckdb

#endif
