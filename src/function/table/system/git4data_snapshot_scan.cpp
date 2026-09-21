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

//! Reads a table as of a pinned reference: the rows a reader at that commit id would have seen.
//! The retention bound keeps those rows and their delete ids alive, so the scan is an ordinary
//! visibility-filtered scan with the pinned bound substituted for "now".

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

//! Refuse a read at a pin when the mapping it would answer through was set *after* that pin.
//!
//! A capture names a branch, and this read resolves that branch's mapping at the time of the read while bounding
//! it by the capture's own pin - so a mapping set after the pin makes the read answer through a table that did
//! not exist at the pin. Measured before this check (3dr): the capture and the forks of it read **no rows at
//! all**, while the branch itself still read 100 and every listing column said nothing was wrong. The mapping
//! records the commit it was set at (decision of 2026-09-21), and this is where that value is used: a mapping
//! with no value (a file written before the column) is *unknown* and checks nothing, which is the behaviour
//! before the column existed.
//! Refuse a read through a mapping whose table is not the table it was made against.
//!
//! The `oid` a mapping also carries is the catalog's own counter and does not survive a reopen, so it catches a
//! table dropped and recreated *within* a session and nothing else. The identity the table itself carries does
//! survive - it is written with the table's metadata - and this is where it is compared (decision 2 of the three
//! format decisions, 2026-09-21). A mapping with no value (a row from before the column, or one written by hand)
//! is *unknown* and checks nothing.
static void Git4DataCheckMappingTable(ClientContext &context, const string &branch_name, const string &table_name,
                                      uint64_t mapping_table_id, const char *what) {
	if (mapping_table_id == 0) {
		return;
	}
	auto table = Git4DataResolveTableOrNull(context, table_name);
	if (!table) {
		// gone: the documented fallback to the branch's pinned image, not a swap
		return;
	}
	if (!Git4DataTableIdentityIsFromDisk(*table)) {
		// The table carries an identity nothing has persisted - it was created by a statement the log replayed,
		// and the statement that assigns an identity is internal and is not logged - so there is no fact about
		// the table to compare the mapping against and nothing is refused. The mapping's row *is* durable, so
		// the table adopts its value: the two agree from here, the next checkpoint writes it, and until then no
		// read is refused on a value this process invented.
		table->GetStorage().GetDataTableInfo()->SetPersistedTableId(mapping_table_id);
		return;
	}
	if (Git4DataTableIdentity(*table) == mapping_table_id) {
		return;
	}
	throw InvalidInputException(
	    "Branch \"%s\" reads table \"%s\" as its %s, and that is not the table it was given: the table carrying "
	    "that name now is another one - dropped and recreated, or replaced while the database was closed - and "
	    "reading it would answer from a table this reference never saw. Bind the branch again with "
	    "git4data_%s.",
	    branch_name, table_name, what, StringUtil::CIEquals(what, "delta") ? "bind_branch_delta" : "bind_branch_table");
}

static void Git4DataCheckMappingAge(const Git4DataBranch &branch, const string &mapping_name,
                                    transaction_t mapping_set_at, transaction_t check_pin, const char *what) {
	if (check_pin == 0 || mapping_set_at == 0 || mapping_set_at <= check_pin) {
		return;
	}
	throw InvalidInputException(
	    "Branch \"%s\" was given %s \"%s\" at commit %llu, and this read is at commit %llu: the line asked for "
	    "predates the %s that would answer it, so the read would come back through a table that did not exist "
	    "then. Read the branch as it stands now, or capture the line again.",
	    branch.name, what, mapping_name, (unsigned long long)mapping_set_at, (unsigned long long)check_pin, what);
}

static Git4DataLineState ResolveGit4DataLineState(ClientContext &context, DuckTransactionManager &transaction_manager,
                                                  const string &table_name, const Git4DataBranch &branch,
                                                  VisibilityBound at, transaction_t check_pin = 0) {
	Git4DataLineState state;
	state.base_bound = at;
	auto storage = Git4DataBranchStorageTable(context, branch, table_name);
	if (!storage.empty()) {
		// the branch's own line is a table: read as it is now, or as it stood at the commit asked for.
		// Its columns are mapped onto the source's by position, so the shape is checked on every read as
		// well as when it was bound: DDL afterwards can leave the table unable to stand in, and the read
		// would otherwise swap or rename what a caller sees - measured, a table holding (v, k) read back
		// as k = 777 and v = 1 from a row that held k = 1 and v = 777.
		auto &storage_entry = Git4DataResolveTable(context, storage);
		{
			auto &source_entry = Git4DataResolveTable(context, table_name);
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
		// the mapping this read is about to use, and when it was set
		for (auto &mapping : branch.storage_tables) {
			// the mapping this read is about - asked of the table itself, because what the row holds is whatever
			// spelling it was written with and `storage` is the name the catalog gives the table
			if (Git4DataMappingNameMatches(storage_entry.ParentSchema().name, storage_entry.name, mapping.storage)) {
				Git4DataCheckMappingAge(branch, storage, mapping.set_at, check_pin, "table");
				Git4DataCheckMappingTable(context, branch.name, storage, mapping.table_id, "table");
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
			                                          VisibilityBound::Through(branch.pin), branch.pin);
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
		Git4DataCheckMappingAge(branch, delta.added, delta.set_at, check_pin, "delta");
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
			// after the oid refusal above, which is the one that speaks for a swap *within* a session: the
			// identity is what speaks across a reopen, where the oids are gone
			Git4DataCheckMappingTable(context, branch.name, delta.added, delta.added_table_id, "delta");
			Git4DataCheckMappingTable(context, branch.name, delta.removed, delta.removed_table_id, "delta");
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
				                                      VisibilityBound::Through(snapshot.pin), snapshot.pin);
				plan.table_name = state.base_table;
				plan.own_storage = state.own_storage;
				plan.bound = state.base_bound;
				plan.deltas = state.deltas;
			}
		}
		return;
	}
	Git4DataBranch branch;
	if (!Git4DataReferenceVisible(context, "branch", name) || !transaction_manager.branches.Get(name, branch)) {
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

Git4DataReadPlan ResolveGit4DataReferencePlan(ClientContext &context,
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
void Git4DataCheckNoGeneratedColumns(ClientContext &context, const string &table_name) {
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

void Git4DataSnapshotScanFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(GetFunction());
}

} // namespace duckdb
