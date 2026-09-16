#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

namespace duckdb {

//! Reads a table as of a pinned reference: the rows a reader at that commit id would have seen.
//! The retention bound keeps those rows and their delete ids alive, so the scan is an ordinary
//! visibility-filtered scan with the pinned bound substituted for "now".
struct Git4DataSnapshotScanBindData : public TableFunctionData {
	string table_name;
	string reference_name;
	Git4DataRefKind kind;
	VisibilityBound bound;
	//! The rows the reference added to its base and the keys it removed, empty when it holds a copy of
	//! its base instead of differing from it
	string delta_added;
	string delta_removed;
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

//! The keys a delta removed from its base, read once while a read is set up. A key is a tuple of
//! arbitrary columns and this is the only place the tuple is compared, so it is held as one string.
static void Git4DataCollectRemovedKeys(ClientContext &context, const string &removed_table,
                                       const vector<LogicalType> &key_types, idx_t key_count,
                                       unordered_set<string> &result) {
	if (removed_table.empty()) {
		return;
	}
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(removed_table)));
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
	scan.Initialize(removed_ids, nullptr);
	QueryContext query_context;
	groups.InitializeScan(query_context, scan.table_state, removed_ids, nullptr);
	ScanOptions options(TransactionData(MAX_TRANSACTION_ID, VisibilityBound::AllCommitted()));
	while (true) {
		chunk.Reset();
		scan.table_state.Scan(options, chunk);
		if (chunk.size() == 0) {
			break;
		}
		for (idx_t row = 0; row < chunk.size(); row++) {
			result.insert(Git4DataKeyString(chunk, positions, row));
		}
	}
}

struct Git4DataSnapshotScanState : public GlobalTableFunctionState {
	TableScanState scan_state;
	//! The rows the reference added, scanned once the base is exhausted
	TableScanState added_scan;
	//! The removed keys, read in full while the scan is set up - a delta removes what it removes, which
	//! is the smaller side by construction
	TableScanState removed_scan;
	DataChunk scan_chunk;
	DataChunk added_chunk;
	DataChunk removed_chunk;
	vector<StorageIndex> column_ids;
	//! Where the key columns sit in the scanned columns, so a row's key can be built from a chunk
	vector<idx_t> key_positions;
	unordered_set<string> removed_keys;
	VisibilityBound bound;
	bool has_delta = false;
	bool reading_added = false;
	bool finished = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

//! The table backing a branch for a source table, or empty when the branch has no line of its own
//! there. A mapping whose table has since been dropped falls back to the pinned image rather than
//! failing, because the branch itself is still a valid reference.
static string Git4DataBranchStorageTable(ClientContext &context, const Git4DataBranch &branch,
                                         const string &table_name) {
	for (auto &entry : branch.storage_tables) {
		if (entry.first != table_name) {
			continue;
		}
		auto storage = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(entry.second)),
		                                                    OnEntryNotFound::RETURN_NULL);
		if (storage) {
			return entry.second;
		}
	}
	return string();
}

//! How a reference is read: the table to scan, the bound to scan it at, and - when the reference
//! differs from its base through a delta rather than through a copy of it - the two tables that say
//! how: the rows it has that the base does not, and the keys it has removed from the base.
struct Git4DataReadPlan {
	string table_name;
	VisibilityBound bound;
	string delta_added;
	string delta_removed;

	bool HasDelta() const {
		return !delta_added.empty() || !delta_removed.empty();
	}
};

//! The plan for one reference. The scan and the diff both come here, so the two cannot disagree about
//! which table a reference means - a disagreement that compares row ids of different tables and
//! reports no change at all.
static void ResolveGit4DataRead(ClientContext &context, DuckTransactionManager &transaction_manager,
                                const string &table_name, const string &name, Git4DataRefKind kind,
                                Git4DataReadPlan &plan) {
	if (kind == Git4DataRefKind::SNAPSHOT) {
		Git4DataSnapshot snapshot;
		if (!transaction_manager.snapshots.Get(name, snapshot)) {
			throw InvalidInputException("Snapshot \"%s\" does not exist", name);
		}
		plan.table_name = table_name;
		plan.bound = VisibilityBound::Through(snapshot.pin);
		if (!snapshot.line.empty()) {
			// It captured a branch's own line, which lives in that branch's storage table once it has
			// been written to. A line whose table is gone falls back to the pinned image, the same way a
			// branch whose storage was dropped does.
			Git4DataBranch line_branch;
			if (transaction_manager.branches.Get(snapshot.line, line_branch)) {
				auto storage = Git4DataBranchStorageTable(context, line_branch, table_name);
				if (!storage.empty()) {
					plan.table_name = storage;
				}
			}
		}
		return;
	}
	Git4DataBranch branch;
	if (!transaction_manager.branches.Get(name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", name);
	}
	auto storage = Git4DataBranchStorageTable(context, branch, table_name);
	if (!storage.empty()) {
		// the branch has a line of its own here, so read that rather than the pinned image - it can
		// have been written to since the branch was taken
		plan.table_name = storage;
		plan.bound = VisibilityBound::AllCommitted();
		return;
	}
	// no copy of its own: the base is whatever the branch started from, read at its pin
	string base_table = table_name;
	if (!branch.base_line.empty()) {
		// forked from a reference that captured another branch's line: read that line as it stood at
		// this pin, which is what makes a fork take a state rather than only a commit
		Git4DataBranch line_branch;
		if (transaction_manager.branches.Get(branch.base_line, line_branch)) {
			auto line_storage = Git4DataBranchStorageTable(context, line_branch, table_name);
			if (!line_storage.empty()) {
				base_table = line_storage;
			}
		}
	}
	plan.table_name = base_table;
	plan.bound = VisibilityBound::Through(branch.pin);
	// a delta says how this branch differs from that base, and its tables are the caller's, so one that
	// has since been dropped falls back to the base rather than failing
	for (auto &delta : branch.deltas) {
		if (delta.source != table_name) {
			continue;
		}
		auto added = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(delta.added)),
		                                                  OnEntryNotFound::RETURN_NULL);
		auto removed = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(delta.removed)),
		                                                    OnEntryNotFound::RETURN_NULL);
		if (added && removed) {
			plan.delta_added = delta.added;
			plan.delta_removed = delta.removed;
		}
	}
}

static Git4DataReadPlan ResolveGit4DataReferencePlan(ClientContext &context,
                                                     DuckTransactionManager &transaction_manager,
                                                     const string &table_name, const string &name) {
	Git4DataRefKind kind;
	if (transaction_manager.snapshots.GetPin(name).IsValid()) {
		kind = Git4DataRefKind::SNAPSHOT;
	} else {
		// the name has to be in one of the two registries, and a caller that passed neither wants to be
		// told that rather than which of the two was looked in
		Git4DataBranch branch;
		if (!transaction_manager.branches.Get(name, branch)) {
			throw InvalidInputException("Reference \"%s\" is neither a snapshot nor a branch", name);
		}
		kind = Git4DataRefKind::BRANCH;
	}
	Git4DataReadPlan plan;
	ResolveGit4DataRead(context, transaction_manager, table_name, name, kind, plan);
	return plan;
}
unique_ptr<FunctionData> Git4DataSnapshotScanFun::CreateBindData(ClientContext &context, const string &table_name,
                                                                 const string &reference_name, Git4DataRefKind kind) {
	auto result = make_uniq<Git4DataSnapshotScanBindData>();
	result->table_name = table_name;
	result->reference_name = reference_name;
	result->kind = kind;

	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	auto &transaction_manager = DuckTransactionManager::Get(*attached);
	Git4DataReadPlan plan;
	ResolveGit4DataRead(context, transaction_manager, table_name, reference_name, kind, plan);
	result->table_name = plan.table_name;
	result->bound = plan.bound;
	result->delta_added = plan.delta_added;
	result->delta_removed = plan.delta_removed;
	return std::move(result);
}

static unique_ptr<FunctionData> Git4DataSnapshotScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = Git4DataSnapshotScanFun::CreateBindData(
	    context, input.inputs[0].GetValue<string>(), input.inputs[1].GetValue<string>(), Git4DataRefKind::SNAPSHOT);

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

	if (bind_data.delta_added.empty() && bind_data.delta_removed.empty()) {
		return std::move(result);
	}
	// A delta: the base is read minus the keys it removed, and the rows it added follow. Both tables
	// carry the source's columns in the same order, so the same column list serves for all three.
	result->has_delta = true;
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
	if (!bind_data.delta_added.empty()) {
		auto &added_table =
		    Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(bind_data.delta_added)));
		result->added_chunk.Initialize(Allocator::Get(context), scan_types);
		result->added_scan.Initialize(result->column_ids, nullptr);
		added_table.GetStorage().GetRowGroupCollection()->InitializeScan(query_context, result->added_scan.table_state,
		                                                                 result->column_ids, nullptr);
	}
	Git4DataCollectRemovedKeys(context, bind_data.delta_removed, ordered_key_types, key_columns.size(),
	                           result->removed_keys);
	return std::move(result);
}

static void Git4DataSnapshotScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<Git4DataSnapshotScanState>();
	if (state.finished) {
		return;
	}
	// the base first, with the keys the delta removed left out
	while (!state.reading_added) {
		state.scan_chunk.Reset();
		// no transaction of our own writes to see, and the pinned bound in place of "now"
		ScanOptions options(TransactionData(MAX_TRANSACTION_ID, state.bound));
		state.scan_state.table_state.Scan(options, state.scan_chunk);
		auto count = state.scan_chunk.size();
		if (count == 0) {
			if (!state.has_delta) {
				state.finished = true;
				return;
			}
			// the base is exhausted; what follows is what the delta added
			state.reading_added = true;
			break;
		}
		if (state.removed_keys.empty()) {
			for (idx_t i = 0; i < output.ColumnCount(); i++) {
				output.data[i].Reference(state.scan_chunk.data[i]);
			}
			output.SetCardinality(count);
			return;
		}
		SelectionVector kept(count);
		idx_t kept_count = 0;
		for (idx_t row = 0; row < count; row++) {
			if (state.removed_keys.find(Git4DataKeyString(state.scan_chunk, state.key_positions, row)) ==
			    state.removed_keys.end()) {
				kept.set_index(kept_count++, row);
			}
		}
		if (kept_count == 0) {
			// every row of this chunk was removed, so the next one is what to look at
			continue;
		}
		for (idx_t i = 0; i < output.ColumnCount(); i++) {
			output.data[i].Slice(state.scan_chunk.data[i], kept, kept_count);
		}
		output.SetCardinality(kept_count);
		return;
	}
	if (!state.reading_added) {
		state.finished = true;
		return;
	}
	// then the rows the branch added, which are its own line and so read as of everything committed
	state.added_chunk.Reset();
	ScanOptions added_options(TransactionData(MAX_TRANSACTION_ID, VisibilityBound::AllCommitted()));
	state.added_scan.table_state.Scan(added_options, state.added_chunk);
	if (state.added_chunk.size() == 0) {
		state.finished = true;
		return;
	}
	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		output.data[i].Reference(state.added_chunk.data[i]);
	}
	output.SetCardinality(state.added_chunk.size());
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
	//! A side that differs from its base through a delta is read through it here too: the rows it added
	//! are part of that side, and the keys it removed are not
	string left_delta_added;
	string left_delta_removed;
	string right_delta_added;
	string right_delta_removed;
};

//! Marks a Change that stands alone rather than being paired with a counterpart
static constexpr row_t NO_COUNTERPART = -1;

struct Git4DataDiffState : public GlobalTableFunctionState {
	//! LEFT walks the first reference, RIGHT the second and works out the difference, EMIT drains it
	enum class Phase : uint8_t { LEFT, RIGHT, EMIT, DONE };

	// one scan per side: a scan state is consumed as it walks, so the second pass needs its own
	TableScanState left_scan;
	TableScanState right_scan;
	DataChunk scan_chunk;
	vector<StorageIndex> column_ids;
	idx_t key_count = 0;
	VisibilityBound left_bound;
	VisibilityBound right_bound;
	unordered_set<row_t> left_rows;
	unordered_set<row_t> right_rows;
	//! Key values of the rows on the left, so a removed row can be paired with the added row that
	//! replaced it. Only filled when the table has a primary key.
	unordered_map<row_t, vector<Value>> left_keys;
	//! Whether the two sides are two different tables, in which case the comparison is made on the
	//! primary key and the scan carries every column so two rows can be told apart by value.
	bool compare_by_key = false;
	//! The keys each side removed from its base, and the table it added rows with. The phases run one
	//! after the other, so one added scan serves both sides and is re-initialized when the side changes.
	unordered_set<string> left_removed_keys;
	unordered_set<string> right_removed_keys;
	string left_added_table;
	string right_added_table;
	string open_added_table;
	TableScanState added_scan;
	DataChunk added_chunk;
	//! The types of the bundled scan, so the added table's chunk can be built to match it
	vector<LogicalType> scan_types;
	//! The number of columns after the keys, contiguous in the scan chunk, when comparing by key
	idx_t value_count = 0;
	//! A row of the left side, held under its primary key
	struct KeyedRow {
		vector<Value> key;
		vector<Value> values;
		row_t row_id;
	};
	unordered_map<hash_t, vector<KeyedRow>> left_by_key;
	struct Change {
		row_t row_id;
		row_t counterpart;
		//! The primary key of the row, carried so a caller can match it without knowing row ids
		vector<Value> key;
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
	result->table_name = table_name;

	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	auto &transaction_manager = DuckTransactionManager::Get(*attached);
	auto left_plan = ResolveGit4DataReferencePlan(context, transaction_manager, table_name, left_name);
	auto right_plan = ResolveGit4DataReferencePlan(context, transaction_manager, table_name, right_name);
	result->left_table = left_plan.table_name;
	result->right_table = right_plan.table_name;
	result->left_bound = left_plan.bound;
	result->right_bound = right_plan.bound;
	result->left_delta_added = left_plan.delta_added;
	result->left_delta_removed = left_plan.delta_removed;
	result->right_delta_added = right_plan.delta_added;
	result->right_delta_removed = right_plan.delta_removed;

	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
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
	auto result =
	    Git4DataDiffFun::CreateBindData(context, input.inputs[0].GetValue<string>(), input.inputs[1].GetValue<string>(),
	                                    input.inputs[2].GetValue<string>());
	names.emplace_back("change");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("row_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("counterpart_row_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	// the key follows, so a caller can match a changed row against the live table by key rather
	// than by a physical row id that the reference scan does not expose
	auto &bind_data = result->Cast<Git4DataDiffBindData>();
	for (idx_t i = 0; i < bind_data.key_columns.size(); i++) {
		names.push_back(bind_data.key_names[i]);
		return_types.push_back(bind_data.key_types[i]);
	}
	return result;
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

static unique_ptr<GlobalTableFunctionState> Git4DataDiffInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<Git4DataDiffBindData>();
	auto result = make_uniq<Git4DataDiffState>();
	result->left_bound = bind_data.left_bound;
	result->right_bound = bind_data.right_bound;
	result->key_count = bind_data.key_columns.size();

	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(bind_data.table_name)));

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
	// what each side removed from its base, read once: a side that differs by a delta is not the table
	Git4DataCollectRemovedKeys(context, bind_data.left_delta_removed, bind_data.key_types, bind_data.key_columns.size(),
	                           result->left_removed_keys);
	Git4DataCollectRemovedKeys(context, bind_data.right_delta_removed, bind_data.key_types,
	                           bind_data.key_columns.size(), result->right_removed_keys);
	result->left_added_table = bind_data.left_delta_added;
	result->right_added_table = bind_data.right_delta_added;

	// each side is read from its own table, which is the same table unless a side is a branch with a
	// line of its own
	QueryContext query_context;
	auto &left_table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(bind_data.left_table)));
	auto &left_groups = *left_table.GetStorage().GetRowGroupCollection();
	result->left_scan.Initialize(result->column_ids, nullptr);
	left_groups.InitializeScan(query_context, result->left_scan.table_state, result->column_ids, nullptr);
	auto &right_table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(bind_data.right_table)));
	auto &right_groups = *right_table.GetStorage().GetRowGroupCollection();
	result->right_scan.Initialize(result->column_ids, nullptr);
	right_groups.InitializeScan(query_context, result->right_scan.table_state, result->column_ids, nullptr);
	return std::move(result);
}

//! Walk the whole table at one bound, handing every visible row and its row id to the callback.
static void Git4DataDiffCollect(ClientContext &context, Git4DataDiffState &state, TableScanState &scan,
                                VisibilityBound bound, const string &added_table,
                                const unordered_set<string> *removed_keys,
                                const std::function<void(DataChunk &, row_t, idx_t)> &callback) {
	// the key columns come first in the scan, so their positions are the first key_count columns
	vector<idx_t> key_positions;
	for (idx_t i = 0; i < state.key_count; i++) {
		key_positions.push_back(i);
	}
	ScanOptions options(TransactionData(MAX_TRANSACTION_ID, bound));
	while (true) {
		state.scan_chunk.Reset();
		scan.table_state.Scan(options, state.scan_chunk);
		auto count = state.scan_chunk.size();
		if (count == 0) {
			break;
		}
		// the row id column is not necessarily flat - a scan may hand back a dictionary
		// the row id sits after the keys, and after the values when those are carried too
		auto row_ids = state.scan_chunk.data[state.key_count + state.value_count].Values<row_t>();
		for (idx_t i = 0; i < count; i++) {
			if (removed_keys &&
			    removed_keys->find(Git4DataKeyString(state.scan_chunk, key_positions, i)) != removed_keys->end()) {
				// this side removed the row from its base, so it is not part of what the side holds
				continue;
			}
			callback(state.scan_chunk, row_ids[i].GetValue(), i);
		}
	}
	if (added_table.empty()) {
		return;
	}
	// then the rows this side added, which are its own and so read as of everything committed
	if (state.open_added_table != added_table) {
		auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(added_table)));
		if (state.added_chunk.ColumnCount() == 0) {
			state.added_chunk.Initialize(Allocator::Get(context), state.scan_types);
		}
		state.added_scan.Initialize(state.column_ids, nullptr);
		QueryContext query_context;
		table.GetStorage().GetRowGroupCollection()->InitializeScan(query_context, state.added_scan.table_state,
		                                                           state.column_ids, nullptr);
		state.open_added_table = added_table;
	}
	ScanOptions added_options(TransactionData(MAX_TRANSACTION_ID, VisibilityBound::AllCommitted()));
	while (true) {
		state.added_chunk.Reset();
		state.added_scan.table_state.Scan(added_options, state.added_chunk);
		auto count = state.added_chunk.size();
		if (count == 0) {
			break;
		}
		auto row_ids = state.added_chunk.data[state.key_count + state.value_count].Values<row_t>();
		for (idx_t i = 0; i < count; i++) {
			callback(state.added_chunk, row_ids[i].GetValue(), i);
		}
	}
}

//! Write up to one vector's worth of rows from a list.
static idx_t Git4DataDiffEmit(DataChunk &output, const vector<Git4DataDiffState::Change> &rows, idx_t &cursor,
                              const char *change) {
	auto remaining = rows.size() - cursor;
	if (remaining == 0) {
		return 0;
	}
	auto count = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
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
		for (idx_t k = 0; k < entry.key.size(); k++) {
			output.data[3 + k].SetValue(i, entry.key[k]);
		}
	}
	cursor += count;
	return count;
}

//! The values of a row after the keys, in the order the scan carries them
static vector<Value> Git4DataDiffRowValues(DataChunk &chunk, idx_t key_count, idx_t value_count, idx_t row) {
	vector<Value> values;
	values.reserve(value_count);
	for (idx_t i = 0; i < value_count; i++) {
		values.push_back(chunk.data[key_count + i].GetValue(row));
	}
	return values;
}

static bool Git4DataDiffSameValues(const vector<Value> &left, const vector<Value> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (!Value::NotDistinctFrom(left[i], right[i])) {
			return false;
		}
	}
	return true;
}

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
			Git4DataDiffCollect(context, state, state.left_scan, state.left_bound, state.left_added_table,
			                    &state.left_removed_keys, [&](DataChunk &chunk, row_t row_id, idx_t row) {
				                    auto key = Git4DataDiffRowKey(chunk, state.key_count, row);
				                    Git4DataDiffState::KeyedRow entry;
				                    entry.key = key;
				                    entry.values =
				                        Git4DataDiffRowValues(chunk, state.key_count, state.value_count, row);
				                    entry.row_id = row_id;
				                    state.left_by_key[Git4DataDiffKeyHash(key)].push_back(std::move(entry));
			                    });
			state.phase = Git4DataDiffState::Phase::RIGHT;
		}
		if (state.phase == Git4DataDiffState::Phase::RIGHT) {
			unordered_set<hash_t> matched;
			Git4DataDiffCollect(
			    context, state, state.right_scan, state.right_bound, state.right_added_table, &state.right_removed_keys,
			    [&](DataChunk &chunk, row_t row_id, idx_t row) {
				    auto key = Git4DataDiffRowKey(chunk, state.key_count, row);
				    auto hash = Git4DataDiffKeyHash(key);
				    auto entry = state.left_by_key.find(hash);
				    if (entry != state.left_by_key.end()) {
					    for (auto &candidate : entry->second) {
						    if (!Git4DataDiffSameKey(candidate.key, key)) {
							    continue;
						    }
						    matched.insert(hash);
						    auto values = Git4DataDiffRowValues(chunk, state.key_count, state.value_count, row);
						    if (!Git4DataDiffSameValues(candidate.values, values)) {
							    state.changed.push_back(Git4DataDiffState::Change {candidate.row_id, row_id, key});
						    }
						    return;
					    }
				    }
				    state.added.push_back(Git4DataDiffState::Change {row_id, NO_COUNTERPART, key});
			    });
			for (auto &entry : state.left_by_key) {
				if (matched.find(entry.first) != matched.end()) {
					continue;
				}
				for (auto &candidate : entry.second) {
					state.removed.push_back(
					    Git4DataDiffState::Change {candidate.row_id, NO_COUNTERPART, candidate.key});
				}
			}
			state.left_by_key.clear();
			state.phase = Git4DataDiffState::Phase::EMIT;
		}
		auto keyed = Git4DataDiffEmit(output, state.changed, state.changed_cursor, "changed");
		if (keyed > 0) {
			return;
		}
		keyed = Git4DataDiffEmit(output, state.removed, state.removed_cursor, "removed");
		if (keyed > 0) {
			return;
		}
		keyed = Git4DataDiffEmit(output, state.added, state.added_cursor, "added");
		if (keyed > 0) {
			return;
		}
		state.phase = Git4DataDiffState::Phase::DONE;
		return;
	}
	if (state.phase == Git4DataDiffState::Phase::LEFT) {
		Git4DataDiffCollect(context, state, state.left_scan, state.left_bound, state.left_added_table,
		                    &state.left_removed_keys, [&](DataChunk &chunk, row_t row_id, idx_t row) {
			                    state.left_rows.insert(row_id);
			                    if (state.key_count > 0) {
				                    state.left_keys.emplace(row_id, Git4DataDiffRowKey(chunk, state.key_count, row));
			                    }
		                    });
		state.phase = Git4DataDiffState::Phase::RIGHT;
	}
	if (state.phase == Git4DataDiffState::Phase::RIGHT) {
		vector<pair<row_t, vector<Value>>> added_rows;
		Git4DataDiffCollect(context, state, state.right_scan, state.right_bound, state.right_added_table,
		                    &state.right_removed_keys, [&](DataChunk &chunk, row_t row_id, idx_t row) {
			                    state.right_rows.insert(row_id);
			                    if (state.left_rows.find(row_id) == state.left_rows.end()) {
				                    added_rows.emplace_back(row_id, Git4DataDiffRowKey(chunk, state.key_count, row));
			                    }
		                    });
		vector<pair<row_t, vector<Value>>> removed_rows;
		for (auto row_id : state.left_rows) {
			if (state.right_rows.find(row_id) != state.right_rows.end()) {
				continue;
			}
			auto key = state.left_keys.find(row_id);
			if (key == state.left_keys.end()) {
				removed_rows.emplace_back(row_id, vector<Value>());
			} else {
				removed_rows.emplace_back(row_id, key->second);
			}
		}
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
					state.added.push_back(Git4DataDiffState::Change {added.first, NO_COUNTERPART, added.second});
				} else {
					removed_taken[match] = true;
					state.changed.push_back(
					    Git4DataDiffState::Change {removed_rows[match].first, added.first, added.second});
				}
			}
		} else {
			for (auto &added : added_rows) {
				state.added.push_back(Git4DataDiffState::Change {added.first, NO_COUNTERPART, added.second});
			}
		}
		for (idx_t i = 0; i < removed_rows.size(); i++) {
			if (!removed_taken[i]) {
				state.removed.push_back(
				    Git4DataDiffState::Change {removed_rows[i].first, NO_COUNTERPART, removed_rows[i].second});
			}
		}
		// the sets and keys are only needed to work out the three lists
		state.left_rows.clear();
		state.right_rows.clear();
		state.left_keys.clear();
		state.phase = Git4DataDiffState::Phase::EMIT;
	}
	auto emitted = Git4DataDiffEmit(output, state.changed, state.changed_cursor, "changed");
	if (emitted > 0) {
		return;
	}
	emitted = Git4DataDiffEmit(output, state.added, state.added_cursor, "added");
	if (emitted > 0) {
		return;
	}
	emitted = Git4DataDiffEmit(output, state.removed, state.removed_cursor, "removed");
	if (emitted > 0) {
		return;
	}
	state.phase = Git4DataDiffState::Phase::DONE;
	output.SetCardinality(0);
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

} // namespace duckdb
