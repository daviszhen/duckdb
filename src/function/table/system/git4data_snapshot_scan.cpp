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
};

struct Git4DataSnapshotScanState : public GlobalTableFunctionState {
	TableScanState scan_state;
	DataChunk scan_chunk;
	vector<StorageIndex> column_ids;
	VisibilityBound bound;
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

unique_ptr<FunctionData> Git4DataSnapshotScanFun::CreateBindData(ClientContext &context, const string &table_name,
                                                                 const string &reference_name, Git4DataRefKind kind) {
	auto result = make_uniq<Git4DataSnapshotScanBindData>();
	result->table_name = table_name;
	result->reference_name = reference_name;
	result->kind = kind;

	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	auto &transaction_manager = DuckTransactionManager::Get(*attached);
	if (kind == Git4DataRefKind::BRANCH) {
		Git4DataBranch branch;
		if (!transaction_manager.branches.Get(reference_name, branch)) {
			throw InvalidInputException("Branch \"%s\" does not exist", reference_name);
		}
		auto storage = Git4DataBranchStorageTable(context, branch, table_name);
		if (!storage.empty()) {
			// the branch has a line of its own here, so read that rather than the pinned image - it
			// can have been written to since the branch was taken
			result->table_name = storage;
			result->bound = VisibilityBound::AllCommitted();
			return std::move(result);
		}
		result->bound = VisibilityBound::Through(branch.pin);
		return std::move(result);
	}
	auto pin = transaction_manager.snapshots.GetPin(reference_name);
	if (!pin.IsValid()) {
		throw InvalidInputException("Snapshot \"%s\" does not exist", reference_name);
	}
	result->bound = VisibilityBound::Through(pin.GetIndex());
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
	return std::move(result);
}

static void Git4DataSnapshotScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<Git4DataSnapshotScanState>();
	if (state.finished) {
		return;
	}
	state.scan_chunk.Reset();
	// no transaction of our own writes to see, and the pinned bound in place of "now"
	ScanOptions options(TransactionData(MAX_TRANSACTION_ID, state.bound));
	state.scan_state.table_state.Scan(options, state.scan_chunk);
	if (state.scan_chunk.size() == 0) {
		state.finished = true;
		return;
	}
	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		output.data[i].Reference(state.scan_chunk.data[i]);
	}
	output.SetCardinality(state.scan_chunk.size());
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
static VisibilityBound ResolveReferenceBound(ClientContext &context, DuckTransactionManager &transaction_manager,
                                             const string &table_name, const string &name, string &read_table) {
	auto pin = transaction_manager.snapshots.GetPin(name);
	if (pin.IsValid()) {
		read_table = table_name;
		return VisibilityBound::Through(pin.GetIndex());
	}
	Git4DataBranch branch;
	if (transaction_manager.branches.Get(name, branch)) {
		auto storage = Git4DataBranchStorageTable(context, branch, table_name);
		if (!storage.empty()) {
			read_table = storage;
			return VisibilityBound::AllCommitted();
		}
		read_table = table_name;
		return VisibilityBound::Through(branch.pin);
	}
	throw InvalidInputException("Reference \"%s\" is neither a snapshot nor a branch", name);
}

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
	result->left_bound = ResolveReferenceBound(context, transaction_manager, table_name, left_name, result->left_table);
	result->right_bound =
	    ResolveReferenceBound(context, transaction_manager, table_name, right_name, result->right_table);

	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
	Git4DataDiffPrimaryKey(table, result->key_columns, result->key_types, result->key_names);
	// A branch with a table of its own is read as that table, and its row ids are its own. Comparing
	// those against another table's row ids would compare numbers that mean different rows, so the
	// comparison moves to the primary key - and without one there is nothing left to compare on.
	result->compare_by_key = result->left_table != result->right_table;
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
static void Git4DataDiffCollect(Git4DataDiffState &state, TableScanState &scan, VisibilityBound bound,
                                const std::function<void(row_t, idx_t)> &callback) {
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
			callback(row_ids[i].GetValue(), i);
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
			Git4DataDiffCollect(state, state.left_scan, state.left_bound, [&](row_t row_id, idx_t row) {
				auto key = Git4DataDiffRowKey(state.scan_chunk, state.key_count, row);
				Git4DataDiffState::KeyedRow entry;
				entry.key = key;
				entry.values = Git4DataDiffRowValues(state.scan_chunk, state.key_count, state.value_count, row);
				entry.row_id = row_id;
				state.left_by_key[Git4DataDiffKeyHash(key)].push_back(std::move(entry));
			});
			state.phase = Git4DataDiffState::Phase::RIGHT;
		}
		if (state.phase == Git4DataDiffState::Phase::RIGHT) {
			unordered_set<hash_t> matched;
			Git4DataDiffCollect(state, state.right_scan, state.right_bound, [&](row_t row_id, idx_t row) {
				auto key = Git4DataDiffRowKey(state.scan_chunk, state.key_count, row);
				auto hash = Git4DataDiffKeyHash(key);
				auto entry = state.left_by_key.find(hash);
				if (entry != state.left_by_key.end()) {
					for (auto &candidate : entry->second) {
						if (!Git4DataDiffSameKey(candidate.key, key)) {
							continue;
						}
						matched.insert(hash);
						auto values = Git4DataDiffRowValues(state.scan_chunk, state.key_count, state.value_count, row);
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
		Git4DataDiffCollect(state, state.left_scan, state.left_bound, [&](row_t row_id, idx_t row) {
			state.left_rows.insert(row_id);
			if (state.key_count > 0) {
				state.left_keys.emplace(row_id, Git4DataDiffRowKey(state.scan_chunk, state.key_count, row));
			}
		});
		state.phase = Git4DataDiffState::Phase::RIGHT;
	}
	if (state.phase == Git4DataDiffState::Phase::RIGHT) {
		vector<pair<row_t, vector<Value>>> added_rows;
		Git4DataDiffCollect(state, state.right_scan, state.right_bound, [&](row_t row_id, idx_t row) {
			state.right_rows.insert(row_id);
			if (state.left_rows.find(row_id) == state.left_rows.end()) {
				added_rows.emplace_back(row_id, Git4DataDiffRowKey(state.scan_chunk, state.key_count, row));
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
