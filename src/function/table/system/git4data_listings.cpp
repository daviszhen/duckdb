//===----------------------------------------------------------------------===//
// Git4Data -- the surfaces that report what the registries hold
//===----------------------------------------------------------------------===//
// The reference listings and the state row, as table functions a statement can select from - which is what
// makes their values assertable rather than only printable. Increment 3en moved them out of
// git4data_snapshot_scan.cpp, and 3fx moved the measurement hooks into git4data_probe_rows.cpp.
//===----------------------------------------------------------------------===//

#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/function/table/git4data_internal.hpp"
#include "duckdb/storage/git4data_registry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

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
unique_ptr<GlobalTableFunctionState> Git4DataListingInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<Git4DataListingState>();
}

void Git4DataListingScan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
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
