//===----------------------------------------------------------------------===//
// Git4Data -- the lifecycle of a reference: create, drop, list, and the limits
//===----------------------------------------------------------------------===//
// The pragmas that make, take back and report snapshots and branches, the reserved-name and limit checks they
// share, and the wrappers that turn the listings into pragmas. Increment 3fn moved them out of
// git4data_pragmas.cpp, which keeps the settings a database has.
//===----------------------------------------------------------------------===//

#include "duckdb/function/pragma/pragma_functions.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>

#ifdef __APPLE__
#include <sys/clonefile.h>
#endif
#ifdef __linux__
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <unistd.h>
#endif

#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/git4data_registry.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/tableref/at_clause.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/literal.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <cctype>

#include "duckdb/function/pragma/git4data_pragmas.hpp"

namespace duckdb {

//! Point every reference to the source table inside a parsed statement at the branch the statement is
//! being applied to. The call writes *the branch*, so a bare `t` in the statement means the branch's
//! line - reading the live table instead would be a silent wrong answer, and it was one: measured, an
//! insert whose rows came from the table itself copied the live table's value (v = 1) where the branch
//! held 101. A reference that already names a reference of its own is left alone, because naming one is
//! a decision rather than an omission.
static void CheckGit4DataReferenceLimit(const string &plural, idx_t count, int64_t limit) {
	if (limit < 0) {
		return;
	}
	if (limit == 0) {
		throw InvalidInputException("%s are disabled for this database", plural);
	}
	if (NumericCast<int64_t>(count) >= limit) {
		throw InvalidInputException("At most %lld %s are allowed for this database, and %llu already exist",
		                            (long long)limit, plural, (unsigned long long)count);
	}
}

//! Set how many snapshots or branches this database may hold: -1 unlimited, 0 disabled, a positive
//! number is the most. The recovery window's own reference is not counted.
//!
//! The value is a row in `git4data.state` rather than a property of the checkpoint's registry block (3ee). The
//! pragma therefore returns a statement where it used to set a field and mark the registries dirty - the mark
//! existed because a checkpoint skips its body when the log is empty, so a quota set in a session that changed
//! nothing else was lost (measured, `limits.test` read -1 for a limit of 3 after a restart). A row is written
//! through the caller's transaction and reaches the log, so there is nothing to remember to flush.
string PragmaGit4DataLimit(ClientContext &context, const FunctionParameters &parameters) {
	auto kind = StringUtil::Lower(parameters.values[0].ToString());
	auto limit = parameters.values[1].GetValue<int64_t>();
	if (limit < -1) {
		throw InvalidInputException("A limit is -1 for unlimited, 0 to disable, or a positive number");
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	string key;
	if (kind == "snapshot") {
		transaction_manager.snapshot_limit = limit;
		key = "snapshot_limit";
	} else if (kind == "branch") {
		transaction_manager.branch_limit = limit;
		key = "branch_limit";
	} else {
		throw InvalidInputException("Limits are set for \"snapshot\" or \"branch\", not \"%s\"", kind);
	}
	auto result = Git4DataEnsureRegistryTables(context);
	result += StringUtil::Format(
	    "DELETE FROM %s.%s WHERE key = '%s';\nINSERT INTO %s.%s VALUES ('%s', '%lld');\n", GIT4DATA_REGISTRY_SCHEMA,
	    GIT4DATA_STATE_TABLE, key, GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_STATE_TABLE, key, (long long)limit);
	return result;
}

string PragmaGit4DataLimits(ClientContext &context, const FunctionParameters &parameters) {
	// the row comes from the table function, which is where its values can also be selected
	return "SELECT * FROM git4data_limits()";
}

//! A name beginning with two underscores is one of the feature's own internal references - the
//! recovery window, a merge's base. Those are looked up by exact name, so a user reference sharing one
//! would be dropped by the machinery that manages them.
static void CheckGit4DataReferenceName(const string &name) {
	if (StringUtil::StartsWith(name, "__")) {
		throw InvalidInputException(
		    "\"%s\" begins with two underscores, which this feature reserves for references it manages "
		    "itself: the point-in-time recovery window and the base a merge reads against. Choose another "
		    "name.",
		    name);
	}
}

string PragmaGit4DataCreateBranch(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	auto parent = parameters.values[1].ToString();
	CheckGit4DataReferenceName(name);
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// the statement that creates the tables comes before the one that inserts this branch's row: both are
	// part of what this pragma returns, and they run in that order
	string result = Git4DataEnsureRegistryTables(context);
	// the other half of the rule the snapshot side states: the two kinds of reference share a name
	// namespace wherever one is named without its kind, so they cannot share a name
	Git4DataSnapshot same_named_snapshot;
	if (transaction_manager.snapshots.Get(name, same_named_snapshot)) {
		throw InvalidInputException("A snapshot named \"%s\" exists, and a branch of that name would be "
		                            "indistinguishable from it wherever a reference is named without its kind",
		                            name);
	}
	// A branch descends from another branch when one is named; otherwise it starts at the newest
	// commit. Either way the branch holds its base down exactly as a pin does.
	//
	// The ancestor may also be a snapshot, which is how a line is started from a state that is no
	// longer the newest one - a snapshot is a commit, and a commit is what a branch is forked from.
	// Such a line has no branch ancestor, so a merge that has to find one between it and another
	// branch still refuses; a pick takes its base explicitly and works.
	transaction_t pin;
	bool parent_is_snapshot = false;
	string base_line;
	if (parent.empty()) {
		pin = transaction_manager.GetLastCommit();
	} else {
		Git4DataBranch parent_branch;
		if (Git4DataReferenceVisible(context, "branch", parent) &&
		    transaction_manager.branches.Get(parent, parent_branch)) {
			// A fork takes the state the parent holds, not the commit the parent was pinned at, whenever
			// the two differ - which they do the moment the parent has a table of its own or a delta. The
			// child carries the parent as the line it started from and pins the commit it forked at, so
			// what it reads is that line as it stood then, and whatever the parent does afterwards is
			// outside the pin. Without this the child reads the parent's base: measured, a parent whose row
			// read 111 forked a child whose row read 1.
			// **A branch reads a line it inherited too** (increment 3bu). A parent with nothing of its own but a
			// `base_line` still reads *that* line, so its state differs from the source at its pin and the fork
			// has to take the line as well as the pin - otherwise the child reads the source and the parent's
			// line is lost one hop down. Measured before this: `b0` differed by a delta (adding one key, removing
			// another), `b1` was forked from it and read that line, and `b2` forked from `b1` carried an empty
			// `base_line` with `b1`'s pin and read the **raw table** - the key the line added was gone and the one
			// it removed was back, silently, from a fork that was supposed to take what its parent holds.
			if (!parent_branch.storage_tables.empty() || !parent_branch.deltas.empty() ||
			    !parent_branch.base_line.empty()) {
				// A fork takes the state the parent holds, not the commit the parent was pinned at,
				// whenever the two differ - which they do the moment the parent has a table of its own or a
				// delta. The child carries the parent as the line it started from and pins the commit it
				// forked at, so what it reads is that line as it stood then, and whatever the parent does
				// afterwards is outside the pin. Without this the child reads the parent's base: measured, a
				// parent whose row read 111 forked a child whose row read 1.
				base_line = parent;
				pin = transaction_manager.GetLastCommit();
			} else {
				pin = parent_branch.pin;
			}
		} else {
			Git4DataSnapshot parent_snapshot;
			if (!Git4DataReferenceVisible(context, "snapshot", parent) ||
			    !transaction_manager.snapshots.Get(parent, parent_snapshot)) {
				throw InvalidInputException("Reference \"%s\" is neither a branch nor a snapshot, so a branch "
				                            "cannot descend from it",
				                            parent);
			}
			if (!parent_snapshot.line.empty()) {
				// What such a reference names is a branch's own state, so the child carries that line: it
				// reads the line's storage at this pin, which is the state the reference captured, and
				// changes the other branch makes later are outside the pin and stay out.
				base_line = parent_snapshot.line;
			}
			pin = parent_snapshot.pin;
			parent_is_snapshot = true;
		}
	}
	CheckGit4DataReferenceLimit("branches", transaction_manager.branches.Count(), transaction_manager.branch_limit);
	auto created = Timestamp::GetCurrentTimestamp();
	if (!transaction_manager.branches.Create(name, pin, parent, created, parent_is_snapshot, base_line)) {
		throw InvalidInputException("Branch \"%s\" already exists", name);
	}
	// uncommitted until this transaction commits, like the snapshot above
	transaction_manager.MarkReferenceUncommitted(
	    "branch", name, &MetaTransaction::Get(context),
	    DuckTransaction::Get(context, transaction_manager.GetDB()).GetTransactionId());
	// the branch is a row now, like a snapshot: it goes to the log with the statement this returns
	context.client_data->git4data_added_records.push_back("branch\t" + name);
	result += StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', %llu, '%s', TIMESTAMP '%s', '%s');",
	                             GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_BRANCHES_TABLE, Git4DataSQLLiteral(name), pin, Git4DataSQLLiteral(parent),
	                             Timestamp::ToString(created), Git4DataSQLLiteral(base_line));
	return result;
}

//! Drop a branch and the storage it was materialized into: that table is the branch's own copy, so
//! leaving it behind would leak a table per branch ever dropped.
string PragmaGit4DataDropBranch(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	auto existing = Git4DataRequiredBranch(context, transaction_manager, name);
	if (transaction_manager.branches.HasChildren(name)) {
		// a descendant still walks through this name to reach its ancestors
		throw InvalidInputException("Branch \"%s\" still has branches descending from it. Drop those branches "
		                            "first.",
		                            name);
	}
	if (transaction_manager.branches.HasLineDependents(name)) {
		// Those branches read this one's **line** at their fork pin, which dropping it would take away. The
		// message said "storage" until increment 3ce, and that is false for the shape that reaches it most
		// easily - measured: with a delta-only parent (`has_storage = false`, `delta_added = s0_add`) reached
		// through a snapshot that captured its line, the dependent reads the *delta*, 5 rows with its added key,
		// and the refusal told the caller about a table the branch does not have. "Line" is the word the rest of
		// this file uses for a copy or a delta, and it is true of both.
		throw InvalidInputException("Branch \"%s\" still has branches forked from a reference that captured its "
		                            "line, and they read it at their fork pin. Drop those branches first.",
		                            name);
	}
	for (auto &snapshot : transaction_manager.snapshots.List()) {
		// the reference reads this branch's line, and dropping the branch would leave it reading the pinned
		// image of the table instead - the same name answering with a state it never captured. Measured, a
		// captured v = 111 read as v = 1 after the branch was dropped. Reference names are exact, so this
		// compares them exactly: a branch "b" and a branch "B" are two lines.
		if (snapshot.line != name) {
			continue;
		}
		throw InvalidInputException("Snapshot \"%s\" captured a line of branch \"%s\", and dropping the branch "
		                            "would leave it reading the table instead. Drop that snapshot first.",
		                            snapshot.name, name);
	}
	string result;
	for (auto &entry : existing.storage_tables) {
		// **Only the tables the feature created go with the branch.** `owned` is what records which those are,
		// and the rule it carries is stated in three other places: the field's own comment ("A table bound by
		// hand is left alone"), the replacement a second materialize makes (it drops the copy it made and not
		// the caller's table), and `git4data_unbind` (it takes the mapping back and drops nothing). Dropping a
		// branch used to be the exception, and it destroyed the caller's table: measured, a branch bound to a
		// table the caller had made and filled lost it to `git4data_drop_branch`, silently, while unbinding
		// the same branch left it - the same table protected by one path and destroyed by another, which is
		// exactly the kind of rule a caller cannot hold in their head. A table bound by hand is left where it
		// is, as a delta's two tables always have been.
		if (!entry.owned) {
			continue;
		}
		// one of the tables it made may have been renamed since, and dropping the name it was given instead
		// would leave the table behind, silently
		auto resolved = Git4DataResolveTableName(context, entry.storage, entry.oid);
		if (resolved.empty()) {
			continue;
		}
		// **A mapping can name one of the registry's own tables, and the drop below would take it with the
		// branch.** Nothing this build writes can: a binding is refused if it aims at one of them (3dn). A
		// *file* written before that could hold such a mapping, and so could one whose records came from the
		// checkpoint block - which is the case the block fallback exists for - so the drop checks what it is
		// about to drop. Refused rather than skipped, because a mapping aimed at the registry is a state to
		// clear and `git4data_unbind` clears it without dropping anything: silently leaving the table behind
		// would leave the caller with a branch they cannot drop and no sentence saying why.
		auto mapped = Git4DataResolveTableOrNull(context, resolved);
		if (mapped && Git4DataIsRegistryTable(*mapped)) {
			Git4DataCheckRegistryTarget(mapped->ParentSchema().name.GetIdentifierName(),
			                            mapped->name.GetIdentifierName());
		}
		// A table two branches were given is not this one's alone to take: dropping it would leave the
		// other reading its pinned image, silently - measured, a branch bound to another branch's copy
		// fell back from v = 111 to the pinned v = 1 when that other branch was dropped. Only the tables
		// no other branch names go with it.
		if (transaction_manager.branches.OtherBranchUsesTable(name, resolved, entry.oid)) {
			continue;
		}
		result += StringUtil::Format("DROP TABLE IF EXISTS %s;\n", Git4DataTableSQLName(resolved));
	}
	transaction_manager.branches.Drop(name);
	// and its rows, or the next open would read the branch back out of the tables: the branch, the table it
	// was given and the delta it differs by all live in one of them
	if (Git4DataRegistryTablesThere(context)) {
		auto escaped = Git4DataSQLLiteral(name);
		result += StringUtil::Format("DELETE FROM %s.%s WHERE name = '%s';", GIT4DATA_REGISTRY_SCHEMA,
		                             GIT4DATA_BRANCHES_TABLE, escaped);
		result += StringUtil::Format("DELETE FROM %s.%s WHERE branch = '%s';", GIT4DATA_REGISTRY_SCHEMA,
		                             GIT4DATA_STORAGE_TABLE, escaped);
		result += StringUtil::Format("DELETE FROM %s.%s WHERE branch = '%s';", GIT4DATA_REGISTRY_SCHEMA,
		                             GIT4DATA_DELTAS_TABLE, escaped);
	}
	if (result.empty()) {
		// a branch with no storage of its own still has to return something to execute
		return "SELECT 1 WHERE false";
	}
	return result;
}

string PragmaGit4DataBranches(ClientContext &context, const FunctionParameters &parameters) {
	// the rows come from the table function, which is where they can also be selected *from*: a pragma's
	// result cannot, so every value in this listing used to be readable by a person and asserted only by
	// its shape
	return "SELECT * FROM git4data_branches()";
}

string PragmaGit4DataLCA(ClientContext &context, const FunctionParameters &parameters) {
	auto left = parameters.values[0].ToString();
	auto right = parameters.values[1].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	auto existing = Git4DataRequiredBranch(context, transaction_manager, left);
	existing = Git4DataRequiredBranch(context, transaction_manager, right);
	string lca;
	if (!transaction_manager.branches.FindLCA(left, right, lca)) {
		// the two share no ancestor, which is what a broken lineage looks like
		return "SELECT NULL::VARCHAR AS lca WHERE false";
	}
	lca = Git4DataSQLLiteral(lca);
	return StringUtil::Format("SELECT '%s'::VARCHAR AS lca", lca);
}

string PragmaGit4DataCreateSnapshot(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	CheckGit4DataReferenceName(name);
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// The tables are created before the row for this reference is inserted - both are parts of the statement
	// this pragma returns, and they run in that order. There is nothing to copy into them: what a file written
	// by the older build carries in its metadata block is not read (see `Git4DataEnsureRegistryTables`).
	string result = Git4DataEnsureRegistryTables(context);
	// A snapshot and a branch with one name would be indistinguishable wherever a reference is named
	// without saying which kind it is - a diff, a pick, a restore, and a merge's own image clauses all
	// resolve a bare name, and they look in the snapshot registry first. Measured: with a branch and a
	// snapshot both called x, a merge of x with x compared the branch against the snapshot and reported
	// a conflict that did not exist. The two names cannot collide.
	Git4DataBranch same_named_branch;
	if (transaction_manager.branches.Get(name, same_named_branch)) {
		throw InvalidInputException("A branch named \"%s\" exists, and a snapshot of that name would be "
		                            "indistinguishable from it wherever a reference is named without its kind",
		                            name);
	}
	CheckGit4DataReferenceLimit("snapshots", transaction_manager.UserSnapshotCount(),
	                            transaction_manager.snapshot_limit);
	// A second argument names a branch, and the reference then captures that branch's own line rather
	// than the table: a branch that has been written to keeps its state in a table of its own, which a
	// pin alone cannot point at. Without it the reference is the table as this commit saw it.
	string line;
	if (parameters.values.size() > 1) {
		line = parameters.values[1].ToString();
		auto branch = Git4DataRequiredBranch(context, transaction_manager, line);
		// **A branch that inherited a line reads it, and that is a line to capture too** (increment 3bv). The
		// refusal is for a branch with no line at all - no mapping of its own and nothing inherited - which
		// reads the source at its pin, so a snapshot of the table says the same thing and the second argument
		// buys nothing. Measured before this: capturing the line of a branch that inherited one was refused
		// with "it reads the table exactly as a snapshot does", which is **false** - `b1`, forked from a branch
		// that differed by a delta, read four rows with the delta's added key present and its removed key
		// absent, while the table had five with the removed key in it - and the caller had no way to capture
		// that state: a snapshot of the branch's *parent* pinned at now would take whatever the parent did
		// since the fork, which is exactly what the branch's pin excludes.
		if (branch.storage_tables.empty() && branch.deltas.empty() && branch.base_line.empty()) {
			throw InvalidInputException("Branch \"%s\" has no line of its own, so there is nothing to capture "
			                            "- it reads the table exactly as a snapshot does",
			                            line);
		}
	}
	// Pin at the newest commit, keep everything committed so far readable
	auto pin = transaction_manager.GetLastCommit();
	auto created = Timestamp::GetCurrentTimestamp();
	if (!transaction_manager.snapshots.Create(name, pin, created, line)) {
		throw InvalidInputException("Snapshot \"%s\" already exists", name);
	}
	// The reference is uncommitted until this transaction commits, and the registries are shared memory: what
	// makes it this transaction's alone is that the row is in no committed state yet. The marker is dropped
	// when its transaction ends, so nothing has to clear it on the way out.
	transaction_manager.MarkReferenceUncommitted(
	    "snapshot", name, &MetaTransaction::Get(context),
	    DuckTransaction::Get(context, transaction_manager.GetDB()).GetTransactionId());
	// The reference is a row in the registry tables from here on: it is written with ordinary DML, in the
	// caller's transaction, so it reaches the log like any other change and the next open brings it back.
	// A checkpoint is no longer what decides whether a reference survives.
	context.client_data->git4data_added_records.push_back("snapshot\t" + name);
	result +=
	    StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', %llu, TIMESTAMP '%s', '%s');", GIT4DATA_REGISTRY_SCHEMA,
	                       GIT4DATA_SNAPSHOTS_TABLE, Git4DataSQLLiteral(name), pin, Timestamp::ToString(created), Git4DataSQLLiteral(line));
	return result;
}

//! Drop one reference from the in-memory registry and leave its row to the caller, which is what a statement the
//! feature generates for itself needs: a pragma that returns a statement cannot be called from one. The merge's
//! base is the one reference that needs this, so it is the only name accepted here - reserving the `__` prefix was
//! not enough to make this safe, because the window is reserved too and forgetting it is *not* ending it.
//! Measured: `git4data_pitr(100)` then `git4data_forget_internal('__pitr__')` left the window enabled with no
//! reference in the registry while its row stayed in `git4data.snapshots` (the listing read 0 rows, the table 1),
//! and the next checkpoint put the reference back at the *current* commit, moving the pin from 7 to 25 - so the
//! reach the window promised shrank without a word.
void PragmaGit4DataForgetInternal(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	if (name != DuckTransactionManager::MERGE_BASE_REFERENCE) {
		throw InvalidInputException(
		    "\"%s\" is not the reference this forgets, which is the base a merge of this feature makes for itself "
		    "and only out of memory. A reference a caller took is dropped with git4data_drop_snapshot, and a "
		    "point-in-time recovery window is ended with git4data_pitr_off.",
		    name);
	}
	GetGit4DataTransactionManager(context).snapshots.Drop(name);
}

string PragmaGit4DataDropSnapshot(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	// The rule the creation side states is enforced where a reference is created, and this face was the way
	// around it. Measured on the window, the one a caller can reach: `git4data_pitr(100)` then
	// `git4data_drop_snapshot('__pitr__')` dropped the reference the window holds its history with while the
	// window stayed on - the session read `true, 100, false`, the same database after a restart read
	// `false, 0, false`, because the row is what a reload decides the window by. The way out it skipped is
	// the one that ends the window on both sides: `git4data_pitr_off` clears the setting and takes the row.
	if (StringUtil::StartsWith(name, "__")) {
		throw InvalidInputException(
		    "\"%s\" is a name this feature reserves for a reference it manages itself, so it cannot be dropped "
		    "by hand. A point-in-time recovery window is ended with git4data_pitr_off.",
		    name);
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// nothing another transaction has not committed can be dropped by this one
	if (!Git4DataReferenceVisible(context, "snapshot", name) || !transaction_manager.snapshots.Drop(name)) {
		throw InvalidInputException("Snapshot \"%s\" does not exist", name);
	}
	// and the row goes with it, or the next open would bring the reference back out of the table
	if (!Git4DataRegistryTablesThere(context)) {
		return string();
	}
	auto escaped = Git4DataSQLLiteral(name);
	return StringUtil::Format("DELETE FROM %s.%s WHERE name = '%s';", GIT4DATA_REGISTRY_SCHEMA,
	                          GIT4DATA_SNAPSHOTS_TABLE, escaped);
}

string PragmaGit4DataSnapshots(ClientContext &context, const FunctionParameters &parameters) {
	// as for the branches above: the rows are the table function's, so a caller (and a test) can select them
	return "SELECT * FROM git4data_snapshots()";
}

string PragmaGit4DataState(ClientContext &context, const FunctionParameters &parameters) {
	// the row comes from the table function, which is where its values can also be selected
	return "SELECT * FROM git4data_state()";
}

//! Base data with updates skipped: what a pinned snapshot needs kept
string PragmaGit4DataProbe(ClientContext &context, const FunctionParameters &parameters) {
	// the row comes from the table function, which is where its numbers can be selected
	return StringUtil::Format("SELECT * FROM git4data_probe('%s')",
	                          Git4DataSQLLiteral(parameters.values[0].ToString()));
}

//! Every physical row including all deleted ones, to tell "the data is gone" apart from
//! "the data is there but filtered out"
string PragmaGit4DataProbeAll(ClientContext &context, const FunctionParameters &parameters) {
	// the row comes from the table function, which is where its numbers can be selected
	return StringUtil::Format("SELECT * FROM git4data_probe_all('%s')",
	                          Git4DataSQLLiteral(parameters.values[0].ToString()));
}

//! Base data with updates skipped, read at an explicitly given visibility bound. Sweeping
//! the bound reveals the effective delete and insert ids: the count changes where the bound
//! crosses an id, which is what tells the two runners apart.
string PragmaGit4DataProbeAt(ClientContext &context, const FunctionParameters &parameters) {
	return StringUtil::Format("SELECT * FROM git4data_probe_at('%s', %lld)",
	                          Git4DataSQLLiteral(parameters.values[0].ToString()),
	                          (long long)parameters.values[1].GetValue<int64_t>());
}

} // namespace duckdb
