//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/duck_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/enums/checkpoint_type.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {
class DuckTransactionManager;
class DuckTransaction;
struct UndoBufferProperties;

//! CleanupInfo collects transactions awaiting cleanup.
//! This ensures we can clean up after releasing the transaction lock.
struct DuckCleanupInfo {
	//! All transactions in a cleanup info share the same lowest_visibility_bound.
	VisibilityBound lowest_visibility_bound;
	//! The bound the version information has to be kept for, which also covers pinned references.
	//! Kept apart from lowest_visibility_bound because that one also decides when a committed
	//! transaction may be released.
	VisibilityBound retention_bound;
	vector<unique_ptr<DuckTransaction>> transactions;

	void Cleanup();
	bool ScheduleCleanup() noexcept;
};

//! A pinned snapshot: a named point on the commit timeline that must stay readable.
struct Git4DataSnapshot {
	string name;
	//! Everything committed up to and including this commit id stays visible
	transaction_t pin;
	//! When the reference was taken, on the wall clock. Commit ids are a counter, not a time, so a
	//! restore by time ("rewind to how it was at 14:30") has to be answered from this.
	timestamp_t created;
	//! The branch whose own line this captured, empty when it captured the table as a commit saw it.
	//! A branch that has been written to lives in a table of its own, so a pin alone cannot name what
	//! it holds; naming the branch is what makes that state readable as a reference.
	string line;
};

//! Registry of pinned snapshots. Pinned bounds fold into the transaction manager's
//! lowest visibility bound, which is what keeps superseded versions alive.
class SnapshotRegistry {
public:
	//! Pin a new snapshot at the given commit id. Returns false if the name exists. When line names a
	//! branch, the reference reads that branch's own line at the pin rather than the table.
	bool Create(const string &name, transaction_t pin, timestamp_t created, const string &line = string());
	//! Release a pinned snapshot. Returns false if the name is unknown.
	bool Drop(const string &name);
	vector<Git4DataSnapshot> List() const;
	idx_t Count() const;
	//! Look a snapshot up by name. Returns false when there is none.
	bool Get(const string &name, Git4DataSnapshot &result) const;
	//! The commit id pinned under this name, or an invalid optional when there is none
	optional_idx GetPin(const string &name) const;
	//! The lowest pinned commit id, or MAX_COMMIT_ID when nothing is pinned.
	//! Reads a single atomic so it is safe to call from noexcept contexts.
	transaction_t LowestPin() const noexcept {
		return lowest_pin.load();
	}
	//! Whether the registry changed since the last checkpoint wrote it
	bool IsDirty() const noexcept {
		return dirty.load();
	}
	void MarkClean() noexcept {
		dirty.store(false);
	}

private:
	//! Recompute the cached minimum pin. Caller must hold the lock.
	void RecomputeLowest();

private:
	mutable mutex lock;
	unordered_map<string, Git4DataSnapshot> snapshots;
	std::atomic<transaction_t> lowest_pin {MAX_COMMIT_ID};
	//! Set by Create/Drop, cleared once a checkpoint has written the registry. A checkpoint
	//! with nothing in the WAL skips its body entirely, so without this a snapshot taken on
	//! an otherwise unchanged database would never reach disk.
	std::atomic<bool> dirty {false};
};

//! A data branch: a named point on the commit timeline that also records where it came from.
//! The parent link is what turns the branches into a tree, and a tree is what makes a least
//! common ancestor computable - which diff and merge both need.
//! A table a branch was given for a source table. The oid is what tells that table from another table
//! that happens to carry the same name later: a name can be dropped and recreated, and a branch that
//! went on reading it would be reading something that has nothing to do with its line. Zero means no oid
//! was recorded, which is what a mapping read back from a file has - the catalog's oids do not survive a
//! reopen, so the check is made within a session and a table swapped while the database was closed is
//! not caught.
struct Git4DataStorageTable {
	string source;
	string storage;
	idx_t oid = 0;
	//! The source table's own oid when the mapping was made. A source table can be *renamed*, and the
	//! mapping is keyed by its name, so without this the branch would silently fall back to its pinned
	//! image and stop reading the line it was given - measured. The catalog's oids survive a rename, so
	//! a read whose name does not match can still recognise the table it was given.
	idx_t source_oid = 0;
	//! Whether `materialize` created the table rather than a caller handing one over. Only a table the
	//! feature created is the feature's to drop: a second materialize replaces the mapping, and taking
	//! the earlier copy with it is what keeps that from costing the caller a table nothing points at.
	//! A table bound by hand is left alone.
	bool owned = false;
};

//! What a branch changes about one source table when it holds no copy of it. Both tables are the
//! caller's: rows to add go into `added`, keys to remove into `removed`, with ordinary SQL.
struct Git4DataBranchDelta {
	//! The source table this delta applies to
	string source;
	//! Rows this branch has that its base does not, with the source table's columns
	string added;
	//! The keys this branch has removed from its base, with the source table's key columns
	string removed;
	//! The oids of those two tables when the delta was bound, so a read can tell them from tables that
	//! take their names after they are dropped. Zero means none was recorded - a delta read back from a
	//! file, since the catalog's oids do not survive a reopen.
	idx_t added_oid = 0;
	idx_t removed_oid = 0;
	//! The source table's own oid when the delta was bound, so the delta follows a source that is renamed
	//! rather than silently ceasing to apply
	idx_t source_oid = 0;
};

struct Git4DataBranch {
	string name;
	//! Everything committed up to and including this commit id is this branch's base
	transaction_t pin;
	//! The branch this one was created from. Empty for a root branch.
	string parent;
	//! When the branch was taken, on the wall clock, as for a snapshot
	timestamp_t created;
	//! Tables this branch has a line of its own for: the source table's name and the table backing
	//! the branch. A branch with no entry for a table is read as the pinned image; one with an entry
	//! is read as that table, which can be written to and so diverges.
	vector<Git4DataStorageTable> storage_tables;
	//! The branch whose line this one was forked from, when it was forked from a reference that
	//! captured one. The child reads that branch's storage at its pin until it has storage of its own,
	//! which is what lets a fork take a state rather than only a commit.
	string base_line;
	//! Tables this branch differs from its base by, instead of holding a copy of it: a branch with a
	//! delta reads its base minus the keys in `removed` plus the rows in `added`, so writing to it
	//! costs what the change costs rather than what the table costs.
	vector<Git4DataBranchDelta> deltas;
};

//! Registry of data branches. A branch is a reference like a snapshot is - both pin the
//! timeline and both have to outlive the process that created them - so both fold into the
//! same retention bound and both keep their ids on disk.
class BranchRegistry {
public:
	//! Create a branch. Returns false if the name exists.
	//! parent_is_snapshot says the parent names a snapshot rather than a branch: a snapshot is a commit,
	//! and a commit is something a branch can be forked from just as well as another branch can. Such a
	//! branch has no ancestor *branch*, so a walk that needs one - a merge looking for a least common
	//! ancestor - stops at it rather than inventing one.
	bool Create(const string &name, transaction_t pin, const string &parent, timestamp_t created,
	            bool parent_is_snapshot = false, const string &base_line = string());
	//! Drop a branch. Returns false if the name is unknown, or if it still has children -
	//! removing it would leave those children's lineage dangling.
	bool Drop(const string &name);
	//! Whether dropping this name would be refused because other branches descend from it
	bool HasChildren(const string &name) const;
	//! Whether any branch was forked from a reference that captured this branch's line. Such a child
	//! reads this branch's storage, so dropping it would leave the child reading a table that is gone.
	bool HasLineDependents(const string &name) const;
	vector<Git4DataBranch> List() const;
	idx_t Count() const;
	//! Look a branch up by name. Returns false when there is none.
	bool Get(const string &name, Git4DataBranch &result) const;
	//! The lowest pinned commit id, or MAX_COMMIT_ID when there is no branch.
	transaction_t LowestPin() const noexcept {
		return lowest_pin.load();
	}
	//! Name of the deepest branch that both are descended from, which may be one of them.
	//! Returns false when they share no ancestor, which is what a broken lineage looks like.
	bool FindLCA(const string &a, const string &b, string &result) const;
	//! Whether the registry changed since the last checkpoint wrote it
	bool IsDirty() const noexcept {
		return dirty.load();
	}
	void MarkClean() noexcept {
		dirty.store(false);
	}
	//! Give a branch its own table for a source table, so writes to it no longer touch the source. The
	//! oid is the table's, and is checked again on every read. `owned` says the feature created that
	//! table rather than a caller handing one over.
	void SetStorageTable(const string &branch, const string &source_table, const string &storage_table,
	                     idx_t storage_oid = 0, idx_t source_oid = 0, bool owned = false);
	//! The table a branch reads for a source table instead of its pinned image, and whether the feature
	//! created it. Returns false when the branch has none, which is also what a branch reading its base
	//! through a delta has.
	bool GetStorageTable(const string &branch, const string &source_table, Git4DataStorageTable &result) const;
	//! Forget a branch's table of its own for a source table, which is what leaves the branch reading its
	//! pinned image again. Returns false when it had no mapping for that source.
	bool ClearStorageTable(const string &branch, const string &source_table);
	//! Record that a branch differs from its base by a delta rather than by a copy of it. A branch with a
	//! table of its own for this source keeps that instead: one line, one source of truth.
	void SetDeltaTable(const string &branch, const string &source_table, const string &added, const string &removed,
	                   idx_t added_oid = 0, idx_t removed_oid = 0, idx_t source_oid = 0);
	//! Forget a branch's delta for a source table, which is what giving it a table of its own does - the
	//! copy is the line from then on, and a delta beside it would be read by nothing.
	void ClearDeltaTable(const string &branch, const string &source_table);
	//! The delta a branch differs from its base by, if it has one. Returns false when it has none, which
	//! is also what a branch with a table of its own has for that source.
	bool GetDeltaTable(const string &branch, const string &source_table, Git4DataBranchDelta &result) const;
	//! Restore a branch read from disk without the checks Create performs
	void Restore(const string &name, transaction_t pin, const string &parent, timestamp_t created,
	             const string &base_line = string());

private:
	//! Recompute the cached minimum pin. Caller must hold the lock.
	void RecomputeLowest();
	//! Ancestors of a branch, nearest first. Caller must hold the lock. False if unknown.
	bool Ancestors(const string &name, vector<string> &result) const;

private:
	mutable mutex lock;
	unordered_map<string, Git4DataBranch> branches;
	std::atomic<transaction_t> lowest_pin {MAX_COMMIT_ID};
	std::atomic<bool> dirty {false};
};

//! The Transaction Manager is responsible for creating and managing
//! transactions
class DuckTransactionManager : public TransactionManager {
public:
	explicit DuckTransactionManager(AttachedDatabase &db);
	~DuckTransactionManager() override;

public:
	static DuckTransactionManager &Get(AttachedDatabase &db);

	//! Start a new transaction
	Transaction &StartTransaction(ClientContext &context) override;
	//! Commit the given transaction
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	//! Rollback the given transaction
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	//! Bound below which row versions may be discarded. Tracks active transactions only,
	//! and also gates releasing committed transactions, so it must not be held back by
	//! anything else - a lower bound delays lock release and stalls CHECKPOINT.
	VisibilityBound LowestVisibilityBound() const {
		return lowest_visibility_bound;
	}
	//! Bound below which row versions must stay readable. Like LowestVisibilityBound but
	//! also covers pinned snapshots, which have no transaction to hold the bound down.
	VisibilityBound LowestRetentionBound() const {
		return VisibilityBound::Min(lowest_visibility_bound, PinnedBound());
	}
	//! Bound imposed by references that outlive the process - pinned snapshots and data
	//! branches - independent of active transactions. A branch is a reference in exactly the
	//! same sense, so it holds the bound down the same way.
	VisibilityBound PinnedBound() const noexcept {
		auto snapshot_pin = snapshots.LowestPin();
		auto branch_pin = branches.LowestPin();
		return VisibilityBound::Through(snapshot_pin < branch_pin ? snapshot_pin : branch_pin);
	}
	//! Pinned snapshots keep superseded versions readable
	SnapshotRegistry snapshots;

	//! The highest bound any reference is pinned at, read from the registries rather than cached: a row
	//! inserted at or after it came after every reference, so none of them sees it.
	VisibilityBound HighestPinnedBound() const;
	//! Whether no transaction that started before the last commit is still active. Such a transaction
	//! reads rows the last commit has already deleted, so it is the one reader a reference's bounds say
	//! nothing about; when there is none, a row the last commit deleted is reachable only by a
	//! reference, and that is what lets a retention pass reclaim the rest.
	bool NoTransactionBelowLastCommit() const {
		return GetLastCommit() < lowest_visibility_bound.load();
	}

	//! Caps on how many references the database may hold: -1 unlimited, 0 disabled, positive is the
	//! most. They bound how much history the reclamation has to keep. The window is not counted.
	int64_t snapshot_limit = -1;
	int64_t branch_limit = -1;
	//! Snapshots the user asked for, which is every one but the recovery window's own
	idx_t UserSnapshotCount() const;

	//! Whether no active transaction is writing. A transaction that inserts or deletes holds the
	//! vacuum lock for as long as it is open, so a vacuum lock that can be taken exclusively says
	//! there is none - and none can appear while a checkpoint holds the checkpoint lock.
	bool NoActiveWriter();
	//! Whether any reference is held: a pinned snapshot or a branch
	bool HasReferences() const {
		return snapshots.Count() > 0 || branches.Count() > 0;
	}

	//! A point-in-time recovery window: one reference that is moved forward as it falls out of the
	//! window, so the last `window` of history stays reachable without anyone remembering to take a
	//! reference. A window of zero moves it at every checkpoint, which keeps nothing.
	static constexpr const char *PITR_REFERENCE = "__pitr__";
	//! The reference a merge names the ancestor's pin with, so both diffs read the state two lines parted
	//! at. It exists for the length of one merge; a merge that fails leaves it, which is why it is not
	//! counted against the quota and not listed - it is not a reference the user asked for.
	static constexpr const char *MERGE_BASE_REFERENCE = "__merge_base__";
	void SetPitr(timestamp_t window, timestamp_t now);
	//! Put a window read from disk back in place without touching the reference it moves
	void RestorePitr(timestamp_t window) {
		pitr_enabled = true;
		pitr_window = window;
	}
	void ClearPitr();
	bool PitrEnabled() const {
		return pitr_enabled;
	}
	timestamp_t PitrWindow() const {
		return pitr_window;
	}
	//! Move the window forward if it has fallen out of it. Called before a checkpoint works out what
	//! it may reclaim, so the move is what decides the reach of that checkpoint.
	void AdvancePitr(timestamp_t now);
	//! Data branches: references with a parent link, so they can be diffed and merged
	BranchRegistry branches;
	transaction_t GetLastCommit() const {
		return last_commit;
	}
	//! The counter that hands out commit ids. A pinned snapshot records one of these, so it
	//! has to be persisted alongside the pins for the pin to still mean anything after a
	//! restart - transaction ids restart from a fixed point in every process.
	transaction_t GetStartTimestampCounter() const {
		return current_start_timestamp;
	}
	//! Continue handing out commit ids above the given value. Called when loading a database
	//! that has pinned snapshots, so their recorded ids stay below future commits.
	void SetMinimumStartTimestamp(transaction_t timestamp) {
		if (timestamp > current_start_timestamp) {
			current_start_timestamp = timestamp;
		}
	}
	optional_idx GetActiveCheckpoint() const {
		auto id = active_checkpoint.load();
		return id == 0 ? optional_idx() : optional_idx(id);
	}
	idx_t NextCheckpointId() {
		return ++next_checkpoint_id;
	}
	void SetActiveCheckpoint(idx_t checkpoint_id);
	void ResetActiveCheckpoint();

	bool IsDuckTransactionManager() override {
		return true;
	}

	//! Obtains a shared lock to the checkpoint lock
	unique_ptr<StorageLockKey> SharedCheckpointLock();
	//! Try to obtain an exclusive checkpoint lock
	unique_ptr<StorageLockKey> TryGetCheckpointLock();
	unique_ptr<StorageLockKey> TryUpgradeCheckpointLock(StorageLockKey &lock);
	unique_ptr<StorageLockKey> SharedVacuumLock();
	unique_ptr<StorageLockKey> TryGetVacuumLock();

	//! Returns the current version of the catalog (incremented whenever anything changes, not stored between restarts)
	DUCKDB_API idx_t GetCatalogVersion(Transaction &transaction);

	void PushCatalogEntry(Transaction &transaction_p, CatalogEntry &entry, data_ptr_t extra_data = nullptr,
	                      idx_t extra_data_size = 0);
	void PushAttach(Transaction &transaction_p, AttachedDatabase &db);

protected:
	struct CheckpointDecision {
		explicit CheckpointDecision(string reason_p);
		explicit CheckpointDecision(CheckpointType type);
		~CheckpointDecision();

		bool can_checkpoint;
		string reason;
		CheckpointType type;
	};

private:
	//! Generates a new commit timestamp
	transaction_t GetCommitTimestamp();
	//! Allocates the cleanup info, and reserves the space RemoveTransaction needs to re-home a transaction.
	//! RemoveTransaction is noexcept, so it cannot do this itself: it must not allocate at all. Call this with
	//! transaction_lock held, immediately before RemoveTransaction, so that a failure to allocate is reported
	//! while the transaction lists are still untouched.
	unique_ptr<DuckCleanupInfo> CreateCleanupInfo();
	//! Remove the given transaction from the list of active transactions
	unique_ptr<DuckCleanupInfo> RemoveTransaction(DuckTransaction &transaction,
	                                              unique_ptr<DuckCleanupInfo> cleanup_info) noexcept;
	//! Remove the given transaction from the list of active transactions
	unique_ptr<DuckCleanupInfo> RemoveTransaction(DuckTransaction &transaction, bool store_transaction,
	                                              unique_ptr<DuckCleanupInfo> cleanup_info) noexcept;
	//! Recompute lowest_visibility_bound over the active transactions, leaving out `exclude`, and
	//! return its index among them (their count when absent). Caller holds the transaction lock
	idx_t UpdateLowestVisibilityBound(optional_ptr<DuckTransaction> exclude) noexcept;
	//! Move the committed transactions below the cleanup info's lowest visibility bound into it.
	//! Caller holds the transaction lock; must not allocate (see CreateCleanupInfo)
	void SweepCommittedTransactions(DuckCleanupInfo &cleanup_info) noexcept;
	//! Hand a cleanup to the background cleanup thread, if it has anything to do
	void QueueCleanup(unique_ptr<DuckCleanupInfo> cleanup_info);

	//! Whether or not we can checkpoint
	CheckpointDecision CanCheckpoint(DuckTransaction &transaction, unique_ptr<StorageLockKey> &checkpoint_lock,
	                                 const UndoBufferProperties &properties);
	//! Get the checkpoint type of an automatic checkpoint
	CheckpointDecision GetCheckpointType(DuckTransaction &transaction, const UndoBufferProperties &undo_properties);

	bool HasOtherTransactions(DuckTransaction &transaction);
	void CleanupTransactions();

private:
	//! The current start timestamp used by transactions
	transaction_t current_start_timestamp;
	bool pitr_enabled = false;
	timestamp_t pitr_window;
	//! The current transaction ID used by transactions
	transaction_t current_transaction_id;
	//! The lowest bound any active transaction reads at. A version preceding it is visible to
	//! every active transaction, so whatever it supersedes can be cleaned up or compacted
	atomic<VisibilityBound> lowest_visibility_bound;
	//! The last commit timestamp
	atomic<transaction_t> last_commit;
	//! The currently active checkpoint, zero when none is running
	atomic<idx_t> active_checkpoint;
	//! Source of checkpoint identities
	atomic<idx_t> next_checkpoint_id = {0};
	//! Set of currently running transactions
	vector<unique_ptr<DuckTransaction>> active_transactions;
	//! Set of recently committed transactions
	vector<unique_ptr<DuckTransaction>> recently_committed_transactions;
	//! The lock used for transaction operations
	mutex transaction_lock;
	//! The checkpoint lock
	StorageLock checkpoint_lock;
	//! The vacuum lock - necessary to start vacuum operations
	StorageLock vacuum_lock;
	//! Lock necessary to start transactions only - used by FORCE CHECKPOINT to prevent new transactions from starting
	mutex start_transaction_lock;

	atomic<idx_t> last_uncommitted_catalog_version = {TRANSACTION_ID_START};
	idx_t last_committed_version = 0;

	//! Only one cleanup can be active at any time.
	mutex cleanup_lock;
	//! Changes to the cleanup queue must be synchronized.
	mutex cleanup_queue_lock;
	//! Cleanups have to happen in-order.
	//! E.g., if one transaction drops a table, and another creates a table,
	//! inverting the cleanup order can result in catalog errors.
	queue<unique_ptr<DuckCleanupInfo>> cleanup_queue;

protected:
	virtual void OnCommitCheckpointDecision(const CheckpointDecision &decision, DuckTransaction &transaction) {
	}
};

} // namespace duckdb
