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
};

//! Registry of pinned snapshots. Pinned bounds fold into the transaction manager's
//! lowest visibility bound, which is what keeps superseded versions alive.
class SnapshotRegistry {
public:
	//! Pin a new snapshot at the given commit id. Returns false if the name exists.
	bool Create(const string &name, transaction_t pin, timestamp_t created);
	//! Release a pinned snapshot. Returns false if the name is unknown.
	bool Drop(const string &name);
	vector<Git4DataSnapshot> List() const;
	idx_t Count() const;
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
struct Git4DataBranch {
	string name;
	//! Everything committed up to and including this commit id is this branch's base
	transaction_t pin;
	//! The branch this one was created from. Empty for a root branch.
	string parent;
	//! When the branch was taken, on the wall clock, as for a snapshot
	timestamp_t created;
};

//! Registry of data branches. A branch is a reference like a snapshot is - both pin the
//! timeline and both have to outlive the process that created them - so both fold into the
//! same retention bound and both keep their ids on disk.
class BranchRegistry {
public:
	//! Create a branch. Returns false if the name exists.
	bool Create(const string &name, transaction_t pin, const string &parent, timestamp_t created);
	//! Drop a branch. Returns false if the name is unknown, or if it still has children -
	//! removing it would leave those children's lineage dangling.
	bool Drop(const string &name);
	//! Whether dropping this name would be refused because other branches descend from it
	bool HasChildren(const string &name) const;
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
	//! Restore a branch read from disk without the checks Create performs
	void Restore(const string &name, transaction_t pin, const string &parent, timestamp_t created);

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
