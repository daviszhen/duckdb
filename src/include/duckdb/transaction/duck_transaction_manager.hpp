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

namespace duckdb {
class DuckTransactionManager;
class DuckTransaction;
struct UndoBufferProperties;

//! CleanupInfo collects transactions awaiting cleanup.
//! This ensures we can clean up after releasing the transaction lock.
struct DuckCleanupInfo {
	//! All transactions in a cleanup info share the same lowest_visibility_bound.
	VisibilityBound lowest_visibility_bound;
	vector<unique_ptr<DuckTransaction>> transactions;

	void Cleanup();
	bool ScheduleCleanup() noexcept;
};

//! A pinned snapshot: a named point on the commit timeline that must stay readable.
struct Git4DataSnapshot {
	string name;
	//! Everything committed up to and including this commit id stays visible
	transaction_t pin;
};

//! Registry of pinned snapshots. Pinned bounds fold into the transaction manager's
//! lowest visibility bound, which is what keeps superseded versions alive.
class SnapshotRegistry {
public:
	//! Pin a new snapshot at the given commit id. Returns false if the name exists.
	bool Create(const string &name, transaction_t pin);
	//! Release a pinned snapshot. Returns false if the name is unknown.
	bool Drop(const string &name);
	vector<Git4DataSnapshot> List() const;
	idx_t Count() const;
	//! The lowest pinned commit id, or MAX_COMMIT_ID when nothing is pinned.
	//! Reads a single atomic so it is safe to call from noexcept contexts.
	transaction_t LowestPin() const noexcept {
		return lowest_pin.load();
	}

private:
	//! Recompute the cached minimum pin. Caller must hold the lock.
	void RecomputeLowest();

private:
	mutable mutex lock;
	unordered_map<string, transaction_t> snapshots;
	std::atomic<transaction_t> lowest_pin {MAX_COMMIT_ID};
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
	//! Bound imposed by pinned snapshots, independent of active transactions
	VisibilityBound PinnedBound() const noexcept {
		return VisibilityBound::Through(snapshots.LowestPin());
	}
	//! Pinned snapshots keep superseded versions readable
	SnapshotRegistry snapshots;
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
