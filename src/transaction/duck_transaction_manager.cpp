#include "duckdb/transaction/duck_transaction_manager.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "duckdb/main/client_data.hpp"

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/transaction_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/checkpoint/checkpoint_options.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

bool SnapshotRegistry::Create(const string &name, transaction_t pin, timestamp_t created, const string &line) {
	lock_guard<mutex> guard(lock);
	if (snapshots.find(name) != snapshots.end()) {
		return false;
	}
	snapshots.emplace(name, Git4DataSnapshot {name, pin, created, line});
	RecomputeLowest();
	dirty.store(true);
	return true;
}

void SnapshotRegistry::Clear() {
	lock_guard<mutex> guard(lock);
	snapshots.clear();
	RecomputeLowest();
	dirty.store(true);
}

bool SnapshotRegistry::Drop(const string &name) {
	lock_guard<mutex> guard(lock);
	auto entry = snapshots.find(name);
	if (entry == snapshots.end()) {
		return false;
	}
	snapshots.erase(entry);
	RecomputeLowest();
	dirty.store(true);
	return true;
}

vector<Git4DataSnapshot> SnapshotRegistry::List() const {
	lock_guard<mutex> guard(lock);
	vector<Git4DataSnapshot> result;
	result.reserve(snapshots.size());
	for (auto &entry : snapshots) {
		result.push_back(entry.second);
	}
	return result;
}

optional_idx SnapshotRegistry::GetPin(const string &name) const {
	lock_guard<mutex> guard(lock);
	auto entry = snapshots.find(name);
	if (entry == snapshots.end()) {
		return optional_idx();
	}
	return optional_idx(entry->second.pin);
}

bool SnapshotRegistry::Get(const string &name, Git4DataSnapshot &result) const {
	lock_guard<mutex> guard(lock);
	auto entry = snapshots.find(name);
	if (entry == snapshots.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

idx_t SnapshotRegistry::Count() const {
	lock_guard<mutex> guard(lock);
	return snapshots.size();
}

bool BranchRegistry::Create(const string &name, transaction_t pin, const string &parent, timestamp_t created,
                            bool parent_is_snapshot, const string &base_line) {
	lock_guard<mutex> guard(lock);
	if (branches.find(name) != branches.end()) {
		return false;
	}
	if (!parent.empty() && !parent_is_snapshot && branches.find(parent) == branches.end()) {
		// a branch has to descend from something that is there, or its lineage cannot be walked
		return false;
	}
	branches.emplace(name, Git4DataBranch {name, pin, parent, created, {}, base_line, {}});
	RecomputeLowest();
	dirty.store(true);
	return true;
}

void BranchRegistry::SetStorageTable(const string &branch, const string &source_table, const string &storage_table,
                                     idx_t storage_oid, idx_t source_oid, bool owned) {
	lock_guard<mutex> guard(lock);
	auto entry = branches.find(branch);
	if (entry == branches.end()) {
		return;
	}
	for (auto &existing : entry->second.storage_tables) {
		if (StringUtil::CIEquals(existing.source, source_table)) {
			existing.storage = storage_table;
			existing.oid = storage_oid;
			existing.source_oid = source_oid;
			existing.owned = owned;
			dirty.store(true);
			return;
		}
	}
	entry->second.storage_tables.push_back(
	    Git4DataStorageTable {source_table, storage_table, storage_oid, source_oid, owned});
	dirty.store(true);
}

bool BranchRegistry::ClearStorageTable(const string &branch, const string &source_table) {
	lock_guard<mutex> guard(lock);
	auto entry = branches.find(branch);
	if (entry == branches.end()) {
		return false;
	}
	auto &storage_tables = entry->second.storage_tables;
	for (idx_t i = 0; i < storage_tables.size(); i++) {
		if (!StringUtil::CIEquals(storage_tables[i].source, source_table)) {
			continue;
		}
		storage_tables.erase(storage_tables.begin() + NumericCast<int64_t>(i));
		dirty.store(true);
		return true;
	}
	return false;
}

bool BranchRegistry::GetStorageTable(const string &branch, const string &source_table, Git4DataStorageTable &result,
                                     idx_t source_oid) const {
	lock_guard<mutex> guard(lock);
	auto entry = branches.find(branch);
	if (entry == branches.end()) {
		return false;
	}
	for (auto &existing : entry->second.storage_tables) {
		if (StringUtil::CIEquals(existing.source, source_table) ||
		    (source_oid != 0 && existing.source_oid == source_oid)) {
			result = existing;
			return true;
		}
	}
	return false;
}

bool BranchRegistry::ClearDeltaTable(const string &branch, const string &source_table) {
	lock_guard<mutex> guard(lock);
	auto entry = branches.find(branch);
	if (entry == branches.end()) {
		return false;
	}
	auto &deltas = entry->second.deltas;
	for (idx_t i = 0; i < deltas.size(); i++) {
		if (!StringUtil::CIEquals(deltas[i].source, source_table)) {
			continue;
		}
		deltas.erase(deltas.begin() + NumericCast<int64_t>(i));
		dirty.store(true);
		return true;
	}
	return false;
}

bool BranchRegistry::GetDeltaTable(const string &branch, const string &source_table, Git4DataBranchDelta &result,
                                   idx_t source_oid) const {
	lock_guard<mutex> guard(lock);
	auto entry = branches.find(branch);
	if (entry == branches.end()) {
		return false;
	}
	for (auto &delta : entry->second.deltas) {
		if (StringUtil::CIEquals(delta.source, source_table) || (source_oid != 0 && delta.source_oid == source_oid)) {
			result = delta;
			return true;
		}
	}
	return false;
}

void BranchRegistry::SetDeltaTable(const string &branch, const string &source_table, const string &added,
                                   const string &removed, idx_t added_oid, idx_t removed_oid, idx_t source_oid) {
	lock_guard<mutex> guard(lock);
	auto entry = branches.find(branch);
	if (entry == branches.end()) {
		return;
	}
	for (auto &delta : entry->second.deltas) {
		if (StringUtil::CIEquals(delta.source, source_table)) {
			delta.added = added;
			delta.removed = removed;
			delta.added_oid = added_oid;
			delta.removed_oid = removed_oid;
			delta.source_oid = source_oid;
			dirty.store(true);
			return;
		}
	}
	entry->second.deltas.push_back(
	    Git4DataBranchDelta {source_table, added, removed, added_oid, removed_oid, source_oid});
	dirty.store(true);
}

void BranchRegistry::Clear() {
	lock_guard<mutex> guard(lock);
	branches.clear();
	RecomputeLowest();
	dirty.store(true);
}

void BranchRegistry::Restore(const string &name, transaction_t pin, const string &parent, timestamp_t created,
                             const string &base_line) {
	lock_guard<mutex> guard(lock);
	branches[name] = Git4DataBranch {name, pin, parent, created, {}, base_line, {}};
	RecomputeLowest();
}

bool BranchRegistry::HasChildren(const string &name) const {
	lock_guard<mutex> guard(lock);
	for (auto &entry : branches) {
		if (entry.second.parent == name) {
			return true;
		}
	}
	return false;
}

bool BranchRegistry::HasLineDependents(const string &name) const {
	lock_guard<mutex> guard(lock);
	for (auto &entry : branches) {
		if (entry.second.base_line == name) {
			return true;
		}
	}
	return false;
}

bool BranchRegistry::OtherBranchUsesTable(const string &name, const string &table, idx_t oid) const {
	lock_guard<mutex> guard(lock);
	for (auto &entry : branches) {
		if (entry.first == name) {
			continue;
		}
		for (auto &storage : entry.second.storage_tables) {
			// the table it was given, and the table it is *about*: a branch's source can be another
			// branch's copy, and dropping that leaves the mapping pointing at nothing
			if (StringUtil::CIEquals(storage.storage, table) || (oid != 0 && storage.oid == oid) ||
			    StringUtil::CIEquals(storage.source, table) || (oid != 0 && storage.source_oid == oid)) {
				return true;
			}
		}
		for (auto &delta : entry.second.deltas) {
			if (StringUtil::CIEquals(delta.added, table) || StringUtil::CIEquals(delta.removed, table) ||
			    StringUtil::CIEquals(delta.source, table) || (oid != 0 && delta.source_oid == oid)) {
				return true;
			}
		}
	}
	return false;
}

bool BranchRegistry::Drop(const string &name) {
	lock_guard<mutex> guard(lock);
	auto entry = branches.find(name);
	if (entry == branches.end()) {
		return false;
	}
	for (auto &other : branches) {
		if (other.second.parent == name) {
			// a descendant still refers to this name to reach its ancestors
			return false;
		}
	}
	branches.erase(entry);
	RecomputeLowest();
	dirty.store(true);
	return true;
}

vector<Git4DataBranch> BranchRegistry::List() const {
	lock_guard<mutex> guard(lock);
	vector<Git4DataBranch> result;
	result.reserve(branches.size());
	for (auto &entry : branches) {
		result.push_back(entry.second);
	}
	return result;
}

idx_t BranchRegistry::Count() const {
	lock_guard<mutex> guard(lock);
	return branches.size();
}

bool BranchRegistry::Get(const string &name, Git4DataBranch &result) const {
	lock_guard<mutex> guard(lock);
	auto entry = branches.find(name);
	if (entry == branches.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

bool BranchRegistry::Ancestors(const string &name, vector<string> &result) const {
	// caller holds the lock
	auto current = name;
	while (!current.empty()) {
		auto entry = branches.find(current);
		if (entry == branches.end()) {
			// A parent that is not a branch is where the walk ends: a branch may descend from a
			// snapshot, which is a commit a line can be forked from, and a snapshot is not part of the
			// branch ancestry. The name is still recorded on the branch, so the listing says what it
			// came from even though the ancestry stops here.
			break;
		}
		result.push_back(current);
		current = entry->second.parent;
	}
	return !result.empty();
}

bool BranchRegistry::FindLCA(const string &a, const string &b, string &result) const {
	lock_guard<mutex> guard(lock);
	vector<string> ancestors_a;
	if (!Ancestors(a, ancestors_a)) {
		return false;
	}
	vector<string> ancestors_b;
	if (!Ancestors(b, ancestors_b)) {
		return false;
	}
	// Both lists are nearest-first, and in a tree the common ancestors form one chain from the
	// least common ancestor up to the root. So the first name of b's list that also appears in
	// a's is the deepest one they share.
	for (auto &candidate : ancestors_b) {
		for (auto &ancestor : ancestors_a) {
			if (ancestor == candidate) {
				result = candidate;
				return true;
			}
		}
	}
	return false;
}

void BranchRegistry::RecomputeLowest() {
	transaction_t lowest = MAX_COMMIT_ID;
	for (auto &entry : branches) {
		if (entry.second.pin < lowest) {
			lowest = entry.second.pin;
		}
	}
	lowest_pin.store(lowest);
}

void SnapshotRegistry::RecomputeLowest() {
	transaction_t lowest = MAX_COMMIT_ID;
	for (auto &entry : snapshots) {
		if (entry.second.pin < lowest) {
			lowest = entry.second.pin;
		}
	}
	lowest_pin.store(lowest);
}

static ErrorData BuildAutocheckpointError(AttachedDatabase &db, const std::exception &ex) {
	ErrorData original(ex);
	string recovery = db.IsInitialDatabase() ? "Reopen the database instance to recover."
	                                         : "Detach and reattach the database to recover.";
	string msg = StringUtil::Format("Transaction COMMIT succeeded and is durable, but the autocheckpoint failed. %s %s",
	                                recovery, original.RawMessage());
	return ErrorData(original.Type(), msg);
}

void DuckCleanupInfo::Cleanup() {
	for (auto &transaction : transactions) {
		if (transaction->awaiting_cleanup) {
			transaction->Cleanup(lowest_visibility_bound, retention_bound);
		}
	}
}

bool DuckCleanupInfo::ScheduleCleanup() noexcept {
	return !transactions.empty();
}

DuckTransactionManager::DuckTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
	// the first timestamp a real transaction can draw: one past the bootstrap stamp, which is also
	// the bound the system transaction reads at
	current_start_timestamp = SYSTEM_TRANSACTION_TIMESTAMP + 1;
	// transaction ID starts very high:
	// it should be much higher than the current start timestamp
	// if transaction_id < start_timestamp for any set of active transactions
	// uncommitted data could be read by
	current_transaction_id = TRANSACTION_ID_START;
	lowest_visibility_bound = VisibilityBound::IncludingUncommitted();
	active_checkpoint = 0;
	if (!db.GetCatalog().IsDuckCatalog()) {
		// Specifically the StorageManager of the DuckCatalog is relied on, with `db.GetStorageManager`
		throw InternalException("DuckTransactionManager should only be created together with a DuckCatalog");
	}
}

DuckTransactionManager::~DuckTransactionManager() {
}

DuckTransactionManager &DuckTransactionManager::Get(AttachedDatabase &db) {
	auto &transaction_manager = TransactionManager::Get(db);
	if (!transaction_manager.IsDuckTransactionManager()) {
		throw InternalException("Calling DuckTransactionManager::Get on non-DuckDB transaction manager");
	}
	return reinterpret_cast<DuckTransactionManager &>(transaction_manager);
}

Transaction &DuckTransactionManager::StartTransaction(ClientContext &context) {
	// obtain the transaction lock during this function
	auto &meta_transaction = MetaTransaction::Get(context);
	unique_lock<mutex> start_lock(start_transaction_lock, std::defer_lock);
	if (!meta_transaction.IsReadOnly()) {
		start_lock.lock();
	}
	lock_guard<mutex> lock(transaction_lock);
	if (current_start_timestamp >= TRANSACTION_ID_START) { // LCOV_EXCL_START
		throw InternalException("Cannot start more transactions, ran out of "
		                        "transaction identifiers!");
	} // LCOV_EXCL_STOP

	// obtain the start time and transaction ID of this transaction
	transaction_t start_time = current_start_timestamp++;
	transaction_t transaction_id = current_transaction_id++;
	// the transaction sees its own writes, and every commit before its start time
	SnapshotView view(transaction_id, VisibilityBound::Before(start_time));
	if (active_transactions.empty()) {
		lowest_visibility_bound = view.visibility_bound;
	}

	// create the actual transaction
	auto transaction = make_uniq<DuckTransaction>(*this, context, start_time, view, last_committed_version);
	auto &transaction_ref = *transaction;

	// store it in the set of active transactions
	active_transactions.push_back(std::move(transaction));
	return transaction_ref;
}

void DuckTransactionManager::SetActiveCheckpoint(idx_t checkpoint_id) {
	active_checkpoint = checkpoint_id;
}

void DuckTransactionManager::ResetActiveCheckpoint() {
	active_checkpoint = 0;
}

DuckTransactionManager::CheckpointDecision::CheckpointDecision(string reason_p)
    : can_checkpoint(false), reason(std::move(reason_p)) {
}

DuckTransactionManager::CheckpointDecision::CheckpointDecision(CheckpointType type) : can_checkpoint(true), type(type) {
}

DuckTransactionManager::CheckpointDecision::~CheckpointDecision() {
}

bool DuckTransactionManager::HasOtherTransactions(DuckTransaction &transaction) {
	for (auto &active_transaction : active_transactions) {
		if (!RefersToSameObject(*active_transaction, transaction)) {
			return true;
		}
	}
	return false;
}

DuckTransactionManager::CheckpointDecision
DuckTransactionManager::CanCheckpoint(DuckTransaction &transaction, unique_ptr<StorageLockKey> &lock,
                                      const UndoBufferProperties &undo_properties) {
	if (db.IsSystem()) {
		return CheckpointDecision("system transaction");
	}
	if (transaction.IsReadOnly()) {
		return CheckpointDecision("transaction is read-only");
	}
	auto &storage_manager = db.GetStorageManager();
	if (!storage_manager.IsLoaded()) {
		return CheckpointDecision("cannot checkpoint while loading");
	}
	if (!transaction.AutomaticCheckpoint(db, undo_properties)) {
		return CheckpointDecision("no reason to automatically checkpoint");
	}
	if (Settings::Get<DebugSkipCheckpointOnCommitSetting>(db.GetDatabase())) {
		return CheckpointDecision("checkpointing on commit disabled through configuration");
	}
	// try to lock the checkpoint lock
	lock = transaction.TryGetCheckpointLock();
	if (!lock) {
		return CheckpointDecision("Failed to obtain checkpoint lock - another thread is writing/checkpointing or "
		                          "another read transaction relies on data that is not yet committed");
	}
	return CheckpointDecision(CheckpointType::FULL_CHECKPOINT);
}

DuckTransactionManager::CheckpointDecision
DuckTransactionManager::GetCheckpointType(DuckTransaction &transaction, const UndoBufferProperties &undo_properties) {
	auto &storage_manager = db.GetStorageManager();
	auto checkpoint_type = CheckpointType::FULL_CHECKPOINT;
	bool has_other_transactions = HasOtherTransactions(transaction);
	if (has_other_transactions) {
		if (undo_properties.has_updates || undo_properties.has_dropped_entries) {
			// if we have made updates/catalog changes in this transaction we cannot checkpoint
			// in the presence of other transactions
			string other_transactions;
			for (auto &active_transaction : active_transactions) {
				if (!RefersToSameObject(*active_transaction, transaction)) {
					if (!other_transactions.empty()) {
						other_transactions += ", ";
					}
					other_transactions += "[" + to_string(active_transaction->GetTransactionId()) + "]";
				}
			}
			if (!other_transactions.empty()) {
				// there are other transactions!
				// these active transactions might need data from BEFORE this transaction
				// we might need to change our strategy here based on what changes THIS transaction has made
				if (undo_properties.has_dropped_entries) {
					// this transaction has changed the catalog - we cannot checkpoint
					return CheckpointDecision(
					    "Transaction has dropped catalog entries and there are other transactions "
					    "active\nActive transactions: " +
					    other_transactions);
				}
				// this transaction has performed updates - we cannot checkpoint
				return CheckpointDecision(
				    "Transaction has performed updates and there are other transactions active\nActive transactions: " +
				    other_transactions);
			}
		}
		// otherwise - we need to do a concurrent checkpoint
		checkpoint_type = CheckpointType::CONCURRENT_CHECKPOINT;
	}
	if (storage_manager.InMemory() && !storage_manager.CompressionIsEnabled()) {
		if (checkpoint_type == CheckpointType::CONCURRENT_CHECKPOINT) {
			return CheckpointDecision("Cannot vacuum, and compression is disabled for in-memory table");
		}
		return CheckpointDecision(CheckpointType::VACUUM_ONLY);
	}
	return CheckpointDecision(checkpoint_type);
}

VisibilityBound DuckTransactionManager::HighestPinnedBound() const {
	transaction_t highest = 0;
	bool any = false;
	for (auto &snapshot : snapshots.List()) {
		highest = any ? MaxValue(highest, snapshot.pin) : snapshot.pin;
		any = true;
	}
	for (auto &branch : branches.List()) {
		highest = any ? MaxValue(highest, branch.pin) : branch.pin;
		any = true;
	}
	if (!any) {
		// nothing pinned: leave the ceiling to the active transactions
		return VisibilityBound::Before(0);
	}
	// a reference's pin is inclusive, so the first id it cannot see is one past it
	return VisibilityBound::Through(highest);
}

bool DuckTransactionManager::NoActiveWriter() {
	auto lock = vacuum_lock.TryGetExclusiveLock();
	return lock != nullptr;
}

idx_t DuckTransactionManager::UserSnapshotCount() const {
	auto count = snapshots.Count();
	if (pitr_enabled && count > 0) {
		// the recovery window holds one reference of its own, which the user did not ask for
		count--;
	}
	Git4DataSnapshot merge_base;
	if (count > 0 && snapshots.Get(MERGE_BASE_REFERENCE, merge_base)) {
		// nor is the base a merge reads against, and a merge that failed leaves it behind
		count--;
	}
	return count;
}

string DuckTransactionManagerPitrLine(timestamp_t window) {
	return StringUtil::Format("window_micros=%lld", window.value);
}

void DuckTransactionManager::SetPitr(timestamp_t window, timestamp_t now, const string &line) {
	pitr_enabled = true;
	pitr_window = window;
	snapshots.Drop(PITR_REFERENCE);
	snapshots.Create(PITR_REFERENCE, last_commit, now, line);
}

void DuckTransactionManager::ClearPitr() {
	pitr_enabled = false;
	snapshots.Drop(PITR_REFERENCE);
}

void DuckTransactionManager::AdvancePitr(timestamp_t now) {
	if (!pitr_enabled) {
		return;
	}
	Git4DataSnapshot current;
	if (!snapshots.Get(PITR_REFERENCE, current)) {
		// the reference was dropped by hand - put it back at the current commit, keeping whatever the
		// window's length was written onto it: the move below and the checkpoint both carry that line on
		snapshots.Create(PITR_REFERENCE, last_commit, now, DuckTransactionManagerPitrLine(pitr_window));
		return;
	}
	if (now.value - current.created.value < pitr_window.value) {
		// still inside the window: leave it where it is, which is what keeps that history readable
		return;
	}
	// out of the window, so the state it holds is no longer promised - move the window forward, carrying the
	// line: it is what says how long the window is, and a moved reference that lost it would come back from
	// a crash with a window of no length at all, which keeps nothing
	auto line = current.line;
	snapshots.Drop(PITR_REFERENCE);
	snapshots.Create(PITR_REFERENCE, last_commit, now, line);
}

void DuckTransactionManager::Checkpoint(ClientContext &context, bool force) {
	if (ValidChecker::IsInvalidated(db)) {
		throw IOException("%s", ValidChecker::InvalidatedMessage(db));
	}
	// before the retention bound is worked out: moving the window is what decides what this
	// checkpoint may reclaim
	AdvancePitr(Timestamp::GetCurrentTimestamp());
	auto &storage_manager = db.GetStorageManager();
	auto current = Transaction::TryGet(context, db);
	if (current) {
		if (force) {
			throw TransactionException(
			    "Cannot FORCE CHECKPOINT: the current transaction has been started for this database");
		} else {
			auto &duck_transaction = current->Cast<DuckTransaction>();
			if (duck_transaction.ChangesMade()) {
				throw TransactionException("Cannot CHECKPOINT: the current transaction has transaction local changes");
			}
		}
	}

	unique_ptr<StorageLockKey> lock;
	if (!force) {
		// not a force checkpoint
		// try to get the checkpoint lock
		lock = checkpoint_lock.TryGetExclusiveLock();
		if (!lock) {
			// we could not manage to get the lock - cancel
			throw TransactionException("Cannot CHECKPOINT: there are other write transactions active. Try using FORCE "
			                           "CHECKPOINT to wait until all active transactions are finished");
		}

	} else {
		// force checkpoint - wait to get an exclusive lock
		// grab the start_transaction_lock to prevent new transactions from starting
		lock_guard<mutex> start_lock(start_transaction_lock);
		// wait until any active transactions are finished
		while (!lock) {
			context.InterruptCheck();
			lock = checkpoint_lock.TryGetExclusiveLock();
		}
	}
	CheckpointOptions options;
	options.retention_bound = LowestRetentionBound();
	options.id_retention_bound = PinnedBound();
	if (snapshots.IsDirty() || branches.IsDirty()) {
		// the registry changed and is not on disk yet; a checkpoint that skips its body
		// because the WAL is empty would leave it there
		options.action = CheckpointAction::ALWAYS_CHECKPOINT;
	}
	if (GetLastCommit() >= LowestRetentionBound()) {
		// we cannot do a full checkpoint if any transaction needs to read old data
		options.type = CheckpointType::CONCURRENT_CHECKPOINT;
		if (HasReferences()) {
			// a reference is holding the retention bound down, and it has no transaction behind it:
			// with nothing writing, vacuum can still reclaim what no reference can reach
			options.can_vacuum_under_reference = NoActiveWriter();
		}
	}

	storage_manager.CreateCheckpoint(context, options);
}

unique_ptr<StorageLockKey> DuckTransactionManager::SharedCheckpointLock() {
	return checkpoint_lock.GetSharedLock();
}

unique_ptr<StorageLockKey> DuckTransactionManager::TryUpgradeCheckpointLock(StorageLockKey &lock) {
	return checkpoint_lock.TryUpgradeCheckpointLock(lock);
}

unique_ptr<StorageLockKey> DuckTransactionManager::TryGetCheckpointLock() {
	return checkpoint_lock.TryGetExclusiveLock();
}

unique_ptr<StorageLockKey> DuckTransactionManager::SharedVacuumLock() {
	return vacuum_lock.GetSharedLock();
}

unique_ptr<StorageLockKey> DuckTransactionManager::TryGetVacuumLock() {
	return vacuum_lock.TryGetExclusiveLock();
}

transaction_t DuckTransactionManager::GetCommitTimestamp() {
	return current_start_timestamp++;
}

void DuckTransactionManager::CleanupTransactions() {
	lock_guard<mutex> c_lock(cleanup_lock);
	while (true) {
		unique_ptr<DuckCleanupInfo> top_cleanup_info;
		{
			lock_guard<mutex> q_lock(cleanup_queue_lock);
			if (cleanup_queue.empty()) {
				// all transactions have been cleaned up - done
				return;
			}
			top_cleanup_info = std::move(cleanup_queue.front());
			cleanup_queue.pop();
		}
		if (top_cleanup_info) {
			top_cleanup_info->Cleanup();
		}
	}
}

ErrorData DuckTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction_p) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();
	// flush the transaction-local blocks of bulk appends before taking any commit locks (see PreFlushOptimisticBlocks)
	ErrorData error = transaction.PreFlushOptimisticBlocks(db);
	unique_lock<mutex> t_lock(transaction_lock);
	if (!db.IsSystem() && !db.IsTemporary()) {
		if (transaction.ChangesMade()) {
			if (transaction.IsReadOnly()) {
				throw InternalException("Attempting to commit a transaction that is read-only but has made changes - "
				                        "this should not be possible");
			}
		}
	}

	// check if we can checkpoint
	unique_ptr<StorageLockKey> lock;
	auto undo_properties = transaction.GetUndoProperties();
	auto checkpoint_decision = CanCheckpoint(transaction, lock, undo_properties);
	unique_lock<mutex> held_wal_lock;
	unique_ptr<StorageCommitState> commit_state;
	bool skip_wal_write_due_to_checkpoint = false;
	bool wal_written = false;
	if (checkpoint_decision.can_checkpoint) {
		// we can perform an automatic checkpoint
		// we have two options:
		// either we write to the WAL, in which case we can perform concurrent commits while running
		// OR we skip writing to the WAL, in which case we cannot perform concurrent commits
		// the reason for this is that if we don't write this transactions' changes to the WAL
		// any failure during checkpoint will cause this transactions' changes to be lost,
		// while later concurrent commits will not be
		// this can cause undefined state, as those commits were made assuming this one was already committed
		if (undo_properties.estimated_size >= Settings::Get<AutoCheckpointSkipWalThresholdSetting>(context)) {
			skip_wal_write_due_to_checkpoint = true;
		}
	}
	bool should_write_to_wal = !error.HasError() && transaction.ShouldWriteToWAL(db);
	if (should_write_to_wal) {
		auto &storage_manager = db.GetStorageManager().Cast<SingleFileStorageManager>();
		// if we are committing changes and we are not doing a "checkpoint instead of WAL write"
		// we need to write to the WAL to make the changes durable
		// since WAL writes can take a long time - we grab the WAL lock here and unlock the transaction lock
		// read-only transactions can bypass this branch and start/commit while the WAL write is happening
		// unlock the transaction lock while we write to the WAL
		// note: we can only drop the transaction lock if we are NOT checkpointing
		// if we are checkpointing, we have already made certain decisions (e.g. the CheckpointType)
		t_lock.unlock();
		// grab the WAL lock and hold it until the entire commit is finished
		held_wal_lock = storage_manager.GetWALLock();

		// Commit the changes to the WAL.
		if (!skip_wal_write_due_to_checkpoint) {
			error = transaction.WriteToWAL(context, db, commit_state);
			wal_written = true;
		}

		// after we finish writing to the WAL we grab the transaction lock again
		t_lock.lock();
	}
	if (!error.HasError() && checkpoint_decision.can_checkpoint) {
		// now that we have the transaction lock again, new transactions can't start
		// figure out the checkpoint type now
		checkpoint_decision = GetCheckpointType(transaction, undo_properties);
		if (should_write_to_wal && skip_wal_write_due_to_checkpoint && !checkpoint_decision.can_checkpoint) {
			// we have not written to the WAL but we have now realized we can't checkpoint after all
			// in order to commit we need backpeddle and write to the WAL after all
			D_ASSERT(held_wal_lock.owns_lock());
			// unlock the transaction lock while we are writing to the WAL
			t_lock.unlock();
			error = transaction.WriteToWAL(context, db, commit_state);
			wal_written = true;
			t_lock.lock();
			skip_wal_write_due_to_checkpoint = false;
		}
	}
	// in-memory databases don't have a WAL - we estimate how large their changeset is based on the undo properties
	if (!db.IsSystem()) {
		auto &storage_manager = db.GetStorageManager();
		if (storage_manager.InMemory() || db.GetRecoveryMode() == RecoveryMode::NO_WAL_WRITES) {
			storage_manager.AddWALSize(undo_properties.estimated_size);
		}
	}
	// obtain a commit id for the transaction
	CommitInfo info;
	info.commit_id = GetCommitTimestamp();

	// commit the UndoBuffer of the transaction
	if (!error.HasError()) {
		if (HasOtherTransactions(transaction)) {
			info.active_transactions = ActiveTransactionState::OTHER_TRANSACTIONS;
		} else {
			info.active_transactions = ActiveTransactionState::NO_OTHER_TRANSACTIONS;
		}
		error = transaction.Commit(db, info, std::move(commit_state));
	}

	if (error.HasError()) {
		DUCKDB_LOG(context, TransactionLogType, db, "Rollback (after failed commit)", info.commit_id);

		// COMMIT not successful: ROLLBACK.
		checkpoint_decision = CheckpointDecision(error.Message());
		transaction.commit_id = 0;

		auto rollback_error = transaction.Rollback();
		if (rollback_error.HasError()) {
			throw FatalException(
			    "Failed to rollback transaction. Cannot continue operation.\nOriginal Error: %s\nRollback Error: %s",
			    error.Message(), rollback_error.Message());
		}
	} else {
		DUCKDB_LOG(context, TransactionLogType, db, "Commit", info.commit_id);
		last_commit = info.commit_id;

		// check if catalog changes were made
		if (transaction.catalog_version >= TRANSACTION_ID_START) {
			transaction.catalog_version = ++last_committed_version;
		}
	}
	OnCommitCheckpointDecision(checkpoint_decision, transaction);

	if (!checkpoint_decision.can_checkpoint && lock) {
		// we won't checkpoint after all due to an error during commit: unlock the checkpoint lock again
		skip_wal_write_due_to_checkpoint = false;
		lock.reset();
	}

	// commit successful: remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	bool store_transaction = undo_properties.has_updates || undo_properties.has_index_deletes ||
	                         undo_properties.has_catalog_changes || error.HasError();

	// Remove the transaction from the list of active transactions and gather cleanup information.
	QueueCleanup(RemoveTransaction(transaction, store_transaction, CreateCleanupInfo()));

	// We do not need to hold the transaction lock during cleanup of transactions,
	// as they (1) have been removed, or (2) enter cleanup_info.
	t_lock.unlock();
	// if we have skipped the WAL write due to checkpoint, we keep the WAL lock while checkpointing
	// this prevents any concurrent transactions from happening during this time
	if (!skip_wal_write_due_to_checkpoint && held_wal_lock.owns_lock()) {
		held_wal_lock.unlock();
	}

	CleanupTransactions();

	// now perform a checkpoint if (1) we are able to checkpoint, and (2) the WAL has reached sufficient size to
	// checkpoint
	if (checkpoint_decision.can_checkpoint) {
		if (!lock || lock->GetType() != StorageLockType::EXCLUSIVE) {
			throw InternalException("Checkpointing requires an exclusive lock to be held");
		}
		// we can unlock the transaction lock while checkpointing
		// checkpoint the database to disk
		CheckpointOptions options;
		options.action = CheckpointAction::ALWAYS_CHECKPOINT;
		options.type = checkpoint_decision.type;
		if (options.type == CheckpointType::CONCURRENT_CHECKPOINT && HasReferences()) {
			// a reference makes the checkpoint concurrent without a transaction behind it, so vacuum
			// may still run as long as nothing is writing
			options.can_vacuum_under_reference = NoActiveWriter();
		}
		// every checkpoint has to know what a pinned snapshot still needs, not just the ones
		// an explicit CHECKPOINT triggers - an automatic one writes the same state to disk
		options.retention_bound = LowestRetentionBound();
		options.id_retention_bound = PinnedBound();
		options.wal_lock = held_wal_lock.owns_lock() ? &held_wal_lock : nullptr;
		auto &storage_manager = db.GetStorageManager();
		try {
			storage_manager.CreateCheckpoint(context, options);
		} catch (std::exception &ex) {
			if (wal_written) {
				context.transaction.SetAutocheckpointError(BuildAutocheckpointError(db, ex));
			} else {
				error.Merge(ErrorData(ex));
			}
		}
	}

	return error;
}

void DuckTransactionManager::RollbackTransaction(Transaction &transaction_p) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();

	DUCKDB_LOG(db.GetDatabase(), TransactionLogType, db, "Rollback", transaction.GetTransactionId());

	// A reference is a row in the registry tables, so this rollback may have taken one away - and the
	// in-memory copy still has it. Conservative on purpose: the rebuild is a four-table read, and it only
	// happens when something asks the feature for something.
	MarkRegistryReloadNeeded();

	ErrorData error;
	{
		// Obtain the transaction lock and roll back.
		lock_guard<mutex> t_lock(transaction_lock);
		error = transaction.Rollback();

		// Remove the transaction from the list of active transactions and gather cleanup information.
		QueueCleanup(RemoveTransaction(transaction, CreateCleanupInfo()));
	}

	CleanupTransactions();

	if (error.HasError()) {
		throw FatalException("Failed to rollback transaction. Cannot continue operation.\nError: %s", error.Message());
	}
}

unique_ptr<DuckCleanupInfo> DuckTransactionManager::CreateCleanupInfo() {
	auto cleanup_info = make_uniq<DuckCleanupInfo>();
	// RemoveTransaction moves the transaction out of active_transactions before re-homing it in one of these
	// lists. It is noexcept, so a failure to allocate there would terminate the process and lose the transaction;
	// reserve everything it can need here instead, where throwing is safe and modifies nothing.
	cleanup_info->transactions.reserve(recently_committed_transactions.size() + 1);
	recently_committed_transactions.reserve(recently_committed_transactions.size() + 1);
	return cleanup_info;
}

unique_ptr<DuckCleanupInfo>
DuckTransactionManager::RemoveTransaction(DuckTransaction &transaction,
                                          unique_ptr<DuckCleanupInfo> cleanup_info) noexcept {
	return RemoveTransaction(transaction, transaction.ChangesMade(), std::move(cleanup_info));
}

unique_ptr<DuckCleanupInfo>
DuckTransactionManager::RemoveTransaction(DuckTransaction &transaction, bool store_transaction,
                                          unique_ptr<DuckCleanupInfo> cleanup_info) noexcept {
	// Nothing here may allocate: CreateCleanupInfo has reserved everything this needs (see its comment).

	// Find the transaction in the active transactions and recompute the lowest bound without it.
	auto t_index = UpdateLowestVisibilityBound(&transaction);
	D_ASSERT(t_index != active_transactions.size());

	// Decide if we need to store the transaction, or if we can schedule it for cleanup.
	auto current_transaction = std::move(active_transactions[t_index]);
	if (store_transaction) {
		// If the transaction made any changes, we need to keep it around.
		if (transaction.commit_id != 0) {
			// The transaction was committed.
			// We add it to the list of recently committed transactions.
			recently_committed_transactions.push_back(std::move(current_transaction));
		} else {
			// The transaction was aborted.
			cleanup_info->transactions.push_back(std::move(current_transaction));
		}
	} else if (transaction.ChangesMade()) {
		// We do not need to store the transaction, directly schedule it for cleanup.
		current_transaction->awaiting_cleanup = true;
		cleanup_info->transactions.push_back(std::move(current_transaction));
	}
	// This one also decides when a committed transaction may be released, so it tracks active
	// transactions alone - holding it down with a reference keeps the checkpoint locks those
	// transactions carry and makes every CHECKPOINT fail.
	cleanup_info->lowest_visibility_bound = LowestVisibilityBound();
	// What the version information has to be kept for is a separate question, and references count:
	// a reader pinned below this commit has to be able to tell "appended after me" from "always
	// there". With the transaction watermark alone the insert markers of a freshly appended row
	// group were dropped, and that row group then read as visible to every reader - including the
	// pin, which saw rows that did not exist when it was taken.
	cleanup_info->retention_bound = LowestRetentionBound();

	// Remove the transaction from the list of active transactions.
	active_transactions.unsafe_erase_at(t_index);

	// Move the committed transactions that no snapshot can still see to the cleanup info.
	SweepCommittedTransactions(*cleanup_info);

	return cleanup_info;
}

idx_t DuckTransactionManager::UpdateLowestVisibilityBound(optional_ptr<DuckTransaction> exclude) noexcept {
	idx_t exclude_index = active_transactions.size();
	auto computed_lowest_visibility_bound = VisibilityBound::AllCommitted();
	for (idx_t i = 0; i < active_transactions.size(); i++) {
		if (exclude && active_transactions[i].get() == exclude.get()) {
			exclude_index = i;
			continue;
		}
		computed_lowest_visibility_bound =
		    VisibilityBound::Min(computed_lowest_visibility_bound, active_transactions[i]->view.visibility_bound);
	}
	lowest_visibility_bound = computed_lowest_visibility_bound;
	return exclude_index;
}

void DuckTransactionManager::SweepCommittedTransactions(DuckCleanupInfo &cleanup_info) noexcept {
	idx_t i = 0;
	for (; i < recently_committed_transactions.size(); i++) {
		D_ASSERT(recently_committed_transactions[i]);
		if (recently_committed_transactions[i]->commit_id >= cleanup_info.lowest_visibility_bound) {
			// recently_committed_transactions is ordered on commit_id.
			// Thus, if the current commit_id is greater than
			// lowest_visibility_bound, any subsequent commit IDs are also greater.
			break;
		}

		recently_committed_transactions[i]->awaiting_cleanup = true;
		cleanup_info.transactions.push_back(std::move(recently_committed_transactions[i]));
	}

	if (i > 0) {
		// We moved these transactions to the list of transactions awaiting GC.
		auto start = recently_committed_transactions.begin();
		auto end = recently_committed_transactions.begin() + static_cast<int64_t>(i);
		recently_committed_transactions.erase(start, end);
	}
}

void DuckTransactionManager::QueueCleanup(unique_ptr<DuckCleanupInfo> cleanup_info) {
	if (cleanup_info->ScheduleCleanup()) {
		lock_guard<mutex> q_lock(cleanup_queue_lock);
		cleanup_queue.emplace(std::move(cleanup_info));
	}
}

idx_t DuckTransactionManager::GetCatalogVersion(Transaction &transaction_p) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();
	return transaction.catalog_version;
}

void DuckTransactionManager::PushCatalogEntry(Transaction &transaction_p, duckdb::CatalogEntry &entry,
                                              duckdb::data_ptr_t extra_data, duckdb::idx_t extra_data_size) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();
	if (!db.IsSystem() && !db.IsTemporary() && transaction.IsReadOnly()) {
		throw InternalException("Attempting to do catalog changes on a transaction that is read-only - "
		                        "this should not be possible");
	}
	transaction.catalog_version = ++last_uncommitted_catalog_version;
	transaction.PushCatalogEntry(entry, extra_data, extra_data_size);
}

void DuckTransactionManager::PushAttach(Transaction &transaction_p, AttachedDatabase &attached_db) {
	auto &transaction = transaction_p.Cast<DuckTransaction>();
	if (!db.IsSystem()) {
		throw InternalException("Can only ATTACH in the system catalog");
	}
	transaction.catalog_version = ++last_uncommitted_catalog_version;
	transaction.PushAttach(attached_db);
}

} // namespace duckdb
