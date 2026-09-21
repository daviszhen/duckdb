#include "duckdb/storage/git4data_registry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_data.hpp"

namespace duckdb {

//! The transaction manager of the database this client is attached to, as the registry lives there
static DuckTransactionManager &Git4DataTransactionManager(ClientContext &context) {
	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	if (!attached) {
		throw InvalidInputException("Git4Data snapshots require an attached database");
	}
	return DuckTransactionManager::Get(*attached);
}

void Git4DataReconcileIfNeeded(ClientContext &context) {
	auto &transaction_manager = Git4DataTransactionManager(context);
	auto connection_flag = context.client_data->git4data_registry_reload_needed;
	auto shared_flag = transaction_manager.ConsumeRegistryReloadNeeded();
	if (!connection_flag && !shared_flag) {
		return;
	}
	context.client_data->git4data_registry_reload_needed = false;
	// What this connection added is dropped from memory and whatever the tables hold is put back - a reconcile
	// rather than a clear-and-reload, which would take another connection's uncommitted records with it
	// (measured, with two connections).
	//
	// The `added` list is *this* connection's, so it is passed only when the rollback was this connection's:
	// it says what its own rollback took back, and none of those records is in a table. A rebuild asked for by
	// **another** connection's rollback must not drop records this one has not committed - they are in no table
	// yet either, which is exactly what the markers are for. Measured with the list passed either way: passing
	// it dropped the caller's own uncommitted snapshot out of memory, so its next read of a reference it had
	// just made was refused.
	static const vector<string> no_records;
	Git4DataReconcileRegistry(transaction_manager.GetDB(),
	                          connection_flag ? context.client_data->git4data_added_records : no_records);
	if (connection_flag) {
		context.client_data->git4data_added_records.clear();
	}
}

bool Git4DataReferenceVisible(ClientContext &context, const string &, const string &) {
	// **Withdrawn in increment 3cv: this always answers true, and the check it used to make is gone.**
	//
	// Increment 3cr made a reference a transaction has added and not committed invisible to every other
	// connection, so that the registries agreed with the row the tables did not hold yet. That is the right
	// rule and it cannot be implemented here: which connection is asking is not observable at the point where
	// the question has to be answered, because the pragma that *makes* a reference runs before the statement it
	// belongs to and in a different client context. Measured, three identities, each of which differs between
	// the pragma and the very next statement of the same connection: the connection id, the `ClientData`, and
	// the connection's `MetaTransaction` (and its `DuckTransaction` is the read-only system transaction, whose
	// id is in the local id space rather than the commit space). Without that identity the check hides a
	// reference from *everyone*, including the connection that has just made it and is inside the transaction
	// that will commit it - measured, `BEGIN; git4data_create_snapshot('s'); SELECT ... AT (SNAPSHOT => 's')`
	// was refused on the connection that made it, until some unrelated rebuild happened to drop the marker, at
	// which point the reference became visible to everyone again.
	//
	// So the feature does not claim reference-level isolation inside a transaction, and the note says so where
	// it used to claim it. What survives from the same work is identity-free and fixes real bugs: a reference a
	// *finished* transaction added is dropped by the next rebuild (3cs, 3ct), which is what closed the window a
	// rolled-back reference stayed visible for. The call sites are left in place: they are where the check would
	// go, and the day the caller's identity is observable they are the change.
	(void)context;
	return true;
}

//===----------------------------------------------------------------------===//
// Creation
//===----------------------------------------------------------------------===//
string Git4DataEnsureRegistryTables(ClientContext &context) {
	if (Git4DataRegistryTablesThere(context)) {
		// already there: the records in them are the truth, and there is nothing else to create
		return string();
	}
	// The tables are created by the statement the pragma returns rather than from inside it: a pragma runs
	// in a transaction that is not allowed to change the catalog (measured: "Attempting to do catalog
	// changes on a transaction that is read-only"), and the statement it returns runs as an ordinary one.
	//
	// **Nothing is migrated into them, and that is a decision rather than an omission** (2026-09-20): the
	// build before this one kept a caller's references in a metadata block of the checkpoint, and carrying
	// those records across was the reason this function used to emit INSERTs for everything already in
	// memory. The block is being deleted rather than maintained - a file written by that build opens with no
	// references in it, as it would if the block had never existed - so a legacy record is not read, not
	// carried, and not kept. What the tables hold is what this build wrote.
	string result;
	result += "CREATE SCHEMA IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA ";\n";
	// A snapshot: the name it is held under, the commit it pins, when it was taken, and the branch whose own
	// line it captured (empty when it captured the table as a commit saw it).
	result += "CREATE TABLE IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_SNAPSHOTS_TABLE
	          "(name VARCHAR, pin UBIGINT, created TIMESTAMP, line VARCHAR);\n";
	result += "CREATE TABLE IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_BRANCHES_TABLE
	          "(name VARCHAR, pin UBIGINT, parent VARCHAR, created TIMESTAMP, base_line VARCHAR);\n";
	// `set_at` is the commit a mapping was set at, and it is what a read at an older pin is refused on: a
	// capture names a branch and resolves its mapping at the time of the read, so a mapping set after the
	// capture's pin would answer through a table that did not exist at that pin - measured as no rows at all,
	// with every listing saying nothing was wrong (3dr, and the decision of 2026-09-21).
	result += "CREATE TABLE IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_STORAGE_TABLE
	          "(branch VARCHAR, source VARCHAR, storage VARCHAR, owned BOOLEAN, set_at UBIGINT);\n";
	result += "CREATE TABLE IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_DELTAS_TABLE
	          "(branch VARCHAR, source VARCHAR, added VARCHAR, removed VARCHAR, set_at UBIGINT);\n";
	// A file written by an earlier build of this branch has those two tables without the column, and a table
	// the caller cannot be asked to rebuild: the statement adds it, with no value in it, which reads as
	// "unknown" and is the behaviour before this column existed.
	result += "ALTER TABLE " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_STORAGE_TABLE
	          " ADD COLUMN IF NOT EXISTS set_at UBIGINT;\n";
	result += "ALTER TABLE " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_DELTAS_TABLE
	          " ADD COLUMN IF NOT EXISTS set_at UBIGINT;\n";
	// The quotas are settings rather than references, and they are rows for the same reason the references are:
	// a row is transactional and reaches the log, where a property of the checkpoint's registry block was
	// neither. `git4data_limit` writes them and the load below reads them back.
	result += "CREATE TABLE IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_STATE_TABLE
	          "(key VARCHAR, value VARCHAR);\n";
	return result;
}

//===----------------------------------------------------------------------===//
// Reading
//===----------------------------------------------------------------------===//
static bool Git4DataRegistrySchema(ClientContext &context, optional_ptr<SchemaCatalogEntry> &result) {
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
	result = catalog.GetSchema(context, Identifier(GIT4DATA_REGISTRY_SCHEMA), OnEntryNotFound::RETURN_NULL);
	return result != nullptr;
}

bool Git4DataRegistryTablesThere(ClientContext &context) {
	auto entry = Catalog::GetEntry<TableCatalogEntry>(
	    context,
	    QualifiedName(vector<Identifier> {Identifier(GIT4DATA_REGISTRY_SCHEMA)}, Identifier(GIT4DATA_SNAPSHOTS_TABLE)),
	    OnEntryNotFound::RETURN_NULL);
	return entry != nullptr;
}

//! The registry table of the given name in the database being loaded. The load runs while that database is
//! being attached, so the catalog is asked for directly rather than through the client's default database -
//! measured: going through the default one there reports "Calling DatabaseManager::GetDefaultDatabase with
//! no database attached".
static optional_ptr<TableCatalogEntry> Git4DataRegistryTable(AttachedDatabase &db, const string &name) {
	auto &catalog = db.GetCatalog();
	// The system transaction, and no client context: the load runs while the database is being attached, and a
	// catalog transaction taken from that client stays on its meta transaction - measured, a later DETACH on
	// the same connection then refused with "the current transaction has outstanding work on it". A load that
	// needed a client would do nothing at all on the path where there is none.
	auto transaction = CatalogTransaction::GetSystemTransaction(db.GetDatabase());
	auto schema = catalog.GetSchema(transaction, Identifier(GIT4DATA_REGISTRY_SCHEMA), OnEntryNotFound::RETURN_NULL);
	if (!schema) {
		return nullptr;
	}
	auto entry = schema->GetEntry(transaction, CatalogType::TABLE_ENTRY, Identifier(name));
	if (!entry) {
		return nullptr;
	}
	return &entry->Cast<TableCatalogEntry>();
}

bool Git4DataRegistryTablesExist(AttachedDatabase &db) {
	return Git4DataRegistryTable(db, GIT4DATA_SNAPSHOTS_TABLE) != nullptr;
}

//! Call the callback once per row of a registry table, as of everything committed. The tables are read with
//! a synthetic all-committed transaction: what they hold has been committed by the time the database serves
//! anything, and the read is not the caller's own view of them.
static void Git4DataScanRegistryTable(AttachedDatabase &db, TableCatalogEntry &table, const vector<LogicalType> &types,
                                      const std::function<void(DataChunk &, idx_t)> &callback) {
	vector<StorageIndex> column_ids;
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.emplace_back(i);
	}
	auto &groups = *table.GetStorage().GetRowGroupCollection();
	// **A collection with no row groups cannot be scanned, and a registry table is empty whenever its kind has
	// no rows yet** - `git4data.branches`, `storage` and `deltas` are all empty in a database whose only
	// reference is a snapshot, which is the ordinary case. `RowGroupCollection::InitializeScan` asserts on the
	// root segment (`row_group_collection.cpp`, `D_ASSERT(row_group)`), so an assertion-enabled build - the
	// configuration upstream's own CI uses - refused to *open* such a database, measured on a fresh file with
	// one snapshot in it. Nothing to read is nothing to hand to the callback.
	if (groups.GetTotalRows() == 0) {
		return;
	}
	TableScanState scan;
	scan.Initialize(column_ids, nullptr);
	QueryContext query_context;
	groups.InitializeScan(query_context, scan.table_state, column_ids, nullptr);
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(db), types);
	while (true) {
		chunk.Reset();
		scan.table_state.Scan(ScanOptions(TransactionData(MAX_TRANSACTION_ID, VisibilityBound::AllCommitted())), chunk);
		auto count = chunk.size();
		if (count == 0) {
			break;
		}
		for (idx_t i = 0; i < count; i++) {
			callback(chunk, i);
		}
	}
}

//! Whether this transaction added the record the key names
static bool Git4DataAddedByThisTransaction(const vector<string> &added, const string &kind, const string &key) {
	for (auto &entry : added) {
		if (entry == kind + "\t" + key) {
			return true;
		}
	}
	return false;
}

void Git4DataReconcileRegistry(AttachedDatabase &db, const vector<string> &added) {
	auto &transaction_manager = DuckTransactionManager::Get(db);
	auto before_snapshots = transaction_manager.snapshots.List();
	auto before_branches = transaction_manager.branches.List();
	// the tables, if they are there, are read again; if they are not, memory is left as it is. What memory
	// holds here is the state the rollback has just taken back, so the internal references are read from the
	// tables alone - the block's copy is the open's to use, not the reconcile's.
	Git4DataLoadRegistry(db);
	auto tables_have_snapshots = Git4DataRegistryTablesExist(db);

	// What this transaction added goes: its rows went with the rollback, and a reference no disk holds must not
	// stay readable. When the tables are there the reload has already dropped them, so this is for the case
	// where they are not - a database whose first reference was the one rolled back.
	for (auto &entry : added) {
		auto kind = entry.substr(0, entry.find('\t'));
		auto key = entry.substr(entry.find('\t') + 1);
		if (kind == "snapshot") {
			Git4DataSnapshot existing;
			if (transaction_manager.snapshots.Get(key, existing) && !tables_have_snapshots) {
				transaction_manager.snapshots.Drop(key);
			}
		} else if (kind == "branch") {
			Git4DataBranch existing;
			if (transaction_manager.branches.Get(key, existing) && !tables_have_snapshots) {
				transaction_manager.branches.Drop(key);
			}
		} else if (kind == "storage" || kind == "delta") {
			auto branch = key.substr(0, key.find('\t'));
			auto source = key.substr(key.find('\t') + 1);
			if (!tables_have_snapshots) {
				transaction_manager.branches.ClearStorageTable(branch, source);
				transaction_manager.branches.ClearDeltaTable(branch, source);
			}
		}
	}

	// Everything else memory held is put back unless this transaction added it: those are the records another
	// connection has not committed yet, and the reload above knows nothing about them.
	for (auto &snapshot : before_snapshots) {
		if (Git4DataAddedByThisTransaction(added, "snapshot", snapshot.name)) {
			continue;
		}
		Git4DataSnapshot existing;
		if (!transaction_manager.snapshots.Get(snapshot.name, existing)) {
			// When the tables are there they are the truth, and a record they do not hold is one a rollback
			// took back - put back only while some transaction is *still adding* it, which is what makes it
			// another connection's uncommitted work instead of a reference that never committed. Measured
			// before this: a rolled-back reference stayed in the shared registry until the rolling-back
			// connection next used the feature, and another connection listed it and read through it until
			// then (increment 3cr measured the window, this closes it).
			if (tables_have_snapshots && !transaction_manager.ReferenceIsBeingAdded("snapshot", snapshot.name)) {
				continue;
			}
			transaction_manager.snapshots.Create(snapshot.name, snapshot.pin, snapshot.created, snapshot.line);
		}
	}
	for (auto &branch : before_branches) {
		if (Git4DataAddedByThisTransaction(added, "branch", branch.name)) {
			continue;
		}
		Git4DataBranch existing;
		if (!transaction_manager.branches.Get(branch.name, existing)) {
			// the same rule as the snapshots above, and the branch's mappings go with it
			if (tables_have_snapshots && !transaction_manager.ReferenceIsBeingAdded("branch", branch.name)) {
				continue;
			}
			transaction_manager.branches.Restore(branch.name, branch.pin, branch.parent, branch.created,
			                                     branch.base_line);
		}
		for (auto &storage : branch.storage_tables) {
			if (Git4DataAddedByThisTransaction(added, "storage", branch.name + "\t" + storage.source)) {
				continue;
			}
			transaction_manager.branches.SetStorageTable(branch.name, storage.source, storage.storage, storage.oid,
			                                             storage.source_oid, storage.owned);
		}
		for (auto &delta : branch.deltas) {
			if (Git4DataAddedByThisTransaction(added, "delta", branch.name + "\t" + delta.source)) {
				continue;
			}
			transaction_manager.branches.SetDeltaTable(branch.name, delta.source, delta.added, delta.removed,
			                                           delta.added_oid, delta.removed_oid, delta.source_oid);
		}
	}
	// A rollback can also have taken the registry tables themselves away: the statement that makes a database's
	// *first* reference creates them, so rolling that one back leaves a database with no tables, and then the
	// reload above returns early and memory is all there is. What memory may not keep is a reference a finished
	// transaction of this process added - which is exactly what a stale marker says - while a record with no
	// marker at all came from the checkpoint's block rather than from a statement of this process, and is left
	// alone. Measured before this: another
	// connection still listed the rolled-back name and still read through it, because nothing looked at the
	// markers in that case.
	if (!tables_have_snapshots) {
		transaction_manager.DropRecordsNobodyIsAdding();
	}
	// The window is a setting as well as a reference, and the reload above decides it from the tables - but
	// only when they are there, and a database whose *first* reference was the one rolled back has none: the
	// transaction that created them is the one that was rolled back. Measured, `BEGIN; git4data_pitr(7);
	// ROLLBACK;` left the window enabled with no reference at all, and the next checkpoint wrote a reference
	// at the *current* commit - so a window the caller rolled back became a real one, seven days of it, and a
	// restart brought it back as `pitr_enabled = true, window_days = 7, reference_held = true`.
	if (!tables_have_snapshots &&
	    Git4DataAddedByThisTransaction(added, "snapshot", DuckTransactionManager::PITR_REFERENCE)) {
		transaction_manager.ClearPitr();
	}
	// The reference a merge names its base with is the other one the reload cannot decide: it is created while
	// the merge's statement is being built, so a rollback would leave it in memory. Left there it is invisible
	// (not listed, not counted) but not inert - measured, it survives a checkpoint and a restart, and it
	// refuses a git4data_drop_branch of the branch whose line it captured, so the branch stayed and with it
	// the history its pin holds (1000 physical rows against a control's 500 after the same deletions). A merge
	// that fails therefore takes its base with it.
	if (Git4DataAddedByThisTransaction(added, "snapshot", DuckTransactionManager::MERGE_BASE_REFERENCE)) {
		transaction_manager.snapshots.Drop(DuckTransactionManager::MERGE_BASE_REFERENCE);
	}
}

void Git4DataLoadRegistry(AttachedDatabase &db) {
	if (!Git4DataRegistryTablesExist(db)) {
		// no registry tables, so this file has no references this build can read. That covers both a file
		// written before the tables existed - whose records are in the checkpoint's metadata block, and are
		// deliberately not carried across (2026-09-20) - and one in which nothing has taken a reference yet.
		// Nothing is rebuilt from the block here, and there is nothing in the tables to read.
		return;
	}
	auto &transaction_manager = DuckTransactionManager::Get(db);
	// The tables are the whole truth for every record now, so both registries are rebuilt from them: the block
	// may hold a state from before changes the log replayed, and a branch made since the last checkpoint is
	// one of those changes.
	//
	// The oids a rebuild cannot bring back are kept from what is already in memory: they are not persisted (the
	// catalog's oids do not survive a reopen), and the feature uses them to follow a source table that was
	// renamed and to refuse a table that took the name of one it was given - measured, a rebuild that dropped
	// them made `git4data_apply('g2', ...)` report no delta for a renamed source and let a swapped storage table
	// be read. A rebuild happens while the process lives, so the ids it already has are still meaningful.
	// The references the engine moves *itself* - the point-in-time window's, and a merge's base - are rows in
	// these tables like any other reference, so the rows below are where they come back from and nothing has
	// to be kept from memory: `git4data_pitr` writes the window's row with its length in the `line` column, and
	// a merge writes its base's. Until 3ef the checkpoint's registry block carried a second copy of the
	// window's reference, because the engine *moves* it at a checkpoint with no statement behind the move, and
	// this function kept whichever of the two was newer. The block carries only the two commit counters now;
	// what that costs is recorded in 3ef (the row can lag the engine's move, which retains history rather than
	// releasing it, and the next feature statement writes the moved pin).
	// The block carries the reference the engine *moved*; it has no statement behind the move, so the row
	// keeps the pin the window was opened at while memory holds the one it was moved to (2026-09-21, option A).
	// Where both are there the newer pin wins: a pin only moves forward, and taking the older one would name a
	// state the checkpoint that moved the window has already released - a read at it then answers a *newer*
	// state while reporting the old pin, which is the wrong answer this comparison exists to prevent.
	Git4DataSnapshot kept_pitr;
	bool has_kept_pitr = transaction_manager.snapshots.Get(DuckTransactionManager::PITR_REFERENCE, kept_pitr);
	auto old_branches = transaction_manager.branches.List();
	transaction_manager.snapshots.Clear();
	transaction_manager.branches.Clear();

	auto snapshots = Git4DataRegistryTable(db, GIT4DATA_SNAPSHOTS_TABLE);
	auto branches = Git4DataRegistryTable(db, GIT4DATA_BRANCHES_TABLE);
	auto storage = Git4DataRegistryTable(db, GIT4DATA_STORAGE_TABLE);
	auto deltas = Git4DataRegistryTable(db, GIT4DATA_DELTAS_TABLE);
	if (!snapshots) {
		return;
	}
	bool pitr_row_seen = false;
	Git4DataScanRegistryTable(
	    db, *snapshots, {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::TIMESTAMP, LogicalType::VARCHAR},
	    [&](DataChunk &chunk, idx_t row) {
		    auto name = chunk.GetValue(0, row).GetValue<string>();
		    if (name == DuckTransactionManager::PITR_REFERENCE) {
			    pitr_row_seen = true;
		    }
		    auto pin = chunk.GetValue(1, row).GetValue<uint64_t>();
		    auto created = chunk.GetValue(2, row).GetValue<timestamp_t>();
		    auto line = chunk.GetValue(3, row).GetValue<string>();
		    if (StringUtil::StartsWith(name, "__")) {
			    if (name == DuckTransactionManager::PITR_REFERENCE) {
				    // The block carries this reference too - it is the one the engine moves with no statement
				    // behind the move (2026-09-21, option A) - so the newer of the two pins is kept. The
				    // *length* comes from the line, and this row is where it lives: the early return that used to
				    // be here was harmless while the block also carried the window's flag and length (103/104),
				    // and it is not any more - measured, `pitr.test` read `pitr_enabled = false` after a restart
				    // with the row's own line never parsed.
				    auto window_line = line.rfind("window_micros=", 0) == 0 ? line : kept_pitr.line;
				    auto window_pin = pin;
				    auto window_created = created;
				    if (has_kept_pitr && kept_pitr.pin >= pin) {
					    window_pin = kept_pitr.pin;
					    window_created = kept_pitr.created;
				    }
				    if (window_line.rfind("window_micros=", 0) == 0) {
					    try {
						    transaction_manager.RestorePitr(timestamp_t(std::stoll(window_line.substr(14))));
					    } catch (...) {
						    // a line that does not parse leaves the window off, which is what the default is
					    }
				    }
				    transaction_manager.snapshots.Create(name, window_pin, window_created, window_line);
				    return;
			    }
			    // An internal reference is a row like any other, and this row is the only record of it: the
			    // window's own reference carries the length of the window in this column, and that is what
			    // makes the window survive a restart - a window opened and then crashed on used to come back
			    // as a reference with no window around it, because the block's copy was only written by a
			    // checkpoint. Read back only from an internal name, where the same column holds this line
			    // rather than the branch a caller's snapshot captured.
			    if (line.rfind("window_micros=", 0) == 0) {
				    try {
					    transaction_manager.RestorePitr(timestamp_t(std::stoll(line.substr(14))));
				    } catch (...) {
					    // a line that does not parse leaves the window off, which is what the default is
				    }
			    }
		    }
		    transaction_manager.snapshots.Create(name, pin, created, line);
	    });
	// The window's reference is a row now, so that row decides whether the window is on: a window turned off
	// is a delete in the caller's transaction, and the block the engine writes at a checkpoint can still hold
	// the reference it had before. Measured, a window turned off and then reopened without a checkpoint came
	// back on - `pitr_enabled = true, reference_held = true` with no row in the table at all - which is a
	// window the caller released, holding history they let go. The block is what a file *without* these
	// tables falls back on, and that case returns above.
	if (!pitr_row_seen) {
		transaction_manager.ClearPitr();
	}
	// branches first: the two mappings are recorded against a branch, and their setters do nothing for a
	// branch that is not there yet
	if (branches) {
		Git4DataScanRegistryTable(db, *branches,
		                          {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR,
		                           LogicalType::TIMESTAMP, LogicalType::VARCHAR},
		                          [&](DataChunk &chunk, idx_t row) {
			                          auto name = chunk.GetValue(0, row).GetValue<string>();
			                          auto pin = chunk.GetValue(1, row).GetValue<uint64_t>();
			                          auto parent = chunk.GetValue(2, row).GetValue<string>();
			                          auto created = chunk.GetValue(3, row).GetValue<timestamp_t>();
			                          auto base_line = chunk.GetValue(4, row).GetValue<string>();
			                          transaction_manager.branches.Restore(name, pin, parent, created, base_line);
		                          });
	}
	if (storage) {
		// `set_at` is the fifth column, and a file written before it has four: the scan is told which shape to
		// read, and a missing value is zero, which reads as "unknown" - no check, the behaviour before the
		// column existed.
		const bool has_set_at = storage->GetColumns().LogicalColumnCount() > 4;
		vector<LogicalType> storage_types {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		                                   LogicalType::BOOLEAN};
		if (has_set_at) {
			storage_types.push_back(LogicalType::UBIGINT);
		}
		Git4DataScanRegistryTable(db, *storage, storage_types, [&](DataChunk &chunk, idx_t row) {
			auto branch = chunk.GetValue(0, row).GetValue<string>();
			auto source = chunk.GetValue(1, row).GetValue<string>();
			auto storage = chunk.GetValue(2, row).GetValue<string>();
			auto owned = chunk.GetValue(3, row).GetValue<bool>();
			// a row written by hand, as an older build's binding wrote one, carries no value here: unknown
			// means "no check", so a NULL is read as zero rather than refused
			auto set_at = has_set_at && !chunk.GetValue(4, row).IsNull() ? chunk.GetValue(4, row).GetValue<uint64_t>()
			                                                             : 0;
			transaction_manager.branches.SetStorageTable(branch, source, storage, 0, 0, owned, set_at);
		});
	}
	if (deltas) {
		const bool has_set_at = deltas->GetColumns().LogicalColumnCount() > 4;
		vector<LogicalType> delta_types {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		                                 LogicalType::VARCHAR};
		if (has_set_at) {
			delta_types.push_back(LogicalType::UBIGINT);
		}
		Git4DataScanRegistryTable(db, *deltas, delta_types, [&](DataChunk &chunk, idx_t row) {
			auto branch = chunk.GetValue(0, row).GetValue<string>();
			auto source = chunk.GetValue(1, row).GetValue<string>();
			auto added = chunk.GetValue(2, row).GetValue<string>();
			auto removed = chunk.GetValue(3, row).GetValue<string>();
			auto set_at = has_set_at && !chunk.GetValue(4, row).IsNull() ? chunk.GetValue(4, row).GetValue<uint64_t>()
			                                                             : 0;
			transaction_manager.branches.SetDeltaTable(branch, source, added, removed, 0, 0, 0, set_at);
		});
	}
	// put the oids the records had back: the tables do not carry them, and they are what follows a renamed
	// table or refuses one that took a name
	for (auto &old_branch : old_branches) {
		Git4DataBranch loaded;
		if (!transaction_manager.branches.Get(old_branch.name, loaded)) {
			continue;
		}
		for (auto &old_storage : old_branch.storage_tables) {
			for (auto &storage : loaded.storage_tables) {
				if (StringUtil::CIEquals(storage.source, old_storage.source)) {
					transaction_manager.branches.SetStorageTable(old_branch.name, storage.source, storage.storage,
					                                             old_storage.oid, old_storage.source_oid,
					                                             storage.owned, storage.set_at);
				}
			}
		}
		for (auto &old_delta : old_branch.deltas) {
			for (auto &delta : loaded.deltas) {
				if (StringUtil::CIEquals(delta.source, old_delta.source)) {
					transaction_manager.branches.SetDeltaTable(old_branch.name, delta.source, delta.added,
					                                           delta.removed, old_delta.added_oid,
					                                           old_delta.removed_oid, old_delta.source_oid,
					                                           delta.set_at);
				}
			}
		}
	}
	// the settings the feature keeps are rows too: the quotas were properties of the checkpoint's registry block
	// until 3ee, and a row is transactional where a block property was only as durable as the next checkpoint.
	// A key that is not there leaves the value at its default, which is what a database with no quotas at all
	// has - and what a file from the older build has, whose block this build does not read.
	if (auto state = Git4DataRegistryTable(db, GIT4DATA_STATE_TABLE)) {
		Git4DataScanRegistryTable(db, *state, {LogicalType::VARCHAR, LogicalType::VARCHAR},
		                          [&](DataChunk &chunk, idx_t row) {
			                          auto key = chunk.GetValue(0, row).GetValue<string>();
			                          auto value = chunk.GetValue(1, row).GetValue<string>();
			                          if (StringUtil::CIEquals(key, "snapshot_limit")) {
				                          transaction_manager.snapshot_limit = std::stoll(value);
			                          } else if (StringUtil::CIEquals(key, "branch_limit")) {
				                          transaction_manager.branch_limit = std::stoll(value);
			                          }
		                          });
	}
	// what is in the tables is on disk, so a checkpoint does not have to force itself for the registries
	transaction_manager.snapshots.MarkClean();
	transaction_manager.branches.MarkClean();
}

//===----------------------------------------------------------------------===//
// The guard
//===----------------------------------------------------------------------===//
optional_ptr<TableCatalogEntry> Git4DataResolveTableOrNull(ClientContext &context, const string &name) {
	// a name taken from the catalog is one identifier, and a dot in it is part of the name - measured:
	// reading a table named "a.b" at a branch looked it up as a table "b" in a schema "a" and found nothing
	auto exact =
	    Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(name)), OnEntryNotFound::RETURN_NULL);
	if (exact) {
		return exact;
	}
	// a name a caller wrote may be qualified, which is the other reading of the same string: with no table
	// of that literal name, the dot separates a schema from a table
	auto qualified = QualifiedName::Parse(name);
	return Catalog::GetEntry<TableCatalogEntry>(context, qualified, OnEntryNotFound::RETURN_NULL);
}

TableCatalogEntry &Git4DataResolveTable(ClientContext &context, const string &name) {
	auto result = Git4DataResolveTableOrNull(context, name);
	if (!result) {
		throw CatalogException("Table with name %s does not exist", name);
	}
	return *result;
}

string Git4DataTableName(const TableCatalogEntry &table) {
	auto &schema = table.ParentSchema().name.GetIdentifierName();
	auto name = table.name.GetIdentifierName();
	if (StringUtil::CIEquals(schema, DEFAULT_SCHEMA)) {
		return name;
	}
	// the bare name would resolve to a table of that name in the default schema, or to none at all. Each
	// component is quoted as its own identifier, so a name holding a dot is still part of a qualified name.
	return KeywordHelper::WriteOptionallyQuoted(schema) + "." + KeywordHelper::WriteOptionallyQuoted(name);
}

string Git4DataTableSQLName(const string &name) {
	// each component quoted as its own identifier, so a name holding a dot still names one table
	return QualifiedName::Parse(name).ToString(QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA);
}

string Git4DataSourceKey(const TableCatalogEntry &table) {
	auto &schema = table.ParentSchema().name.GetIdentifierName();
	auto name = table.name.GetIdentifierName();
	// qualified even when the table is in the default schema, unlike the name a mapping's *tables* are held
	// under: a reference is keyed by the table it is about, two schemas can hold a table of the same name, and
	// a bare key would resolve through the search path rather than to the table the statement named - which is
	// the ambiguity a reference read used to refuse.
	return KeywordHelper::WriteOptionallyQuoted(schema) + "." + KeywordHelper::WriteOptionallyQuoted(name);
}

string Git4DataSourceKey(ClientContext &context, const string &written) {
	// a name may be written any way a table can be named - bare, qualified, quoted - so the key comes from the
	// table it reaches rather than from the text. A name that resolves to nothing is returned as written, and
	// the caller's own error is the one that surfaces.
	auto entry = Git4DataResolveTableOrNull(context, written);
	if (!entry) {
		return written;
	}
	return Git4DataSourceKey(*entry);
}

bool Git4DataSourceKeyMatches(ClientContext &context, const string &stored, const string &key) {
	if (StringUtil::CIEquals(stored, key)) {
		return true;
	}
	// A record written before keys carried a schema holds the name the caller wrote, which the search path
	// resolved when it was written, so the stored name is matched by the table it resolves to as well - and
	// both sides are resolved rather than compared as text, since `t` and `main.t` are one table while `t`
	// under a search path that answers `other.t` is not.
	auto stored_entry = Git4DataResolveTableOrNull(context, stored);
	auto key_entry = Git4DataResolveTableOrNull(context, key);
	if (stored_entry && key_entry) {
		return stored_entry->oid == key_entry->oid;
	}
	// neither name resolves - the table is gone, which is what an apply after a drop looks like - so the two
	// are compared as names: a stored name without a schema is the default schema's, exactly as it was written
	auto stored_name = QualifiedName::Parse(stored);
	auto key_name = QualifiedName::Parse(key);
	auto stored_schema =
	    stored_name.Schema().empty() ? string(DEFAULT_SCHEMA) : stored_name.Schema().GetIdentifierName();
	auto key_schema = key_name.Schema().empty() ? string(DEFAULT_SCHEMA) : key_name.Schema().GetIdentifierName();
	return StringUtil::CIEquals(stored_schema, key_schema) &&
	       StringUtil::CIEquals(stored_name.Name().GetIdentifierName(), key_name.Name().GetIdentifierName());
}

void Git4DataCheckSourceBoundTable(ClientContext &context, const Git4DataBranch &branch, const string &stored_source,
                                   idx_t stored_oid, idx_t requested_oid) {
	// A rename matches by oid and a mapping read back from a file carries no oid at all, so both are left alone:
	// what this refuses is a name that reaches a table the mapping was not made against.
	if (stored_oid == 0 || requested_oid == 0 || stored_oid == requested_oid) {
		return;
	}
	throw InvalidInputException(
	    "Branch \"%s\" has a mapping for \"%s\", and the table carrying that name now is not the one the "
	    "mapping was made against - the table it was made against is no longer reachable under that name. Bind "
	    "the mapping "
	    "again (git4data_bind_branch_delta, git4data_bind_branch_table) or take it back with git4data_unbind.",
	    // `git4data_materialize` was in that list and cannot work here: it copies what the branch *reads*, and
	    // what it reads is the mapping this refusal is about, so it answers with this same sentence (measured,
	    // increment 3cc) - a message that names a way out which refuses with the message is a loop, and the first
	    // version of this one had it.
	    branch.name, stored_source);
}

optional_ptr<const Git4DataStorageTable> Git4DataFindStorageTable(ClientContext &context, const Git4DataBranch &branch,
                                                                  const string &key, idx_t source_oid) {
	for (auto &entry : branch.storage_tables) {
		if (Git4DataSourceKeyMatches(context, entry.source, key) ||
		    (source_oid != 0 && entry.source_oid == source_oid)) {
			return &entry;
		}
	}
	return nullptr;
}

optional_ptr<const Git4DataBranchDelta> Git4DataFindDeltaTable(ClientContext &context, const Git4DataBranch &branch,
                                                               const string &key, idx_t source_oid) {
	for (auto &entry : branch.deltas) {
		if (Git4DataSourceKeyMatches(context, entry.source, key) ||
		    (source_oid != 0 && entry.source_oid == source_oid)) {
			return &entry;
		}
	}
	return nullptr;
}

bool Git4DataIsRegistrySchema(const string &schema) {
	return StringUtil::CIEquals(schema, GIT4DATA_REGISTRY_SCHEMA);
}

bool Git4DataIsRegistryTable(const TableCatalogEntry &table) {
	return Git4DataIsRegistrySchema(table.ParentSchema().name.GetIdentifierName());
}

void Git4DataCheckRegistryWrite(const TableCatalogEntry &table) {
	if (!Git4DataIsRegistryTable(table)) {
		return;
	}
	throw InvalidInputException(
	    "\"%s\" is one of the tables the reference registry is kept in, and a reference's state cannot be "
	    "edited through SQL: a snapshot's row carries the pin that holds the history it reads, a branch's rows "
	    "say which tables it reads, and the feature's own statements write them. Use the git4data_* pragmas.",
	    table.name.GetIdentifierName());
}

void Git4DataCheckRegistrySchema(const string &schema) {
	if (!StringUtil::CIEquals(schema, GIT4DATA_REGISTRY_SCHEMA)) {
		return;
	}
	throw InvalidInputException(
	    "\"%s\" is the schema the reference registry is kept in, and dropping or altering it would take every "
	    "snapshot and branch with it. Use the git4data_* pragmas to drop a reference.",
	    schema);
}

void Git4DataCheckRegistryCreate(const string &schema, const string &table) {
	if (!StringUtil::CIEquals(schema, GIT4DATA_REGISTRY_SCHEMA)) {
		return;
	}
	throw InvalidInputException(
	    "\"%s\" is in the schema the reference registry is kept in, and that schema is the feature's own: its "
	    "tables are how a reference's state is kept, so a statement cannot create one there. Use the git4data_* "
	    "pragmas.",
	    table);
}

void Git4DataCheckRegistrySchemaCreate(const string &schema) {
	if (!StringUtil::CIEquals(schema, GIT4DATA_REGISTRY_SCHEMA)) {
		return;
	}
	throw InvalidInputException(
	    "\"%s\" is the name of the schema the reference registry is kept in, and the name is reserved: a "
	    "schema of that name is hidden from every listing, and no statement can create or edit anything in it. "
	    "Use a different name.",
	    schema);
}

void Git4DataCheckReferenceAtName(const string &name, bool branch, const string &what) {
	throw BinderException("AT (%s => ...) reads a table as the reference saw it, and \"%s\" is %s: a reference "
	                      "is kept for the table a statement names. Name that table, or read without the clause.",
	                      branch ? "BRANCH" : "SNAPSHOT", name, what);
}

void Git4DataCheckRegistryTarget(const string &schema, const string &table) {
	if (!StringUtil::CIEquals(schema, GIT4DATA_REGISTRY_SCHEMA)) {
		return;
	}
	throw InvalidInputException(
	    "\"%s\" is one of the tables the reference registry is kept in, and a reference's state cannot be "
	    "edited through SQL: a snapshot's row carries the pin that holds the history it reads, and a "
	    "branch's rows say which tables it reads. Use the git4data_* pragmas.",
	    table);
}

} // namespace duckdb
