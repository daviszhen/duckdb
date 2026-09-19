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

//===----------------------------------------------------------------------===//
// Creation
//===----------------------------------------------------------------------===//
string Git4DataEnsureRegistryTables(ClientContext &context) {
	if (Git4DataRegistryTablesThere(context)) {
		// already there: nothing to create and nothing to migrate, and the records in them are the truth
		return string();
	}
	// The tables are created by the statement the pragma returns rather than from inside it: a pragma runs
	// in a transaction that is not allowed to change the catalog (measured: "Attempting to do catalog
	// changes on a transaction that is read-only"), and the statement it returns runs as an ordinary one.
	string result;
	result += "CREATE SCHEMA IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA ";\n";
	// A snapshot: the name it is held under, the commit it pins, when it was taken, and the branch whose own
	// line it captured (empty when it captured the table as a commit saw it).
	result += "CREATE TABLE IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_SNAPSHOTS_TABLE
	          "(name VARCHAR, pin UBIGINT, created TIMESTAMP, line VARCHAR);\n";
	// A branch and the two mappings are created here too, so a later build that writes them finds them;
	// nothing writes them yet, and the metadata block still carries them.
	result += "CREATE TABLE IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_BRANCHES_TABLE
	          "(name VARCHAR, pin UBIGINT, parent VARCHAR, created TIMESTAMP, base_line VARCHAR);\n";
	result += "CREATE TABLE IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_STORAGE_TABLE
	          "(branch VARCHAR, source VARCHAR, storage VARCHAR, owned BOOLEAN);\n";
	result += "CREATE TABLE IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_DELTAS_TABLE
	          "(branch VARCHAR, source VARCHAR, added VARCHAR, removed VARCHAR);\n";
	// Everything the file already holds has to come across, because the tables are the truth for the
	// records they carry the moment they exist - and only now, or every later call would insert them again.
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	auto &migrating = Git4DataTransactionManager(context);
	for (auto &snapshot : migrating.snapshots.List()) {
		result += StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', %llu, TIMESTAMP '%s', '%s');\n",
		                             GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_SNAPSHOTS_TABLE, escape(snapshot.name),
		                             snapshot.pin, Timestamp::ToString(snapshot.created), escape(snapshot.line));
	}
	// A branch is three records - the branch, the table it was given for a source, and the delta it differs by
	// - and every one of them has to come across. The tables are the whole truth for a record the moment they
	// exist, so a record left in the block is one the next open drops: measured, with only the snapshots
	// migrated, a file whose records were in the block lost every branch and every mapping as soon as a pragma
	// created the tables, and the reads then reported "Branch ... does not exist" while the block still held it.
	for (auto &branch : migrating.branches.List()) {
		result += StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', %llu, '%s', TIMESTAMP '%s', '%s');\n",
		                             GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_BRANCHES_TABLE, escape(branch.name),
		                             branch.pin, escape(branch.parent), Timestamp::ToString(branch.created),
		                             escape(branch.base_line));
		for (auto &storage : branch.storage_tables) {
			result += StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', '%s', '%s', %s);\n",
			                             GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_STORAGE_TABLE, escape(branch.name),
			                             escape(storage.source), escape(storage.storage),
			                             storage.owned ? "true" : "false");
		}
		for (auto &delta : branch.deltas) {
			result += StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', '%s', '%s', '%s');\n",
			                             GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_DELTAS_TABLE, escape(branch.name),
			                             escape(delta.source), escape(delta.added), escape(delta.removed));
		}
	}
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
	TableScanState scan;
	scan.Initialize(column_ids, nullptr);
	auto &groups = *table.GetStorage().GetRowGroupCollection();
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
	// the tables, if they are there, are read again; if they are not, memory is left as it is
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
			transaction_manager.snapshots.Create(snapshot.name, snapshot.pin, snapshot.created, snapshot.line);
		}
	}
	for (auto &branch : before_branches) {
		if (Git4DataAddedByThisTransaction(added, "branch", branch.name)) {
			continue;
		}
		Git4DataBranch existing;
		if (!transaction_manager.branches.Get(branch.name, existing)) {
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
}

void Git4DataLoadRegistry(AttachedDatabase &db) {
	if (!Git4DataRegistryTablesExist(db)) {
		// an older file, whose records are in the checkpoint's metadata block: they were loaded by the block
		// reader, and there is nothing here to read
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
	Git4DataScanRegistryTable(
	    db, *snapshots, {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::TIMESTAMP, LogicalType::VARCHAR},
	    [&](DataChunk &chunk, idx_t row) {
		    auto name = chunk.GetValue(0, row).GetValue<string>();
		    auto pin = chunk.GetValue(1, row).GetValue<uint64_t>();
		    auto created = chunk.GetValue(2, row).GetValue<timestamp_t>();
		    auto line = chunk.GetValue(3, row).GetValue<string>();
		    transaction_manager.snapshots.Create(name, pin, created, line);
	    });
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
		Git4DataScanRegistryTable(
		    db, *storage, {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN},
		    [&](DataChunk &chunk, idx_t row) {
			    auto branch = chunk.GetValue(0, row).GetValue<string>();
			    auto source = chunk.GetValue(1, row).GetValue<string>();
			    auto storage = chunk.GetValue(2, row).GetValue<string>();
			    auto owned = chunk.GetValue(3, row).GetValue<bool>();
			    transaction_manager.branches.SetStorageTable(branch, source, storage, 0, 0, owned);
		    });
	}
	if (deltas) {
		Git4DataScanRegistryTable(
		    db, *deltas, {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
		    [&](DataChunk &chunk, idx_t row) {
			    auto branch = chunk.GetValue(0, row).GetValue<string>();
			    auto source = chunk.GetValue(1, row).GetValue<string>();
			    auto added = chunk.GetValue(2, row).GetValue<string>();
			    auto removed = chunk.GetValue(3, row).GetValue<string>();
			    transaction_manager.branches.SetDeltaTable(branch, source, added, removed);
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
					                                             storage.owned);
				}
			}
		}
		for (auto &old_delta : old_branch.deltas) {
			for (auto &delta : loaded.deltas) {
				if (StringUtil::CIEquals(delta.source, old_delta.source)) {
					transaction_manager.branches.SetDeltaTable(old_branch.name, delta.source, delta.added,
					                                           delta.removed, old_delta.added_oid,
					                                           old_delta.removed_oid, old_delta.source_oid);
				}
			}
		}
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
	auto stored_schema = stored_name.Schema().empty() ? string(DEFAULT_SCHEMA) : stored_name.Schema().GetIdentifierName();
	auto key_schema = key_name.Schema().empty() ? string(DEFAULT_SCHEMA) : key_name.Schema().GetIdentifierName();
	return StringUtil::CIEquals(stored_schema, key_schema) &&
	       StringUtil::CIEquals(stored_name.Name().GetIdentifierName(), key_name.Name().GetIdentifierName());
}

optional_ptr<const Git4DataStorageTable> Git4DataFindStorageTable(ClientContext &context,
                                                                const Git4DataBranch &branch,
                                                            const string &key, idx_t source_oid) {
	for (auto &entry : branch.storage_tables) {
		if (Git4DataSourceKeyMatches(context, entry.source, key) ||
		    (source_oid != 0 && entry.source_oid == source_oid)) {
			return &entry;
		}
	}
	return nullptr;
}

optional_ptr<const Git4DataBranchDelta> Git4DataFindDeltaTable(ClientContext &context,
                                                             const Git4DataBranch &branch,
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
