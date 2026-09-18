#include "duckdb/storage/git4data_registry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/column_definition.hpp"
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
	    context, QualifiedName(vector<Identifier> {Identifier(GIT4DATA_REGISTRY_SCHEMA)},
	                           Identifier(GIT4DATA_SNAPSHOTS_TABLE)),
	    OnEntryNotFound::RETURN_NULL);
	return entry != nullptr;
}

//! The registry table of the given name in the database being loaded. The load runs while that database is
//! being attached, so the catalog is asked for directly rather than through the client's default database -
//! measured: going through the default one there reports "Calling DatabaseManager::GetDefaultDatabase with
//! no database attached".
static optional_ptr<TableCatalogEntry> Git4DataRegistryTable(ClientContext &client, AttachedDatabase &db,
                                                             const string &name) {
	auto &catalog = db.GetCatalog();
	// The system transaction, not the attaching client's: the load runs while the database is being attached,
	// and a catalog transaction taken from that client stays on its meta transaction - measured, a later
	// DETACH on the same connection then refused with "the current transaction has outstanding work on it".
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

bool Git4DataRegistryTablesExist(QueryContext context, AttachedDatabase &db) {
	auto client = context.GetClientContext();
	if (!client) {
		// no client context to look through: nothing can be read, and the metadata block is what the load
		// already did
		return false;
	}
	return Git4DataRegistryTable(*client, db, GIT4DATA_SNAPSHOTS_TABLE) != nullptr;
}

//! Call the callback once per row of a registry table, as of everything committed. The tables are read with
//! a synthetic all-committed transaction: what they hold has been committed by the time the database serves
//! anything, and the read is not the caller's own view of them.
static void Git4DataScanRegistryTable(ClientContext &context, TableCatalogEntry &table,
                                      const vector<LogicalType> &types,
                                      const std::function<void(DataChunk &, idx_t)> &callback) {
	vector<StorageIndex> column_ids;
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.emplace_back(i);
	}
	TableScanState scan;
	scan.Initialize(column_ids, nullptr);
	auto &groups = *table.GetStorage().GetRowGroupCollection();
	QueryContext query_context(context);
	groups.InitializeScan(query_context, scan.table_state, column_ids, nullptr);
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), types);
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

void Git4DataLoadRegistry(QueryContext context, AttachedDatabase &db) {
	auto client = context.GetClientContext();
	if (!client || !Git4DataRegistryTablesExist(context, db)) {
		// an older file, whose records are in the checkpoint's metadata block and are already loaded
		return;
	}
	auto &transaction_manager = DuckTransactionManager::Get(db);
	// The tables are the whole truth for every record now, so both registries are rebuilt from them: the block
	// may hold a state from before changes the log replayed, and a branch made since the last checkpoint is
	// one of those changes.
	transaction_manager.snapshots.Clear();
	transaction_manager.branches.Clear();

	auto snapshots = Git4DataRegistryTable(*client, db, GIT4DATA_SNAPSHOTS_TABLE);
	auto branches = Git4DataRegistryTable(*client, db, GIT4DATA_BRANCHES_TABLE);
	auto storage = Git4DataRegistryTable(*client, db, GIT4DATA_STORAGE_TABLE);
	auto deltas = Git4DataRegistryTable(*client, db, GIT4DATA_DELTAS_TABLE);
	if (!snapshots) {
		return;
	}
	Git4DataScanRegistryTable(
	    *client, *snapshots,
	    {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::TIMESTAMP, LogicalType::VARCHAR},
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
	Git4DataScanRegistryTable(*client, *branches,
	                          {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::TIMESTAMP,
	                           LogicalType::VARCHAR},
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
	Git4DataScanRegistryTable(*client, *storage,
	                          {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN},
	                          [&](DataChunk &chunk, idx_t row) {
		                          auto branch = chunk.GetValue(0, row).GetValue<string>();
		                          auto source = chunk.GetValue(1, row).GetValue<string>();
		                          auto storage = chunk.GetValue(2, row).GetValue<string>();
		                          auto owned = chunk.GetValue(3, row).GetValue<bool>();
		                          transaction_manager.branches.SetStorageTable(branch, source, storage, 0, 0, owned);
	                          });
	}
	if (deltas) {
	Git4DataScanRegistryTable(*client, *deltas,
	                          {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                          [&](DataChunk &chunk, idx_t row) {
		                          auto branch = chunk.GetValue(0, row).GetValue<string>();
		                          auto source = chunk.GetValue(1, row).GetValue<string>();
		                          auto added = chunk.GetValue(2, row).GetValue<string>();
		                          auto removed = chunk.GetValue(3, row).GetValue<string>();
		                          transaction_manager.branches.SetDeltaTable(branch, source, added, removed);
	                          });
	}
	// what is in the tables is on disk, so a checkpoint does not have to force itself for the registries
	transaction_manager.snapshots.MarkClean();
	transaction_manager.branches.MarkClean();
}

//===----------------------------------------------------------------------===//
// The guard
//===----------------------------------------------------------------------===//
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
