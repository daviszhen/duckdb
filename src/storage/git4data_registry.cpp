#include "duckdb/storage/git4data_registry.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
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
	// `table_id` is the identity the mapped table carries (`DataTableInfo::persisted_table_id`), and it is what
	// outlives a reopen: the catalog's oids do not (3bs, 3ds), so without it a table dropped and recreated under
	// the same name while the database was closed is read as the table the mapping was made against - silently.
	result += "CREATE TABLE IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_STORAGE_TABLE
	          "(branch VARCHAR, source VARCHAR, storage VARCHAR, owned BOOLEAN, set_at UBIGINT, table_id UBIGINT);\n";
	result += "CREATE TABLE IF NOT EXISTS " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_DELTAS_TABLE
	          "(branch VARCHAR, source VARCHAR, added VARCHAR, removed VARCHAR, set_at UBIGINT, added_table_id "
	          "UBIGINT, removed_table_id UBIGINT);\n";
	// A file written by an earlier build of this branch has those two tables without the column, and a table
	// the caller cannot be asked to rebuild: the statement adds it, with no value in it, which reads as
	// "unknown" and is the behaviour before this column existed.
	result += "ALTER TABLE " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_STORAGE_TABLE
	          " ADD COLUMN IF NOT EXISTS set_at UBIGINT;\n";
	result += "ALTER TABLE " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_DELTAS_TABLE
	          " ADD COLUMN IF NOT EXISTS set_at UBIGINT;\n";
	result += "ALTER TABLE " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_STORAGE_TABLE
	          " ADD COLUMN IF NOT EXISTS table_id UBIGINT;\n";
	result += "ALTER TABLE " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_DELTAS_TABLE
	          " ADD COLUMN IF NOT EXISTS added_table_id UBIGINT;\n";
	result += "ALTER TABLE " GIT4DATA_REGISTRY_SCHEMA "." GIT4DATA_DELTAS_TABLE
	          " ADD COLUMN IF NOT EXISTS removed_table_id UBIGINT;\n";
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

} // namespace duckdb
