//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/git4data_registry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {
class AttachedDatabase;
class ClientContext;
class TableCatalogEntry;
struct QueryContext;

//! The schema the reference registry is kept in, and the tables inside it.
//!
//! A normal schema rather than an internal one, and that is forced rather than chosen: the engine allows an
//! internal entry only in the system catalog or as a schema named "main" (catalog_set.cpp:133), and the
//! system catalog is rebuilt at every startup, so an internal table could not be persisted. What the
//! internal flag would have given - being hidden, and refused for ALTER and DROP - is done here instead:
//! see Git4DataIsRegistryTable and Git4DataCheckRegistryTarget.
#define GIT4DATA_REGISTRY_SCHEMA "git4data"
#define GIT4DATA_SNAPSHOTS_TABLE "snapshots"
#define GIT4DATA_BRANCHES_TABLE "branches"
#define GIT4DATA_STORAGE_TABLE "storage"
#define GIT4DATA_DELTAS_TABLE "deltas"

//! Create the schema and the four tables the references are kept in, if they are not there yet, and return
//! the statements that migrate what a file written before them already holds into them. The tables become
//! the truth for the records they carry the moment they exist, so a reference that only the metadata block
//! knows about has to be copied across as they are created - and only then, or every later call would
//! insert the references again.
string Git4DataEnsureRegistryTables(ClientContext &context);
//! Whether those tables are there. This is what tells a file written before them from one written after:
//! the older form carries the same records in the checkpoint's metadata block instead.
bool Git4DataRegistryTablesExist(AttachedDatabase &db);
//! The same question from a statement, which has a client context of its own
bool Git4DataRegistryTablesThere(ClientContext &context);
//! Read the tables back into the in-memory registries, replacing what the metadata block loaded. Called
//! once the log has been replayed and before the database serves anything: a change to a reference is an
//! ordinary change to those tables, so the replay is what brings the last of them back.
void Git4DataLoadRegistry(AttachedDatabase &db);
//! Throw when a statement writes one of the registry's tables, or drops or alters it. They are ordinary
//! tables to the engine, so this is the only thing between a caller and a state the feature depends on: a
//! deleted snapshot row takes its pin with it, and the history that pin holds down is then reclaimed under a
//! reference that still names it, which reads as a wrong answer rather than as an error.
void Git4DataCheckRegistryWrite(const TableCatalogEntry &table);
//! The same for a schema a statement names: dropping the schema the registry lives in is another way to take
//! every reference at once.
void Git4DataCheckRegistrySchema(const string &schema);
//! Rebuild the in-memory registries after a rollback. When the tables are there they are read again; when
//! they are not and the file's records never came from them, the rollback took the tables with it - the very
//! first reference of a database creates them in the same transaction - so the registries are emptied rather
//! than left holding references that are no longer anywhere on disk.
void Git4DataReloadRegistry(AttachedDatabase &db);
//! Whether a schema name is the one the registry keeps its references in
bool Git4DataIsRegistrySchema(const string &schema);
//! Whether a table is one the registry keeps its references in
bool Git4DataIsRegistryTable(const TableCatalogEntry &table);
//! Throw when a statement writes or drops one of the registry's tables, or the schema that holds them.
//! They are ordinary tables to the engine, so this is the only thing standing between a caller and a state
//! the feature depends on: a deleted snapshot row takes its pin with it, and the history that pin holds
//! down would be reclaimed under a reference that still names it.
void Git4DataCheckRegistryTarget(const string &schema, const string &table);

} // namespace duckdb
