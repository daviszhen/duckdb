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
#define GIT4DATA_BRANCHES_TABLE  "branches"
#define GIT4DATA_STORAGE_TABLE   "storage"
#define GIT4DATA_DELTAS_TABLE    "deltas"

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
//! Rebuild after a rollback on the connection that made it. What that transaction added is dropped, what the
//! tables hold is put back, and anything else in memory is left alone: another connection's uncommitted
//! records are not this one's to remove, which a clear-and-reload would do - measured, with two connections,
//! one of them rolling back a real transaction under the other's feet.
//! `added` holds the records this transaction added, as "<kind>\t<key>" strings.
void Git4DataReconcileRegistry(AttachedDatabase &db, const vector<string> &added);
//! Resolve a table by a name a record holds, which is written one of two ways. A name a caller wrote may be
//! qualified, and then a dot separates a schema from a table (which `QualifiedName(Identifier("s2.t"))` does
//! not do: it looks for a table whose *name* is "s2.t"). A name taken from the catalog is one identifier, so
//! a table called `a.b` is found by that name and not as a table `b` in a schema `a`. The literal name is
//! tried first, so a table carrying that name exactly is the one reached, and the qualified reading answers
//! the rest.
TableCatalogEntry &Git4DataResolveTable(ClientContext &context, const string &name);
//! The same, or nothing when it does not resolve
optional_ptr<TableCatalogEntry> Git4DataResolveTableOrNull(ClientContext &context, const string &name);
//! The name a table is held under in the registry, which resolves back to it: the schema it is in, unless
//! that is the default one. Records name tables as strings and every lookup resolves that string again, so a
//! bare name for a table outside the default schema would reach another table or none.
string Git4DataTableName(const TableCatalogEntry &table);
//! A possibly-qualified name as SQL, with every component quoted as its own identifier
string Git4DataTableSQLName(const string &name);

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
