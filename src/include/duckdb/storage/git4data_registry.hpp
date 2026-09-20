//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/git4data_registry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/duck_transaction_manager.hpp"
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
//! The one table that holds no references: the database-wide settings the feature keeps, which used to be
//! properties of the checkpoint's registry block. Rows are `key`/`value`.
#define GIT4DATA_STATE_TABLE     "state"

//! Create the schema and the four tables the references are kept in, if they are not there yet, and return
//! the statement that does it. **Nothing is migrated into them**: the build before this one kept a caller's
//! references in a metadata block of the checkpoint, and the decision of 2026-09-20 is that the block is
//! deleted rather than carried, so a file written by that build opens with the tables this statement creates
//! - empty - and the records in its block are not read.
string Git4DataEnsureRegistryTables(ClientContext &context);
//! Whether those tables are there. A file that has none is a file whose references this build does not have:
//! either one written before the tables existed, or one in which nothing has taken a reference yet.
bool Git4DataRegistryTablesExist(AttachedDatabase &db);
//! The same question from a statement, which has a client context of its own
bool Git4DataRegistryTablesThere(ClientContext &context);
//! Read the tables back into the in-memory registries, replacing what the metadata block loaded. Called
//! once the log has been replayed and before the database serves anything: a change to a reference is an
//! ordinary change to those tables, so the replay is what brings the last of them back.
//!
//! The registries are rebuilt from the tables, and from nothing else: the references the engine moves itself
//! are rows too (the window's row carries its length in `line`), so there is no second copy to keep.
//! as well as from the tables, and only the open wants that: there, memory holds the checkpoint's block,
//! which carries the pin the engine *moved* at a checkpoint and which no row can know. After a rollback,
//! memory holds the state the rollback has just undone - measured, a rolled-back `git4data_pitr(1)` won over
//! the row the rollback restored and left the window at one day - so the tables decide alone.
void Git4DataLoadRegistry(AttachedDatabase &db);
//! Throw when a statement writes one of the registry's tables, or drops or alters it. They are ordinary
//! tables to the engine, so this is the only thing between a caller and a state the feature depends on: a
//! deleted snapshot row takes its pin with it, and the history that pin holds down is then reclaimed under a
//! reference that still names it, which reads as a wrong answer rather than as an error.
void Git4DataCheckRegistryWrite(const TableCatalogEntry &table);
//! The same for a schema a statement names: dropping the schema the registry lives in is another way to take
//! every reference at once.
void Git4DataCheckRegistrySchema(const string &schema);
//! Throw when a statement creates a table in the schema the registry is kept in. DROP and ALTER are not the
//! only ways to take it apart: measured, `CREATE OR REPLACE TABLE git4data.snapshots (a INTEGER)` replaced the
//! table and every reference with it, and `CREATE TABLE git4data.mine (...)` put a caller's table in the
//! feature's own schema - neither goes through the guard the write, drop and alter statements have.
void Git4DataCheckRegistryCreate(const string &schema, const string &table);
//! Throw when a statement tries to create the schema the registry is kept in. The name is reserved whether or
//! not the feature has created that schema yet, and both halves were measured before this guard: on a database
//! with references the caller met the catalog's duplicate-name error - `Schema with name "git4data" already
//! exists!` - which names a schema that no listing shows, and on a fresh one the schema was created and then
//! disappeared from every listing while every statement inside it was refused as the registry's.
void Git4DataCheckRegistrySchemaCreate(const string &schema);
//! Throw when `AT (SNAPSHOT/BRANCH => ...)` is written on something that is not a table. A reference is kept for
//! the table a statement names, so nothing else can be read at one - and measured before this guard, a view and
//! a common table expression both *accepted* the clause and ignored it: the read answered from the live table
//! while the caller had asked for a state. The refusal lives here rather than beside its call site because the
//! call site is a shared binder file, and the audit's refusal scan reads the feature's own files.
void Git4DataCheckReferenceAtName(const string &name, bool branch, const string &what);
//! Rebuild the registries if a rollback has happened since the last rebuild, whichever connection made it.
//! The registries are shared, so a rollback anywhere can have taken a record away that memory still holds:
//! consuming the flag on the transaction manager - not only the one on the calling client - is what keeps a
//! rolled-back reference from being readable by another connection until the rolling-back one asks again.
void Git4DataReconcileIfNeeded(ClientContext &context);

//! Whether a caller may use a reference of this kind under this name. A reference is a row in the registry
//! tables, so one another transaction has added and has not committed is not there yet - the tables say so
//! themselves, and this is what keeps the in-memory registries from disagreeing with them. Measured before it:
//! a snapshot created inside one connection's transaction was listed by another connection and read through
//! it, while `SELECT ... FROM git4data.snapshots` correctly showed nothing.
bool Git4DataReferenceVisible(ClientContext &context, const string &kind, const string &name);

//! Rebuild the in-memory registries after a rollback. When the tables are there they are read again; when
//! they are not and the file's records never came from them, the rollback took the tables with it - the very
//! first reference of a database creates them in the same transaction - so the registries are emptied rather
//! than left holding references that are no longer anywhere on disk.
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
//! The key a reference is held under for a table: its schema and its name, always qualified, because two
//! schemas can hold a table of the same name and a bare key would also resolve through the search path rather
//! than to the table the statement named.
string Git4DataSourceKey(const TableCatalogEntry &table);
//! The same key for the table a caller named, however it was written; a name that resolves to nothing is
//! returned as written, so the caller's own error is the one that surfaces.
string Git4DataSourceKey(ClientContext &context, const string &written);
//! Whether a record's stored source name is about the table a key names. A record written before keys carried
//! a schema holds the name the caller wrote, so the stored name is matched as text first and then by the table
//! it resolves to.
bool Git4DataSourceKeyMatches(ClientContext &context, const string &stored, const string &key);
//! The table a branch was given for a source, or the delta it differs by, matched by key and then by the table
//! a stored name resolves to and by the source's oid. A record written before keys carried a schema holds the
//! name the caller wrote, and a source that has been renamed matches by neither name - the oid is what follows
//! it, exactly as on a read.
optional_ptr<const Git4DataStorageTable> Git4DataFindStorageTable(ClientContext &context, const Git4DataBranch &branch,
                                                                  const string &key, idx_t source_oid = 0);
optional_ptr<const Git4DataBranchDelta> Git4DataFindDeltaTable(ClientContext &context, const Git4DataBranch &branch,
                                                               const string &key, idx_t source_oid = 0);

//! Refuse a mapping whose source name now reaches a **different** table than the one it was made against.
//!
//! A mapping is found by the key a read names and by the oid of the table it was made against, because a rename
//! takes the name away and leaves the oid. What the oid also says is that a name which now reaches another table
//! is not the table the mapping was made against - and that was silent: measured (increment 3cb), after
//! `DROP TABLE s; CREATE TABLE s (...)`, a branch bound to `s` read the *new* table as if it were the one it was
//! given, and a write through `git4data_apply` landed in the mapping's own tables as that table's line, while the
//! listing said `delta_source = s` and `delta_missing = false`. The mapping's own tables have refused this since
//! they were built ("the tables carrying those names now are not them"); the source half was missing. A mapping
//! whose oid is unknown - one read back from a file, where the catalog's ids do not survive - is not compared, so
//! this fires only where the engine can tell.
void Git4DataCheckSourceBoundTable(ClientContext &context, const Git4DataBranch &branch, const string &stored_source,
                                   idx_t stored_oid, idx_t requested_oid);

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
