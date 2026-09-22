#include "duckdb/function/table/git4data_internal.hpp"
#include "duckdb/storage/git4data_registry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

//! The table carrying this oid in the default schema, or nothing. A mapping is written in *names*, and a
//! name can be dropped, recreated or renamed under it; the oid is what survives a rename, so it is what
//! lets a read find the table a mapping was made against when its name no longer resolves.

static optional_ptr<TableCatalogEntry> Git4DataTableByOid(ClientContext &context, idx_t oid) {
	if (oid == 0) {
		return nullptr;
	}
	auto &schema = Catalog::GetCatalog(context, INVALID_CATALOG).GetSchema(context, DEFAULT_SCHEMA);
	optional_ptr<TableCatalogEntry> result;
	schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		if (result || entry.oid != oid) {
			return;
		}
		result = &entry.Cast<TableCatalogEntry>();
	});
	return result;
}

string Git4DataResolveTableName(ClientContext &context, const string &name, idx_t oid) {
	auto named = Git4DataResolveTableOrNull(context, name);
	if (named) {
		return name;
	}
	auto by_oid = Git4DataTableByOid(context, oid);
	if (!by_oid) {
		return string();
	}
	return Git4DataTableName(*by_oid);
}

//! A reference is keyed by the table a statement named, and the key is resolved again behind the read. The
//! binder is the last place where that table is still known, so a key that does not resolve back to it is
//! refused here rather than answered from whatever it does reach: the key carries a schema and a name and no
//! catalog, so a read of a table in another attached database is keyed by what names the default database's
//! table - measured, the read would be answered with that table's line.
void Git4DataSnapshotScanFun::CheckReferenceSource(ClientContext &context, TableCatalogEntry &table,
                                                   const string &key) {
	auto by_key = Git4DataResolveTableOrNull(context, key);
	if (by_key && by_key->oid == table.oid) {
		return;
	}
	throw InvalidInputException(
	    "Table \"%s\" is reached as \"%s\" in catalog %s, and a reference is keyed by the table a read names: "
	    "the key \"%s\" reaches %s, so the read would be answered from that table's line. A reference is kept "
	    "in the database it belongs to, and a table of another database cannot be its source.",
	    table.name.GetIdentifierName(), table.ParentSchema().name.GetIdentifierName(), table.ParentCatalog().GetName(),
	    key,
	    by_key ? "\"" + by_key->name.GetIdentifierName() + "\" in schema \"" +
	                 by_key->ParentSchema().name.GetIdentifierName() + "\""
	           : "no table here");
}

//! The table backing a branch for a source table, or empty when the branch has no line of its own
//! there. A mapping whose table has since been dropped falls back to the pinned image rather than
//! failing, because the branch itself is still a valid reference.
//!
//! A table with the *name* of one that was dropped is another matter: it is a different table, and
//! reading it would give the branch a line that has nothing to do with what the branch was given. The
//! oid recorded when the mapping was made is what tells them apart - within a session, since the
//! catalog's oids do not survive a reopen.
static string Git4DataBranchStorageTable(ClientContext &context, const Git4DataBranch &branch,
                                         const string &table_name) {
	// The mapping is keyed by the source table, and a source table can be renamed. The name it was given
	// under then matches nothing, and the branch would silently read its pinned image instead of the line it
	// holds - measured. The source's oid survives a rename, so a name that matches nothing is looked up by
	// oid as well, and the mapping follows the table it was made against. A mapping written before the key
	// carried a schema is matched by the table its stored name resolves to, which is what its name meant.
	auto requested = Git4DataResolveTableOrNull(context, table_name);
	for (auto &entry : branch.storage_tables) {
		if (!Git4DataSourceKeyMatches(context, entry.source, table_name)) {
			if (!requested || entry.source_oid == 0 || entry.source_oid != requested->oid) {
				continue;
			}
		} else {
			// the name matched, which is not the same as the mapping having been made against *this* table: a
			// table dropped and another created with its name reaches here, and a read of it would be answered
			// from the line this branch was given for the table that is gone
			Git4DataCheckSourceBoundTable(context, branch, entry.source, entry.source_oid,
			                              requested ? requested->oid : 0);
		}
		auto storage = Git4DataResolveTableOrNull(context, entry.storage);
		if (!storage) {
			// the name it was given no longer resolves, which is what a rename looks like: the oid is
			// what tells the table it was given from any other
			storage = Git4DataTableByOid(context, entry.oid);
		}
		if (!storage) {
			continue;
		}
		// the name it has now, not the one the mapping was written with
		auto resolved = Git4DataTableName(*storage);
		if (entry.oid != 0 && entry.oid != storage->oid) {
			throw InvalidInputException(
			    "Branch \"%s\" was given table \"%s\" for \"%s\", and the table carrying that name now is not it - "
			    "the table it was given is no longer reachable under that name. Point the branch at a table of its "
			    "own again with git4data_bind_branch_table.",
			    branch.name, entry.storage, table_name);
		}
		return resolved;
	}
	return string();
}

//! How a reference is read: the table to scan, the bound to scan it at, and the overlays that stand
//! between the two. Usually none (a pin of a table, or a copy of it), one for a branch that differs by a
//! delta - and **two** for a branch that inherited a line which differs by a delta and has one of its
//! own, which apply in order: the inherited line first, then what the branch has done since.

//! Where a branch's line lives at a commit, and what it differs by. A branch is a pin plus what it
//! inherits, so its state at a commit is one table read at a bound - the base, which does not move -
//! plus the overlays it carries, each read at the commit its own line pinned. AllCommitted as the bound
//! means the state now.
struct Git4DataLineState {
	string base_table;
	//! Whether `base_table` is a branch's own copy rather than the source table
	bool own_storage = false;
	VisibilityBound base_bound;
	vector<Git4DataDeltaLayer> deltas;
};

//! A delta's two tables have to be able to stand in for the source: the added rows are read as rows of
//! it, and the removed table names its rows by key. Checked when the delta is named and again on every
//! read, because DDL afterwards can leave either of them unable to - which the read would otherwise
//! discover as an index out of range rather than as a sentence.
static void Git4DataCheckDeltaColumns(TableCatalogEntry &source, TableCatalogEntry &added, TableCatalogEntry &removed) {
	auto &source_columns = source.GetColumns();
	if (added.GetColumns().GetColumnNames() != source_columns.GetColumnNames() ||
	    added.GetColumns().GetColumnTypes() != source_columns.GetColumnTypes()) {
		throw InvalidInputException("Table \"%s\" holds the rows added to \"%s\", and it no longer has that table's "
		                            "columns",
		                            added.name.GetIdentifierName(), source.name.GetIdentifierName());
	}
	vector<StorageIndex> key_columns;
	vector<LogicalType> key_types;
	vector<Identifier> key_names;
	Git4DataDiffPrimaryKey(source, key_columns, key_types, key_names);
	vector<string> expected_names;
	vector<LogicalType> expected_types;
	for (idx_t i = 0; i < source_columns.LogicalColumnCount(); i++) {
		auto &column = source_columns.GetColumn(LogicalIndex(i));
		auto name = column.Name().GetIdentifierName();
		bool is_key = false;
		for (auto &key_name : key_names) {
			if (key_name.GetIdentifierName() == name) {
				is_key = true;
				break;
			}
		}
		if (!is_key) {
			continue;
		}
		expected_names.push_back(name);
		expected_types.push_back(column.Type());
	}
	if (removed.GetColumns().GetColumnNames() != expected_names ||
	    removed.GetColumns().GetColumnTypes() != expected_types) {
		throw InvalidInputException("Table \"%s\" holds the keys removed from \"%s\", and it no longer has that "
		                            "table's key columns",
		                            removed.name.GetIdentifierName(), source.name.GetIdentifierName());
	}
}

//! Whether one table's columns are the first columns of another's: the same names and types, in the same order,
//! and fewer of them. That is the shape a copy has when the table it was made from gained a column afterwards,
//! and it is a different thing from a table of a different shape - there the columns a read maps by position
//! really would be someone else's.
static bool Git4DataDiffColumnsArePrefix(const vector<string> &names, const vector<LogicalType> &types,
                                         const vector<string> &other_names, const vector<LogicalType> &other_types) {
	if (names.size() >= other_names.size() || names.size() != types.size()) {
		return false;
	}
	for (idx_t i = 0; i < names.size(); i++) {
		if (names[i] != other_names[i] || types[i] != other_types[i]) {
			return false;
		}
	}
	return true;
}

//! Refuse a read at a pin when the mapping it would answer through was set *after* that pin.
//!
//! A capture names a branch, and this read resolves that branch's mapping at the time of the read while bounding
//! it by the capture's own pin - so a mapping set after the pin makes the read answer through a table that did
//! not exist at the pin. Measured before this check (3dr): the capture and the forks of it read **no rows at
//! all**, while the branch itself still read 100 and every listing column said nothing was wrong. The mapping
//! records the commit it was set at (decision of 2026-09-21), and this is where that value is used: a mapping
//! with no value (a file written before the column) is *unknown* and checks nothing, which is the behaviour
//! before the column existed.
//! Refuse a read through a mapping whose table is not the table it was made against.
//!
//! The `oid` a mapping also carries is the catalog's own counter and does not survive a reopen, so it catches a
//! table dropped and recreated *within* a session and nothing else. The identity the table itself carries does
//! survive - it is written with the table's metadata - and this is where it is compared (decision 2 of the three
//! format decisions, 2026-09-21). A mapping with no value (a row from before the column, or one written by hand)
//! is *unknown* and checks nothing.
static void Git4DataCheckMappingTable(ClientContext &context, const string &branch_name, const string &table_name,
                                      uint64_t mapping_table_id, const char *what) {
	if (mapping_table_id == 0) {
		return;
	}
	auto table = Git4DataResolveTableOrNull(context, table_name);
	if (!table) {
		// gone: the documented fallback to the branch's pinned image, not a swap
		return;
	}
	if (!Git4DataTableIdentityIsFromDisk(*table)) {
		// The table carries an identity nothing has persisted - it was created by a statement the log replayed,
		// and the statement that assigns an identity is internal and is not logged - so there is no fact about
		// the table to compare the mapping against and nothing is refused. The mapping's row *is* durable, so
		// the table adopts its value here: the two agree, the next checkpoint writes it, and until then no read
		// is refused on a value this process invented.
		//
		// **This write looks exactly like the laundering 3el retracted in the checkpoint writer, and it is not
		// the same thing - measured, increment 3fk.** Removing it was tried: the crash-durability instrument
		// went from 54 checks 0 failed to 55 checks 2 failed and the load-after-replay instrument from 26/0 to
		// 27/3, because a table the *log* recreated has no identity that can come from anywhere else. The
		// writer mints a fresh one for every table it checkpoints, so without this write the mapping's value and
		// the table's diverge at the next checkpoint and a legitimate read is refused. The two cases - a table
		// the log replayed, and a table that took the mapping's name - are indistinguishable here, which is the
		// boundary the note calls F-B, and telling them apart needs a durable per-instance token written when
		// the table is created. Until that exists, this write stays and the laundering it enables is documented
		// rather than removed.
		table->GetStorage().GetDataTableInfo()->SetPersistedTableId(mapping_table_id);
		return;
	}
	if (Git4DataTableIdentity(*table) == mapping_table_id) {
		return;
	}
	throw InvalidInputException(
	    "Branch \"%s\" reads table \"%s\" as its %s, and that is not the table it was given: the table carrying "
	    "that name now is another one - dropped and recreated, or replaced while the database was closed - and "
	    "reading it would answer from a table this reference never saw. Bind the branch again with "
	    "git4data_%s.",
	    branch_name, table_name, what, StringUtil::CIEquals(what, "delta") ? "bind_branch_delta" : "bind_branch_table");
}

static void Git4DataCheckMappingAge(const Git4DataBranch &branch, const string &mapping_name,
                                    transaction_t mapping_set_at, transaction_t check_pin, const char *what) {
	if (check_pin == 0 || mapping_set_at == 0 || mapping_set_at <= check_pin) {
		return;
	}
	throw InvalidInputException(
	    "Branch \"%s\" was given %s \"%s\" at commit %llu, and this read is at commit %llu: the line asked for "
	    "predates the %s that would answer it, so the read would come back through a table that did not exist "
	    "then. Read the branch as it stands now, or capture the line again.",
	    branch.name, what, mapping_name, (unsigned long long)mapping_set_at, (unsigned long long)check_pin, what);
}

static Git4DataLineState ResolveGit4DataLineState(ClientContext &context, DuckTransactionManager &transaction_manager,
                                                  const string &table_name, const Git4DataBranch &branch,
                                                  VisibilityBound at, transaction_t check_pin = 0) {
	Git4DataLineState state;
	state.base_bound = at;
	auto storage = Git4DataBranchStorageTable(context, branch, table_name);
	if (!storage.empty()) {
		// the branch's own line is a table: read as it is now, or as it stood at the commit asked for.
		// Its columns are mapped onto the source's by position, so the shape is checked on every read as
		// well as when it was bound: DDL afterwards can leave the table unable to stand in, and the read
		// would otherwise swap or rename what a caller sees - measured, a table holding (v, k) read back
		// as k = 777 and v = 1 from a row that held k = 1 and v = 777.
		auto &storage_entry = Git4DataResolveTable(context, storage);
		{
			auto &source_entry = Git4DataResolveTable(context, table_name);
			auto storage_names = storage_entry.GetColumns().GetColumnNames();
			auto source_names = source_entry.GetColumns().GetColumnNames();
			auto storage_types = storage_entry.GetColumns().GetColumnTypes();
			auto source_types = source_entry.GetColumns().GetColumnTypes();
			if (storage_names != source_names || storage_types != source_types) {
				if (Git4DataDiffColumnsArePrefix(storage_names, storage_types, source_names, source_types)) {
					// The table has the source's columns and not the ones added since: nothing is swapped,
					// the read is simply short of them. What it needs is the columns themselves, which the
					// caller can add to it and keep the branch's own line - the table is theirs.
					throw InvalidInputException(
					    "Branch \"%s\" reads table \"%s\" as its own line for \"%s\", which has columns that "
					    "table does not, so a read of the branch would be short of them. Add them to it (ALTER "
					    "TABLE \"%s\" ADD COLUMN ...), or copy the source's current shape with another "
					    "git4data_materialize.",
					    branch.name, storage, table_name, storage);
				}
				throw InvalidInputException(
				    "Branch \"%s\" reads table \"%s\" as its own line for \"%s\", and that table no longer has "
				    "that table's columns in that order - a read maps them by position, so the values would be "
				    "someone else's. Point the branch at a table of its own again with git4data_bind_branch_table.",
				    branch.name, storage, table_name);
			}
		}
		// the mapping this read is about to use, and when it was set
		for (auto &mapping : branch.storage_tables) {
			// the mapping this read is about - asked of the table itself, because what the row holds is whatever
			// spelling it was written with and `storage` is the name the catalog gives the table
			if (Git4DataMappingNameMatches(storage_entry.ParentSchema().name, storage_entry.name, mapping.storage)) {
				Git4DataCheckMappingAge(branch, storage, mapping.set_at, check_pin, "table");
				Git4DataCheckMappingTable(context, branch.name, storage, mapping.table_id, "table");
			}
		}
		state.base_table = storage;
		state.own_storage = true;
	} else if (!branch.base_line.empty()) {
		// it inherited a line, and that inheritance is fixed at this branch's pin - whatever commit is
		// being asked about, the state it started from is the one the line had when it forked
		Git4DataBranch line_branch;
		if (transaction_manager.branches.Get(branch.base_line, line_branch)) {
			auto inherited = ResolveGit4DataLineState(context, transaction_manager, table_name, line_branch,
			                                          VisibilityBound::Through(branch.pin), branch.pin);
			state.base_table = inherited.base_table;
			state.own_storage = inherited.own_storage;
			state.base_bound = inherited.base_bound;
			// the line it inherited carries its own overlays, at the commits *it* pinned them - the
			// branch's own delta is appended after them below, so they are applied first
			state.deltas = inherited.deltas;
		}
	}
	if (state.base_table.empty()) {
		// no line of its own and nothing inherited: the base is the source table at this branch's pin
		state.base_table = table_name;
		state.base_bound = VisibilityBound::Through(branch.pin);
	}
	// the branch's own delta, read at the commit whose state was asked for
	Git4DataDeltaLayer own;
	own.bound = at;
	for (auto &delta : branch.deltas) {
		Git4DataCheckMappingAge(branch, delta.added, delta.set_at, check_pin, "delta");
		// the source's name can change under the mapping, and its oid is what survives that
		auto &requested = Git4DataResolveTable(context, table_name);
		if (!Git4DataSourceKeyMatches(context, delta.source, table_name)) {
			if (delta.source_oid == 0 || delta.source_oid != requested.oid) {
				continue;
			}
		} else {
			// the same refusal as the storage mapping's: a name that now reaches another table is not the one
			// this mapping was made against
			Git4DataCheckSourceBoundTable(context, branch, delta.source, delta.source_oid, requested.oid);
		}
		// the delta's tables are the caller's: one that has since been dropped falls back to the base, and
		// one that was *renamed* is found by oid, because the caller meant that table
		auto added_table = Git4DataResolveTableOrNull(context, delta.added);
		if (!added_table) {
			added_table = Git4DataTableByOid(context, delta.added_oid);
		}
		auto removed_table = Git4DataResolveTableOrNull(context, delta.removed);
		if (!removed_table) {
			removed_table = Git4DataTableByOid(context, delta.removed_oid);
		}
		if (added_table && removed_table) {
			// a table that took the name of one of these after it was dropped is a different table, and
			// reading it would apply a delta the caller never made
			if ((delta.added_oid != 0 && delta.added_oid != added_table->oid) ||
			    (delta.removed_oid != 0 && delta.removed_oid != removed_table->oid)) {
				throw InvalidInputException(
				    "Branch \"%s\" was given tables \"%s\" and \"%s\" for \"%s\", and the tables carrying those names "
				    "now are not them - the tables it was given are no longer reachable under those names. Bind the "
				    "delta "
				    "again with git4data_bind_branch_delta.",
				    branch.name, delta.added, delta.removed, table_name);
			}
			// after the oid refusal above, which is the one that speaks for a swap *within* a session: the
			// identity is what speaks across a reopen, where the oids are gone
			Git4DataCheckMappingTable(context, branch.name, delta.added, delta.added_table_id, "delta");
			Git4DataCheckMappingTable(context, branch.name, delta.removed, delta.removed_table_id, "delta");
			auto &source = Git4DataResolveTable(context, table_name);
			Git4DataCheckDeltaColumns(source, *added_table, *removed_table);
			own.added = Git4DataTableName(*added_table);
			own.removed = Git4DataTableName(*removed_table);
		}
	}
	if (!own.added.empty() || !own.removed.empty()) {
		// The overlays apply in order, the inherited ones first: what the line it took looks like, then
		// what the branch has done since. Each is read at the commit its own line pinned, which is why
		// they cannot be collapsed into one - the inherited one does not move with the branch.
		state.deltas.push_back(own);
	}
	return state;
}

//! The plan for one reference. The scan and the diff both come here, so the two cannot disagree about
//! which table a reference means - a disagreement that compares row ids of different tables and
//! reports no change at all.
void ResolveGit4DataRead(ClientContext &context, DuckTransactionManager &transaction_manager,
                                const string &table_name, const string &name, Git4DataRefKind kind,
                                Git4DataReadPlan &plan) {
	if (kind == Git4DataRefKind::SNAPSHOT) {
		Git4DataSnapshot snapshot;
		// A snapshot another transaction added and has not committed is not there yet, the same way its row in
		// the registry table is not: the registries are shared memory and the table is the caller's view
		if (!Git4DataReferenceVisible(context, "snapshot", name) ||
		    !transaction_manager.snapshots.Get(name, snapshot)) {
			throw InvalidInputException("Snapshot \"%s\" does not exist", name);
		}
		plan.table_name = table_name;
		plan.bound = VisibilityBound::Through(snapshot.pin);
		if (!snapshot.line.empty()) {
			// It captured a branch's own line, so what it names is that line as it stood at the pin -
			// which is a base that does not move plus that branch's overlays read at the pin. A line whose
			// branch is gone falls back to the pinned image, the way a branch whose storage was dropped
			// does.
			Git4DataBranch line_branch;
			if (transaction_manager.branches.Get(snapshot.line, line_branch)) {
				auto state = ResolveGit4DataLineState(context, transaction_manager, table_name, line_branch,
				                                      VisibilityBound::Through(snapshot.pin), snapshot.pin);
				plan.table_name = state.base_table;
				plan.own_storage = state.own_storage;
				plan.bound = state.base_bound;
				plan.deltas = state.deltas;
			}
		}
		return;
	}
	Git4DataBranch branch;
	if (!Git4DataReferenceVisible(context, "branch", name) || !transaction_manager.branches.Get(name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", name);
	}
	// what the branch holds now: its base, and its own delta if it has one
	auto state =
	    ResolveGit4DataLineState(context, transaction_manager, table_name, branch, VisibilityBound::AllCommitted());
	plan.table_name = state.base_table;
	plan.own_storage = state.own_storage;
	plan.bound = state.base_bound;
	plan.deltas = state.deltas;
}

Git4DataReadPlan ResolveGit4DataReferencePlan(ClientContext &context,
                                                     DuckTransactionManager &transaction_manager,
                                                     const string &table_name, const string &name) {
	if (name == DuckTransactionManager::LIVE_REFERENCE) {
		// The table as it stands now, read through the caller's own transaction - including rows it has not
		// committed, which is what "the live table" means everywhere else in this feature. A caller asking
		// what a branch has that the table does not would otherwise have to take a reference first, and a
		// reference pins the timeline: the answer is about *now*, so keeping history alive for it would be a
		// cost paid for a question that needs none. Its bound is everything committed, and the side is read
		// as the caller's own table so that its uncommitted rows are part of it.
		Git4DataReadPlan live;
		live.table_name = table_name;
		live.own_storage = true;
		live.bound = VisibilityBound::AllCommitted();
		return live;
	}
	Git4DataRefKind kind;
	if (transaction_manager.snapshots.GetPin(name).IsValid()) {
		kind = Git4DataRefKind::SNAPSHOT;
	} else {
		// the name has to be in one of the two registries, and a caller that passed neither wants to be
		// told that rather than which of the two was looked in
		Git4DataBranch branch;
		if (!transaction_manager.branches.Get(name, branch)) {
			throw InvalidInputException("Reference \"%s\" is neither a snapshot nor a branch (the table as it "
			                            "stands now is named \"%s\")",
			                            name, DuckTransactionManager::LIVE_REFERENCE);
		}
		kind = Git4DataRefKind::BRANCH;
	}
	Git4DataReadPlan plan;
	ResolveGit4DataRead(context, transaction_manager, table_name, name, kind, plan);
	return plan;
}
//! Refuse a table that has a generated column, which a reference read cannot produce.
//!
//! **Measured, and it was a wrong answer rather than a refusal.** The engine does not store a generated column
//! (`pragma_storage_info` lists no segment for a `g INTEGER GENERATED ALWAYS AS (v * 2)` column), so the value a
//! caller sees is computed by the binder on top of an ordinary scan - and a reference reads a table's *stored*
//! columns through its own row-group scan, which is not that. It used to take the column's physical index anyway
//! and return whatever sat in that slot: the `g` above read back as the row's own index (0 and 1) while reading
//! the table gave 20 and 40.
//!
//! Every surface reads through here or through the diff's bind, and **the diff alone would work**: it skips
//! generated columns by design (`Git4DataDiffInit`), because it compares keys and compares values only to tell a
//! changed row from one that is the same - so it never needs a column it cannot read. Refusing it anyway is the
//! choice this file makes, because a table with a generated column that can be diffed but not read is a rule a
//! caller has to hold in their head. What would make the whole feature work is **materializing the column**: a
//! copy spells out its columns, and one written as an ordinary column rather than as the expression would hold
//! real values, so a *materialized* branch could be read. That is a design decision (the copy's column is then
//! writable, where the source's is not) and it is recorded rather than taken.
void Git4DataCheckNoGeneratedColumns(ClientContext &context, const string &table_name) {
	auto &source = Git4DataResolveTable(context, table_name);
	for (auto &column : source.GetColumns().Logical()) {
		if (!column.Generated()) {
			continue;
		}
		throw InvalidInputException(
		    "Table \"%s\" has a generated column (%s), and a reference reads a table's stored columns through "
		    "its own scan - a generated column is not stored, so the read would have to compute it, which is what "
		    "the binder does and this scan does not. Make the column an ordinary one, or read the table without a "
		    "reference.",
		    table_name, column.Name());
	}
}
} // namespace duckdb
