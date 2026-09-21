//===----------------------------------------------------------------------===//
// Git4Data -- the feature's pragmas
//===----------------------------------------------------------------------===//
// Moved out of pragma_functions.cpp by increment 3er without changing a line of any of them, apart from taking
// the feature's `set.AddFunction` lines out of that file's registration and into RegisterGit4DataPragmas below.
// What they share with the rest of the feature is in git4data_registry.hpp and the read path's own header.
//===----------------------------------------------------------------------===//

#include "duckdb/function/pragma/pragma_functions.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>

#ifdef __APPLE__
#include <sys/clonefile.h>
#endif
#ifdef __linux__
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <unistd.h>
#endif

#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/git4data_registry.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/tableref/at_clause.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/literal.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <cctype>

#include "duckdb/function/pragma/git4data_pragmas.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Git4Data snapshots
//===----------------------------------------------------------------------===//

DuckTransactionManager &GetGit4DataTransactionManager(ClientContext &context) {
	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	if (!attached) {
		throw InvalidInputException("Git4Data snapshots require an attached database");
	}
	auto &transaction_manager = DuckTransactionManager::Get(*attached);
	// A rollback took rows out of the registry tables and memory still holds them. It may have been another
	// connection's rollback - the registries are shared - so the flag that asks for a rebuild is consumed on
	// the transaction manager as well as on this client, and every pragma comes through here.
	Git4DataReconcileIfNeeded(context);
	return transaction_manager;
}

//! A string as the body of a SQL literal: the quotes doubled, so what sits between them is one value and the
//! string cannot end the literal early. Every name, reference and table spelling this file interpolates goes
//! through here - it was eleven copies of the same lambda before, each one a place to get it wrong.
string Git4DataSQLLiteral(const string &value) {
	return StringUtil::Replace(value, "'", "''");
}

//! The branch a pragma was given, or the refusal each of them makes when it is not there: a branch another
//! transaction added and has not committed is not there yet, which is the same rule the reads follow. One
//! function rather than eleven copies, which is what this was - the copies had drifted only in the name of the
//! variable they filled in, and a message is worth getting right once.
Git4DataBranch Git4DataRequiredBranch(ClientContext &context, DuckTransactionManager &transaction_manager,
                                             const string &name) {
	Git4DataBranch branch;
	if (!Git4DataReferenceVisible(context, "branch", name) || !transaction_manager.branches.Get(name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", name);
	}
	return branch;
}

//! The AT unit a reference has to be read through, throwing when neither registry has the name
const char *ResolveGit4DataReference(DuckTransactionManager &transaction_manager, const string &name) {
	if (transaction_manager.snapshots.GetPin(name).IsValid()) {
		return "SNAPSHOT";
	}
	Git4DataBranch branch;
	if (transaction_manager.branches.Get(name, branch)) {
		return "BRANCH";
	}
	throw InvalidInputException("Reference \"%s\" is neither a snapshot nor a branch", name);
}

//! A table read as of a reference, spelled the way the binder accepts it. The unit has to match
//! the registry the name lives in, or the read is looked up in the wrong one.
string FormatGit4DataAtClause(const string &table_name, const string &reference, const char *unit) {
	return StringUtil::Format("%s AT (%s => '%s')", Git4DataTableSQLName(table_name), unit,
	                          Git4DataSQLLiteral(reference));
}

//! The primary key's column names, empty when the table has none
vector<string> Git4DataPrimaryKeyColumnNames(TableCatalogEntry &table) {
	vector<string> result;
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		if (!unique.IsPrimaryKey()) {
			continue;
		}
		for (auto &name : unique.GetColumnNames()) {
			result.push_back(name.GetIdentifierName());
		}
		break;
	}
	return result;
}

//! Restore a table's contents to what a reference saw.
//!
//! **A table with a primary key is put right by writing only the rows that differ.** The diff against the
//! *live* table says which keys the reference does not have and which it has with other values - those are
//! deleted - and then the reference's rows for those keys, and for the keys the table is missing, are
//! inserted. A restore that has one row to put right writes one row, and the rows that were already right
//! keep their row ids: since a diff of two references of one table compares row ids, a restore that rewrote
//! every row made a later diff of the reference taken before it and the one taken after report the whole
//! table as changed, measured at three changed on a table of three.
//!
//! **A table with no key is rewound by rewriting it**, guarded by the same condition, which is what says
//! "nothing to do" without reading the reference twice: a content-based match cannot count duplicate rows, so
//! there is no key to delete by and no key to insert by, and the rows themselves are the only identity. A
//! restore has never needed a key and still does not.
static string BuildGit4DataRestore(ClientContext &context, const string &table_name, const string &reference,
                                   const char *unit) {
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
	// A restore writes the *live* table through the feature's own statements, so the table it writes cannot be
	// one of the registry's: measured, with a reference keyed to `git4data.snapshots` (a materialize of it is
	// allowed, and it is a read), `SET schema='git4data'; git4data_restore('snapshots', ...)` rewrote the
	// registry table - the write the guard refuses a caller, reached through the flag that lets the feature's
	// own statements past it.
	Git4DataCheckRegistryTarget(table.ParentSchema().name.GetIdentifierName(), table.name.GetIdentifierName());
	auto quoted_table = Git4DataTableSQLName(table_name);
	auto at_clause = FormatGit4DataAtClause(table_name, reference, unit);

	// The two images differ as multisets, in either direction. It is read before the delete, and it is what
	// keeps a restore of what the table already has from writing anything at all - neither the rows nor the
	// diff that would find them: the condition short-circuits the subqueries that follow it.
	auto differs = StringUtil::Format("EXISTS (SELECT * FROM %s EXCEPT ALL SELECT * FROM %s) OR EXISTS (SELECT * FROM "
	                                  "%s EXCEPT ALL SELECT * FROM %s)",
	                                  quoted_table, at_clause, at_clause, quoted_table);

	auto key_columns = Git4DataPrimaryKeyColumnNames(table);
	if (key_columns.empty()) {
		string result;
		result += StringUtil::Format("DELETE FROM %s WHERE %s;\n", quoted_table, differs);
		result += StringUtil::Format("INSERT INTO %s SELECT * FROM %s WHERE %s;", quoted_table, at_clause, differs);
		return result;
	}
	string key_list;
	for (idx_t i = 0; i < key_columns.size(); i++) {
		if (i > 0) {
			key_list += ", ";
		}
		key_list += KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
	}
	// The live table is one side of this diff, which names it the way the rest of the surface does, and the
	// reference is the other: what the table has and the reference does not, and the other way round.
	auto diff = StringUtil::Format("git4data_diff('%s', '%s', '%s')", Git4DataSQLLiteral(table_name),
	                               DuckTransactionManager::LIVE_REFERENCE, Git4DataSQLLiteral(reference));
	string result;
	// The condition is parenthesized: it is a disjunction, and `AND` binds tighter than `OR`, so without the
	// brackets the key filter would attach to its second half alone - measured, a restore that generated
	// `WHERE EXISTS (...) OR EXISTS (...) AND (k) IN (...)` deleted and re-inserted every row, which is the
	// whole table rewritten by a statement that was supposed to write the rows that differ.
	result += StringUtil::Format(
	    "DELETE FROM %s WHERE (%s) AND (%s) IN (SELECT %s FROM %s WHERE g4d_change IN ('removed', 'changed'));\n",
	    quoted_table, differs, key_list, key_list, diff);
	// Only the keys the table is missing: a key the reference added is missing by definition, and one whose
	// row was just deleted is missing by the time this runs, so it takes the reference's values.
	result += StringUtil::Format(
	    "INSERT INTO %s SELECT * FROM %s WHERE (%s) AND (%s) IN (SELECT %s FROM %s WHERE g4d_change IN ('added', "
	    "'changed')) AND (%s) NOT IN (SELECT %s FROM %s);",
	    quoted_table, at_clause, differs, key_list, key_list, diff, key_list, key_list, quoted_table);
	return result;
}

//! Keep the last N days of history reachable without anyone taking a reference by hand: one
//! reference is moved forward as it falls out of the window, at each checkpoint. A window of zero
//! keeps nothing, which is what a test can rely on.
static string PragmaGit4DataPitr(ClientContext &context, const FunctionParameters &parameters) {
	auto days = parameters.values[0].GetValue<int64_t>();
	if (days < 0) {
		throw InvalidInputException("A point-in-time recovery window cannot be negative");
	}
	constexpr int64_t MICROS_PER_DAY = 24LL * 60 * 60 * 1000000;
	// the tables first, because the statement this returns is what creates them and the window's own row is
	// inserted after them in the same statement
	auto result = Git4DataEnsureRegistryTables(context);
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	auto created = Timestamp::GetCurrentTimestamp();
	auto window = timestamp_t(days * MICROS_PER_DAY);
	// the reference carries the length of the window it belongs to: it is the one thing about the window that
	// a crash before the next checkpoint leaves behind, and a restored window that has forgotten its length
	// would keep nothing at all
	transaction_manager.SetPitr(window, created, DuckTransactionManagerPitrLine(window));
	// The window's reference is a row like any other reference now, written with ordinary DML in the
	// caller's transaction, so it reaches the log. It used to be the checkpoint's registry block alone,
	// which meant a crash before the next checkpoint lost the whole window: measured, a window opened,
	// written to, and killed without a checkpoint came back as reference_held = false and the history it
	// promised was then reclaimed. The engine still moves the reference *at* a checkpoint, where there is
	// no transaction to write a row in, so the block keeps the newer pin and the load takes the newer of
	// the two.
	Git4DataSnapshot held;
	if (!transaction_manager.snapshots.Get(DuckTransactionManager::PITR_REFERENCE, held)) {
		return result;
	}
	auto name = string(DuckTransactionManager::PITR_REFERENCE);
	context.client_data->git4data_added_records.push_back("snapshot\t" + name);
	result += StringUtil::Format("DELETE FROM %s.%s WHERE name = '%s';\n", GIT4DATA_REGISTRY_SCHEMA,
	                             GIT4DATA_SNAPSHOTS_TABLE, Git4DataSQLLiteral(name));
	result += StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', %llu, TIMESTAMP '%s', '%s');",
	                             GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_SNAPSHOTS_TABLE, Git4DataSQLLiteral(name), held.pin,
	                             Timestamp::ToString(held.created), Git4DataSQLLiteral(held.line));
	return result;
}

static string PragmaGit4DataPitrOff(ClientContext &context, const FunctionParameters &parameters) {
	GetGit4DataTransactionManager(context).ClearPitr();
	// and the row goes with it, or the next open would bring the window back on a database that turned it off
	if (!Git4DataRegistryTablesThere(context)) {
		return string();
	}
	return StringUtil::Format("DELETE FROM %s.%s WHERE name = '%s';", GIT4DATA_REGISTRY_SCHEMA,
	                          GIT4DATA_SNAPSHOTS_TABLE, Git4DataSQLLiteral(string(DuckTransactionManager::PITR_REFERENCE)));
}

static string PragmaGit4DataPitrState(ClientContext &context, const FunctionParameters &parameters) {
	// the row comes from the table function, which is where its values can also be selected
	return "SELECT * FROM git4data_pitr_state()";
}

static string PragmaGit4DataRestore(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto reference = parameters.values[1].ToString();
	auto unit = ResolveGit4DataReference(GetGit4DataTransactionManager(context), reference);
	return BuildGit4DataRestore(context, table_name, reference, unit);
}

//! Rewind a table to the newest reference taken at or before a wall-clock time. Commit ids are a
//! counter, so "as of 14:30" can only be answered from the timestamp each reference records; the
//! reach is therefore as far back as references are still held.
static string PragmaGit4DataRestoreAt(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto target = parameters.values[1].DefaultCastAs(LogicalType::TIMESTAMP);
	auto target_ts = TimestampValue::Get(target);
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	string best;
	timestamp_t best_ts;
	bool found = false;
	auto consider = [&](const string &name, timestamp_t created) {
		if (created > target_ts || (found && created <= best_ts)) {
			return;
		}
		best = name;
		best_ts = created;
		found = true;
	};
	for (auto &snapshot : transaction_manager.snapshots.List()) {
		consider(snapshot.name, snapshot.created);
	}
	for (auto &branch : transaction_manager.branches.List()) {
		consider(branch.name, branch.created);
	}
	if (!found) {
		throw InvalidInputException("No snapshot or branch was taken at or before %s, so there is nothing to "
		                            "restore \"%s\" to",
		                            target.ToString(), table_name);
	}
	// the name was chosen from the registries, so it resolves
	auto unit = ResolveGit4DataReference(transaction_manager, best);
	return BuildGit4DataRestore(context, table_name, best, unit);
}

//! Apply the changes one reference has relative to another onto the live table. Expressed as SQL
//! rather than as a new physical operator: the read side already exists (the reference scan), the
//! write side is ordinary DML, and the difference between the two references is what git4data_diff
//! computes. What this adds is the composition, and the decision that a pick needs a primary key -
//! without one there is nothing to match a current row against, and applying a change to the wrong
//! row would be worse than refusing to guess.
//! The predicate that says two aliased rows hold the same values, NULL-safe column by column

//! The column names of a table, if it is there. Used to keep generated names clear of them.
void Git4DataAddColumnNames(ClientContext &context, const string &table_name, case_insensitive_set_t &names) {
	auto entry = Git4DataResolveTableOrNull(context, table_name);
	if (!entry) {
		return;
	}
	for (auto &column : entry->GetColumns().Logical()) {
		names.insert(column.Name().GetIdentifierName());
	}
}

//! The names a branch's mappings for one source are held under: the key, and whatever a record written before
//! keys carried a schema holds. The statements that replace a mapping have to take every one of them, or a
//! legacy row stays beside the new one and a read takes whichever it sees first.
vector<string> Git4DataStoredSourceNames(ClientContext &context, const Git4DataBranch &branch, const string &key,
                                                idx_t source_oid, const string &record_name) {
	vector<string> result;
	result.push_back(key);
	result.push_back(record_name);
	auto remember = [&](const string &stored, idx_t stored_oid) {
		if (stored_oid != 0 && source_oid != 0 && stored_oid != source_oid) {
			return;
		}
		if (!Git4DataSourceKeyMatches(context, stored, key)) {
			return;
		}
		if (std::find(result.begin(), result.end(), stored) == result.end()) {
			result.push_back(stored);
		}
	};
	for (auto &entry : branch.storage_tables) {
		remember(entry.source, entry.source_oid);
	}
	for (auto &entry : branch.deltas) {
		remember(entry.source, entry.source_oid);
	}
	return result;
}

//! Those names as an SQL list, escaped
string Git4DataEscapedList(const vector<string> &names) {
	string result;
	for (auto &name : names) {
		if (!result.empty()) {
			result += ", ";
		}
		result += "'" + Git4DataSQLLiteral(name) + "'";
	}
	return result;
}


//! A copy-on-write clone of one file, which is what "zero-copy" means at this level: the copy costs
//! nothing at the moment it is made and shares its blocks with the original until either side writes.
static void Git4DataCloneFile(const string &source, const string &target) {
#ifdef __APPLE__
	if (clonefile(source.c_str(), target.c_str(), 0) != 0) {
		throw InvalidInputException("Could not clone \"%s\" to \"%s\": %s", source, target, strerror(errno));
	}
#elif defined(__linux__)
	auto source_fd = open(source.c_str(), O_RDONLY);
	if (source_fd < 0) {
		throw InvalidInputException("Could not open \"%s\": %s", source, strerror(errno));
	}
	auto target_fd = open(target.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
	if (target_fd < 0) {
		close(source_fd);
		throw InvalidInputException("Could not create \"%s\": %s", target, strerror(errno));
	}
	auto result = ioctl(target_fd, FICLONE, source_fd);
	auto saved_errno = errno;
	close(target_fd);
	close(source_fd);
	if (result != 0) {
		throw InvalidInputException("Could not clone \"%s\" to \"%s\": %s", source, target, strerror(saved_errno));
	}
#else
	throw NotImplementedException("A zero-copy clone needs a file system that can do one, and this platform has no "
	                              "clone call - the database can still be copied with the file system's own tools");
#endif
}

//! Clone the database into a file of its own. The clone is a copy of the *file*, so it holds everything
//! committed; the write-ahead log comes with it when there is one, or the clone would be missing what
//! the log holds. Writing to the clone afterwards leaves the original alone and the other way round,
//! which is the file system's copy-on-write and not something this feature has to maintain.
static string PragmaGit4DataCloneDatabase(ClientContext &context, const FunctionParameters &parameters) {
	auto target = parameters.values[0].ToString();
	if (target.empty()) {
		throw InvalidInputException("A clone needs a path to write to");
	}
	auto &db_manager = DatabaseManager::Get(context);
	auto attached = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	auto source = attached->StoredPath();
	if (source.empty()) {
		throw InvalidInputException("This database has no file of its own, so there is nothing to clone");
	}
	auto &fs = FileSystem::GetFileSystem(context);
	if (fs.FileExists(target)) {
		throw InvalidInputException("\"%s\" already exists - a clone does not overwrite", target);
	}
	// No refusal for references made since the last checkpoint any more: the registry is kept in tables, so a
	// reference is an ordinary change in the log and the clone, which takes the log with it, carries it. The
	// refusal this replaces was for a registry that only a checkpoint could write.
	Git4DataCloneFile(source, target);
	// the log is part of the database until the next checkpoint merges it, so the clone needs it too or
	// it would be missing whatever the log holds
	auto wal = source + ".wal";
	bool cloned_log = false;
	if (fs.FileExists(wal)) {
		Git4DataCloneFile(wal, target + ".wal");
		cloned_log = true;
	}
	return StringUtil::Format("SELECT '%s'::VARCHAR AS source, '%s'::VARCHAR AS clone, %s::BOOLEAN AS with_log",
	                          Git4DataSQLLiteral(source), Git4DataSQLLiteral(target), cloned_log ? "true" : "false");
}

//! Point every reference to the source table inside a parsed statement at the branch the statement is
//! being applied to. The call writes *the branch*, so a bare `t` in the statement means the branch's
//! line - reading the live table instead would be a silent wrong answer, and it was one: measured, an
//! insert whose rows came from the table itself copied the live table's value (v = 1) where the branch
//! held 101. A reference that already names a reference of its own is left alone, because naming one is
//! a decision rather than an omission.
static void CheckGit4DataReferenceLimit(const string &plural, idx_t count, int64_t limit) {
	if (limit < 0) {
		return;
	}
	if (limit == 0) {
		throw InvalidInputException("%s are disabled for this database", plural);
	}
	if (NumericCast<int64_t>(count) >= limit) {
		throw InvalidInputException("At most %lld %s are allowed for this database, and %llu already exist",
		                            (long long)limit, plural, (unsigned long long)count);
	}
}

//! Set how many snapshots or branches this database may hold: -1 unlimited, 0 disabled, a positive
//! number is the most. The recovery window's own reference is not counted.
//!
//! The value is a row in `git4data.state` rather than a property of the checkpoint's registry block (3ee). The
//! pragma therefore returns a statement where it used to set a field and mark the registries dirty - the mark
//! existed because a checkpoint skips its body when the log is empty, so a quota set in a session that changed
//! nothing else was lost (measured, `limits.test` read -1 for a limit of 3 after a restart). A row is written
//! through the caller's transaction and reaches the log, so there is nothing to remember to flush.
static string PragmaGit4DataLimit(ClientContext &context, const FunctionParameters &parameters) {
	auto kind = StringUtil::Lower(parameters.values[0].ToString());
	auto limit = parameters.values[1].GetValue<int64_t>();
	if (limit < -1) {
		throw InvalidInputException("A limit is -1 for unlimited, 0 to disable, or a positive number");
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	string key;
	if (kind == "snapshot") {
		transaction_manager.snapshot_limit = limit;
		key = "snapshot_limit";
	} else if (kind == "branch") {
		transaction_manager.branch_limit = limit;
		key = "branch_limit";
	} else {
		throw InvalidInputException("Limits are set for \"snapshot\" or \"branch\", not \"%s\"", kind);
	}
	auto result = Git4DataEnsureRegistryTables(context);
	result += StringUtil::Format(
	    "DELETE FROM %s.%s WHERE key = '%s';\nINSERT INTO %s.%s VALUES ('%s', '%lld');\n", GIT4DATA_REGISTRY_SCHEMA,
	    GIT4DATA_STATE_TABLE, key, GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_STATE_TABLE, key, (long long)limit);
	return result;
}

static string PragmaGit4DataLimits(ClientContext &context, const FunctionParameters &parameters) {
	// the row comes from the table function, which is where its values can also be selected
	return "SELECT * FROM git4data_limits()";
}

//! A name beginning with two underscores is one of the feature's own internal references - the
//! recovery window, a merge's base. Those are looked up by exact name, so a user reference sharing one
//! would be dropped by the machinery that manages them.
static void CheckGit4DataReferenceName(const string &name) {
	if (StringUtil::StartsWith(name, "__")) {
		throw InvalidInputException(
		    "\"%s\" begins with two underscores, which this feature reserves for references it manages "
		    "itself: the point-in-time recovery window and the base a merge reads against. Choose another "
		    "name.",
		    name);
	}
}

static string PragmaGit4DataCreateBranch(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	auto parent = parameters.values[1].ToString();
	CheckGit4DataReferenceName(name);
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// the statement that creates the tables comes before the one that inserts this branch's row: both are
	// part of what this pragma returns, and they run in that order
	string result = Git4DataEnsureRegistryTables(context);
	// the other half of the rule the snapshot side states: the two kinds of reference share a name
	// namespace wherever one is named without its kind, so they cannot share a name
	Git4DataSnapshot same_named_snapshot;
	if (transaction_manager.snapshots.Get(name, same_named_snapshot)) {
		throw InvalidInputException("A snapshot named \"%s\" exists, and a branch of that name would be "
		                            "indistinguishable from it wherever a reference is named without its kind",
		                            name);
	}
	// A branch descends from another branch when one is named; otherwise it starts at the newest
	// commit. Either way the branch holds its base down exactly as a pin does.
	//
	// The ancestor may also be a snapshot, which is how a line is started from a state that is no
	// longer the newest one - a snapshot is a commit, and a commit is what a branch is forked from.
	// Such a line has no branch ancestor, so a merge that has to find one between it and another
	// branch still refuses; a pick takes its base explicitly and works.
	transaction_t pin;
	bool parent_is_snapshot = false;
	string base_line;
	if (parent.empty()) {
		pin = transaction_manager.GetLastCommit();
	} else {
		Git4DataBranch parent_branch;
		if (Git4DataReferenceVisible(context, "branch", parent) &&
		    transaction_manager.branches.Get(parent, parent_branch)) {
			// A fork takes the state the parent holds, not the commit the parent was pinned at, whenever
			// the two differ - which they do the moment the parent has a table of its own or a delta. The
			// child carries the parent as the line it started from and pins the commit it forked at, so
			// what it reads is that line as it stood then, and whatever the parent does afterwards is
			// outside the pin. Without this the child reads the parent's base: measured, a parent whose row
			// read 111 forked a child whose row read 1.
			// **A branch reads a line it inherited too** (increment 3bu). A parent with nothing of its own but a
			// `base_line` still reads *that* line, so its state differs from the source at its pin and the fork
			// has to take the line as well as the pin - otherwise the child reads the source and the parent's
			// line is lost one hop down. Measured before this: `b0` differed by a delta (adding one key, removing
			// another), `b1` was forked from it and read that line, and `b2` forked from `b1` carried an empty
			// `base_line` with `b1`'s pin and read the **raw table** - the key the line added was gone and the one
			// it removed was back, silently, from a fork that was supposed to take what its parent holds.
			if (!parent_branch.storage_tables.empty() || !parent_branch.deltas.empty() ||
			    !parent_branch.base_line.empty()) {
				// A fork takes the state the parent holds, not the commit the parent was pinned at,
				// whenever the two differ - which they do the moment the parent has a table of its own or a
				// delta. The child carries the parent as the line it started from and pins the commit it
				// forked at, so what it reads is that line as it stood then, and whatever the parent does
				// afterwards is outside the pin. Without this the child reads the parent's base: measured, a
				// parent whose row read 111 forked a child whose row read 1.
				base_line = parent;
				pin = transaction_manager.GetLastCommit();
			} else {
				pin = parent_branch.pin;
			}
		} else {
			Git4DataSnapshot parent_snapshot;
			if (!Git4DataReferenceVisible(context, "snapshot", parent) ||
			    !transaction_manager.snapshots.Get(parent, parent_snapshot)) {
				throw InvalidInputException("Reference \"%s\" is neither a branch nor a snapshot, so a branch "
				                            "cannot descend from it",
				                            parent);
			}
			if (!parent_snapshot.line.empty()) {
				// What such a reference names is a branch's own state, so the child carries that line: it
				// reads the line's storage at this pin, which is the state the reference captured, and
				// changes the other branch makes later are outside the pin and stay out.
				base_line = parent_snapshot.line;
			}
			pin = parent_snapshot.pin;
			parent_is_snapshot = true;
		}
	}
	CheckGit4DataReferenceLimit("branches", transaction_manager.branches.Count(), transaction_manager.branch_limit);
	auto created = Timestamp::GetCurrentTimestamp();
	if (!transaction_manager.branches.Create(name, pin, parent, created, parent_is_snapshot, base_line)) {
		throw InvalidInputException("Branch \"%s\" already exists", name);
	}
	// uncommitted until this transaction commits, like the snapshot above
	transaction_manager.MarkReferenceUncommitted(
	    "branch", name, &MetaTransaction::Get(context),
	    DuckTransaction::Get(context, transaction_manager.GetDB()).GetTransactionId());
	// the branch is a row now, like a snapshot: it goes to the log with the statement this returns
	context.client_data->git4data_added_records.push_back("branch\t" + name);
	result += StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', %llu, '%s', TIMESTAMP '%s', '%s');",
	                             GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_BRANCHES_TABLE, Git4DataSQLLiteral(name), pin, Git4DataSQLLiteral(parent),
	                             Timestamp::ToString(created), Git4DataSQLLiteral(base_line));
	return result;
}

//! Drop a branch and the storage it was materialized into: that table is the branch's own copy, so
//! leaving it behind would leak a table per branch ever dropped.
static string PragmaGit4DataDropBranch(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	auto existing = Git4DataRequiredBranch(context, transaction_manager, name);
	if (transaction_manager.branches.HasChildren(name)) {
		// a descendant still walks through this name to reach its ancestors
		throw InvalidInputException("Branch \"%s\" still has branches descending from it. Drop those branches "
		                            "first.",
		                            name);
	}
	if (transaction_manager.branches.HasLineDependents(name)) {
		// Those branches read this one's **line** at their fork pin, which dropping it would take away. The
		// message said "storage" until increment 3ce, and that is false for the shape that reaches it most
		// easily - measured: with a delta-only parent (`has_storage = false`, `delta_added = s0_add`) reached
		// through a snapshot that captured its line, the dependent reads the *delta*, 5 rows with its added key,
		// and the refusal told the caller about a table the branch does not have. "Line" is the word the rest of
		// this file uses for a copy or a delta, and it is true of both.
		throw InvalidInputException("Branch \"%s\" still has branches forked from a reference that captured its "
		                            "line, and they read it at their fork pin. Drop those branches first.",
		                            name);
	}
	for (auto &snapshot : transaction_manager.snapshots.List()) {
		// the reference reads this branch's line, and dropping the branch would leave it reading the pinned
		// image of the table instead - the same name answering with a state it never captured. Measured, a
		// captured v = 111 read as v = 1 after the branch was dropped. Reference names are exact, so this
		// compares them exactly: a branch "b" and a branch "B" are two lines.
		if (snapshot.line != name) {
			continue;
		}
		throw InvalidInputException("Snapshot \"%s\" captured a line of branch \"%s\", and dropping the branch "
		                            "would leave it reading the table instead. Drop that snapshot first.",
		                            snapshot.name, name);
	}
	string result;
	for (auto &entry : existing.storage_tables) {
		// **Only the tables the feature created go with the branch.** `owned` is what records which those are,
		// and the rule it carries is stated in three other places: the field's own comment ("A table bound by
		// hand is left alone"), the replacement a second materialize makes (it drops the copy it made and not
		// the caller's table), and `git4data_unbind` (it takes the mapping back and drops nothing). Dropping a
		// branch used to be the exception, and it destroyed the caller's table: measured, a branch bound to a
		// table the caller had made and filled lost it to `git4data_drop_branch`, silently, while unbinding
		// the same branch left it - the same table protected by one path and destroyed by another, which is
		// exactly the kind of rule a caller cannot hold in their head. A table bound by hand is left where it
		// is, as a delta's two tables always have been.
		if (!entry.owned) {
			continue;
		}
		// one of the tables it made may have been renamed since, and dropping the name it was given instead
		// would leave the table behind, silently
		auto resolved = Git4DataResolveTableName(context, entry.storage, entry.oid);
		if (resolved.empty()) {
			continue;
		}
		// **A mapping can name one of the registry's own tables, and the drop below would take it with the
		// branch.** Nothing this build writes can: a binding is refused if it aims at one of them (3dn). A
		// *file* written before that could hold such a mapping, and so could one whose records came from the
		// checkpoint block - which is the case the block fallback exists for - so the drop checks what it is
		// about to drop. Refused rather than skipped, because a mapping aimed at the registry is a state to
		// clear and `git4data_unbind` clears it without dropping anything: silently leaving the table behind
		// would leave the caller with a branch they cannot drop and no sentence saying why.
		auto mapped = Git4DataResolveTableOrNull(context, resolved);
		if (mapped && Git4DataIsRegistryTable(*mapped)) {
			Git4DataCheckRegistryTarget(mapped->ParentSchema().name.GetIdentifierName(),
			                            mapped->name.GetIdentifierName());
		}
		// A table two branches were given is not this one's alone to take: dropping it would leave the
		// other reading its pinned image, silently - measured, a branch bound to another branch's copy
		// fell back from v = 111 to the pinned v = 1 when that other branch was dropped. Only the tables
		// no other branch names go with it.
		if (transaction_manager.branches.OtherBranchUsesTable(name, resolved, entry.oid)) {
			continue;
		}
		result += StringUtil::Format("DROP TABLE IF EXISTS %s;\n", Git4DataTableSQLName(resolved));
	}
	transaction_manager.branches.Drop(name);
	// and its rows, or the next open would read the branch back out of the tables: the branch, the table it
	// was given and the delta it differs by all live in one of them
	if (Git4DataRegistryTablesThere(context)) {
		auto escaped = Git4DataSQLLiteral(name);
		result += StringUtil::Format("DELETE FROM %s.%s WHERE name = '%s';", GIT4DATA_REGISTRY_SCHEMA,
		                             GIT4DATA_BRANCHES_TABLE, escaped);
		result += StringUtil::Format("DELETE FROM %s.%s WHERE branch = '%s';", GIT4DATA_REGISTRY_SCHEMA,
		                             GIT4DATA_STORAGE_TABLE, escaped);
		result += StringUtil::Format("DELETE FROM %s.%s WHERE branch = '%s';", GIT4DATA_REGISTRY_SCHEMA,
		                             GIT4DATA_DELTAS_TABLE, escaped);
	}
	if (result.empty()) {
		// a branch with no storage of its own still has to return something to execute
		return "SELECT 1 WHERE false";
	}
	return result;
}

static string PragmaGit4DataBranches(ClientContext &context, const FunctionParameters &parameters) {
	// the rows come from the table function, which is where they can also be selected *from*: a pragma's
	// result cannot, so every value in this listing used to be readable by a person and asserted only by
	// its shape
	return "SELECT * FROM git4data_branches()";
}

static string PragmaGit4DataLCA(ClientContext &context, const FunctionParameters &parameters) {
	auto left = parameters.values[0].ToString();
	auto right = parameters.values[1].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	auto existing = Git4DataRequiredBranch(context, transaction_manager, left);
	existing = Git4DataRequiredBranch(context, transaction_manager, right);
	string lca;
	if (!transaction_manager.branches.FindLCA(left, right, lca)) {
		// the two share no ancestor, which is what a broken lineage looks like
		return "SELECT NULL::VARCHAR AS lca WHERE false";
	}
	lca = Git4DataSQLLiteral(lca);
	return StringUtil::Format("SELECT '%s'::VARCHAR AS lca", lca);
}

static string PragmaGit4DataCreateSnapshot(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	CheckGit4DataReferenceName(name);
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// The tables are created before the row for this reference is inserted - both are parts of the statement
	// this pragma returns, and they run in that order. There is nothing to copy into them: what a file written
	// by the older build carries in its metadata block is not read (see `Git4DataEnsureRegistryTables`).
	string result = Git4DataEnsureRegistryTables(context);
	// A snapshot and a branch with one name would be indistinguishable wherever a reference is named
	// without saying which kind it is - a diff, a pick, a restore, and a merge's own image clauses all
	// resolve a bare name, and they look in the snapshot registry first. Measured: with a branch and a
	// snapshot both called x, a merge of x with x compared the branch against the snapshot and reported
	// a conflict that did not exist. The two names cannot collide.
	Git4DataBranch same_named_branch;
	if (transaction_manager.branches.Get(name, same_named_branch)) {
		throw InvalidInputException("A branch named \"%s\" exists, and a snapshot of that name would be "
		                            "indistinguishable from it wherever a reference is named without its kind",
		                            name);
	}
	CheckGit4DataReferenceLimit("snapshots", transaction_manager.UserSnapshotCount(),
	                            transaction_manager.snapshot_limit);
	// A second argument names a branch, and the reference then captures that branch's own line rather
	// than the table: a branch that has been written to keeps its state in a table of its own, which a
	// pin alone cannot point at. Without it the reference is the table as this commit saw it.
	string line;
	if (parameters.values.size() > 1) {
		line = parameters.values[1].ToString();
		auto branch = Git4DataRequiredBranch(context, transaction_manager, line);
		// **A branch that inherited a line reads it, and that is a line to capture too** (increment 3bv). The
		// refusal is for a branch with no line at all - no mapping of its own and nothing inherited - which
		// reads the source at its pin, so a snapshot of the table says the same thing and the second argument
		// buys nothing. Measured before this: capturing the line of a branch that inherited one was refused
		// with "it reads the table exactly as a snapshot does", which is **false** - `b1`, forked from a branch
		// that differed by a delta, read four rows with the delta's added key present and its removed key
		// absent, while the table had five with the removed key in it - and the caller had no way to capture
		// that state: a snapshot of the branch's *parent* pinned at now would take whatever the parent did
		// since the fork, which is exactly what the branch's pin excludes.
		if (branch.storage_tables.empty() && branch.deltas.empty() && branch.base_line.empty()) {
			throw InvalidInputException("Branch \"%s\" has no line of its own, so there is nothing to capture "
			                            "- it reads the table exactly as a snapshot does",
			                            line);
		}
	}
	// Pin at the newest commit, keep everything committed so far readable
	auto pin = transaction_manager.GetLastCommit();
	auto created = Timestamp::GetCurrentTimestamp();
	if (!transaction_manager.snapshots.Create(name, pin, created, line)) {
		throw InvalidInputException("Snapshot \"%s\" already exists", name);
	}
	// The reference is uncommitted until this transaction commits, and the registries are shared memory: what
	// makes it this transaction's alone is that the row is in no committed state yet. The marker is dropped
	// when its transaction ends, so nothing has to clear it on the way out.
	transaction_manager.MarkReferenceUncommitted(
	    "snapshot", name, &MetaTransaction::Get(context),
	    DuckTransaction::Get(context, transaction_manager.GetDB()).GetTransactionId());
	// The reference is a row in the registry tables from here on: it is written with ordinary DML, in the
	// caller's transaction, so it reaches the log like any other change and the next open brings it back.
	// A checkpoint is no longer what decides whether a reference survives.
	context.client_data->git4data_added_records.push_back("snapshot\t" + name);
	result +=
	    StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', %llu, TIMESTAMP '%s', '%s');", GIT4DATA_REGISTRY_SCHEMA,
	                       GIT4DATA_SNAPSHOTS_TABLE, Git4DataSQLLiteral(name), pin, Timestamp::ToString(created), Git4DataSQLLiteral(line));
	return result;
}

//! Drop an internal reference from the in-memory registry and leave its row to the caller, which is what a
//! statement the feature generates for itself needs: a pragma that returns a statement cannot be called from
//! one. It is limited to the names the feature reserves, so a caller cannot forget a reference this way - a
//! forgotten snapshot would read again the moment its row was loaded.
static void PragmaGit4DataForgetInternal(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	if (!StringUtil::StartsWith(name, "__")) {
		throw InvalidInputException(
		    "\"%s\" is not one of the references this feature manages for itself, and this only forgets one of "
		    "those from memory. Use git4data_drop_snapshot, which takes its row with it.",
		    name);
	}
	GetGit4DataTransactionManager(context).snapshots.Drop(name);
}

static string PragmaGit4DataDropSnapshot(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// nothing another transaction has not committed can be dropped by this one
	if (!Git4DataReferenceVisible(context, "snapshot", name) || !transaction_manager.snapshots.Drop(name)) {
		throw InvalidInputException("Snapshot \"%s\" does not exist", name);
	}
	// and the row goes with it, or the next open would bring the reference back out of the table
	if (!Git4DataRegistryTablesThere(context)) {
		return string();
	}
	auto escaped = Git4DataSQLLiteral(name);
	return StringUtil::Format("DELETE FROM %s.%s WHERE name = '%s';", GIT4DATA_REGISTRY_SCHEMA,
	                          GIT4DATA_SNAPSHOTS_TABLE, escaped);
}

static string PragmaGit4DataSnapshots(ClientContext &context, const FunctionParameters &parameters) {
	// as for the branches above: the rows are the table function's, so a caller (and a test) can select them
	return "SELECT * FROM git4data_snapshots()";
}

static string PragmaGit4DataState(ClientContext &context, const FunctionParameters &parameters) {
	// the row comes from the table function, which is where its values can also be selected
	return "SELECT * FROM git4data_state()";
}

//! Base data with updates skipped: what a pinned snapshot needs kept
static string PragmaGit4DataProbe(ClientContext &context, const FunctionParameters &parameters) {
	// the row comes from the table function, which is where its numbers can be selected
	return StringUtil::Format("SELECT * FROM git4data_probe('%s')",
	                          Git4DataSQLLiteral(parameters.values[0].ToString()));
}

//! Every physical row including all deleted ones, to tell "the data is gone" apart from
//! "the data is there but filtered out"
static string PragmaGit4DataProbeAll(ClientContext &context, const FunctionParameters &parameters) {
	// the row comes from the table function, which is where its numbers can be selected
	return StringUtil::Format("SELECT * FROM git4data_probe_all('%s')",
	                          Git4DataSQLLiteral(parameters.values[0].ToString()));
}

//! Base data with updates skipped, read at an explicitly given visibility bound. Sweeping
//! the bound reveals the effective delete and insert ids: the count changes where the bound
//! crosses an id, which is what tells the two runners apart.
static string PragmaGit4DataProbeAt(ClientContext &context, const FunctionParameters &parameters) {
	return StringUtil::Format("SELECT * FROM git4data_probe_at('%s', %lld)",
	                          Git4DataSQLLiteral(parameters.values[0].ToString()),
	                          (long long)parameters.values[1].GetValue<int64_t>());
}

void RegisterGit4DataPragmas(BuiltinFunctions &set) {

	// the branch whose line to capture is optional, so a snapshot of the table as a commit saw it
	// keeps working with one argument
	set.AddFunction(PragmaFunction::PragmaCall("git4data_create_snapshot", PragmaGit4DataCreateSnapshot,
	                                           {LogicalType::VARCHAR}, LogicalType::VARCHAR));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_drop_snapshot", PragmaGit4DataDropSnapshot, {LogicalType::VARCHAR}));
	// internal: what a statement the feature generates for itself calls to drop one of its own references
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_forget_internal", PragmaGit4DataForgetInternal, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_snapshots", PragmaGit4DataSnapshots));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_create_branch", PragmaGit4DataCreateBranch,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_drop_branch", PragmaGit4DataDropBranch, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_branches", PragmaGit4DataBranches));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_materialize", PragmaGit4DataMaterialize,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_clone_database", PragmaGit4DataCloneDatabase, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall(
	    "git4data_bind_branch_delta", PragmaGit4DataBindBranchDelta,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_unbind", PragmaGit4DataUnbind,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	// a delta branch written through the SQL a caller would have written anyway
	set.AddFunction(PragmaFunction::PragmaCall("git4data_apply", PragmaGit4DataApply,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	// who created the table is optional, so a binding written by hand keeps the three-argument form and
	// its table is not the feature's to drop
	// the generated form: materialize's SQL binds in memory and writes the row itself
	set.AddFunction(PragmaFunction::PragmaCall("git4data_bind_internal", PragmaGit4DataBindInternal,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                           LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_bind_branch_table", PragmaGit4DataBindBranchTable,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                           LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaCall(
	    "git4data_merge", PragmaGit4DataMerge,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_lca", PragmaGit4DataLCA, {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	// the policy is optional, so a pick that does not name one keeps overwriting
	set.AddFunction(PragmaFunction::PragmaCall("git4data_pick", PragmaGit4DataPick,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                           LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_restore", PragmaGit4DataRestore,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_restore_at", PragmaGit4DataRestoreAt,
	                                           {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_pitr", PragmaGit4DataPitr, {LogicalType::BIGINT}));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_pitr_off", PragmaGit4DataPitrOff));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_pitr_state", PragmaGit4DataPitrState));
	set.AddFunction(
	    PragmaFunction::PragmaCall("git4data_limit", PragmaGit4DataLimit, {LogicalType::VARCHAR, LogicalType::BIGINT}));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_limits", PragmaGit4DataLimits));
	set.AddFunction(PragmaFunction::PragmaStatement("git4data_state", PragmaGit4DataState));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_probe", PragmaGit4DataProbe, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_probe_all", PragmaGit4DataProbeAll, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("git4data_probe_at", PragmaGit4DataProbeAt,
	                                           {LogicalType::VARCHAR, LogicalType::BIGINT}));
}

} // namespace duckdb
