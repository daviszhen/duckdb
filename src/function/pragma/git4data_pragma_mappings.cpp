//===----------------------------------------------------------------------===//
// Git4Data -- the pragmas that give a branch a line: materialize, bind, unbind
//===----------------------------------------------------------------------===//
// `git4data_materialize` copies what a branch reads into a table of its own; `git4data_bind_branch_table` and
// `git4data_bind_branch_delta` point a branch at a table, or at the two tables a delta lives in;
// `git4data_unbind` takes that back. The mappings themselves are rows in the registry.
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

//! The columns every table a generated statement reads brings into scope: the source table, and for
//! each named reference the table that reference reads its rows from - a branch with a line of its own
//! reads that table rather than the source's columns.
string PragmaGit4DataMaterialize(ClientContext &context, const FunctionParameters &parameters) {
	auto source_table = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto storage_table = parameters.values[2].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	auto branch = Git4DataRequiredBranch(context, transaction_manager, branch_name);
	// the source has to be there, and the target must not be. The name may be written any way a table can be
	// named, and what a mapping is recorded against is the *table*: the key carries its schema, so a branch's
	// line for one of two same-named tables is never read for the other.
	auto &table = Git4DataResolveTable(context, source_table);
	auto source_key = Git4DataSourceKey(table);
	// what the row is written under keeps the name the table is usually called by, so a mapping for `t` stays
	// readable as `t`; what it is *matched* by is the key, which carries the schema
	auto record_name = Git4DataTableName(table);
	auto key_columns = Git4DataPrimaryKeyColumnNames(table);
	// the name a caller writes may be qualified, so it is parsed rather than taken as one identifier
	if (Git4DataResolveTableOrNull(context, storage_table)) {
		throw InvalidInputException("Table \"%s\" already exists", storage_table);
	}
	// The mapping is registered by the last statement rather than here: the copy below reads the
	// branch, and a branch with a mapping already reads its own table - which at that moment is the
	// empty one just created, so the copy would bring nothing over.
	//
	// The columns are spelled out rather than taken with CREATE TABLE AS, because that does not carry
	// the primary key over - and the key is what tells two rows apart for a diff, a pick or a merge
	// afterwards. Nothing else of the source's definition is copied.
	string columns;
	for (auto &column : table.GetColumns().Logical()) {
		if (!columns.empty()) {
			columns += ", ";
		}
		columns +=
		    KeywordHelper::WriteOptionallyQuoted(column.Name().GetIdentifierName()) + " " + column.Type().ToString();
	}
	for (idx_t i = 0; i < key_columns.size(); i++) {
		columns += i == 0 ? ", PRIMARY KEY (" : ", ";
		columns += KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
		if (i + 1 == key_columns.size()) {
			columns += ")";
		}
	}
	auto quoted_storage = Git4DataTableSQLName(storage_table);
	auto escaped_source = Git4DataSQLLiteral(record_name);
	auto stored_source =
	    Git4DataEscapedList(Git4DataStoredSourceNames(context, branch, source_key, table.oid, record_name));
	auto escaped_branch = Git4DataSQLLiteral(branch_name);
	// what the row records is the name as SQL, not the argument as typed: a caller who writes `"t"` means `t`,
	// and a mapping is looked up again by what its name resolves to
	auto escaped_storage = Git4DataSQLLiteral(quoted_storage);
	// A second materialize of one source replaces the mapping, and the copy the first one made is then a
	// table of the identical line with nothing pointing at it - measured: the branch read the new table
	// while the first was still there, and dropping the branch took only the second, so the first
	// survived that too. Materializing costs a table, so the one this feature created is dropped here.
	// The caller's own table is left alone, and the drop comes last so the copy above still reads the
	// branch as it was - the branch reads the table of its own that is about to go.
	string replaced;
	auto existing = Git4DataFindStorageTable(context, branch, source_key, table.oid);
	if (existing && existing->owned && existing->storage != storage_table) {
		auto resolved = Git4DataResolveTableName(context, existing->storage, existing->oid);
		// it is not this branch's alone to take either: another branch can have been given it, or be *about*
		// it, and taking it would leave that one reading its pin or with nothing to read - measured, a
		// second materialize dropped the source table of a branch materialized from the first copy
		if (!resolved.empty() &&
		    !transaction_manager.branches.OtherBranchUsesTable(branch_name, resolved, existing->oid)) {
			replaced = StringUtil::Format("\nDROP TABLE IF EXISTS %s;", Git4DataTableSQLName(resolved));
		}
	}
	// The binding is written out rather than called through git4data_bind_branch_table: that one returns the
	// row's statements, and a pragma that returns a statement cannot be called from one - the same wall the
	// merge's cleanup hit. So the memory half is the internal pragma and the row is plain DML here.
	// the copy's identity, minted here because the row below is written by this statement and the table it names
	// only exists once the statement runs: the pragma that follows the CREATE puts this same value on the table
	auto identity = Git4DataNewTableIdentity();
	return StringUtil::Format("CREATE TABLE %s (%s);\nINSERT INTO %s SELECT * FROM %s AT (BRANCH => '%s');\n"
	                          "PRAGMA git4data_bind_internal('%s', '%s', '%s', 'true', '%llu');\n"
	                          "DELETE FROM %s.%s WHERE branch = '%s' AND source IN (%s);\n"
	                          "INSERT INTO %s.%s VALUES ('%s', '%s', '%s', true, %llu, %llu);%s",
	                          quoted_storage, columns, quoted_storage, Git4DataTableSQLName(source_key), escaped_branch,
	                          escaped_source, escaped_branch, escaped_storage, (unsigned long long)identity,
	                          GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_STORAGE_TABLE, escaped_branch, stored_source,
	                          GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_STORAGE_TABLE, escaped_branch, escaped_source,
	                          escaped_storage, (unsigned long long)transaction_manager.GetStartTimestampCounter(),
	                          (unsigned long long)identity, replaced);
}

//! Take back what a branch was given for a source table: its table of its own, or the delta it differs
//! by. A mapping whose table is dropped falls back to the pinned image rather than failing - that rule is
//! deliberate, because the branch is still a valid reference - but it is silent, and until now there was
//! no way to say "stop reading that table, the branch reads its pin now". This is that way, and it is
//! also what turns the fallback from a state nobody can see into one a caller can see and clear.
string PragmaGit4DataUnbind(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	auto branch = Git4DataRequiredBranch(context, transaction_manager, branch_name);
	// a source table can be renamed under the mapping, and a read follows it by oid; the mapping is taken
	// back the same way, so the table's new name reaches what its old one recorded
	auto named = Git4DataResolveTableOrNull(context, table_name);
	auto source_oid = named ? named->oid : 0;
	// what a mapping is recorded against is the table, so the key is what finds it - and the key of a name
	// that resolves to nothing is the name itself, which is what a mapping for a dropped source is held under
	auto source_key = named ? Git4DataSourceKey(*named) : table_name;
	// what it unbinds is a row in the registry tables as well, so the statement it returns drops it: a
	// mapping left behind would be read back at the next open
	auto drop_row = [&](const char *table, const string &cleared) {
		if (!Git4DataRegistryTablesThere(context)) {
			return string();
		}
		return StringUtil::Format("DELETE FROM %s.%s WHERE branch = '%s' AND source = '%s';", GIT4DATA_REGISTRY_SCHEMA,
		                          table, Git4DataSQLLiteral(branch_name), Git4DataSQLLiteral(cleared));
	};
	auto storage = Git4DataFindStorageTable(context, branch, source_key, source_oid);
	if (storage) {
		// the mapping is found and cleared by the same comparison, so a name that differs only in case
		// cannot be reported as unbound while the mapping stays - measured, that is exactly what happened
		// before the clearing comparison was made case-insensitive with the finding one. The name cleared
		// by is the one the mapping was written with, which is also the one an oid match found.
		if (!transaction_manager.branches.ClearStorageTable(branch_name, storage->source)) {
			throw InternalException("Could not unbind branch \"%s\" from \"%s\"", branch_name, table_name);
		}
		// the table itself is the caller's: it was created for the branch by materialize, and a caller
		// who gave one by hand would not expect this call to drop it either
		return drop_row(GIT4DATA_STORAGE_TABLE, storage->source) +
		       StringUtil::Format("SELECT '%s'::VARCHAR AS branch, '%s'::VARCHAR AS table_name, 'storage' AS "
		                          "unbound, '%s'::VARCHAR AS was",
		                          Git4DataSQLLiteral(branch_name), Git4DataSQLLiteral(table_name), Git4DataSQLLiteral(storage->storage));
	}
	auto delta = Git4DataFindDeltaTable(context, branch, source_key, source_oid);
	if (delta) {
		if (!transaction_manager.branches.ClearDeltaTable(branch_name, delta->source)) {
			throw InternalException("Could not unbind branch \"%s\" from \"%s\"", branch_name, table_name);
		}
		return drop_row(GIT4DATA_DELTAS_TABLE, delta->source) +
		       StringUtil::Format("SELECT '%s'::VARCHAR AS branch, '%s'::VARCHAR AS table_name, 'delta' AS "
		                          "unbound, '%s'::VARCHAR AS was",
		                          Git4DataSQLLiteral(branch_name), Git4DataSQLLiteral(table_name), Git4DataSQLLiteral(delta->added));
	}
	throw InvalidInputException("Branch \"%s\" has neither a table of its own nor a delta for \"%s\", so "
	                            "there is nothing to unbind",
	                            branch_name, table_name);
}

//! Record that a branch reads a table of its own. Only meaningful as the last statement of a
//! materialize, once the copy has been made. The optional fourth argument says the feature created the
//! table rather than the caller handing one over, which decides whether a later materialize may drop it.
static void Git4DataBindBranchTableImpl(ClientContext &context, const FunctionParameters &parameters) {
	auto source_table = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto storage_table = parameters.values[2].ToString();
	auto owned = false;
	if (parameters.values.size() > 3) {
		auto flag = StringUtil::Lower(parameters.values[3].ToString());
		if (flag != "true" && flag != "false") {
			throw InvalidInputException(
			    "The fourth argument of git4data_bind_branch_table says whether the table was created for the branch, "
			    "so it is true or false, not \"%s\"",
			    parameters.values[3].ToString());
		}
		owned = flag == "true";
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	auto branch = Git4DataRequiredBranch(context, transaction_manager, branch_name);
	// both tables have to be there, or the mapping would be a promise the reads then have to break
	auto &source_entry = Git4DataResolveTable(context, source_table);
	// a mapping is recorded against the *table*: the key carries its schema, so a branch's line for one of two
	// same-named tables is not read for the other
	auto source_key = Git4DataSourceKey(source_entry);
	auto record_name = Git4DataTableName(source_entry);
	auto &storage_entry = Git4DataResolveTable(context, storage_table);
	// The table a binding points the feature at is written by the feature's *own* statements - a materialize
	// fills it, an apply lands rows in a delta's - and those are marked internal, which is what lets them past
	// the guard that refuses a caller's write to the registry tables. So a binding aimed at one of those tables
	// is a way to write the registry through SQL after all: measured, with a source shaped like
	// `git4data.snapshots`, `git4data_apply` landed a row naming a snapshot called `forged` with a pin of the
	// caller's choosing, and `SELECT ... AT (SNAPSHOT => 'forged')` then read through it. The guard below
	// refuses the aim rather than the shot, because every writer goes through a binding.
	Git4DataCheckRegistryTarget(storage_entry.ParentSchema().name.GetIdentifierName(),
	                            storage_entry.name.GetIdentifierName());
	// The read maps a branch's own table onto the source's columns *by position*, so the table has to have
	// the source's columns in the source's order. Measured: a table holding (v, k) where the source holds
	// (k, v) read back as k = 777 and v = 1 from a row that held k = 1 and v = 777. The delta path has
	// always checked this for its two tables; the storage path did not.
	if (storage_entry.GetColumns().GetColumnNames() != source_entry.GetColumns().GetColumnNames() ||
	    storage_entry.GetColumns().GetColumnTypes() != source_entry.GetColumns().GetColumnTypes()) {
		throw InvalidInputException("Table \"%s\" is read as the rows of \"%s\" for this branch, and a read maps "
		                            "its columns by position - so it needs that table's columns, in that order.",
		                            storage_table, source_table);
	}
	// A branch's own line has to be a table of its own. Pointed at the source itself the branch stops
	// being a branch: measured, it then read the *live* table - 11 rows after a row was inserted and 6
	// after five were deleted, where its pin held 10 either way.
	if (source_entry.oid == storage_entry.oid) {
		throw InvalidInputException("Table \"%s\" is \"%s\" itself, and a branch's own line has to be a table "
		                            "of its own - given the source, the branch reads the live table instead of the "
		                            "state it pins. Materialize the branch to get a copy.",
		                            storage_table, source_table);
	}
	// the table is the caller's and it exists, so its identity is minted here (a fresh value, not whatever the
	// engine assigned when it was created): the mapping records that value, and the checkpoint writes it with
	// the table
	Git4DataMintTableIdentity(Git4DataResolveTable(context, storage_table));
	// the name the mapping records and every row about this mapping holds: the table's own name, so a caller
	// who spelled it `"t"` or `main.t` leaves a mapping that is found by the name the catalog gives the table
	auto stored_storage = Git4DataTableName(storage_entry);
	// both oids go with the mapping: the storage table's so a read can tell it from another table that
	// takes its name after it is dropped, and the source's so the mapping follows the source if it is
	// renamed
	// the commit this mapping is set at. The counter is handed out to the *next* transaction, so it is above
	// every id already committed - which is what makes it usable as "after every pin that exists now": a
	// capture taken earlier, in this transaction or any other, pins something below it.
	// the table's own identity, which is what tells it from a table that takes its name while the database is
	// closed - and the value the mapping's row carries (the row is written by the caller's form below, and for
	// a copy `materialize` makes by the internal pragma that runs after the CREATE TABLE)
	transaction_manager.branches.SetStorageTable(branch_name, record_name, stored_storage, storage_entry.oid,
	                                             source_entry.oid, owned,
	                                             transaction_manager.GetStartTimestampCounter(),
	                                             Git4DataTableIdentity(storage_entry));
	// the copy is the branch's line from here on, so a delta beside it would be a second one that nothing
	// reads - the branch reads its own table the moment it has one. A delta recorded before keys carried a
	// schema is held under the name it was written with, so it is cleared by that name.
	auto existing_delta = Git4DataFindDeltaTable(context, branch, source_key, source_entry.oid);
	transaction_manager.branches.ClearDeltaTable(branch_name, existing_delta ? existing_delta->source : source_key);
}

//! The caller's form of the same thing: it also returns the row that records the mapping, so the branch
//! reads the same way after a restart. The row replaces whatever was there for that source - one line, one
//! source - and the delta it replaces goes with it, which is what the implementation clears in memory.
string PragmaGit4DataBindBranchTable(ClientContext &context, const FunctionParameters &parameters) {
	auto source_table = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto storage_table = parameters.values[2].ToString();
	// the implementation parses and validates the fourth argument, so this reads it the same way rather than
	// casting: the cast's message is not the one the pragma documents (measured, "it is true or false, not
	// \"maybe\"" is what the test expects)
	auto owned = false;
	if (parameters.values.size() > 3) {
		auto flag = StringUtil::Lower(parameters.values[3].ToString());
		if (flag != "true" && flag != "false") {
			throw InvalidInputException(
			    "The fourth argument of git4data_bind_branch_table says whether the table was created for the branch, "
			    "so it is true or false, not \"%s\"",
			    parameters.values[3].ToString());
		}
		owned = flag == "true";
	}
	// the branch is read before the binding, because what the mapping replaces is what it held *before* - a row
	// written by a build that named the source differently is one this statement has to take with it
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// A branch another transaction added and has not committed is not there yet, so a mapping cannot be made
	// against it - the same rule the reads follow
	if (!Git4DataReferenceVisible(context, "branch", branch_name)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
	Git4DataBranch before;
	transaction_manager.branches.Get(branch_name, before);
	auto &bound_source = Git4DataResolveTable(context, source_table);
	auto source_key = Git4DataSourceKey(bound_source);
	auto record_name = Git4DataTableName(bound_source);
	auto stored =
	    Git4DataEscapedList(Git4DataStoredSourceNames(context, before, source_key, bound_source.oid, record_name));
	// the name the row records is the table's own, not the argument as typed, for the reason in the binding below
	auto stored_storage = Git4DataTableName(Git4DataResolveTable(context, storage_table));
	// **Rebinding replaces the mapping, so it leaves the same orphan a second materialize does.** A table the
	// feature created for this branch is a table of the identical line once nothing points at it, and the
	// measured case is the one `materialize` already handles: the caller binds a table of their own over a copy
	// materialize made, and the copy stays behind with no row naming it - and no listing shows it, so the caller
	// would have to remember the name to clean it up. The rule is the same one, so the same three conditions
	// apply: the table has to be the feature's (`owned`), it must not be the table being bound now, and no other
	// branch may name it. A table the caller bound by hand is left where it is (3dp), and so is a copy another
	// branch was given.
	string replaced;
	auto previous = Git4DataFindStorageTable(context, before, source_key, bound_source.oid);
	if (previous && previous->owned) {
		auto old_resolved = Git4DataResolveTableName(context, previous->storage, previous->oid);
		// compared by what the names resolve to rather than as text: this surface is case-insensitive
		// everywhere else, and re-binding the same table under a different spelling must not drop it
		auto new_resolved = Git4DataResolveTableName(context, storage_table, 0);
		if (!old_resolved.empty() && !StringUtil::CIEquals(old_resolved, new_resolved) &&
		    !transaction_manager.branches.OtherBranchUsesTable(branch_name, old_resolved, previous->oid)) {
			replaced = StringUtil::Format("\nDROP TABLE IF EXISTS %s;", Git4DataTableSQLName(old_resolved));
		}
	}
	Git4DataBindBranchTableImpl(context, parameters);
	string result = Git4DataEnsureRegistryTables(context);
	result += StringUtil::Format("DELETE FROM %s.%s WHERE branch = '%s' AND source IN (%s);", GIT4DATA_REGISTRY_SCHEMA,
	                             GIT4DATA_STORAGE_TABLE, Git4DataSQLLiteral(branch_name), stored);
	result += StringUtil::Format("DELETE FROM %s.%s WHERE branch = '%s' AND source IN (%s);", GIT4DATA_REGISTRY_SCHEMA,
	                             GIT4DATA_DELTAS_TABLE, Git4DataSQLLiteral(branch_name), stored);
	context.client_data->git4data_added_records.push_back("storage\t" + branch_name + "\t" + record_name);
	result += StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', '%s', '%s', %s, %llu, %llu);",
	                             GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_STORAGE_TABLE, Git4DataSQLLiteral(branch_name),
	                             Git4DataSQLLiteral(record_name), Git4DataSQLLiteral(stored_storage),
	                             owned ? "true" : "false",
	                             (unsigned long long)transaction_manager.GetStartTimestampCounter(),
	                             (unsigned long long)Git4DataTableIdentity(Git4DataResolveTable(context, storage_table)));
	// last, as in materialize: the row naming the new table is written before the old copy goes
	return result + replaced;
}

//! The generated form: the SQL materialize returns binds the branch in memory and writes the row itself,
//! because a pragma that returns a statement cannot be called from one.
//! The memory half of a binding, run by `materialize`'s own generated statement *after* the CREATE TABLE it
//! begins with - which is what makes it the one place that can put an identity on a copy the feature just made:
//! at the time the pragma that generates that statement runs, the table does not exist yet. The identity the
//! binder minted for the copy arrives as the fifth argument, so the table, the mapping's row and memory all
//! carry the same value.
void PragmaGit4DataBindInternal(ClientContext &context, const FunctionParameters &parameters) {
	// the identity first: the implementation below records the identity the table *has*, and the value this call
	// carries is the one the row already holds - so the table has to be wearing it before the memory half runs
	// (measured: the other order left memory recording the id the CREATE TABLE minted and the row holding the
	// one the binder chose, and every read of that mapping was refused as a swap)
	if (parameters.values.size() >= 5) {
		auto storage_table = parameters.values[2].ToString();
		auto identity = parameters.values[4].ToString();
		auto &storage = Git4DataResolveTable(context, storage_table);
		storage.GetStorage().GetDataTableInfo()->SetPersistedTableId(std::stoull(identity));
	}
	Git4DataBindBranchTableImpl(context, parameters);
}

//! Record that a branch differs from its base by a delta rather than by a copy of it. The two tables
//! are the caller's and are written with ordinary SQL: rows to add with the source table's columns,
//! keys to remove with the source table's key columns. Reads of the branch then serve the base minus
//! those keys plus those rows, which is what makes writing to a branch cost the change rather than the
//! table.
string PragmaGit4DataBindBranchDelta(ClientContext &context, const FunctionParameters &parameters) {
	auto source_table = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto added_table = parameters.values[2].ToString();
	auto removed_table = parameters.values[3].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	auto branch = Git4DataRequiredBranch(context, transaction_manager, branch_name);
	auto &source = Git4DataResolveTable(context, source_table);
	// a delta is recorded against the *table*, so its key carries the schema: two same-named tables in two
	// schemas have two lines, and neither is read for the other
	auto source_key = Git4DataSourceKey(source);
	auto record_name = Git4DataTableName(source);
	auto &added = Git4DataResolveTable(context, added_table);
	auto &removed = Git4DataResolveTable(context, removed_table);
	// both sides are written by the feature's own statements - an apply lands the added rows and records the
	// removed keys - so neither can be one of the registry's tables, for the reason spelled out at
	// `git4data_bind_branch_table`
	Git4DataCheckRegistryTarget(added.ParentSchema().name.GetIdentifierName(), added.name.GetIdentifierName());
	Git4DataCheckRegistryTarget(removed.ParentSchema().name.GetIdentifierName(), removed.name.GetIdentifierName());
	// the added rows are read as rows of the source table, so the two have to be interchangeable
	auto &source_columns = source.GetColumns();
	// the added rows are read as rows of the source table, so the two have to be interchangeable
	if (added.GetColumns().GetColumnNames() != source_columns.GetColumnNames() ||
	    added.GetColumns().GetColumnTypes() != source_columns.GetColumnTypes()) {
		throw InvalidInputException("Table \"%s\" holds the rows added to \"%s\", so it needs that table's columns "
		                            "in that order",
		                            added_table, source_table);
	}
	// the removed table names the rows to hide from the base, so it carries exactly the key columns, in
	// the order the source table declares them
	auto key_names = Git4DataPrimaryKeyColumnNames(source);
	if (key_names.empty()) {
		throw InvalidInputException("Table \"%s\" has no primary key, so a delta cannot say which rows it removes",
		                            source_table);
	}
	vector<string> expected_key_names;
	vector<LogicalType> expected_key_types;
	for (idx_t i = 0; i < source_columns.LogicalColumnCount(); i++) {
		auto &column = source_columns.GetColumn(LogicalIndex(i));
		auto name = column.Name().GetIdentifierName();
		if (std::find(key_names.begin(), key_names.end(), name) == key_names.end()) {
			continue;
		}
		expected_key_names.push_back(name);
		expected_key_types.push_back(column.Type());
	}
	if (removed.GetColumns().GetColumnNames() != expected_key_names ||
	    removed.GetColumns().GetColumnTypes() != expected_key_types) {
		throw InvalidInputException("Table \"%s\" holds the keys removed from \"%s\", so it needs exactly those "
		                            "%llu key column(s), in the order the source declares them",
		                            removed_table, source_table, expected_key_names.size());
	}
	// Neither side of a delta can be the source itself: the added rows are read *as* rows of it, so the
	// same table there means every row is read twice and the branch follows the live table - measured, a
	// ten-row source with itself as the added table read twenty rows and then twenty-one; and the keys
	// removed from it are keys of the live table, which the branch would hide by.
	if (added.oid == source.oid) {
		throw InvalidInputException("Table \"%s\" cannot hold the rows added to it: a branch's delta adds rows "
		                            "to what it inherits, so the added table has to be one of its own. Every row "
		                            "would be read twice, and the branch would follow the live table.",
		                            added_table);
	}
	if (removed.oid == source.oid) {
		throw InvalidInputException("Table \"%s\" cannot hold the keys removed from it: those keys are the live "
		                            "table's own rows, which the branch would then hide. The removed table has to "
		                            "be one of the branch's.",
		                            removed_table);
	}
	// One table cannot be both sides of a delta: the added rows are rows the branch *has* and the removed
	// keys are keys it does *not*, so a key in the same table for both means a row that is hidden and
	// shown at once. A source table whose only column is its key satisfies both column checks, which is
	// how this was reachable at all - measured: a delete written through such a delta left the row
	// visible while the table quietly gained its key.
	if (added.oid == removed.oid) {
		throw InvalidInputException("Table \"%s\" cannot hold both the rows added to \"%s\" and the keys removed "
		                            "from it: the two sides mean opposite things. Name two tables.",
		                            added_table, source_table);
	}
	// A branch that inherited a line which differs by a delta may be given one of its own: the two
	// overlays apply in order, the inherited one at the commit the branch forked at and this one as of
	// now, which is what a read of it walks. Nothing is refused here for that shape any more.
	for (auto &storage_entry : branch.storage_tables) {
		if (!Git4DataSourceKeyMatches(context, storage_entry.source, source_key) &&
		    (storage_entry.source_oid == 0 || storage_entry.source_oid != source.oid)) {
			continue;
		}
		// one line, one source: a branch that already reads a table of its own reads that, and a delta
		// beside it would be a second answer to the same question
		throw InvalidInputException("Branch \"%s\" already has a table of its own for \"%s\", so it reads that "
		                            "rather than a delta. A delta is for a branch that has no copy.",
		                            branch_name, source_table);
	}
	Git4DataMintTableIdentity(added);
	Git4DataMintTableIdentity(removed);
	auto stored_added = Git4DataTableName(added);
	auto stored_removed = Git4DataTableName(removed);
	transaction_manager.branches.SetDeltaTable(branch_name, record_name, stored_added, stored_removed, added.oid,
	                                           removed.oid, source.oid,
	                                           transaction_manager.GetStartTimestampCounter(),
	                                           Git4DataTableIdentity(added), Git4DataTableIdentity(removed));
	// the delta is a row too, and it replaces whatever that branch had for this source: one line, one source,
	// under every name a record for that table could have been written with
	auto stored = Git4DataEscapedList(Git4DataStoredSourceNames(context, branch, source_key, source.oid, record_name));
	string result = Git4DataEnsureRegistryTables(context);
	result += StringUtil::Format("DELETE FROM %s.%s WHERE branch = '%s' AND source IN (%s);", GIT4DATA_REGISTRY_SCHEMA,
	                             GIT4DATA_DELTAS_TABLE, Git4DataSQLLiteral(branch_name), stored);
	context.client_data->git4data_added_records.push_back("delta\t" + branch_name + "\t" + record_name);
	result += StringUtil::Format(
	    "INSERT INTO %s.%s VALUES ('%s', '%s', '%s', '%s', %llu, %llu, %llu);", GIT4DATA_REGISTRY_SCHEMA,
	    GIT4DATA_DELTAS_TABLE, Git4DataSQLLiteral(branch_name),
	    Git4DataSQLLiteral(record_name), Git4DataSQLLiteral(stored_added),
	    Git4DataSQLLiteral(stored_removed),
	    (unsigned long long)transaction_manager.GetStartTimestampCounter(),
	    (unsigned long long)Git4DataTableIdentity(added), (unsigned long long)Git4DataTableIdentity(removed));
	return result;
}

} // namespace duckdb
