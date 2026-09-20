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

namespace duckdb {

static void PragmaEnableProfilingStatement(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_profiler = true;
}

void RegisterEnableProfiling(BuiltinFunctions &set) {
	PragmaFunctionSet functions("");
	functions.AddFunction(PragmaFunction::PragmaStatement(Identifier(), PragmaEnableProfilingStatement));

	set.AddFunction("enable_profile", functions);
	set.AddFunction("enable_profiling", functions);
}

static void PragmaDisableProfiling(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_profiler = false;
}

static void PragmaEnableProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_progress_bar = true;
}

static void PragmaDisableProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_progress_bar = false;
}

static void PragmaEnablePrintProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).print_progress_bar = true;
}

static void PragmaDisablePrintProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).print_progress_bar = false;
}

//! Warn that a pragma that no longer does anything is on its way out
static void WarnDeprecatedPragma(ClientContext &context, const char *name) {
	DUCKDB_LOG_WARNING(
	    context,
	    StringUtil::Format("The '%s' pragma no longer has any effect; it is deprecated and will be removed in a "
	                       "future release.",
	                       name));
}

static void PragmaEnableVerification(ClientContext &context, const FunctionParameters &parameters) {
	WarnDeprecatedPragma(context, "enable_verification");
}

static void PragmaDisableVerification(ClientContext &context, const FunctionParameters &parameters) {
	WarnDeprecatedPragma(context, "disable_verification");
}

static void PragmaEnableForceParallelism(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_parallelism = true;
}

static void PragmaForceCheckpoint(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.force_checkpoint = true;
}

static void PragmaDisableForceParallelism(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_parallelism = false;
}

static void PragmaEnableObjectCache(ClientContext &context, const FunctionParameters &parameters) {
	WarnDeprecatedPragma(context, "enable_object_cache");
}

static void PragmaDisableObjectCache(ClientContext &context, const FunctionParameters &parameters) {
	WarnDeprecatedPragma(context, "disable_object_cache");
}

static void PragmaEnableCheckpointOnShutdown(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.checkpoint_on_shutdown = true;
}

static void PragmaDisableCheckpointOnShutdown(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.checkpoint_on_shutdown = false;
}

static void PragmaEnableOptimizer(ClientContext &context, const FunctionParameters &parameters) {
	Settings::Set<EnableOptimizerSetting>(context, SetScope::SESSION, Value::BOOLEAN(true));
}

static void PragmaDisableOptimizer(ClientContext &context, const FunctionParameters &parameters) {
	Settings::Set<EnableOptimizerSetting>(context, SetScope::SESSION, Value::BOOLEAN(false));
}

//===----------------------------------------------------------------------===//
// Git4Data snapshots
//===----------------------------------------------------------------------===//

static DuckTransactionManager &GetGit4DataTransactionManager(ClientContext &context) {
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

//! The AT unit a reference has to be read through, throwing when neither registry has the name
static const char *ResolveGit4DataReference(DuckTransactionManager &transaction_manager, const string &name) {
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
static string FormatGit4DataAtClause(const string &table_name, const string &reference, const char *unit) {
	return StringUtil::Format("%s AT (%s => '%s')", Git4DataTableSQLName(table_name), unit,
	                          StringUtil::Replace(reference, "'", "''"));
}

//! The primary key's column names, empty when the table has none
static vector<string> Git4DataPrimaryKeyColumnNames(TableCatalogEntry &table) {
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
	auto quoted_table = Git4DataTableSQLName(table_name);
	auto at_clause = FormatGit4DataAtClause(table_name, reference, unit);
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};

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
	auto diff = StringUtil::Format("git4data_diff('%s', '%s', '%s')", escape(table_name),
	                               DuckTransactionManager::LIVE_REFERENCE, escape(reference));
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
	// the tables first: the migration copies what the file already holds into them, and it has to see the
	// references that were there before this statement opened the window
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
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	auto name = string(DuckTransactionManager::PITR_REFERENCE);
	context.client_data->git4data_added_records.push_back("snapshot\t" + name);
	result += StringUtil::Format("DELETE FROM %s.%s WHERE name = '%s';\n", GIT4DATA_REGISTRY_SCHEMA,
	                             GIT4DATA_SNAPSHOTS_TABLE, escape(name));
	result += StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', %llu, TIMESTAMP '%s', '%s');",
	                             GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_SNAPSHOTS_TABLE, escape(name), held.pin,
	                             Timestamp::ToString(held.created), escape(held.line));
	return result;
}

static string PragmaGit4DataPitrOff(ClientContext &context, const FunctionParameters &parameters) {
	GetGit4DataTransactionManager(context).ClearPitr();
	// and the row goes with it, or the next open would bring the window back on a database that turned it off
	if (!Git4DataRegistryTablesThere(context)) {
		return string();
	}
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	return StringUtil::Format("DELETE FROM %s.%s WHERE name = '%s';", GIT4DATA_REGISTRY_SCHEMA,
	                          GIT4DATA_SNAPSHOTS_TABLE, escape(string(DuckTransactionManager::PITR_REFERENCE)));
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
static string BuildGit4DataRowComparison(TableCatalogEntry &table, const string &left_alias, const string &right_alias);

//! The column names of a table, if it is there. Used to keep generated names clear of them.
static void Git4DataAddColumnNames(ClientContext &context, const string &table_name, case_insensitive_set_t &names) {
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
static vector<string> Git4DataStoredSourceNames(ClientContext &context, const Git4DataBranch &branch, const string &key,
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
static string Git4DataEscapedList(const vector<string> &names) {
	string result;
	for (auto &name : names) {
		if (!result.empty()) {
			result += ", ";
		}
		result += "'" + StringUtil::Replace(name, "'", "''") + "'";
	}
	return result;
}

//! The columns every table a generated statement reads brings into scope: the source table, and for
//! each named reference the table that reference reads its rows from - a branch with a line of its own
//! reads that table rather than the source's columns.
static case_insensitive_set_t Git4DataScopeColumnNames(ClientContext &context,
                                                       DuckTransactionManager &transaction_manager,
                                                       const string &table_name, const vector<string> &references) {
	case_insensitive_set_t names;
	Git4DataAddColumnNames(context, table_name, names);
	auto entry = Git4DataResolveTableOrNull(context, table_name);
	auto key = entry ? Git4DataSourceKey(*entry) : table_name;
	for (auto &reference : references) {
		Git4DataBranch branch;
		if (!transaction_manager.branches.Get(reference, branch)) {
			continue;
		}
		auto storage = Git4DataFindStorageTable(context, branch, key, entry ? entry->oid : 0);
		if (!storage) {
			continue;
		}
		auto resolved = Git4DataResolveTableName(context, storage->storage, storage->oid);
		if (!resolved.empty()) {
			Git4DataAddColumnNames(context, resolved, names);
		}
	}
	return names;
}

//! A name for one of the feature's own aliases or helper tables that no column in the statement carries.
//! An alias is in scope beside those columns, so one that a column happens to share makes the generated
//! SQL ambiguous - measured, with a column named g4d_b: "Ambiguous reference to column name \"g4d_b\""
//! from the merge. Names are lengthened until they are clear rather than refused, because the caller's
//! table is not at fault and the aliases are the feature's own. A column named like a name this function
//! is *asked* for simply moves the feature's name along.
static string Git4DataFreeName(const string &base, const case_insensitive_set_t &taken) {
	auto name = base;
	while (taken.find(name) != taken.end()) {
		name += "_";
	}
	return name;
}

//! The name of one of the feature's temporary helper tables, which has to clear a second thing as well:
//! the statement that creates it is `CREATE OR REPLACE TEMP TABLE`, so a table the caller already holds
//! under that name would be replaced and then dropped along with the helper. Measured: a caller's own
//! `CREATE TEMP TABLE g4d_merge_conflicts (x)` holding a row was gone after a merge, and the pick's
//! helper has the same shape. A permanent table of that name is not destroyed - the temporary one
//! shadows it for the length of the group, and the drop takes the temporary one - but it is left alone
//! rather than shadowed, which costs only a longer internal name.
static string Git4DataFreeHelperName(ClientContext &context, const string &base, const case_insensitive_set_t &taken) {
	auto name = base;
	while (
	    taken.find(name) != taken.end() ||
	    Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(name)), OnEntryNotFound::RETURN_NULL)) {
		name += "_";
	}
	return name;
}

static string PragmaGit4DataPick(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto source = parameters.values[1].ToString();
	auto base = parameters.values[2].ToString();
	// Overwriting is what a pick did before there was a choice, so it stays the default - it answers
	// "make the table look like the source", and the caller named the direction. fail and skip are
	// there because the live table may have moved on since the base, and overwriting would throw away
	// a decision the pick never saw.
	auto policy = parameters.values.size() > 3 ? StringUtil::Lower(parameters.values[3].ToString()) : "overwrite";
	if (policy != "overwrite" && policy != "fail" && policy != "skip") {
		throw InvalidInputException("Pick policy has to be overwrite, fail or skip, not \"%s\"", policy);
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	// both names resolve before any statement is generated, so a typo cannot half-apply a pick
	auto source_unit = ResolveGit4DataReference(transaction_manager, source);
	auto base_unit = ResolveGit4DataReference(transaction_manager, base);

	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(table_name)));
	auto key_columns = Git4DataPrimaryKeyColumnNames(table);
	if (key_columns.empty()) {
		throw InvalidInputException("Table \"%s\" has no primary key, so a pick cannot tell which rows to "
		                            "apply the changes to",
		                            table_name);
	}
	string key_list;
	for (idx_t i = 0; i < key_columns.size(); i++) {
		if (i > 0) {
			key_list += ", ";
		}
		key_list += KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
	}
	auto quoted_table = Git4DataTableSQLName(table_name);
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	auto diff = StringUtil::Format("git4data_diff('%s', '%s', '%s')", escape(table_name), escape(base), escape(source));

	// The key is what the two sides are matched on, not a row id: a reference scan does not expose
	// row ids, and the key is the identity the diff already pairs updates by.
	// A conflict is a key the source changed whose live row has also changed since the base, and where
	// the two sides did not end up with the same row. Both sides making the same change is agreement,
	// not a conflict - the same rule a merge applies.
	auto base_image = FormatGit4DataAtClause(table_name, base, base_unit);
	auto source_image = FormatGit4DataAtClause(table_name, source, source_unit);
	// the statement reads the live table and both images, so no alias may be a column of any of them
	auto taken = Git4DataScopeColumnNames(context, transaction_manager, table_name, {base, source});
	auto alias_live = Git4DataFreeName("g4d_l", taken);
	auto alias_base = Git4DataFreeName("g4d_b", taken);
	auto alias_source = Git4DataFreeName("g4d_s", taken);
	auto alias_keys = Git4DataFreeName("g4d_tk", taken);
	auto conflict_name = Git4DataFreeHelperName(context, "g4d_pick_conflicts", taken);
	string conflict_table;
	if (policy != "overwrite") {
		string key_list_joined;
		string join_live;
		string join_base;
		string join_source;
		string row_comparison_base;
		string row_comparison_source;
		string presence_base;
		string presence_source;
		for (idx_t i = 0; i < key_columns.size(); i++) {
			auto quoted = KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
			if (i > 0) {
				key_list_joined += ", ";
				join_live += " AND ";
				join_base += " AND ";
				join_source += " AND ";
				presence_base += " OR ";
				presence_source += " OR ";
			}
			key_list_joined += alias_keys + "." + quoted;
			join_live += StringUtil::Format("%s.%s = %s.%s", alias_live, quoted, alias_keys, quoted);
			join_base += StringUtil::Format("%s.%s = %s.%s", alias_base, quoted, alias_keys, quoted);
			join_source += StringUtil::Format("%s.%s = %s.%s", alias_source, quoted, alias_keys, quoted);
			// a key is never null, so a null one in an outer join means that side has no such row
			presence_base +=
			    StringUtil::Format("(%s.%s IS NULL) <> (%s.%s IS NULL)", alias_live, quoted, alias_base, quoted);
			presence_source +=
			    StringUtil::Format("(%s.%s IS NULL) <> (%s.%s IS NULL)", alias_live, quoted, alias_source, quoted);
		}
		auto live_key = alias_live + "." + KeywordHelper::WriteOptionallyQuoted(key_columns[0]);
		auto changed_from_base = StringUtil::Format("((%s) OR (%s IS NOT NULL AND NOT (%s)))", presence_base, live_key,
		                                            BuildGit4DataRowComparison(table, alias_live, alias_base));
		auto differs_from_source =
		    StringUtil::Format("((%s) OR (%s IS NOT NULL AND NOT (%s)))", presence_source, live_key,
		                       BuildGit4DataRowComparison(table, alias_live, alias_source));
		conflict_table = StringUtil::Format(
		    "CREATE OR REPLACE TEMP TABLE %s AS WITH %s AS (SELECT %s FROM %s WHERE g4d_change IN "
		    "('added', 'removed', 'changed')), %s AS (SELECT * FROM %s), %s AS (SELECT * FROM %s), %s AS "
		    "(SELECT * FROM %s) SELECT %s FROM %s LEFT JOIN %s ON (%s) LEFT JOIN %s ON (%s) LEFT JOIN "
		    "%s ON (%s) WHERE (%s) AND (%s);\n",
		    conflict_name, alias_keys, key_list, diff, alias_live, quoted_table, alias_base, base_image, alias_source,
		    source_image, key_list_joined, alias_keys, alias_live, join_live, alias_base, join_base, alias_source,
		    join_source, changed_from_base, differs_from_source);
	}

	// A changed row is applied as a delete followed by an insert: there is no in-place update to reach
	// for here, and the outcome is the same.
	string result = conflict_table;
	if (policy == "fail") {
		result += StringUtil::Format(
		    "SELECT CASE WHEN EXISTS (SELECT 1 FROM %s) THEN error('pick conflict: the live "
		    "table changed the same rows since the base - pass policy skip to leave them, or overwrite to "
		    "take the source') ELSE 0 END;\n",
		    conflict_name);
	}
	auto conflict_filter =
	    policy == "skip" ? StringUtil::Format(" AND (%s) NOT IN (SELECT %s FROM %s)", key_list, key_list, conflict_name)
	                     : string();
	result += StringUtil::Format("DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change IN ('removed', "
	                             "'changed')%s);\n",
	                             quoted_table, key_list, key_list, diff, conflict_filter);
	// Only the keys the live table is actually missing: a row the source added may well still be there,
	// because the live table is usually the source's own continuation, and inserting it again would
	// collide with its primary key instead of restoring anything. The keys of changed rows were removed
	// by the statement above, so they are missing by the time this runs and take the source's values.
	result += StringUtil::Format("INSERT INTO %s SELECT * FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change "
	                             "IN ('added', 'changed')%s) AND (%s) NOT IN (SELECT %s FROM %s);",
	                             quoted_table, source_image, key_list, key_list, diff, conflict_filter, key_list,
	                             key_list, quoted_table);
	if (!conflict_table.empty()) {
		result += StringUtil::Format("\nDROP TABLE %s;", conflict_name);
	}
	return result;
}

//! Give a branch a table of its own for a source table, which is what lets it be written to: a
//! branch read as the pinned image cannot diverge, because the image is shared with everything else
//! on the timeline. The copy is made by the statement this returns; the mapping is recorded first so
//! a read of the branch can see it, and a mapping whose table never appears falls back to the pinned
//! image instead of failing.
static string PragmaGit4DataMaterialize(ClientContext &context, const FunctionParameters &parameters) {
	auto source_table = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto storage_table = parameters.values[2].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	Git4DataBranch branch;
	if (!Git4DataReferenceVisible(context, "branch", branch_name) ||
	    !transaction_manager.branches.Get(branch_name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
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
	auto escaped_source = StringUtil::Replace(record_name, "'", "''");
	auto stored_source =
	    Git4DataEscapedList(Git4DataStoredSourceNames(context, branch, source_key, table.oid, record_name));
	auto escaped_branch = StringUtil::Replace(branch_name, "'", "''");
	auto escaped_storage = StringUtil::Replace(storage_table, "'", "''");
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
	return StringUtil::Format("CREATE TABLE %s (%s);\nINSERT INTO %s SELECT * FROM %s AT (BRANCH => '%s');\n"
	                          "PRAGMA git4data_bind_internal('%s', '%s', '%s', 'true');\n"
	                          "DELETE FROM %s.%s WHERE branch = '%s' AND source IN (%s);\n"
	                          "INSERT INTO %s.%s VALUES ('%s', '%s', '%s', true);%s",
	                          quoted_storage, columns, quoted_storage, Git4DataTableSQLName(source_key), escaped_branch,
	                          escaped_source, escaped_branch, escaped_storage, GIT4DATA_REGISTRY_SCHEMA,
	                          GIT4DATA_STORAGE_TABLE, escaped_branch, stored_source, GIT4DATA_REGISTRY_SCHEMA,
	                          GIT4DATA_STORAGE_TABLE, escaped_branch, escaped_source, escaped_storage, replaced);
}

//! Take back what a branch was given for a source table: its table of its own, or the delta it differs
//! by. A mapping whose table is dropped falls back to the pinned image rather than failing - that rule is
//! deliberate, because the branch is still a valid reference - but it is silent, and until now there was
//! no way to say "stop reading that table, the branch reads its pin now". This is that way, and it is
//! also what turns the fallback from a state nobody can see into one a caller can see and clear.
static string PragmaGit4DataUnbind(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	Git4DataBranch branch;
	if (!Git4DataReferenceVisible(context, "branch", branch_name) ||
	    !transaction_manager.branches.Get(branch_name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
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
		                          table, escape(branch_name), escape(cleared));
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
		                          escape(branch_name), escape(table_name), escape(storage->storage));
	}
	auto delta = Git4DataFindDeltaTable(context, branch, source_key, source_oid);
	if (delta) {
		if (!transaction_manager.branches.ClearDeltaTable(branch_name, delta->source)) {
			throw InternalException("Could not unbind branch \"%s\" from \"%s\"", branch_name, table_name);
		}
		return drop_row(GIT4DATA_DELTAS_TABLE, delta->source) +
		       StringUtil::Format("SELECT '%s'::VARCHAR AS branch, '%s'::VARCHAR AS table_name, 'delta' AS "
		                          "unbound, '%s'::VARCHAR AS was",
		                          escape(branch_name), escape(table_name), escape(delta->added));
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
	Git4DataBranch branch;
	if (!Git4DataReferenceVisible(context, "branch", branch_name) ||
	    !transaction_manager.branches.Get(branch_name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
	// both tables have to be there, or the mapping would be a promise the reads then have to break
	auto &source_entry = Git4DataResolveTable(context, source_table);
	// a mapping is recorded against the *table*: the key carries its schema, so a branch's line for one of two
	// same-named tables is not read for the other
	auto source_key = Git4DataSourceKey(source_entry);
	auto record_name = Git4DataTableName(source_entry);
	auto &storage_entry = Git4DataResolveTable(context, storage_table);
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
	// both oids go with the mapping: the storage table's so a read can tell it from another table that
	// takes its name after it is dropped, and the source's so the mapping follows the source if it is
	// renamed
	transaction_manager.branches.SetStorageTable(branch_name, record_name, storage_table, storage_entry.oid,
	                                             source_entry.oid, owned);
	// the copy is the branch's line from here on, so a delta beside it would be a second one that nothing
	// reads - the branch reads its own table the moment it has one. A delta recorded before keys carried a
	// schema is held under the name it was written with, so it is cleared by that name.
	auto existing_delta = Git4DataFindDeltaTable(context, branch, source_key, source_entry.oid);
	transaction_manager.branches.ClearDeltaTable(branch_name, existing_delta ? existing_delta->source : source_key);
}

//! The caller's form of the same thing: it also returns the row that records the mapping, so the branch
//! reads the same way after a restart. The row replaces whatever was there for that source - one line, one
//! source - and the delta it replaces goes with it, which is what the implementation clears in memory.
static string PragmaGit4DataBindBranchTable(ClientContext &context, const FunctionParameters &parameters) {
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
	Git4DataBindBranchTableImpl(context, parameters);
	string result = Git4DataEnsureRegistryTables(context);
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	result += StringUtil::Format("DELETE FROM %s.%s WHERE branch = '%s' AND source IN (%s);", GIT4DATA_REGISTRY_SCHEMA,
	                             GIT4DATA_STORAGE_TABLE, escape(branch_name), stored);
	result += StringUtil::Format("DELETE FROM %s.%s WHERE branch = '%s' AND source IN (%s);", GIT4DATA_REGISTRY_SCHEMA,
	                             GIT4DATA_DELTAS_TABLE, escape(branch_name), stored);
	context.client_data->git4data_added_records.push_back("storage\t" + branch_name + "\t" + record_name);
	result += StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', '%s', '%s', %s);", GIT4DATA_REGISTRY_SCHEMA,
	                             GIT4DATA_STORAGE_TABLE, escape(branch_name), escape(record_name),
	                             escape(storage_table), owned ? "true" : "false");
	return result;
}

//! The generated form: the SQL materialize returns binds the branch in memory and writes the row itself,
//! because a pragma that returns a statement cannot be called from one.
static void PragmaGit4DataBindInternal(ClientContext &context, const FunctionParameters &parameters) {
	Git4DataBindBranchTableImpl(context, parameters);
}

//! Record that a branch differs from its base by a delta rather than by a copy of it. The two tables
//! are the caller's and are written with ordinary SQL: rows to add with the source table's columns,
//! keys to remove with the source table's key columns. Reads of the branch then serve the base minus
//! those keys plus those rows, which is what makes writing to a branch cost the change rather than the
//! table.
static string PragmaGit4DataBindBranchDelta(ClientContext &context, const FunctionParameters &parameters) {
	auto source_table = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto added_table = parameters.values[2].ToString();
	auto removed_table = parameters.values[3].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	Git4DataBranch branch;
	if (!Git4DataReferenceVisible(context, "branch", branch_name) ||
	    !transaction_manager.branches.Get(branch_name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
	auto &source = Git4DataResolveTable(context, source_table);
	// a delta is recorded against the *table*, so its key carries the schema: two same-named tables in two
	// schemas have two lines, and neither is read for the other
	auto source_key = Git4DataSourceKey(source);
	auto record_name = Git4DataTableName(source);
	auto &added = Git4DataResolveTable(context, added_table);
	auto &removed = Git4DataResolveTable(context, removed_table);
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
	transaction_manager.branches.SetDeltaTable(branch_name, record_name, added_table, removed_table, added.oid,
	                                           removed.oid, source.oid);
	// the delta is a row too, and it replaces whatever that branch had for this source: one line, one source,
	// under every name a record for that table could have been written with
	auto stored = Git4DataEscapedList(Git4DataStoredSourceNames(context, branch, source_key, source.oid, record_name));
	string result = Git4DataEnsureRegistryTables(context);
	result += StringUtil::Format("DELETE FROM %s.%s WHERE branch = '%s' AND source IN (%s);", GIT4DATA_REGISTRY_SCHEMA,
	                             GIT4DATA_DELTAS_TABLE, StringUtil::Replace(branch_name, "'", "''"), stored);
	context.client_data->git4data_added_records.push_back("delta\t" + branch_name + "\t" + record_name);
	result += StringUtil::Format(
	    "INSERT INTO %s.%s VALUES ('%s', '%s', '%s', '%s');", GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_DELTAS_TABLE,
	    StringUtil::Replace(branch_name, "'", "''"), StringUtil::Replace(record_name, "'", "''"),
	    StringUtil::Replace(added_table, "'", "''"), StringUtil::Replace(removed_table, "'", "''"));
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
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	return StringUtil::Format("SELECT '%s'::VARCHAR AS source, '%s'::VARCHAR AS clone, %s::BOOLEAN AS with_log",
	                          escape(source), escape(target), cloned_log ? "true" : "false");
}

//! Point every reference to the source table inside a parsed statement at the branch the statement is
//! being applied to. The call writes *the branch*, so a bare `t` in the statement means the branch's
//! line - reading the live table instead would be a silent wrong answer, and it was one: measured, an
//! insert whose rows came from the table itself copied the live table's value (v = 1) where the branch
//! held 101. A reference that already names a reference of its own is left alone, because naming one is
//! a decision rather than an omission.
static void Git4DataPointExprAtBranch(ParsedExpression &expr, const string &table_name, const string &branch_name);

static void Git4DataPointRefAtBranch(TableRef &ref, const string &table_name, const string &branch_name) {
	if (ref.type != TableReferenceType::BASE_TABLE) {
		// the iterator has already walked into joins, subqueries and table functions
		return;
	}
	auto &base = ref.Cast<BaseTableRef>();
	if (base.at_clause || base.GetQualifiedName().Name().GetIdentifierName() != table_name) {
		return;
	}
	// a ConstantExpression is built from a Literal, and a string constant is a string literal
	base.at_clause =
	    make_uniq<AtClause>(Identifier("BRANCH"), make_uniq<ConstantExpression>(Literal::String(branch_name)));
}

static void Git4DataPointQueryAtBranch(QueryNode &node, const string &table_name, const string &branch_name) {
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node, [&](unique_ptr<ParsedExpression> &child) { Git4DataPointExprAtBranch(*child, table_name, branch_name); },
	    [&](TableRef &ref) { Git4DataPointRefAtBranch(ref, table_name, branch_name); });
}

static void Git4DataPointExprAtBranch(ParsedExpression &expr, const string &table_name, const string &branch_name) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expr.Cast<SubqueryExpression>();
		if (subquery.Subquery()) {
			Git4DataPointQueryAtBranch(*subquery.SubqueryMutable()->node, table_name, branch_name);
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<ParsedExpression> &child) { Git4DataPointExprAtBranch(*child, table_name, branch_name); });
}

//! The table a write statement names has to be the table the delta is for, resolved and compared - the
//! name alone would match a same-named table in another schema.
static void Git4DataCheckApplyTargetTable(ClientContext &context, const QualifiedName &written_name,
                                          TableCatalogEntry &source) {
	auto &table_name = source.name.GetIdentifierName();
	// the name is not the table: a statement can reach a same-named table in another schema, and the write
	// is recorded against the name the pragma resolved. Both are resolved here, so the table is compared
	// rather than its name.
	auto by_name = Catalog::GetEntry<TableCatalogEntry>(context, written_name, OnEntryNotFound::RETURN_NULL);
	if (by_name && by_name->oid == source.oid) {
		return;
	}
	// The clause has to be true of the table the statement actually named: "a different table of that name"
	// describes the same-named table in another schema, and it was said about a table with a *different* name
	// too - measured (increment 3cj), `apply('a', 'br', 'INSERT INTO other ...')` answered "the statement writes
	// "other", which is a different table of that name, in schema "main"", where the name is the thing that is
	// not the same.
	auto same_name = by_name && StringUtil::CIEquals(by_name->name.GetIdentifierName(), table_name);
	throw InvalidInputException(
	    "git4data_apply was given \"%s\", but the statement writes \"%s\", which is %s: the write would be "
	    "recorded in the delta of \"%s\". Write the table the branch's delta is for.",
	    table_name, written_name.ToString(),
	    by_name ? (same_name
	                   ? "a different table of that name, in schema \"" +
	                         by_name->ParentSchema().name.GetIdentifierName() + "\""
	                   : "a different table")
	            : "not a table",
	    table_name);
}

//! The statement a delta branch is written through has to name the table the delta is for, and name it
//! directly: a write through another name would be recorded against the wrong line, and a join has no
//! single row of that table to record.
static void Git4DataCheckApplyTarget(ClientContext &context, TableRef &ref, TableCatalogEntry &source) {
	auto &table_name = source.name.GetIdentifierName();
	if (ref.type != TableReferenceType::BASE_TABLE) {
		throw InvalidInputException("git4data_apply takes a statement that names \"%s\" directly, not one that "
		                            "writes through another table or a subquery",
		                            table_name);
	}
	auto &base = ref.Cast<BaseTableRef>();
	auto written = base.GetQualifiedName().Name().GetIdentifierName();
	// a name in SQL is not case-sensitive, and neither is this: measured, "Mixed" and mixed are the same
	// table, and comparing them as strings refused a statement that wrote the right one
	if (!StringUtil::CIEquals(written, table_name)) {
		throw InvalidInputException("git4data_apply was given \"%s\", but the statement writes \"%s\": the statement "
		                            "has to write the table the branch's delta is for",
		                            table_name, written);
	}
	Git4DataCheckApplyTargetTable(context, base.GetQualifiedName(), source);
}

//! Apply one statement of ordinary SQL to a branch that inherits by delta. The statement is translated
//! rather than executed: what holds a delta branch's line is the two tables that say which base keys it
//! hides and which rows it adds, so a write is an edit to those two - and the caller writes the SQL it
//! would have written anyway, at a cost set by the change rather than by the table.
//!
//! The branch's own image is read for every shape, never the live table, and it is read *before* the
//! delta is edited, so the rows a statement touches are the rows the branch shows:
//!
//!   DELETE   the affected keys go into `removed` and the branch's own added rows for those keys go
//!   UPDATE   the keys the statement touched go into `removed` and the new rows into `added`. A key the
//!            statement rewrites is therefore a removal and an addition, which is what an update is in
//!            a delta's terms - the pairing a read needs, kept by the feature rather than by the caller
//!   INSERT   the rows go into `added`. A key the branch was hiding stays hidden: the branch does not
//!            show that row, so inserting it adds one, and the base row stays where the delta put it
static string PragmaGit4DataApply(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto branch_name = parameters.values[1].ToString();
	auto dml = parameters.values[2].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	Git4DataBranch branch;
	if (!Git4DataReferenceVisible(context, "branch", branch_name) ||
	    !transaction_manager.branches.Get(branch_name, branch)) {
		throw InvalidInputException("Branch \"%s\" does not exist", branch_name);
	}
	// the source table can be renamed under the mapping, and the mapping follows the table it was bound to:
	// the oid finds the delta a name no longer does, exactly as it does on a read
	auto already = Git4DataResolveTableOrNull(context, table_name);
	auto write_key = already ? Git4DataSourceKey(*already) : table_name;
	auto found_delta = Git4DataFindDeltaTable(context, branch, write_key, already ? already->oid : 0);

	if (!found_delta) {
		throw InvalidInputException(
		    "Branch \"%s\" does not differ from its base by a delta for \"%s\", so a write has nowhere to be "
		    "recorded. Give the branch a delta for that table first, or materialize the branch and write to the "
		    "table it then has of its own.",
		    branch_name, table_name);
	}
	auto delta = *found_delta;
	auto &table = Git4DataResolveTable(context, table_name);
	auto key_columns = Git4DataPrimaryKeyColumnNames(table);
	if (key_columns.empty()) {
		throw InvalidInputException("Table \"%s\" has no primary key, so a delta cannot say which rows a write "
		                            "removes",
		                            table_name);
	}
	auto added_table = Git4DataResolveTableName(context, delta.added, delta.added_oid);
	auto removed_table = Git4DataResolveTableName(context, delta.removed, delta.removed_oid);
	if (added_table.empty() || removed_table.empty()) {
		throw InvalidInputException("The delta branch \"%s\" differs by is gone: \"%s\" or \"%s\" is not there "
		                            "any more",
		                            branch_name, delta.added, delta.removed);
	}

	Parser parser(context.GetParserOptions());
	parser.ParseQuery(dml);
	if (parser.statements.empty()) {
		throw InvalidInputException("git4data_apply needs at least one statement to apply");
	}

	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	auto quoted_source = Git4DataTableSQLName(table_name);
	auto quoted_added = Git4DataTableSQLName(added_table);
	auto quoted_removed = Git4DataTableSQLName(removed_table);
	auto at_clause = FormatGit4DataAtClause(table_name, branch_name, "BRANCH");
	// the statement reads the source table, the branch's image of it and the two delta tables, so no
	// alias may be a column of any of them
	auto taken = Git4DataScopeColumnNames(context, transaction_manager, table_name, {branch_name});
	Git4DataAddColumnNames(context, added_table, taken);
	Git4DataAddColumnNames(context, removed_table, taken);
	auto rows_name = Git4DataFreeHelperName(context, "g4d_apply_rows", taken);
	auto keys_name = Git4DataFreeHelperName(context, "g4d_apply_keys", taken);
	auto alias_rows = Git4DataFreeName("g4d_ar", taken);
	auto alias_keys = Git4DataFreeName("g4d_ak", taken);
	auto alias_added = Git4DataFreeName("g4d_aa", taken);
	auto alias_removed = Git4DataFreeName("g4d_arr", taken);
	auto alias_branch = Git4DataFreeName("g4d_ab", taken);

	string key_list;
	string column_list;
	for (idx_t i = 0; i < key_columns.size(); i++) {
		if (i > 0) {
			key_list += ", ";
		}
		key_list += KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
	}
	for (auto &column : table.GetColumns().Logical()) {
		if (!column_list.empty()) {
			column_list += ", ";
		}
		column_list += KeywordHelper::WriteOptionallyQuoted(column.Name().GetIdentifierName());
	}
	auto match = [&](const string &left_alias, const string &right_alias) {
		string result;
		for (auto &key : key_columns) {
			if (!result.empty()) {
				result += " AND ";
			}
			auto quoted = KeywordHelper::WriteOptionallyQuoted(key);
			result += StringUtil::Format("%s.%s = %s.%s", left_alias, quoted, right_alias, quoted);
		}
		return result;
	};
	auto branch_image = StringUtil::Format("(SELECT * FROM %s) %s", at_clause, alias_branch);
	// a hidden base row stays hidden, and an editing statement never names a key twice in the table that
	// hides rows: the table is the caller's and may well carry the primary key
	auto add_removals = [&](const string &from_name, const string &from_alias) {
		return StringUtil::Format("INSERT INTO %s (%s) SELECT %s FROM %s %s WHERE NOT EXISTS (SELECT 1 FROM %s %s "
		                          "WHERE %s);\n",
		                          quoted_removed, key_list, key_list, from_name, from_alias, quoted_removed,
		                          alias_removed, match(alias_removed, from_alias));
	};

	// the statements read the branch, so their own references to the table are pointed at it before
	// anything is rendered
	for (auto &statement : parser.statements) {
		switch (statement->type) {
		case StatementType::INSERT_STATEMENT:
			Git4DataPointQueryAtBranch(*statement->Cast<InsertStatement>().node, table_name, branch_name);
			break;
		case StatementType::UPDATE_STATEMENT:
			Git4DataPointQueryAtBranch(*statement->Cast<UpdateStatement>().node, table_name, branch_name);
			break;
		case StatementType::DELETE_STATEMENT:
			Git4DataPointQueryAtBranch(*statement->Cast<DeleteStatement>().node, table_name, branch_name);
			break;
		default:
			break;
		}
	}

	// Every statement is translated against the same delta, and the whole group is handed back as one
	// string so the caller's statements land in one transaction: either all of them are recorded or
	// none is, which is what makes a group a unit of work rather than a sequence of writes.
	auto translate = [&](SQLStatement &statement) {
		switch (statement.type) {
		case StatementType::DELETE_STATEMENT: {
			auto &node = statement.Cast<DeleteStatement>().node;
			if (!node->using_clauses.empty()) {
				throw InvalidInputException("git4data_apply does not take DELETE ... USING: the rows to remove have to "
				                            "be the branch's own, which a second table is not");
			}
			if (!node->returning_list.empty()) {
				throw InvalidInputException("git4data_apply does not take RETURNING: nothing is written where the rows "
				                            "would come from");
			}
			Git4DataCheckApplyTarget(context, *node->table, table);
			auto condition = node->condition ? node->condition->ToString() : string();
			string result =
			    StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM %s%s;\n", rows_name, key_list,
			                       at_clause, condition.empty() ? string() : " WHERE " + condition);
			result +=
			    StringUtil::Format("DELETE FROM %s %s WHERE EXISTS (SELECT 1 FROM %s %s WHERE %s);\n", quoted_added,
			                       alias_added, rows_name, alias_rows, match(alias_added, alias_rows));
			result += add_removals(rows_name, alias_rows);
			result += StringUtil::Format("DROP TABLE %s;", rows_name);
			return result;
		}
		case StatementType::UPDATE_STATEMENT: {
			auto &node = statement.Cast<UpdateStatement>().node;
			if (node->from_table) {
				throw InvalidInputException("git4data_apply does not take UPDATE ... FROM: the rows to change have to "
				                            "be the branch's own, which a joined table is not");
			}
			if (!node->returning_list.empty()) {
				throw InvalidInputException("git4data_apply does not take RETURNING: nothing is written where the rows "
				                            "would come from");
			}
			Git4DataCheckApplyTarget(context, *node->table, table);
			auto &set_info = *node->set_info;
			auto condition = set_info.condition ? set_info.condition->ToString() : string();
			// the new rows: the branch's own columns, with the assigned ones replaced by what they are
			// assigned. A row's other columns are the row's own, which is what a SET list means.
			vector<pair<string, string>> assignments;
			for (idx_t i = 0; i < set_info.columns.size(); i++) {
				assignments.emplace_back(set_info.columns[i].GetIdentifierName(), set_info.expressions[i]->ToString());
			}
			string new_columns;
			for (auto &column : table.GetColumns().Logical()) {
				auto name = column.Name().GetIdentifierName();
				if (!new_columns.empty()) {
					new_columns += ", ";
				}
				string value;
				for (auto &assignment : assignments) {
					if (assignment.first == name) {
						value = assignment.second;
						break;
					}
				}
				// every column is named: a SET expression that is a literal or an expression takes its
				// name from the expression otherwise, and the rows table is joined on its keys -
				// measured, `SET k = 5` produced a column called "5" and the join could not find the key
				new_columns += value.empty() ? KeywordHelper::WriteOptionallyQuoted(name)
				                             : (value + " AS " + KeywordHelper::WriteOptionallyQuoted(name));
			}
			string result =
			    StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM %s%s;\n", keys_name, key_list,
			                       at_clause, condition.empty() ? string() : " WHERE " + condition);
			result += StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM %s%s;\n", rows_name,
			                             new_columns, at_clause, condition.empty() ? string() : " WHERE " + condition);
			result +=
			    StringUtil::Format("DELETE FROM %s %s WHERE EXISTS (SELECT 1 FROM %s %s WHERE %s);\n", quoted_added,
			                       alias_added, keys_name, alias_keys, match(alias_added, alias_keys));
			result += add_removals(keys_name, alias_keys);
			// A key the statement rewrites onto a key the branch already shows is the collision the same
			// statement against the table itself raises, and here it was a silent wrong answer: measured,
			// `UPDATE t SET k = 5 WHERE k = 3` with both keys present left the branch showing *two* rows
			// for k = 5, where the table itself refuses with a duplicate key. Keys the statement is
			// replacing are not collisions - a row keeping its key, or two rows swapping theirs, is an
			// ordinary update - so the check asks whether a new key is one the branch shows and the
			// statement is not rewriting.
			result += StringUtil::Format(
			    "INSERT INTO %s SELECT * FROM %s %s WHERE (SELECT CASE WHEN EXISTS (SELECT 1 FROM %s JOIN %s %s ON "
			    "(%s) WHERE NOT EXISTS (SELECT 1 FROM %s %s WHERE %s)) THEN error('update collides with a row the "
			    "branch already shows: two rows would carry the same key, which the table itself refuses. Delete "
			    "the row that key belongs to, or rewrite it to a key the branch does not have') ELSE 1 END) = 1;\n",
			    quoted_added, rows_name, alias_rows, branch_image, rows_name, alias_rows,
			    match(alias_branch, alias_rows), keys_name, alias_keys, match(alias_keys, alias_branch));
			result += StringUtil::Format("DROP TABLE %s;\nDROP TABLE %s;", keys_name, rows_name);
			return result;
		}
		case StatementType::INSERT_STATEMENT: {
			auto &node = statement.Cast<InsertStatement>().node;
			if (node->on_conflict_info) {
				throw InvalidInputException("git4data_apply does not take ON CONFLICT: a branch that keeps its own "
				                            "changes has no conflict for the base to resolve");
			}
			if (!node->returning_list.empty()) {
				throw InvalidInputException("git4data_apply does not take RETURNING: nothing is written where the rows "
				                            "would come from");
			}
			if (node->default_values) {
				throw InvalidInputException("git4data_apply does not take DEFAULT VALUES: the defaults are the source "
				                            "table's, and the table this branch adds rows to is not it");
			}
			if (node->column_order != InsertColumnOrder::INSERT_BY_POSITION) {
				throw InvalidInputException("git4data_apply does not take INSERT ... BY NAME");
			}
			if (!node->select_statement) {
				throw InvalidInputException("git4data_apply needs rows to insert");
			}
			// the rows are *for* the table the delta is for, and the statement is translated rather than
			// executed, so the name it writes is never read again. The target is a qualified name here; the
			// insert's table reference is not always there.
			Git4DataCheckApplyTargetTable(context, node->qualified_name, table);
			// the column list the insert names, or the table's own columns in order when it names none
			vector<string> insert_columns;
			for (auto &column : node->columns) {
				insert_columns.push_back(column.GetIdentifierName());
			}
			if (insert_columns.empty()) {
				for (auto &column : table.GetColumns().Logical()) {
					insert_columns.push_back(column.Name().GetIdentifierName());
				}
			}
			string insert_list;
			for (idx_t i = 0; i < insert_columns.size(); i++) {
				if (i > 0) {
					insert_list += ", ";
				}
				insert_list += KeywordHelper::WriteOptionallyQuoted(insert_columns[i]);
			}
			// The rows are landed in a table whose columns are the ones the insert names, in that order, so
			// the rows can be checked and then added by position. Naming them here rather than aliasing the
			// select avoids depending on what the parser calls the columns of a VALUES list - it calls them
			// `valueslist`, measured.
			string declared;
			for (auto &name : insert_columns) {
				auto &column = table.GetColumns().GetColumn(Identifier(name));
				if (!declared.empty()) {
					declared += ", ";
				}
				declared += KeywordHelper::WriteOptionallyQuoted(name) + " " + column.Type().ToString();
			}
			string result = StringUtil::Format("CREATE OR REPLACE TEMP TABLE %s (%s);\n", rows_name, declared);
			result += StringUtil::Format("INSERT INTO %s (%s) %s;\n", rows_name, insert_list,
			                             node->select_statement->ToString());
			result += StringUtil::Format(
			    "INSERT INTO %s (%s) SELECT * FROM %s WHERE (SELECT CASE WHEN EXISTS (SELECT 1 FROM %s JOIN %s %s ON "
			    "(%s)) THEN error('insert collides with a row the branch already shows: use an update, which a delta "
			    "can record, or delete the row first') ELSE 1 END) = 1;\n",
			    quoted_added, insert_list, rows_name, branch_image, rows_name, alias_rows,
			    match(alias_branch, alias_rows));
			result += StringUtil::Format("DROP TABLE %s;", rows_name);
			return result;
		}
		default:
			throw InvalidInputException("git4data_apply takes INSERT, UPDATE and DELETE statements, and this one is "
			                            "not one of them");
		}
	};
	string result;
	for (auto &statement : parser.statements) {
		result += translate(*statement);
		result += "\n";
	}
	return result;
}

//! The columns of one image compared against the other's, for telling a real merge conflict from two
//! branches that made the same change.
static string BuildGit4DataRowComparison(TableCatalogEntry &table, const string &left_alias,
                                         const string &right_alias) {
	string result;
	for (auto &column : table.GetColumns().Logical()) {
		if (!result.empty()) {
			result += " AND ";
		}
		auto name = KeywordHelper::WriteOptionallyQuoted(column.Name().GetIdentifierName());
		result += StringUtil::Format("%s.%s IS NOT DISTINCT FROM %s.%s", left_alias, name, right_alias, name);
	}
	return result;
}

//! The snapshot a merge between these two lines should use as its base, when one of them was forked from a
//! *captured line*. Such a branch's parent is a snapshot rather than a branch, so the branch-ancestor walk
//! stops at it and a merge that has to find an ancestor between it and the line's branch refuses: measured,
//! `Branches "C" and "B" share no ancestor to merge against`. The state the two parted at is exactly what that
//! snapshot holds - it captured the line at the commit the fork started from - and a diff takes a snapshot as
//! readily as a branch, so its name is the base.
//!
//! The snapshot is not an *ancestor* of the line's branch: it was taken from it, and the branch has moved on
//! since. So the question is not what both sides descend from but what they parted at: the snapshot this side
//! descends from, whose captured line is on the other side's branch chain.
static string Git4DataSharedSnapshotAncestor(DuckTransactionManager &transaction_manager, const string &a,
                                             const string &b) {
	auto all = transaction_manager.branches.List();
	auto parent_of = [&](const string &name) {
		for (auto &branch : all) {
			if (branch.name == name) {
				return branch.parent;
			}
		}
		return string();
	};
	// the branches a line descends from, nearest first: the walk ends where the parent is not a branch
	auto branch_chain = [&](const string &name) {
		vector<string> chain;
		auto current = name;
		for (idx_t depth = 0; !current.empty() && depth <= all.size(); depth++) {
			auto parent = parent_of(current);
			if (parent.empty() && current != name) {
				break;
			}
			chain.push_back(current);
			Git4DataSnapshot parent_snapshot;
			if (!parent.empty() && transaction_manager.snapshots.Get(parent, parent_snapshot)) {
				break;
			}
			current = parent;
		}
		return chain;
	};
	// the nearest snapshot a line descends from, and the line it captured: a fork from a captured line is the
	// only way a snapshot gets into a parent chain
	auto captured = [&](const string &name, string &snapshot_name, string &line) {
		auto current = name;
		for (idx_t depth = 0; !current.empty() && depth <= all.size(); depth++) {
			Git4DataSnapshot snapshot;
			if (transaction_manager.snapshots.Get(current, snapshot)) {
				snapshot_name = current;
				line = snapshot.line;
				return;
			}
			current = parent_of(current);
		}
	};
	auto chain_a = branch_chain(a);
	auto chain_b = branch_chain(b);
	for (idx_t side = 0; side < 2; side++) {
		string snapshot_name;
		string line;
		captured(side == 0 ? a : b, snapshot_name, line);
		auto &other = side == 0 ? chain_b : chain_a;
		if (snapshot_name.empty() || line.empty()) {
			continue;
		}
		if (std::find(other.begin(), other.end(), line) != other.end()) {
			return snapshot_name;
		}
	}
	return string();
}

//! Merge one branch into another, three-way, with their least common ancestor as the base. The left
//! branch is where the result lands, so it has to have storage of its own to be written to.
//!
//! A key is in conflict when both sides changed it relative to the ancestor. Two sides that ended up
//! holding the same row have not conflicted, which is what the row comparison is for - without it,
//! both branches adding the same row would be reported as a conflict and fail a policy of fail.
static string PragmaGit4DataMerge(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto left_name = parameters.values[1].ToString();
	auto right_name = parameters.values[2].ToString();
	auto policy = StringUtil::Lower(parameters.values[3].ToString());
	if (policy != "fail" && policy != "skip" && policy != "accept") {
		throw InvalidInputException("Merge policy has to be fail, skip or accept, not \"%s\"", policy);
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	Git4DataBranch left;
	if (!Git4DataReferenceVisible(context, "branch", left_name) ||
	    !transaction_manager.branches.Get(left_name, left)) {
		throw InvalidInputException("Branch \"%s\" does not exist", left_name);
	}
	Git4DataBranch right;
	if (!Git4DataReferenceVisible(context, "branch", right_name) ||
	    !transaction_manager.branches.Get(right_name, right)) {
		throw InvalidInputException("Branch \"%s\" does not exist", right_name);
	}
	string merge_into;
	auto &merge_source = Git4DataResolveTable(context, table_name);
	auto merge_key = Git4DataSourceKey(merge_source);
	for (auto &entry : left.storage_tables) {
		if (Git4DataSourceKeyMatches(context, entry.source, merge_key) ||
		    (entry.source_oid != 0 && entry.source_oid == merge_source.oid)) {
			// the table the branch was given can have been renamed, and the merge writes into it
			merge_into = Git4DataResolveTableName(context, entry.storage, entry.oid);
		}
	}
	// A branch does not have to have a table of its own to be merged into: one that differs by a delta has
	// somewhere to record the change too - the same two tables git4data_apply records in - so the landing
	// statements below are written as delta rows instead of as changes to a table, and the "live" side of the
	// conflict rule is read as the branch's own view rather than as a table.
	optional_ptr<const Git4DataBranchDelta> merge_delta;
	if (merge_into.empty()) {
		merge_delta = Git4DataFindDeltaTable(context, left, merge_key, merge_source.oid);
		if (merge_delta) {
			// The merge lands in this mapping, so it has to be the one the target was given. This is load-bearing
			// and not covered by the read path: measured, with it removed the refusal comes from the merge's own
			// ancestor lookup instead ("Branches ... share no ancestor to merge against"), which is a different
			// sentence about a different problem - the read of the target never happens.
			Git4DataCheckSourceBoundTable(context, left, merge_delta->source, merge_delta->source_oid,
			                              merge_source.oid);
		}
		if (!merge_delta) {
			throw InvalidInputException("Branch \"%s\" has neither a table of its own nor a delta for table "
			                            "\"%s\", so a merge has nowhere to land - materialize it, or give it a "
			                            "delta with git4data_bind_branch_delta",
			                            left_name, table_name);
		}
	}
	string ancestor;
	if (!transaction_manager.branches.FindLCA(left_name, right_name, ancestor)) {
		// A child forked from a captured line has no *branch* ancestor, so what it parted from is the snapshot
		// it was forked from - and that snapshot is a reference the diffs below take, exactly as an ancestor's
		// name is. Naming it here is what makes such a line mergeable.
		ancestor = Git4DataSharedSnapshotAncestor(transaction_manager, left_name, right_name);
		if (ancestor.empty()) {
			throw InvalidInputException("Branches \"%s\" and \"%s\" share no ancestor to merge against", left_name,
			                            right_name);
		}
	}

	// The base a three-way merge needs is the state the two lines parted at. An ancestor without a table
	// of its own already reads as that state: its name resolves to its pinned image. An ancestor with one
	// would read as the table instead, which is the pinned image plus everything it has done since, and a
	// merge based on that applies the ancestor's own changes backwards - measured, silently. So a branch
	// with storage is asked the questions below, which end in naming its pin directly rather than its
	// name. Materializing is a single INSERT .. SELECT, which is what makes the first question answerable:
	// every row of such a table carries one insert id and none of them is deleted.
	Git4DataBranch ancestor_branch;
	// A diff reads the base as the branch name; only the case below has to name a commit instead
	string base_reference = ancestor;
	bool drop_merge_base = false;
	if (transaction_manager.branches.Get(ancestor, ancestor_branch)) {
		// Naming the ancestor's pin as the base, which is what the cases below end in. The line it inherited
		// goes with the pin: a branch forked from a captured line reads *that* line, so its pin alone names
		// the source table at that commit - the state it parted at is that line as it stood then, which is
		// what the reference below has to name. Measured: without the line, a merge whose ancestor had a copy
		// of its own read the source's values as the base and reported a conflict on a row only one side had
		// touched.
		auto name_pin_as_base = [&]() {
			transaction_manager.snapshots.Drop(DuckTransactionManager::MERGE_BASE_REFERENCE);
			if (!transaction_manager.snapshots.Create(DuckTransactionManager::MERGE_BASE_REFERENCE, ancestor_branch.pin,
			                                          Timestamp::GetCurrentTimestamp(), ancestor_branch.base_line)) {
				throw InternalException("Could not create the internal merge base reference");
			}
			context.client_data->git4data_added_records.push_back("snapshot\t" +
			                                                      string(DuckTransactionManager::MERGE_BASE_REFERENCE));
			base_reference = DuckTransactionManager::MERGE_BASE_REFERENCE;
			drop_merge_base = true;
		};
		// The state an ancestor's copy was written from is its own line, read at the commit the two sides
		// forked at: that is what each of them saw, and it is not the source's pinned image, because the
		// ancestor's own changes were already part of its line when they forked. Measured: an ancestor whose
		// copy held 111 for a row, with two sides that both read 111 and one of them changing it to 222, was
		// merged against the source's 1 - so both sides looked changed and the merge was refused as a conflict
		// although only one had touched the row. The commit is the one the *merge target* parted at, which is
		// the fork's own pin: `fork_state_merge.test` pins that choice, and it is what makes the merge bring
		// over what the ancestor changed after the target forked (222 in that test) as the other side's change
		// rather than as something the target already had.
		// The commit the two sides parted at: the fork pin of whichever of them is not the ancestor. When one of
		// the two *is* the ancestor - merging a child back into its parent - the ancestor's own pin is where the
		// *ancestor* started, not where they parted, so the other side's pin is the one that names the state.
		auto fork_commit = [&]() {
			transaction_t fork = 0;
			Git4DataBranch fork_branch;
			for (auto &side : {left_name, right_name}) {
				if (side == ancestor) {
					continue;
				}
				if (transaction_manager.branches.Get(side, fork_branch)) {
					fork = fork_branch.pin;
					break;
				}
			}
			return fork;
		};
		// Naming the base as the ancestor's own *line* at that commit, which is what carries the ancestor's own
		// change: a delta lives in two tables and a copy is a table of its own, and neither is pointed at by a
		// commit. Naming it as the line it *inherited* instead, against the ancestor's own pin, reads the raw
		// source - measured below, in both shapes.
		auto name_line_as_base = [&]() {
			auto fork = fork_commit();
			if (fork == 0) {
				// Both sides are the ancestor (a branch merged with itself): there is no fork to name, and the
				// ancestor's own pin is what is left.
				fork = ancestor_branch.pin;
			}
			transaction_manager.snapshots.Drop(DuckTransactionManager::MERGE_BASE_REFERENCE);
			if (!transaction_manager.snapshots.Create(DuckTransactionManager::MERGE_BASE_REFERENCE, fork,
			                                          Timestamp::GetCurrentTimestamp(), ancestor)) {
				throw InternalException("Could not create the internal merge base reference");
			}
			context.client_data->git4data_added_records.push_back("snapshot\t" +
			                                                      string(DuckTransactionManager::MERGE_BASE_REFERENCE));
			base_reference = DuckTransactionManager::MERGE_BASE_REFERENCE;
			drop_merge_base = true;
		};
		// Which of the two is the state the two sides parted at depends on when they forked, and the pin says
		// which: a fork from a parent that has a line of its own pins the commit it forked at, so a merge
		// target whose pin is newer than the ancestor's own parted *inside* that line. Both the ancestor's own
		// copy below and its own delta go through this, because both are its line: a delta is a change it made
		// to the source, and naming the pin instead reads the source without it. **Measured, and corrected in
		// increment 3bp (`fb4e40e8a5`)**: the fork commit is the one the side that is *not* the ancestor forked
		// at, so the question is whether that side's pin is newer than the ancestor's - asking it of the merge
		// target alone sent "merge a child into its parent" down the pin path, where a parent whose line is a
		// delta read as the raw source and one whose line is a copy that was written to read as the source's
		// values, losing the parent's own change from the base.
		auto name_the_parting_state = [&]() {
			if (fork_commit() > ancestor_branch.pin) {
				name_line_as_base();
			} else {
				name_pin_as_base();
			}
		};
		// A delta is the ancestor's own change to the source, so its name reads as its line *now*; whether its
		// two tables are empty yet is not asked, because naming the pin is right either way.
		// A delta is the ancestor's own change to the source, so its name reads as its line *now*; whether its
		// two tables are empty yet is not asked, because naming the pin is right either way.
		for (auto &delta_entry : ancestor_branch.deltas) {
			if (Git4DataSourceKeyMatches(context, delta_entry.source, merge_key) ||
			    (delta_entry.source_oid != 0 && delta_entry.source_oid == merge_source.oid)) {
				// A delta is the ancestor's own change to the source and lives in two tables, which no pin
				// points at, so the base has to name the ancestor's *line* at the commit the sides parted at -
				// not the ancestor's pin against the line it inherited. Measured: with that pair the base was
				// the raw source at the ancestor's own start commit, so the ancestor's own additions and
				// removals came back as the other side's changes - a merge into it reported "removed 3" (a key
				// the ancestor itself had removed) and never saw the key the other side had really removed,
				// which then stayed in the target while the merge reported success.
				name_line_as_base();
			}
		}
		for (auto &storage_entry : ancestor_branch.storage_tables) {
			if (!Git4DataSourceKeyMatches(context, storage_entry.source, merge_key) &&
			    (storage_entry.source_oid == 0 || storage_entry.source_oid != merge_source.oid)) {
				continue;
			}
			auto &storage_table = Git4DataResolveTable(context, storage_entry.storage);
			auto &storage_groups = *storage_table.GetStorage().GetRowGroupCollection();
			transaction_t copy_commit = 0;
			bool unwritten = true;
			// The rows the caller has appended to this copy and not committed are in its local storage,
			// and a copy with any of them has been written to. Measured: an uncommitted insert into an
			// ancestor's copy left the walk below saying it was still the copy it was given, so the merge
			// took the ancestor's current line as its base and both sides looked changed.
			auto &ancestor_transaction = DuckTransaction::Get(context, storage_table.GetStorage().GetAttached());
			if (LocalStorage::Get(ancestor_transaction).Find(storage_table.GetStorage())) {
				unwritten = false;
			}
			for (idx_t i = 0; i < storage_groups.GetRowGroupCount(); i++) {
				auto row_group = storage_groups.GetRowGroup(NumericCast<int64_t>(i));
				if (!row_group) {
					continue;
				}
				transaction_t insertion = 0;
				if (!row_group->OneUnchangedInsertion(insertion)) {
					unwritten = false;
					break;
				}
				if (i == 0) {
					copy_commit = insertion;
				} else if (insertion != copy_commit) {
					// written in more than one statement, so it is not the copy it was given
					unwritten = false;
					break;
				}
			}
			if (unwritten) {
				// One unchanged insertion cannot tell an empty copy from one that has had rows inserted
				// since, so ask the source instead: a branch's table is made from the branch's pinned
				// image, so if the source table held no rows at that commit then the copy held none
				// either - and a table that holds rows now has been written to.
				idx_t rows_at_copy = 0;
				if (storage_groups.GetTotalRows() > 0 || storage_groups.GetRowGroupCount() > 0) {
					auto &source_table = Git4DataResolveTable(context, table_name);
					auto &source_groups = *source_table.GetStorage().GetRowGroupCollection();
					ScanOptions pinned(
					    TransactionData(MAX_TRANSACTION_ID, VisibilityBound::Through(ancestor_branch.pin)));
					pinned.insert_type = InsertedScanType::ALL_ROWS;
					pinned.delete_type = DeletedScanType::OMIT_COMMITTED_DELETES;
					for (idx_t i = 0; i < source_groups.GetRowGroupCount(); i++) {
						auto row_group = source_groups.GetRowGroup(NumericCast<int64_t>(i));
						if (row_group) {
							rows_at_copy += row_group->GetRowCountFor(pinned);
						}
					}
					if (rows_at_copy == 0) {
						unwritten = false;
					}
				}
			}
			if (!unwritten) {
				// Written to since: what the table holds now is the line this branch started from plus its own
				// changes, and reading *it* as the base would apply those changes backwards. Where the two
				// sides parted depends on when they forked: a side forked *after* the ancestor had a line of
				// its own parted at that line as it stood then, and a side forked before it parted at the
				// ancestor's pinned image, which is what naming the pin reads. `merge_lineage.test` and
				// `storage_writes.test` are the second case (their sides were forked before the ancestor was
				// given a copy) and `fork_state_merge.test` the first; a pin newer than the ancestor's own is
				// what tells them apart, because a fork from a parent with a line pins the commit it forked at.
				name_the_parting_state();
			}
		}
	}

	auto &table = Git4DataResolveTable(context, table_name);
	auto key_columns = Git4DataPrimaryKeyColumnNames(table);
	if (key_columns.empty()) {
		throw InvalidInputException("Table \"%s\" has no primary key, so a merge cannot tell which rows are "
		                            "the same row",
		                            table_name);
	}
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	// The statement reads the source table, the table being merged into and the right side's image, so
	// no alias may be a column of any of them - a column named g4d_b made this ambiguous, measured.
	auto taken = Git4DataScopeColumnNames(context, transaction_manager, table_name, {right_name});
	Git4DataAddColumnNames(context, merge_into, taken);
	auto alias_left = Git4DataFreeName("g4d_l", taken);
	auto alias_right = Git4DataFreeName("g4d_r", taken);
	auto alias_both = Git4DataFreeName("g4d_both", taken);
	auto alias_both_rows = Git4DataFreeName("g4d_b", taken);
	auto alias_x = Git4DataFreeName("g4d_x", taken);
	auto alias_y = Git4DataFreeName("g4d_y", taken);
	auto conflict_name = Git4DataFreeHelperName(context, "g4d_merge_conflicts", taken);
	string key_list;
	string key_list_both;
	string key_match_xy;
	string key_match_by;
	string key_match_x_by;
	for (idx_t i = 0; i < key_columns.size(); i++) {
		auto quoted = KeywordHelper::WriteOptionallyQuoted(key_columns[i]);
		if (i > 0) {
			key_list += ", ";
			key_list_both += ", ";
			key_match_xy += " AND ";
			key_match_by += " AND ";
			key_match_x_by += " AND ";
		}
		key_list += quoted;
		key_list_both += alias_both_rows + "." + quoted;
		key_match_xy += alias_x + "." + quoted + " = " + alias_y + "." + quoted;
		key_match_by += alias_y + "." + quoted + " = " + alias_both_rows + "." + quoted;
		key_match_x_by += alias_x + "." + quoted + " = " + alias_both_rows + "." + quoted;
	}
	auto quoted_table = Git4DataTableSQLName(table_name);
	auto quoted_into = Git4DataTableSQLName(merge_into);
	auto read_target = merge_into.empty() ? FormatGit4DataAtClause(table_name, left_name, "BRANCH") : quoted_into;
	auto diff_left = StringUtil::Format("git4data_diff('%s', '%s', '%s')", escape(table_name), escape(base_reference),
	                                    escape(left_name));
	auto diff_right = StringUtil::Format("git4data_diff('%s', '%s', '%s')", escape(table_name), escape(base_reference),
	                                     escape(right_name));
	auto right_image =
	    FormatGit4DataAtClause(table_name, right_name, ResolveGit4DataReference(transaction_manager, right_name));

	string result = StringUtil::Format(
	    "CREATE OR REPLACE TEMP TABLE %s AS WITH %s AS (SELECT %s FROM %s), "
	    "%s AS (SELECT %s FROM %s), %s AS (SELECT %s FROM %s INTERSECT SELECT %s FROM "
	    "%s) SELECT %s FROM %s %s WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM %s) "
	    "%s JOIN (SELECT * FROM %s) %s ON (%s) WHERE %s AND (%s)) "
	    // A key that both sides *removed* is agreement, not a conflict: the comparison above cannot say so,
	    // because there is no row on either side to compare, and its NOT EXISTS is then true - measured, two
	    // sides that had deleted the same row were refused as a conflict although nothing was left to
	    // disagree about. A key both sides changed is only a conflict if a row survives on one of them.
	    "AND (EXISTS (SELECT 1 FROM (SELECT * FROM %s) %s WHERE %s) OR EXISTS (SELECT 1 FROM (SELECT * FROM %s) "
	    "%s WHERE %s));\n",
	    conflict_name, alias_left, key_list, diff_left, alias_right, key_list, diff_right, alias_both, key_list,
	    alias_left, key_list, alias_right, key_list_both, alias_both, alias_both_rows, read_target, alias_x,
	    right_image, alias_y, key_match_xy, key_match_by, BuildGit4DataRowComparison(table, alias_x, alias_y),
	    right_image, alias_x, key_match_x_by, read_target, alias_y, key_match_by);
	if (policy == "fail") {
		result += StringUtil::Format("SELECT CASE WHEN EXISTS (SELECT 1 FROM %s) THEN error('merge conflict: use "
		                             "policy skip or accept, or resolve it first') ELSE 0 END;\n",
		                             conflict_name);
	}
	// with accept the right side wins outright; otherwise a conflicting key is left as the left has it
	auto conflict_filter = policy == "accept" ? string()
	                                          : StringUtil::Format(" AND (%s) NOT IN (SELECT %s FROM %s)", key_list,
	                                                               key_list, conflict_name);
	// Accepting means the right side's row replaces whatever the left has for that key, including when
	// the right side added it and the left also has one - so every key the right touched is cleared
	// first, not only the ones it removed or changed.
	auto changed_filter = policy == "accept" ? "'added', 'removed', 'changed'" : "'removed', 'changed'";
	auto dropped_keys = StringUtil::Format("SELECT %s FROM %s WHERE g4d_change IN (%s)%s", key_list, diff_right,
	                                        changed_filter, conflict_filter);
	auto added_keys = StringUtil::Format(
	    "SELECT %s FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change IN ('added', 'changed')%s) AND (%s) "
	    "NOT IN (SELECT %s FROM %s)",
	    key_list, right_image, key_list, key_list, diff_right, conflict_filter, key_list, key_list, read_target);
	auto added_rows =
	    StringUtil::Format("SELECT * FROM %s WHERE (%s) IN (%s)", right_image, key_list, added_keys);
	if (merge_delta) {
		// the delta is where the change is recorded: a key the merge removes loses the branch's own override
		// and is recorded removed, and a row it adds is recorded added and loses any removal - the two
		// statements git4data_apply renders for a delete and an insert, against the same two tables
		auto delta_added = Git4DataResolveTableName(context, merge_delta->added, merge_delta->added_oid);
		auto delta_removed = Git4DataResolveTableName(context, merge_delta->removed, merge_delta->removed_oid);
		if (delta_added.empty() || delta_removed.empty()) {
			throw InvalidInputException("The delta branch \"%s\" differs by is gone: \"%s\" or \"%s\" is not "
			                            "there any more",
			                            left_name, merge_delta->added, merge_delta->removed);
		}
		auto quoted_added = Git4DataTableSQLName(delta_added);
		auto quoted_removed = Git4DataTableSQLName(delta_removed);
		result += StringUtil::Format("DELETE FROM %s WHERE (%s) IN (%s);\n", quoted_added, key_list, dropped_keys);
		result += StringUtil::Format("INSERT INTO %s (%s) SELECT %s FROM (%s) g4d_dk WHERE (%s) NOT IN (SELECT %s FROM "
		                             "%s) AND (%s) IN (SELECT %s FROM %s);\n",
		                             quoted_removed, key_list, key_list, dropped_keys, key_list, key_list,
		                             quoted_removed, key_list, key_list, read_target);
		result += StringUtil::Format("INSERT INTO %s %s;\n", quoted_added, added_rows);
		result += StringUtil::Format("DELETE FROM %s WHERE (%s) IN (%s);\n", quoted_removed, key_list, added_keys);
	} else {
		result += StringUtil::Format("DELETE FROM %s WHERE (%s) IN (%s);\n", quoted_into, key_list, dropped_keys);
		result += StringUtil::Format(
		    "INSERT INTO %s SELECT * FROM %s WHERE (%s) IN (SELECT %s FROM %s WHERE g4d_change IN ('added', "
		    "'changed')%s) AND (%s) NOT IN (SELECT %s FROM %s);\n",
		    quoted_into, right_image, key_list, key_list, diff_right, conflict_filter, key_list, key_list, quoted_into);
	}
	result += StringUtil::Format("DROP TABLE %s;", conflict_name);

	if (drop_merge_base) {
		// The reference was only there for the two diffs above, which have run by now, so it goes. Its row
		// goes with plain DML and the in-memory entry with the pragma below rather than through
		// git4data_drop_snapshot: that one *returns* the row's DELETE now, and SQL a pragma returns is
		// executed as statements, where a statement-returning pragma cannot be called - measured, and the
		// merge refused with "PRAGMA function does not have a function specified" until this was written
		// this way.
		if (Git4DataRegistryTablesThere(context)) {
			result += StringUtil::Format("\nDELETE FROM %s.%s WHERE name = '%s';", GIT4DATA_REGISTRY_SCHEMA,
			                             GIT4DATA_SNAPSHOTS_TABLE, DuckTransactionManager::MERGE_BASE_REFERENCE);
		}
		result += StringUtil::Format("\nPRAGMA git4data_forget_internal('%s');",
		                             DuckTransactionManager::MERGE_BASE_REFERENCE);
	}
	return result;
}

//! Refuse one more reference when the database is at its limit. An exceeded limit stops new
//! references without dropping any: what exists was paid for already.
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
static void PragmaGit4DataLimit(ClientContext &context, const FunctionParameters &parameters) {
	auto kind = StringUtil::Lower(parameters.values[0].ToString());
	auto limit = parameters.values[1].GetValue<int64_t>();
	if (limit < -1) {
		throw InvalidInputException("A limit is -1 for unlimited, 0 to disable, or a positive number");
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	if (kind == "snapshot") {
		transaction_manager.snapshot_limit = limit;
	} else if (kind == "branch") {
		transaction_manager.branch_limit = limit;
	} else {
		throw InvalidInputException("Limits are set for \"snapshot\" or \"branch\", not \"%s\"", kind);
	}
	// the quota is written in the same block as the registry, and a checkpoint skips its body when the log is
	// empty: without this, setting a quota and checkpointing in a session that changed nothing else lost it -
	// measured, limits.test read -1 for a limit of 3 after a restart
	transaction_manager.snapshots.MarkDirty();
	transaction_manager.branches.MarkDirty();
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
	// before the branch joins the registry in memory: the migration into the tables reads that registry
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
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	context.client_data->git4data_added_records.push_back("branch\t" + name);
	result += StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', %llu, '%s', TIMESTAMP '%s', '%s');",
	                             GIT4DATA_REGISTRY_SCHEMA, GIT4DATA_BRANCHES_TABLE, escape(name), pin, escape(parent),
	                             Timestamp::ToString(created), escape(base_line));
	return result;
}

//! Drop a branch and the storage it was materialized into: that table is the branch's own copy, so
//! leaving it behind would leak a table per branch ever dropped.
static string PragmaGit4DataDropBranch(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	Git4DataBranch existing;
	if (!Git4DataReferenceVisible(context, "branch", name) ||
	    !transaction_manager.branches.Get(name, existing)) {
		throw InvalidInputException("Branch \"%s\" does not exist", name);
	}
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
		// dropping the branch takes the tables it was given with it, and one of them may have been renamed
		// since - dropping the name it was given instead would leave the table behind, silently
		auto resolved = Git4DataResolveTableName(context, entry.storage, entry.oid);
		if (resolved.empty()) {
			continue;
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
		auto escaped = StringUtil::Replace(name, "'", "''");
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
	Git4DataBranch existing;
	if (!Git4DataReferenceVisible(context, "branch", left) ||
	    !transaction_manager.branches.Get(left, existing)) {
		throw InvalidInputException("Branch \"%s\" does not exist", left);
	}
	if (!Git4DataReferenceVisible(context, "branch", right) ||
	    !transaction_manager.branches.Get(right, existing)) {
		throw InvalidInputException("Branch \"%s\" does not exist", right);
	}
	string lca;
	if (!transaction_manager.branches.FindLCA(left, right, lca)) {
		// the two share no ancestor, which is what a broken lineage looks like
		return "SELECT NULL::VARCHAR AS lca WHERE false";
	}
	lca = StringUtil::Replace(lca, "'", "''");
	return StringUtil::Format("SELECT '%s'::VARCHAR AS lca", lca);
}

static string PragmaGit4DataCreateSnapshot(ClientContext &context, const FunctionParameters &parameters) {
	auto name = parameters.values[0].ToString();
	CheckGit4DataReferenceName(name);
	auto &transaction_manager = GetGit4DataTransactionManager(context);
	// The tables are created, and what the file already holds migrated into them, *before* the new reference
	// joins the in-memory registry: the migration reads that registry, so doing it after would write the row
	// this call is about twice - measured, a clone of a database with one snapshot had two rows for it.
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
		Git4DataBranch branch;
		if (!Git4DataReferenceVisible(context, "branch", line) ||
		    !transaction_manager.branches.Get(line, branch)) {
			throw InvalidInputException("Branch \"%s\" does not exist", line);
		}
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
	auto escape = [](const string &value) {
		return StringUtil::Replace(value, "'", "''");
	};
	context.client_data->git4data_added_records.push_back("snapshot\t" + name);
	result +=
	    StringUtil::Format("INSERT INTO %s.%s VALUES ('%s', %llu, TIMESTAMP '%s', '%s');", GIT4DATA_REGISTRY_SCHEMA,
		                   GIT4DATA_SNAPSHOTS_TABLE, escape(name), pin, Timestamp::ToString(created), escape(line));
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
	auto escaped = StringUtil::Replace(name, "'", "''");
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
	                          StringUtil::Replace(parameters.values[0].ToString(), "'", "''"));
}

//! Every physical row including all deleted ones, to tell "the data is gone" apart from
//! "the data is there but filtered out"
static string PragmaGit4DataProbeAll(ClientContext &context, const FunctionParameters &parameters) {
	// the row comes from the table function, which is where its numbers can be selected
	return StringUtil::Format("SELECT * FROM git4data_probe_all('%s')",
	                          StringUtil::Replace(parameters.values[0].ToString(), "'", "''"));
}

//! Base data with updates skipped, read at an explicitly given visibility bound. Sweeping
//! the bound reveals the effective delete and insert ids: the count changes where the bound
//! crosses an id, which is what tells the two runners apart.
static string PragmaGit4DataProbeAt(ClientContext &context, const FunctionParameters &parameters) {
	return StringUtil::Format("SELECT * FROM git4data_probe_at('%s', %lld)",
	                          StringUtil::Replace(parameters.values[0].ToString(), "'", "''"),
	                          (long long)parameters.values[1].GetValue<int64_t>());
}

void PragmaFunctions::RegisterFunction(BuiltinFunctions &set) {
	RegisterEnableProfiling(set);

	// NOTE: use of Pragma functions is discouraged. Instead, opt for adding a regular table function to be invoked with
	// CALL.
	//       see for example the "enable_logging" function

	set.AddFunction(PragmaFunction::PragmaStatement("disable_profile", PragmaDisableProfiling));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_profiling", PragmaDisableProfiling));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_verification", PragmaEnableVerification));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verification", PragmaDisableVerification));

	set.AddFunction(PragmaFunction::PragmaStatement("verify_parallelism", PragmaEnableForceParallelism));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verify_parallelism", PragmaDisableForceParallelism));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_object_cache", PragmaEnableObjectCache));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_object_cache", PragmaDisableObjectCache));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_optimizer", PragmaEnableOptimizer));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_optimizer", PragmaDisableOptimizer));

	set.AddFunction(PragmaFunction::PragmaStatement("force_checkpoint", PragmaForceCheckpoint));

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

	set.AddFunction(PragmaFunction::PragmaStatement("enable_progress_bar", PragmaEnableProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_progress_bar", PragmaDisableProgressBar));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_print_progress_bar", PragmaEnablePrintProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_print_progress_bar", PragmaDisablePrintProgressBar));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_checkpoint_on_shutdown", PragmaEnableCheckpointOnShutdown));
	set.AddFunction(
	    PragmaFunction::PragmaStatement("disable_checkpoint_on_shutdown", PragmaDisableCheckpointOnShutdown));
}

} // namespace duckdb
