//===----------------------------------------------------------------------===//
// Git4Data -- `git4data_merge`: the three-way merge and the base it reads
//===----------------------------------------------------------------------===//
// The pragma, the comparison it builds rows with, and the helpers that decide which state the two sides parted
// at - `Git4DataCreateMergeBase`, `Git4DataMergeForkCommit`, `Git4DataSharedSnapshotAncestor`, and the
// predicate that asks whether an ancestor's copy is still the copy it was given.
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

#include "duckdb/function/pragma/git4data_pragmas.hpp"

namespace duckdb {



//! The columns of one image compared against the other's, for telling a real merge conflict from two
//! branches that made the same change.
string BuildGit4DataRowComparison(TableCatalogEntry &table, const string &left_alias,
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
//! The internal reference a three-way merge names its base with: the source as of `commit`, read as the line
//! `line`. It replaces any reference of the same name, because a merge drops its own when it is done and a stale
//! one would answer the next merge with the state of the one before. Both ways of naming a base go through here
//! and differ only in what they pass - which is the point: this was ten lines written twice inside the merge.
static string Git4DataCreateMergeBase(ClientContext &context, DuckTransactionManager &transaction_manager,
                                      transaction_t commit, const string &line) {
	transaction_manager.snapshots.Drop(DuckTransactionManager::MERGE_BASE_REFERENCE);
	if (!transaction_manager.snapshots.Create(DuckTransactionManager::MERGE_BASE_REFERENCE, commit,
	                                          Timestamp::GetCurrentTimestamp(), line)) {
		throw InternalException("Could not create the internal merge base reference");
	}
	context.client_data->git4data_added_records.push_back("snapshot\t" +
	                                                      string(DuckTransactionManager::MERGE_BASE_REFERENCE));
	return DuckTransactionManager::MERGE_BASE_REFERENCE;
}

//! The commit the two sides parted at: the fork pin of whichever of them is not the ancestor. When one of the two
//! *is* the ancestor - merging a child back into its parent - the ancestor's own pin is where the *ancestor*
//! started rather than where they parted, so the other side's pin is what names the state. The measured account
//! of what that rule fixes is at the call site.
static transaction_t Git4DataMergeForkCommit(DuckTransactionManager &transaction_manager, const string &left_name,
                                             const string &right_name, const string &ancestor) {
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
}

//! Whether the ancestor's copy of the source is still the copy it was given. A copy is written by one
//! `INSERT .. SELECT`, so "untouched" is a shape the storage itself can be asked about: every row group carries
//! one unchanged insertion, and none of them was made by a later statement.
static bool Git4DataAncestorCopyUnwritten(ClientContext &context, const string &table_name,
                                          TableCatalogEntry &storage_table, transaction_t ancestor_pin) {
	auto &storage_groups = *storage_table.GetStorage().GetRowGroupCollection();
	// The rows the caller has appended to this copy and not committed are in its local storage, and a copy with
	// any of them has been written to. Measured: an uncommitted insert into an ancestor's copy left the walk
	// below saying it was still the copy it was given, so the merge took the ancestor's current line as its base
	// and both sides looked changed.
	auto &ancestor_transaction = DuckTransaction::Get(context, storage_table.GetStorage().GetAttached());
	if (LocalStorage::Get(ancestor_transaction).Find(storage_table.GetStorage())) {
		return false;
	}
	transaction_t copy_commit = 0;
	for (idx_t i = 0; i < storage_groups.GetRowGroupCount(); i++) {
		auto row_group = storage_groups.GetRowGroup(NumericCast<int64_t>(i));
		if (!row_group) {
			continue;
		}
		transaction_t insertion = 0;
		if (!row_group->OneUnchangedInsertion(insertion)) {
			return false;
		}
		if (i == 0) {
			copy_commit = insertion;
		} else if (insertion != copy_commit) {
			// written in more than one statement, so it is not the copy it was given
			return false;
		}
	}
	// One unchanged insertion cannot tell an empty copy from one that has had rows inserted since, so ask the
	// source instead: a branch's table is made from the branch's pinned image, so if the source table held no
	// rows at that commit then the copy held none either - and a table that holds rows now has been written to.
	if (storage_groups.GetTotalRows() > 0 || storage_groups.GetRowGroupCount() > 0) {
		auto &source_table = Git4DataResolveTable(context, table_name);
		auto &source_groups = *source_table.GetStorage().GetRowGroupCollection();
		ScanOptions pinned(TransactionData(MAX_TRANSACTION_ID, VisibilityBound::Through(ancestor_pin)));
		pinned.insert_type = InsertedScanType::ALL_ROWS;
		pinned.delete_type = DeletedScanType::OMIT_COMMITTED_DELETES;
		idx_t rows_at_copy = 0;
		for (idx_t i = 0; i < source_groups.GetRowGroupCount(); i++) {
			auto row_group = source_groups.GetRowGroup(NumericCast<int64_t>(i));
			if (row_group) {
				rows_at_copy += row_group->GetRowCountFor(pinned);
			}
		}
		if (rows_at_copy == 0) {
			return false;
		}
	}
	return true;
}

string PragmaGit4DataMerge(ClientContext &context, const FunctionParameters &parameters) {
	auto table_name = parameters.values[0].ToString();
	auto left_name = parameters.values[1].ToString();
	auto right_name = parameters.values[2].ToString();
	auto policy = StringUtil::Lower(parameters.values[3].ToString());
	if (policy != "fail" && policy != "skip" && policy != "accept") {
		throw InvalidInputException("Merge policy has to be fail, skip or accept, not \"%s\"", policy);
	}
	auto &transaction_manager = GetGit4DataTransactionManager(context);

	auto left = Git4DataRequiredBranch(context, transaction_manager, left_name);
	auto right = Git4DataRequiredBranch(context, transaction_manager, right_name);
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
			base_reference = Git4DataCreateMergeBase(context, transaction_manager, ancestor_branch.pin,
			                                         ancestor_branch.base_line);
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
			return Git4DataMergeForkCommit(transaction_manager, left_name, right_name, ancestor);
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
			base_reference = Git4DataCreateMergeBase(context, transaction_manager, fork, ancestor);
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
			if (!Git4DataAncestorCopyUnwritten(context, table_name, storage_table, ancestor_branch.pin)) {
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
	auto diff_left = StringUtil::Format("git4data_diff('%s', '%s', '%s')", Git4DataSQLLiteral(table_name), Git4DataSQLLiteral(base_reference),
	                                    Git4DataSQLLiteral(left_name));
	auto diff_right = StringUtil::Format("git4data_diff('%s', '%s', '%s')", Git4DataSQLLiteral(table_name), Git4DataSQLLiteral(base_reference),
	                                     Git4DataSQLLiteral(right_name));
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
	auto added_rows = StringUtil::Format("SELECT * FROM %s WHERE (%s) IN (%s)", right_image, key_list, added_keys);
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

} // namespace duckdb
