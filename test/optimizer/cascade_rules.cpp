// Unit tests for the cascade rules, one case per rule.
//
// The migration plan (cascade-orca-notes/ORCA_RULE_MIGRATION_PLAN.md) asks for a unit test per
// ORCA rule, and these are the first ones: they pin down what each rule *is* - the name it
// reports, the kind that decides when the search may run it, and the shape it claims to match -
// which is exactly what breaks silently when a rule is renamed, retyped or pointed at the wrong
// operator. Tests of what a rule *produces* are added with each rule's transformation.
#include <set>

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/cascade/cascades/rules/apply_to_join.hpp"
#include "duckdb/cascade/cascades/rules/collapse_project.hpp"
#include "duckdb/cascade/cascades/rules/correlated_apply_to_join.hpp"
#include "duckdb/cascade/cascades/rules/expand_nary_join.hpp"
#include "duckdb/cascade/cascades/rules/group_apply_by_outer_columns.hpp"
#include "duckdb/cascade/cascades/rules/left_outer_apply_to_join.hpp"
#include "duckdb/cascade/cascades/rules/binder_side_invariants.hpp"
#include "duckdb/cascade/cascades/rules/select_2_filter.hpp"
#include "duckdb/cascade/cascades/rules/lift_local_predicate.hpp"
#include "duckdb/cascade/cascades/rules/push_filter_below_groupby.hpp"
#include "duckdb/cascade/cascades/rules/semi_apply_to_join.hpp"

using namespace duckdb;


namespace {

//! A three-input inner join whose every step has a predicate, plus the shapes the NAry rules need:
//! the child groups carry the bindings the conditions refer to, which is what the expansion reads to
//! place each predicate.
void BuildThreeInputJoin(CascadesOptimizer &optimizer, GroupId &target, vector<GroupId> &inputs) {
	auto &memo = optimizer.GetMemo();
	target = memo.AddGroup();
	for (idx_t i = 0; i < 3; i++) {
		auto group = memo.AddGroup();
		auto scan = make_uniq<LogicalDummyScan>(TableIndex(200 + i));
		optimizer.AddExpression(group, memo.MakeExpr(std::move(scan), {}));
		memo.GetGroup(group).exprs[0]->bindings = {ColumnBinding(TableIndex(200 + i), ProjectionIndex(0))};
		inputs.push_back(group);
	}
}

unique_ptr<LogicalComparisonJoin> ThreeInputConditions() {
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	for (idx_t step = 0; step + 1 < 3; step++) {
		auto left = make_uniq<BoundColumnRefExpression>("c", LogicalType::INTEGER,
		                                              ColumnBinding(TableIndex(200 + step), ProjectionIndex(0)));
		auto right = make_uniq<BoundColumnRefExpression>("c", LogicalType::INTEGER,
		                                                ColumnBinding(TableIndex(201 + step), ProjectionIndex(0)));
		join->conditions.emplace_back(std::move(left), std::move(right), ExpressionType::COMPARE_EQUAL);
	}
	return join;
}

//! No join in the produced chain may be condition-free: that would be a cartesian product.
bool ChainHasNoConditionFreeJoin(Memo &memo, GroupId group) {
	auto &g = memo.GetGroup(group);
	if (g.exprs.empty()) {
		return false;
	}
	auto &expr = g.exprs.back();
	if (expr->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN || !expr->op) {
		return false;
	}
	if (expr->op->Cast<LogicalComparisonJoin>().conditions.empty()) {
		return false;
	}
	return expr->children.size() == 2;
}

} // namespace

TEST_CASE("cascade rule: the smallest-first NAry expansion keeps every step connected", "[cascade]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto binder = Binder::CreateBinder(*con.context);
	CascadesOptimizer optimizer(*binder, *con.context);
	GroupId target;
	vector<GroupId> inputs;
	BuildThreeInputJoin(optimizer, target, inputs);

	GroupExpr expr;
	expr.type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
	expr.op = ThreeInputConditions();
	expr.children = inputs;

	ExpandNAryJoinMinCard rule;
	REQUIRE(rule.Promise(optimizer, expr) != CascadesRulePromise::NONE);
	REQUIRE(rule.Apply(optimizer, target, expr));
	REQUIRE(ChainHasNoConditionFreeJoin(optimizer.GetMemo(), target));
}

TEST_CASE("cascade rule: the DP NAry expansion offers only connected orders", "[cascade]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto binder = Binder::CreateBinder(*con.context);
	CascadesOptimizer optimizer(*binder, *con.context);
	GroupId target;
	vector<GroupId> inputs;
	BuildThreeInputJoin(optimizer, target, inputs);

	GroupExpr expr;
	expr.type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
	expr.op = ThreeInputConditions();
	expr.children = inputs;

	ExpandNAryJoinDP rule;
	REQUIRE(rule.Promise(optimizer, expr) != CascadesRulePromise::NONE);
	REQUIRE(rule.Apply(optimizer, target, expr));
	// Every alternative it offered has to be connected, and it has to have offered at least one.
	auto &group = optimizer.GetMemo().GetGroup(target);
	REQUIRE(group.exprs.size() >= 2);
	REQUIRE(ChainHasNoConditionFreeJoin(optimizer.GetMemo(), target));
}

namespace {

struct RuleContract {
	const char *name;
	CascadesRuleKind kind;
	LogicalOperatorType matches;
	// How many children the trigger shape needs: the matching test builds exactly that many, so a rule
	// whose Matches also checks the child count is exercised rather than assumed.
	idx_t children;
	// Whether Matches looks only at the type. Rules that also want the operator cannot be exercised by
	// a bare shape, and saying so is better than a green assertion that means nothing.
	bool type_only;
	LogicalOperatorType does_not_match;
};

// Every rule in the memo, with the contract it has to keep. One row per rule: the name the log
// shows, the kind the search loop reads, the shape it claims, and a shape it must refuse.
const RuleContract RULE_CONTRACTS[] = {
    {"apply_to_join", CascadesRuleKind::SUBSTITUTION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     2, false, LogicalOperatorType::LOGICAL_PROJECTION},
    {"semi_apply_to_join", CascadesRuleKind::SUBSTITUTION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     2, false, LogicalOperatorType::LOGICAL_FILTER},
    {"correlated_apply_to_join", CascadesRuleKind::SUBSTITUTION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     2, false, LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY},
    // SUBSTITUTION, not EXPLORATION: the rule moves a filter rather than offering an alternative,
    // and the first run of this test is what caught the difference (it had been written down the
    // other way round, in the rule order the migration notes happened to list).
    {"lift_local_predicate", CascadesRuleKind::SUBSTITUTION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     1, false, LogicalOperatorType::LOGICAL_LIMIT},
    {"group_apply_by_outer_columns", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     2, false, LogicalOperatorType::LOGICAL_ORDER_BY},
    {"push_filter_below_groupby", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_FILTER,
     1, false, LogicalOperatorType::LOGICAL_DISTINCT},
    {"collapse_project", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_PROJECTION,
     1, true, LogicalOperatorType::LOGICAL_FILTER},
    // ORCA EXformIds 1/2/3: the NAry join expansion family, migrated together because the three
    // differ only in the order they pick.
    {"expand_nary_join", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_COMPARISON_JOIN,
     3, true, LogicalOperatorType::LOGICAL_PROJECTION},
    {"expand_nary_join_min_card", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_COMPARISON_JOIN,
     3, true, LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY},
    {"expand_nary_join_dp", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_COMPARISON_JOIN,
     3, true, LogicalOperatorType::LOGICAL_FILTER},
    // ORCA EXformId 13: migrated as the invariant a Filter has to satisfy, since DuckDB has no Select.
    {"select_2_filter", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_FILTER,
     1, true, LogicalOperatorType::LOGICAL_DISTINCT},
    // ORCA EXformIds 10, 17, 18, 19, 20, 21: one parameterised rule per invariant, batched because
    // they are the same migration - the shape the binder already established, checked instead of
    // assumed.
    {"unnest_tvf", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_UNNEST,
     1, true, LogicalOperatorType::LOGICAL_LIMIT},
    {"simplify_select_with_subquery", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_FILTER,
     1, true, LogicalOperatorType::LOGICAL_ORDER_BY},
    {"simplify_project_with_subquery", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_PROJECTION,
     1, true, LogicalOperatorType::LOGICAL_DISTINCT},
    {"select_2_apply", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     2, true, LogicalOperatorType::LOGICAL_LIMIT},
    {"project_2_apply", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     2, true, LogicalOperatorType::LOGICAL_ORDER_BY},
    {"gbagg_2_apply", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     2, true, LogicalOperatorType::LOGICAL_DISTINCT},
    // ORCA EXformIds 14, 15, 16: the selection reaching an index get, checked as the access path the
    // get has to carry.
    {"select_2_index_get", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_GET,
     0, true, LogicalOperatorType::LOGICAL_DISTINCT},
    {"select_2_dynamic_index_get", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_GET,
     0, true, LogicalOperatorType::LOGICAL_LIMIT},
    {"select_2_partial_dynamic_index_get", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_GET,
     0, true, LogicalOperatorType::LOGICAL_ORDER_BY},
    // ORCA EXformId 34: the outer counterpart of apply_to_join.
    {"left_outer_apply_to_join", CascadesRuleKind::SUBSTITUTION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     2, true, LogicalOperatorType::LOGICAL_PROJECTION},
    // ORCA EXformIds 22, 23, 24: the sub-query join and the selection over an index, checked through
    // the invariant that no join condition still holds a sub-query.
    {"subq_join_2_apply", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_COMPARISON_JOIN,
     2, true, LogicalOperatorType::LOGICAL_FILTER},
    {"subq_nary_join_2_apply", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_COMPARISON_JOIN,
     2, true, LogicalOperatorType::LOGICAL_DISTINCT},
    {"inner_join_2_index_get_apply", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_COMPARISON_JOIN,
     2, true, LogicalOperatorType::LOGICAL_LIMIT},
};


// The migration ledger in cascade-orca-notes/ORCA_RULE_MIGRATION_PLAN.md says which ORCA xform each
// rule stands for. This is that table in code: one place to read, one place to check. An entry of
// -1 means the rule has no counterpart in the authoritative list yet - it is a statement, not a gap
// - and a rule that starts declaring an id in code has to agree with the row here, so the ledger and
// the rules cannot drift apart quietly.
struct RuleOrcaId {
	const char *name;
	// A rule can cover more than one ORCA xform (a correlated and an uncorrelated variant, say), so a
	// row lists them; -1 means none is attributed yet - a statement rather than a gap.
	vector<int> orca_ids;
};

const RuleOrcaId RULE_ORCA_IDS[] = {
    // Migrated and effective.
    {"expand_nary_join", {1}},
    {"expand_nary_join_min_card", {2}},
    {"expand_nary_join_dp", {3}},
    {"collapse_project", {139}},
    {"select_2_filter", {13}},
    // Invariants of the shapes the binder already established.
    {"unnest_tvf", {10}},
    {"select_2_index_get", {14}},
    {"select_2_dynamic_index_get", {15}},
    {"select_2_partial_dynamic_index_get", {16}},
    {"simplify_select_with_subquery", {17}},
    {"simplify_project_with_subquery", {18}},
    {"select_2_apply", {19}},
    {"project_2_apply", {20}},
    {"gbagg_2_apply", {21}},
    // The rules that were in the memo before this migration; their ORCA ids are attributed in the
    // ledger, and the declaration in code follows as each one is verified.
    // Requires an inner Apply with no correlated columns: CXformInnerApply2InnerJoinNoCorrelations,
    // read off the rule's own promise rather than guessed.
    {"apply_to_join", {31}},
    // Correlated semi and anti: its promise asks for a semi/anti Apply with correlated columns and
    // no condition, which is the correlated variant of each - not the NoCorrelations ones.
    {"semi_apply_to_join", {36, 39}},
    // The outer counterpart of apply_to_join: an uncorrelated left outer Apply.
    {"left_outer_apply_to_join", {34}},
    {"subq_join_2_apply", {22}},
    {"subq_nary_join_2_apply", {23}},
    {"inner_join_2_index_get_apply", {24}},
    // Correlated Apply with no condition, for inner (30, and the outer-key variant 26), semi (36) and
    // anti (39) joins: exactly the types its ReplacesWithCorrelatedJoin accepts. Outer joins are not
    // among them, so this rule does not cover the left outer xforms.
    {"correlated_apply_to_join", {26, 30, 36, 39}},
    {"lift_local_predicate", {-1}},
    {"group_apply_by_outer_columns", {-1}},
    {"push_filter_below_groupby", {-1}},
};

} // namespace

TEST_CASE("cascade rule: the declared contract of every registered rule", "[cascade]") {
	// The rules table is what the search loop and the migration ledger both depend on, so it is
	// checked against the rules themselves rather than trusted.
	vector<unique_ptr<CascadesRule>> rules;
	rules.push_back(make_uniq<ApplyToJoin>());
	rules.push_back(make_uniq<SemiApplyToJoin>());
	rules.push_back(make_uniq<CorrelatedApplyToJoin>());
	rules.push_back(make_uniq<LiftLocalPredicate>());
	rules.push_back(make_uniq<GroupApplyByOuterColumns>());
	rules.push_back(make_uniq<PushFilterBelowGroupBy>());
	rules.push_back(make_uniq<CollapseProject>());
	rules.push_back(make_uniq<ExpandNAryJoin>());
	rules.push_back(make_uniq<ExpandNAryJoinMinCard>());
	rules.push_back(make_uniq<ExpandNAryJoinDP>());
	rules.push_back(make_uniq<Select2Filter>());
	rules.push_back(make_uniq<BinderSideInvariantRule>("unnest_tvf", 10, LogicalOperatorType::LOGICAL_UNNEST,
                                                    BinderSideInvariant::HAS_EXPRESSIONS));
	rules.push_back(make_uniq<BinderSideInvariantRule>("simplify_select_with_subquery", 17,
                                                    LogicalOperatorType::LOGICAL_FILTER,
                                                    BinderSideInvariant::NO_SUBQUERY));
	rules.push_back(make_uniq<BinderSideInvariantRule>("simplify_project_with_subquery", 18,
                                                    LogicalOperatorType::LOGICAL_PROJECTION,
                                                    BinderSideInvariant::NO_SUBQUERY));
	rules.push_back(make_uniq<BinderSideInvariantRule>("select_2_apply", 19, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
                                                    BinderSideInvariant::TWO_INPUTS));
	rules.push_back(make_uniq<BinderSideInvariantRule>("project_2_apply", 20, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
                                                    BinderSideInvariant::TWO_INPUTS));
	rules.push_back(make_uniq<BinderSideInvariantRule>("gbagg_2_apply", 21, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
                                                    BinderSideInvariant::TWO_INPUTS));
	rules.push_back(make_uniq<BinderSideInvariantRule>("select_2_index_get", 14, LogicalOperatorType::LOGICAL_GET,
                                                    BinderSideInvariant::GET_HAS_ACCESS_PATH));
	rules.push_back(make_uniq<BinderSideInvariantRule>("select_2_dynamic_index_get", 15, LogicalOperatorType::LOGICAL_GET,
                                                    BinderSideInvariant::GET_HAS_ACCESS_PATH));
	rules.push_back(make_uniq<BinderSideInvariantRule>("select_2_partial_dynamic_index_get", 16,
                                                    LogicalOperatorType::LOGICAL_GET,
                                                    BinderSideInvariant::GET_HAS_ACCESS_PATH));
	rules.push_back(make_uniq<LeftOuterApplyToJoin>());
	rules.push_back(make_uniq<BinderSideInvariantRule>("subq_join_2_apply", 22,
                                                    LogicalOperatorType::LOGICAL_COMPARISON_JOIN,
                                                    BinderSideInvariant::NO_SUBQUERY));
	rules.push_back(make_uniq<BinderSideInvariantRule>("subq_nary_join_2_apply", 23,
                                                    LogicalOperatorType::LOGICAL_COMPARISON_JOIN,
                                                    BinderSideInvariant::NO_SUBQUERY));
	rules.push_back(make_uniq<BinderSideInvariantRule>("inner_join_2_index_get_apply", 24,
                                                    LogicalOperatorType::LOGICAL_COMPARISON_JOIN,
                                                    BinderSideInvariant::NO_SUBQUERY));

	set<int> declared_ids;
	REQUIRE(rules.size() == sizeof(RULE_CONTRACTS) / sizeof(RULE_CONTRACTS[0]));
	for (idx_t i = 0; i < rules.size(); i++) {
		auto &rule = *rules[i];
		auto &contract = RULE_CONTRACTS[i];
		INFO("rule " << rule.Name());
		CHECK(string(rule.Name()) == string(contract.name));
		CHECK(rule.Kind() == contract.kind);

		// The trigger shape: built here rather than taken from the corpus, so a rule whose shape the
		// corpus happens not to contain is still exercised.
		GroupExpr trigger;
		trigger.type = contract.matches;
		for (idx_t c = 0; c < contract.children; c++) {
			trigger.children.push_back(c);
		}
		// The migration ledger says which ORCA xform a rule stands for. Checking the declaration
		// against the authoritative range, and against the other rules, keeps that table honest: a
		// wrong id or two rules claiming one xform is bookkeeping that only reading would catch.
		auto orca_id = rule.OrcaId();
		bool in_range = orca_id == -1 || (orca_id >= 0 && orca_id <= 151);
		CHECK(in_range);
		// The ledger-row loop below is the single place that records ids, so that the uniqueness check
		// sees each id once.

		// The ledger table above has to agree with what the rules declare, and with itself.
		{
			const RuleOrcaId *row = nullptr;
			for (auto &candidate : RULE_ORCA_IDS) {
				if (string(candidate.name) == string(rule.Name())) {
					row = &candidate;
					break;
				}
			}
			REQUIRE(row != nullptr);
			bool declared_listed = false;
			for (auto id : row->orca_ids) {
				bool in_range = id == -1 || (id >= 0 && id <= 151);
				CHECK(in_range);
				if (id != -1) {
					// Coverage can be shared between rules - ORCA does not make xforms exclusive - so
					// the table records which ids are attributed at all, not which rule owns them.
					declared_ids.insert(id);
				}
				if (id == rule.OrcaId()) {
					declared_listed = true;
				}
			}
			if (rule.OrcaId() != -1) {
				// Declared in code: the ledger row has to list it.
				CHECK(declared_listed);
			}
		}

		if (contract.type_only) {
			CHECK(rule.Matches(trigger));
		} else if (!rule.Matches(trigger)) {
			WARN("rule " << rule.Name() << " needs the operator as well: a bare shape is not enough");
		}

		// And a shape it has to refuse: the same child count under a different operator.
		GroupExpr other;
		other.type = contract.does_not_match;
		for (idx_t c = 0; c < contract.children; c++) {
			other.children.push_back(c);
		}
		CHECK(!rule.Matches(other));
	}
}

// The physical family: ORCA's implementation rules are DuckDB's, so what a migration of one of them
// has to show is that the logical shape reaches the host's physical operator. These are the first
// two attributed rules - CXformProject2ComputeScalar (EXformId 0, projection) and CXformGet2TableScan
// (EXformId 4, scan) - and the pattern the rest of the family follows.
TEST_CASE("cascade physical attribution: the host plans the shape with its own operator", "[cascade]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE(con.Query("CREATE TABLE t(a INTEGER, b INTEGER)"));
	REQUIRE(con.Query("INSERT INTO t VALUES (1, 2), (3, 4)"));

	// The projection has to be one the host cannot omit: a pass-through projection is folded away by
	// plan_projection.cpp ("check if this projection can be omitted entirely"), which is the host's
	// own optimisation and not part of this attribution. Computed columns keep it.
	auto result = con.Query("EXPLAIN SELECT a + b AS s, b * 2 AS d FROM t WHERE b > 0");
	REQUIRE(result);
	REQUIRE(result->RowCount() > 0);
	auto plan = result->GetValue(1, 0).ToString();

	// EXformId 0: the projection becomes the host's projection. The plan this version prints names
	// operators in CamelCase ("Projection", "Seq Scan"), which is what the checks below pin down.
	REQUIRE(plan.find("Projection") != string::npos);
	// EXformId 4: the scan becomes the host's table scan. "Seq Scan" is the name this version uses;
	// "Table Scan" is accepted too, so the test pins the attribution rather than the spelling.
	bool scanned = plan.find("Seq Scan") != string::npos || plan.find("Table Scan") != string::npos;
	REQUIRE(scanned);

	// EXformId 9: a constant relation - a VALUES list - is planned as the host's column data scan
	// (plan_column_data_get.cpp). The name the plan prints for it is checked here.
	auto constants = con.Query("EXPLAIN SELECT * FROM (VALUES (1), (2)) t(x)");
	REQUIRE(constants);
	REQUIRE(constants->RowCount() > 0);
	auto constants_plan = constants->GetValue(1, 0).ToString();
	REQUIRE(constants_plan.find("Column Data Scan") != string::npos);

	// EXformIds 27/29/45/46/47/48: the join implementations are the host's choice between its hash
	// join and its nested loop join. Which one it picks for a shape is the attribution, so each shape
	// is checked for the operator the host plans it with: equality joins get the hash join, and a
	// join without an equality gets the nested loop one.
	REQUIRE(con.Query("CREATE TABLE ja(x INTEGER)"));
	REQUIRE(con.Query("CREATE TABLE jb(x INTEGER)"));
	REQUIRE(con.Query("INSERT INTO ja VALUES (1), (2)"));
	REQUIRE(con.Query("INSERT INTO jb VALUES (1), (3)"));

	auto hash_join = con.Query("EXPLAIN SELECT * FROM ja JOIN jb ON ja.x = jb.x");
	REQUIRE(hash_join);
	auto hash_plan = hash_join->GetValue(1, 0).ToString();
	CHECK(hash_plan.find("Hash Join") != string::npos);

	auto nested_loop = con.Query("EXPLAIN SELECT * FROM ja JOIN jb ON ja.x < jb.x");
	REQUIRE(nested_loop);
	auto nested_plan = nested_loop->GetValue(1, 0).ToString();
	CHECK(nested_plan.find("Nested Loop Join") != string::npos);

	// The outer and the semi variant reach the same implementations.
	auto outer_join = con.Query("EXPLAIN SELECT * FROM ja LEFT JOIN jb ON ja.x = jb.x");
	REQUIRE(outer_join);
	CHECK(outer_join->GetValue(1, 0).ToString().find("Hash Join") != string::npos);

	auto semi_join = con.Query("EXPLAIN SELECT * FROM ja WHERE EXISTS (SELECT 1 FROM jb WHERE jb.x = ja.x)");
	REQUIRE(semi_join);
	CHECK(semi_join->GetValue(1, 0).ToString().find("Hash Join") != string::npos);
}

// The shapes the corpus does not contain still have to be exercised, and that means driving Apply
// rather than only checking Matches. This is the smallest fixture that can: a client context and a
// binder, a memo with three groups, and a synthesized expression to hand the rule.
TEST_CASE("cascade rule: the outer Apply rule builds the join it claims", "[cascade]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto binder = Binder::CreateBinder(*con.context);
	CascadesOptimizer optimizer(*binder, *con.context);

	auto target = optimizer.GetMemo().AddGroup();
	auto left = optimizer.GetMemo().AddGroup();
	auto right = optimizer.GetMemo().AddGroup();

	GroupExpr expr;
	expr.type = LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
	expr.op = make_uniq<LogicalDependentJoin>(JoinType::LEFT);
	expr.children = {left, right};

	LeftOuterApplyToJoin rule;
	// The precondition holds: an outer Apply with no correlation and no condition.
	REQUIRE(rule.Promise(optimizer, expr) != CascadesRulePromise::NONE);
	REQUIRE(rule.Apply(optimizer, target, expr));

	// What it promised to build: a comparison join over the same two inputs.
	auto &group = optimizer.GetMemo().GetGroup(target);
	REQUIRE(!group.exprs.empty());
	REQUIRE(group.exprs.back()->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
	REQUIRE(group.exprs.back()->children.size() == 2);
	REQUIRE(group.exprs.back()->children[0] == left);
	REQUIRE(group.exprs.back()->children[1] == right);

	// And the shapes it must refuse: an inner Apply is apply_to_join's case, not this rule's, and a
	// rule that transformed it anyway would turn a left outer join into an inner one.
	GroupExpr inner;
	inner.type = LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
	inner.op = make_uniq<LogicalDependentJoin>(JoinType::INNER);
	inner.children = {left, right};
	CHECK(rule.Promise(optimizer, inner) == CascadesRulePromise::NONE);
	CHECK(!rule.Apply(optimizer, target, inner));

	// A correlated Apply needs the parameterisation this rule does not have, so it declines too. The
	// correlated columns go in through CorrelatedColumns::AddColumn, whose CorrelatedColumnInfo can be
	// built from a column reference - the collection is not a vector, which is why this is not a
	// push_back.
	GroupExpr correlated;
	correlated.type = LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
	correlated.op = make_uniq<LogicalDependentJoin>(JoinType::LEFT);
	correlated.children = {left, right};
	BoundColumnRefExpression correlated_ref("c", LogicalType::INTEGER,
	                                        ColumnBinding(TableIndex(7), ProjectionIndex(0)));
	correlated.op->Cast<LogicalDependentJoin>().correlated_columns.AddColumn(
	    CorrelatedColumnInfo(correlated_ref));
	CHECK(rule.Promise(optimizer, correlated) == CascadesRulePromise::NONE);
	CHECK(!rule.Apply(optimizer, target, correlated));
}

// The NAry family at transformation level: a three-input join with a predicate for each step, so the
// conditions have somewhere to go and the expansion cannot fall back to a cartesian product.
TEST_CASE("cascade rule: the NAry expansion builds a binary tree over the same columns", "[cascade]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto binder = Binder::CreateBinder(*con.context);
	CascadesOptimizer optimizer(*binder, *con.context);
	auto &memo = optimizer.GetMemo();

	auto target = memo.AddGroup();
	vector<GroupId> inputs;
	// One input per child, each carrying a single column: the bindings are what the conditions below
	// refer to, and the expansion reads them off the child groups.
	for (idx_t i = 0; i < 3; i++) {
		auto group = memo.AddGroup();
		auto scan = make_uniq<LogicalDummyScan>(TableIndex(100 + i));
		memo.GetGroup(group); // (the group exists; the expression is added next)
		optimizer.AddExpression(group, memo.MakeExpr(std::move(scan), {}));
		memo.GetGroup(group).exprs[0]->bindings = {ColumnBinding(TableIndex(100 + i), ProjectionIndex(0))};
		inputs.push_back(group);
	}

	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	// input0.c0 = input1.c0, and input1.c0 = input2.c0: connected, so every step has a condition.
	for (idx_t step = 0; step + 1 < 3; step++) {
		auto left = make_uniq<BoundColumnRefExpression>("c", LogicalType::INTEGER,
		                                              ColumnBinding(TableIndex(100 + step), ProjectionIndex(0)));
		auto right = make_uniq<BoundColumnRefExpression>("c", LogicalType::INTEGER,
		                                                ColumnBinding(TableIndex(101 + step), ProjectionIndex(0)));
		join->conditions.emplace_back(std::move(left), std::move(right), ExpressionType::COMPARE_EQUAL);
	}

	GroupExpr expr;
	expr.type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
	expr.op = std::move(join);
	expr.children = inputs;

	ExpandNAryJoin rule;
	REQUIRE(rule.Matches(expr));
	REQUIRE(rule.Promise(optimizer, expr) != CascadesRulePromise::NONE);
	REQUIRE(rule.Apply(optimizer, target, expr));

	// The alternative offered: a comparison join over an inner one and the last input.
	auto &group = memo.GetGroup(target);
	REQUIRE(!group.exprs.empty());
	REQUIRE(group.exprs.back()->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
	REQUIRE(group.exprs.back()->children.size() == 2);
	REQUIRE(group.exprs.back()->children[1] == inputs[2]);
	auto inner = group.exprs.back()->children[0];
	REQUIRE(memo.GetGroup(inner).exprs.back()->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
	// Every step carried a condition, so nothing became a cartesian product.
	REQUIRE(!memo.GetGroup(inner).exprs.back()->op->Cast<LogicalComparisonJoin>().conditions.empty());
	REQUIRE(!group.exprs.back()->op->Cast<LogicalComparisonJoin>().conditions.empty());
}
