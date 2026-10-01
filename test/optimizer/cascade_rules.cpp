// Unit tests for the cascade rules, one case per rule.
//
// The migration plan (cascade-orca-notes/ORCA_RULE_MIGRATION_PLAN.md) asks for a unit test per
// ORCA rule, and these are the first ones: they pin down what each rule *is* - the name it
// reports, the kind that decides when the search may run it, and the shape it claims to match -
// which is exactly what breaks silently when a rule is renamed, retyped or pointed at the wrong
// operator. Tests of what a rule *produces* are added with each rule's transformation.
#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/rules/apply_to_join.hpp"
#include "duckdb/cascade/cascades/rules/collapse_project.hpp"
#include "duckdb/cascade/cascades/rules/correlated_apply_to_join.hpp"
#include "duckdb/cascade/cascades/rules/expand_nary_join.hpp"
#include "duckdb/cascade/cascades/rules/group_apply_by_outer_columns.hpp"
#include "duckdb/cascade/cascades/rules/binder_side_invariants.hpp"
#include "duckdb/cascade/cascades/rules/select_2_filter.hpp"
#include "duckdb/cascade/cascades/rules/lift_local_predicate.hpp"
#include "duckdb/cascade/cascades/rules/push_filter_below_groupby.hpp"
#include "duckdb/cascade/cascades/rules/semi_apply_to_join.hpp"

using namespace duckdb;

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
	rules.push_back(make_uniq<BinderSideInvariantRule>("unnest_tvf", LogicalOperatorType::LOGICAL_UNNEST,
                                                    BinderSideInvariant::HAS_EXPRESSIONS));
	rules.push_back(make_uniq<BinderSideInvariantRule>("simplify_select_with_subquery",
                                                    LogicalOperatorType::LOGICAL_FILTER,
                                                    BinderSideInvariant::NO_SUBQUERY));
	rules.push_back(make_uniq<BinderSideInvariantRule>("simplify_project_with_subquery",
                                                    LogicalOperatorType::LOGICAL_PROJECTION,
                                                    BinderSideInvariant::NO_SUBQUERY));
	rules.push_back(make_uniq<BinderSideInvariantRule>("select_2_apply", LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
                                                    BinderSideInvariant::TWO_INPUTS));
	rules.push_back(make_uniq<BinderSideInvariantRule>("project_2_apply", LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
                                                    BinderSideInvariant::TWO_INPUTS));
	rules.push_back(make_uniq<BinderSideInvariantRule>("gbagg_2_apply", LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
                                                    BinderSideInvariant::TWO_INPUTS));
	rules.push_back(make_uniq<BinderSideInvariantRule>("select_2_index_get", LogicalOperatorType::LOGICAL_GET,
                                                    BinderSideInvariant::GET_HAS_ACCESS_PATH));
	rules.push_back(make_uniq<BinderSideInvariantRule>("select_2_dynamic_index_get", LogicalOperatorType::LOGICAL_GET,
                                                    BinderSideInvariant::GET_HAS_ACCESS_PATH));
	rules.push_back(make_uniq<BinderSideInvariantRule>("select_2_partial_dynamic_index_get",
                                                    LogicalOperatorType::LOGICAL_GET,
                                                    BinderSideInvariant::GET_HAS_ACCESS_PATH));

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
}
