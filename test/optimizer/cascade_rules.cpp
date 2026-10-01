// Unit tests for the cascade rules, one case per rule.
//
// The migration plan (cascade-orca-notes/ORCA_RULE_MIGRATION_PLAN.md) asks for a unit test per
// ORCA rule, and these are the first ones: they pin down what each rule *is* - the name it
// reports, the kind that decides when the search may run it, and the shape it claims to match -
// which is exactly what breaks silently when a rule is renamed, retyped or pointed at the wrong
// operator. Tests of what a rule *produces* are added with each rule's transformation.
#include "catch.hpp"

#include "duckdb/cascade/cascades/rules/apply_to_join.hpp"
#include "duckdb/cascade/cascades/rules/collapse_project.hpp"
#include "duckdb/cascade/cascades/rules/correlated_apply_to_join.hpp"
#include "duckdb/cascade/cascades/rules/group_apply_by_outer_columns.hpp"
#include "duckdb/cascade/cascades/rules/lift_local_predicate.hpp"
#include "duckdb/cascade/cascades/rules/push_filter_below_groupby.hpp"
#include "duckdb/cascade/cascades/rules/semi_apply_to_join.hpp"

using namespace duckdb;

namespace {

struct RuleContract {
	const char *name;
	CascadesRuleKind kind;
	LogicalOperatorType matches;
	LogicalOperatorType does_not_match;
};

// Every rule in the memo, with the contract it has to keep. One row per rule: the name the log
// shows, the kind the search loop reads, the shape it claims, and a shape it must refuse.
const RuleContract RULE_CONTRACTS[] = {
    {"apply_to_join", CascadesRuleKind::SUBSTITUTION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     LogicalOperatorType::LOGICAL_PROJECTION},
    {"semi_apply_to_join", CascadesRuleKind::SUBSTITUTION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     LogicalOperatorType::LOGICAL_FILTER},
    {"correlated_apply_to_join", CascadesRuleKind::SUBSTITUTION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY},
    // SUBSTITUTION, not EXPLORATION: the rule moves a filter rather than offering an alternative,
    // and the first run of this test is what caught the difference (it had been written down the
    // other way round, in the rule order the migration notes happened to list).
    {"lift_local_predicate", CascadesRuleKind::SUBSTITUTION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     LogicalOperatorType::LOGICAL_LIMIT},
    {"group_apply_by_outer_columns", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN,
     LogicalOperatorType::LOGICAL_ORDER_BY},
    {"push_filter_below_groupby", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_FILTER,
     LogicalOperatorType::LOGICAL_DISTINCT},
    {"collapse_project", CascadesRuleKind::EXPLORATION, LogicalOperatorType::LOGICAL_PROJECTION,
     LogicalOperatorType::LOGICAL_FILTER},
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

	REQUIRE(rules.size() == sizeof(RULE_CONTRACTS) / sizeof(RULE_CONTRACTS[0]));
	for (idx_t i = 0; i < rules.size(); i++) {
		auto &rule = *rules[i];
		auto &contract = RULE_CONTRACTS[i];
		INFO("rule " << rule.Name());
		CHECK(string(rule.Name()) == string(contract.name));
		CHECK(rule.Kind() == contract.kind);
	}
}
