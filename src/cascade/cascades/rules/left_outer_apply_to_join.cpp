#include "duckdb/cascade/cascades/rules/left_outer_apply_to_join.hpp"
#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
namespace duckdb {
LeftOuterApplyToJoin::LeftOuterApplyToJoin()
    : CascadesRule(CascadesRuleKind::SUBSTITUTION, "left_outer_apply_to_join") {}
bool LeftOuterApplyToJoin::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && expr.children.size() == 2;
}
CascadesRulePromise LeftOuterApplyToJoin::Promise(CascadesOptimizer &, GroupExpr &expr) {
	string reason;
	if (!expr.op) {
		reason = "the Apply has no operator";
	} else {
		auto &apply = expr.op->Cast<LogicalDependentJoin>();
		if (!apply.correlated_columns.empty()) {
			reason = "the Apply is correlated, which is another rule's case";
		} else if (apply.join_type != JoinType::LEFT) {
			reason = "the Apply is not a left outer one";
		} else if (apply.condition) {
			reason = "the Apply carries a condition, which would be the join's ON predicate";
		} else {
			return CascadesRulePromise::MEDIUM;
		}
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) rule " + string(Name()) + ": " + reason);
	}
	return CascadesRulePromise::NONE;
}
bool LeftOuterApplyToJoin::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	if (!expr.op) {
		return false;
	}
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	if (!apply.correlated_columns.empty() || apply.join_type != JoinType::LEFT || apply.condition) {
		return false;
	}
	// The same two inputs, as a left outer join: the columns are the same ones in the same order, so
	// nothing above moves.
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
	auto children = expr.children;
	optimizer.AddExpression(group, optimizer.GetMemo().MakeExpr(std::move(join), std::move(children)));
	return true;
}
} // namespace duckdb
