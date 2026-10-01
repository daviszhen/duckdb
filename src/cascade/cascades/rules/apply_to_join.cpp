#include "duckdb/cascade/cascades/rules/apply_to_join.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/printer.hpp"

#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

bool ApplyToJoin::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && expr.children.size() == 2;
}

CascadesRulePromise ApplyToJoin::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	if (!apply.correlated_columns.empty()) {
		// The right side reads the left side: removing the Apply is the correlated case's job,
		// which needs the predicates extracted and the columns exposed.
		if (CascadeConfig::PrintPlans()) {
			Printer::Print("--- cascade(cascades) rule " + string(Name()) + ": the Apply is correlated (" +
			               std::to_string(apply.correlated_columns.size()) + " columns)");
		}
		return CascadesRulePromise::NONE;
	}
	if (apply.join_type != JoinType::INNER) {
		// An outer Apply without correlation is still an outer join, with its own null semantics.
		if (CascadeConfig::PrintPlans()) {
			Printer::Print("--- cascade(cascades) rule " + string(Name()) +
			               ": join_type=" + EnumUtil::ToString(apply.join_type) + " is not INNER");
		}
		return CascadesRulePromise::NONE;
	}
	if (apply.condition) {
		// A condition has to respect the join's side convention (each side of a comparison names
		// one child), which is what the decorrelator's AddJoinCondition ordering is about. Not
		// this rule's case.
		if (CascadeConfig::PrintPlans()) {
			Printer::Print("--- cascade(cascades) rule " + string(Name()) + ": the Apply carries an ON condition");
		}
		return CascadesRulePromise::NONE;
	}
	return CascadesRulePromise::HIGH;
}

bool ApplyToJoin::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	auto &memo = optimizer.GetMemo();
	// A comparison join with no conditions is a cross product - that is how DuckDB represents one
	// internally, and the physical planner emits a cross product for it - and it is the only join
	// a rule can build here, since LogicalCrossProduct's constructor wants two operators rather
	// than the two child *groups* a memo expression has.
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	// The same rows the Apply produced: this is a substitution, not a change of plan shape.
	join->estimated_cardinality = expr.op->estimated_cardinality;
	join->has_estimated_cardinality = expr.op->has_estimated_cardinality;
	optimizer.AddExpression(group, memo.MakeExpr(std::move(join), expr.children));
	return true;
}

} // namespace duckdb
