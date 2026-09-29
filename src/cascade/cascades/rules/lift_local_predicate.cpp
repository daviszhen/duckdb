#include "duckdb/cascade/cascades/rules/lift_local_predicate.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascade_correlation.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

bool LiftLocalPredicate::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && expr.children.size() == 2;
}

namespace {

bool ExposesBothSides(JoinType type) {
	// Only these pass the right side's columns through, which is what makes the group's columns
	// unchanged when the predicate moves above the Apply.
	return type == JoinType::INNER || type == JoinType::LEFT || type == JoinType::RIGHT ||
	       type == JoinType::OUTER;
}

} // namespace

CascadesRulePromise LiftLocalPredicate::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	if (!ExposesBothSides(apply.join_type)) {
		// Semi and anti applies quantify over the right side: "some matching row satisfies p" is
		// not "the row satisfies p", so the predicate must not move above them.
		return CascadesRulePromise::NONE;
	}
	auto &memo = optimizer.GetMemo();
	auto &right_group = memo.GetGroup(expr.children[1]);
	// The shape is a filter on the right side of the Apply.
	GroupExpr *right_filter = nullptr;
	for (auto &candidate : right_group.exprs) {
		if (candidate->type == LogicalOperatorType::LOGICAL_FILTER && !candidate->children.empty()) {
			right_filter = candidate.get();
			break;
		}
	}
	if (!right_filter) {
		if (CascadeConfig::PrintPlans()) {
			string types;
			for (auto &candidate : right_group.exprs) {
				types += (types.empty() ? "" : ",") + EnumUtil::ToString(candidate->type);
			}
			Printer::Print("--- cascade(cascades) rule " + string(Name()) +
			               ": no filter on the Apply's right side (it holds: " + types + ")");
		}
		return CascadesRulePromise::NONE;
	}
	// Every predicate has to read the right side only. One that reads the left side is the
	// correlation itself, and moving it is the other half of identity (3).
	OptimizationContext context;
	context.group = expr.children[0];
	auto left = memo.WinnerOf(context);
	if (!left || !left->op) {
		return CascadesRulePromise::NONE;
	}
	auto left_bindings = left->op->GetColumnBindings();
	for (auto &predicate : right_filter->op->expressions) {
		if (ApplyReadsBindings(*predicate, left_bindings)) {
			return CascadesRulePromise::NONE;
		}
	}
	return CascadesRulePromise::MEDIUM;
}

bool LiftLocalPredicate::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	auto &memo = optimizer.GetMemo();
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	auto &right_group = memo.GetGroup(expr.children[1]);
	GroupExpr *right_filter = nullptr;
	for (auto &candidate : right_group.exprs) {
		if (candidate->type == LogicalOperatorType::LOGICAL_FILTER && !candidate->children.empty()) {
			right_filter = candidate.get();
			break;
		}
	}
	if (!right_filter) {
		return false;
	}

	// The Apply with the filter's child as its right side: the predicates are not gone, they are
	// above it now. The correlation list is untouched, because a predicate that reads only the
	// right side never was part of it.
	auto rebuilt = make_uniq<LogicalDependentJoin>(apply.join_type);
	if (apply.condition) {
		rebuilt->condition = apply.condition->Copy();
	}
	rebuilt->correlated_columns = apply.correlated_columns;
	rebuilt->perform_delim = apply.perform_delim;
	rebuilt->any_join = apply.any_join;
	rebuilt->propagate_null_values = apply.propagate_null_values;
	rebuilt->estimated_cardinality = apply.estimated_cardinality;
	rebuilt->has_estimated_cardinality = apply.has_estimated_cardinality;

	auto apply_group = memo.AddGroup();
	optimizer.AddExpression(
	    apply_group, memo.MakeExpr(std::move(rebuilt), {expr.children[0], right_filter->children[0]}));

	auto lifted = make_uniq<LogicalFilter>();
	lifted->estimated_cardinality = right_filter->op->estimated_cardinality;
	lifted->has_estimated_cardinality = right_filter->op->has_estimated_cardinality;
	for (auto &predicate : right_filter->op->expressions) {
		lifted->expressions.push_back(predicate->Copy());
	}
	optimizer.AddExpression(group, memo.MakeExpr(std::move(lifted), {apply_group}));
	return true;
}

} // namespace duckdb
