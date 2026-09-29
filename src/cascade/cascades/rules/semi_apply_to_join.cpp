#include "duckdb/cascade/cascades/rules/semi_apply_to_join.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascade_correlation.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

bool SemiApplyToJoin::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && expr.children.size() == 2;
}

namespace {

//! The projection between the Apply and the filter, and the filter below it.
bool FindShape(CascadesOptimizer &optimizer, GroupExpr &expr, GroupExpr *&projection, GroupExpr *&filter) {
	auto &memo = optimizer.GetMemo();
	for (auto &candidate : memo.GetGroup(expr.children[1]).exprs) {
		if (candidate->type != LogicalOperatorType::LOGICAL_PROJECTION || candidate->children.size() != 1) {
			continue;
		}
		for (auto &below : memo.GetGroup(candidate->children[0]).exprs) {
			if (below->type == LogicalOperatorType::LOGICAL_FILTER && below->children.size() == 1) {
				projection = candidate.get();
				filter = below.get();
				return true;
			}
		}
	}
	return false;
}

bool IsSemiOrAnti(JoinType type) {
	return type == JoinType::SEMI || type == JoinType::ANTI;
}

//! Do the predicates of this filter pair the two sides, all of them?
bool PredicatesPairTheSides(GroupExpr &filter, const vector<ColumnBinding> &left_bindings,
                            const vector<ColumnBinding> &right_bindings) {
	if (filter.op->expressions.empty()) {
		return false;
	}
	for (auto &predicate : filter.op->expressions) {
		if (!BoundComparisonExpression::IsComparison(*predicate)) {
			return false;
		}
		auto &comparison = predicate->Cast<BoundFunctionExpression>();
		auto &lhs = BoundComparisonExpression::Left(comparison);
		auto &rhs = BoundComparisonExpression::Right(comparison);
		bool lhs_left = ApplyReadsBindings(lhs, left_bindings);
		bool lhs_right = ApplyReadsBindings(lhs, right_bindings);
		bool rhs_left = ApplyReadsBindings(rhs, left_bindings);
		bool rhs_right = ApplyReadsBindings(rhs, right_bindings);
		if (!((lhs_left && rhs_right) || (lhs_right && rhs_left))) {
			return false;
		}
	}
	return true;
}

} // namespace

CascadesRulePromise SemiApplyToJoin::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	if (apply.condition || apply.correlated_columns.empty()) {
		return CascadesRulePromise::NONE;
	}
	if (!IsSemiOrAnti(apply.join_type)) {
		if (CascadeConfig::PrintPlans()) {
			Printer::Print(StringUtil::Format("--- cascade(cascades) rule %s: join_type=%d is not semi/anti",
			                                  Name(), (int)apply.join_type));
		}
		return CascadesRulePromise::NONE;
	}
	GroupExpr *projection = nullptr;
	GroupExpr *filter = nullptr;
	if (!FindShape(optimizer, expr, projection, filter)) {
		return CascadesRulePromise::NONE;
	}
	auto &memo = optimizer.GetMemo();
	auto &left_group = memo.GetGroup(expr.children[0]);
	auto &right_group = memo.GetGroup(filter->children[0]);
	if (left_group.exprs.empty() || right_group.exprs.empty()) {
		return CascadesRulePromise::NONE;
	}
	if (!PredicatesPairTheSides(*filter, left_group.exprs[0]->bindings, right_group.exprs[0]->bindings)) {
		return CascadesRulePromise::NONE;
	}
	return CascadesRulePromise::HIGH;
}

bool SemiApplyToJoin::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	GroupExpr *projection = nullptr;
	GroupExpr *filter = nullptr;
	if (!FindShape(optimizer, expr, projection, filter)) {
		return false;
	}
	auto &memo = optimizer.GetMemo();
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	auto &left_group = memo.GetGroup(expr.children[0]);
	auto &right_group = memo.GetGroup(filter->children[0]);
	if (left_group.exprs.empty() || right_group.exprs.empty()) {
		return false;
	}

	auto join = make_uniq<LogicalComparisonJoin>(apply.join_type);
	for (auto &predicate : filter->op->expressions) {
		auto copy = predicate->Copy();
		auto &comparison = copy->Cast<BoundFunctionExpression>();
		auto &lhs = BoundComparisonExpression::LeftMutable(comparison);
		auto &rhs = BoundComparisonExpression::RightMutable(comparison);
		bool lhs_left = ApplyReadsBindings(*lhs, left_group.exprs[0]->bindings);
		bool lhs_right = ApplyReadsBindings(*lhs, right_group.exprs[0]->bindings);
		bool rhs_right = ApplyReadsBindings(*rhs, right_group.exprs[0]->bindings);
		if (lhs_right && !lhs_left && !rhs_right) {
			// A join condition has a side convention: the left expression names the left child.
			auto moved = std::move(lhs);
			lhs = std::move(rhs);
			rhs = std::move(moved);
			BoundComparisonExpression::FlipType(comparison);
		}
		join->conditions.emplace_back(std::move(lhs), std::move(rhs), comparison.GetExpressionType());
	}
	join->estimated_cardinality = expr.op->estimated_cardinality;
	join->has_estimated_cardinality = expr.op->has_estimated_cardinality;

	// The right side without its filter: the projection, over the filter's own child.
	auto &projection_op = projection->op->Cast<LogicalProjection>();
	vector<unique_ptr<Expression>> select_list;
	for (auto &projection_expression : projection_op.expressions) {
		select_list.push_back(projection_expression->Copy());
	}
	auto rebuilt = make_uniq<LogicalProjection>(projection_op.table_index, std::move(select_list));
	rebuilt->estimated_cardinality = projection_op.estimated_cardinality;
	rebuilt->has_estimated_cardinality = projection_op.has_estimated_cardinality;

	auto right_without_filter = memo.AddGroup();
	optimizer.AddExpression(right_without_filter, memo.MakeExpr(std::move(rebuilt), {filter->children[0]}));
	optimizer.AddExpression(group, memo.MakeExpr(std::move(join), {expr.children[0], right_without_filter}));
	return true;
}

} // namespace duckdb
