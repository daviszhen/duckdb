#include "duckdb/cascade/cascades/rules/correlated_apply_to_join.hpp"

// OPEN: what this rewrite still needs, and what has been measured. Kept here because the
// measurements are the expensive part and they must not be rediscovered.
//
// The rewrite below replaces the Apply's own group with a MARK join. That is *not* what DuckDB's
// own decorrelation does: comparing the two plans for decorrelation.test:27 shows the enforcer
// producing a SEMI join that keeps the projection and has no mark filter above it, while this rule
// produces a MARK join, drops the projection, and leaves the parent's "SUBQUERY" filter in place.
// Three attempts at the correct shape (build SEMI/ANTI, splice the absorbed pair out of the
// parents) were measured and reverted; what they showed:
//
//   * The pair to splice is (this Apply's group, the mark filter's group above it). Memo::ParentsOf
//     and Memo::ReplaceExpression/Reschedule exist for that, and Memo::FindMarkConsumer already
//     answers SEMI versus ANTI - the two Apply nodes are identical in every field, the negation
//     lives only in the consumer.
//   * With the splice in place the rule did work - "spliced group 5, join SEMI in group 8",
//     enforced 30 -> 25 - and then failed to bind:
//         Failed to bind column reference "b" [0.0] (bindings: {#[7.0], #[7.1]})
//     The available set is a *correlated copy* of the two-column left table at another table
//     index, i.e. exactly what the decorrelator's ExposeRightColumns and DecrementCorrelationDepth
//     maintain. Moving the condition is not enough; the column exposure has to be reproduced.
//   * The same rule body spliced twice in one build and not at all in another (applied=4 but
//     ParentsOf(consumer_group) empty), which is not yet explained. Before writing the exposure
//     code, that has to be understood: print FindMarkConsumer's answer and ParentsOf's result on
//     the failing statement in both versions and compare. A consumer with no parents is the root
//     of the memo, and a SEMI join cannot stand in for it (it exposes the left side only).
//
// Both matrices are green with this rule as it stands (it is inert on the corpus), so the shape
// above is a design note, not a description of what the code does today.

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascade_correlation.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

bool CorrelatedApplyToJoin::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && expr.children.size() == 2;
}

namespace {

bool FindCorrelatedShape(CascadesOptimizer &optimizer, GroupExpr &expr, GroupExpr *&projection, GroupExpr *&filter) {
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

bool ReplacesWithCorrelatedJoin(JoinType type) {
	return type == JoinType::SEMI || type == JoinType::ANTI || type == JoinType::MARK;
}

bool PredicatesPairBothSides(GroupExpr &filter, const vector<ColumnBinding> &left_bindings,
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

CascadesRulePromise CorrelatedApplyToJoin::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	if (apply.condition || apply.correlated_columns.empty()) {
		return CascadesRulePromise::NONE;
	}
	if (!ReplacesWithCorrelatedJoin(apply.join_type)) {
		return CascadesRulePromise::NONE;
	}
	GroupExpr *projection = nullptr;
	GroupExpr *filter = nullptr;
	if (!FindCorrelatedShape(optimizer, expr, projection, filter)) {
		return CascadesRulePromise::NONE;
	}
	auto &memo = optimizer.GetMemo();
	auto &left_group = memo.GetGroup(expr.children[0]);
	auto &right_group = memo.GetGroup(filter->children[0]);
	if (left_group.exprs.empty() || right_group.exprs.empty()) {
		return CascadesRulePromise::NONE;
	}
	if (!PredicatesPairBothSides(*filter, left_group.exprs[0]->bindings, right_group.exprs[0]->bindings)) {
		return CascadesRulePromise::NONE;
	}
	for (idx_t candidate_group = 0; candidate_group < memo.GroupCount(); candidate_group++) {
		for (auto &candidate : memo.GetGroup(candidate_group).exprs) {
			if (!candidate->op) {
				continue;
			}
			for (auto &expression : candidate->op->expressions) {
				bool negated = false;
				ExpressionIterator::VisitExpression<BoundOperatorExpression>(
				    *expression, [&](const BoundOperatorExpression &node) {
					    if (node.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
						    negated = true;
					    }
				    });
				ExpressionIterator::VisitExpression<BoundFunctionExpression>(
				    *expression, [&](const BoundFunctionExpression &node) {
					    if (node.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
						    negated = true;
					    }
				    });
				if (negated) {
					if (CascadeConfig::PrintPlans()) {
						Printer::Print("--- cascade(cascades) rule " + string(Name()) +
						               " refused: the plan negates the mark consumer");
					}
					return CascadesRulePromise::NONE;
				}
			}
		}
	}
	return CascadesRulePromise::HIGH;
}

bool CorrelatedApplyToJoin::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	GroupExpr *projection = nullptr;
	GroupExpr *filter = nullptr;
	if (!FindCorrelatedShape(optimizer, expr, projection, filter)) {
		return false;
	}
	auto &memo = optimizer.GetMemo();
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	auto &left_group = memo.GetGroup(expr.children[0]);
	auto &right_group = memo.GetGroup(filter->children[0]);
	if (left_group.exprs.empty() || right_group.exprs.empty()) {
		return false;
	}

	{
		// The consumer decides SEMI versus ANTI: the two Apply nodes are identical, the negation
		// lives above them. Printed (and thus checked) here because this is where the group id is
		// known, and because the rewrite that follows has to take the consumer with it.
		GroupId consumer_group = INVALID_GROUP_ID;
		GroupExpr *consumer = nullptr;
		bool negated = false;
		if (memo.FindMarkConsumer(group, consumer_group, consumer, negated) && CascadeConfig::PrintPlans()) {
			Printer::Print(StringUtil::Format(
			    "--- cascade(cascades)   mark consumer: filter in group %llu negated=%d -> would become %s",
			    (unsigned long long)consumer_group, (int)negated, negated ? "ANTI" : "SEMI"));
		}
	}
	auto join = make_uniq<LogicalComparisonJoin>(apply.join_type);
	if (apply.join_type == JoinType::MARK) {
		join->mark_index = apply.mark_index;
	}
	for (auto &predicate : filter->op->expressions) {
		auto copy = predicate->Copy();
		auto &comparison = copy->Cast<BoundFunctionExpression>();
		auto &lhs = BoundComparisonExpression::LeftMutable(comparison);
		auto &rhs = BoundComparisonExpression::RightMutable(comparison);
		bool lhs_left = ApplyReadsBindings(*lhs, left_group.exprs[0]->bindings);
		bool lhs_right = ApplyReadsBindings(*lhs, right_group.exprs[0]->bindings);
		bool rhs_right = ApplyReadsBindings(*rhs, right_group.exprs[0]->bindings);
		if (lhs_right && !lhs_left && !rhs_right) {
			auto moved = std::move(lhs);
			lhs = std::move(rhs);
			rhs = std::move(moved);
			BoundComparisonExpression::FlipType(comparison);
		}
		join->conditions.emplace_back(std::move(lhs), std::move(rhs), comparison.GetExpressionType());
	}
	join->estimated_cardinality = expr.op->estimated_cardinality;
	join->has_estimated_cardinality = expr.op->has_estimated_cardinality;

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
