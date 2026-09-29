#include "duckdb/cascade/apply_decorrelation.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

//! How often a column binding is read across a plan.
struct BindingUse {
	ColumnBinding binding;
	idx_t count = 0;
};

static void CountUses(const LogicalOperator &op, vector<BindingUse> &uses) {
	auto bump = [&](const ColumnBinding &binding) {
		for (auto &entry : uses) {
			if (entry.binding == binding) {
				entry.count++;
				return;
			}
		}
		uses.push_back(BindingUse {binding, 1});
	};
	auto bump_expression = [&](const Expression &expr) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    expr, [&](const BoundColumnRefExpression &colref) { bump(colref.Binding()); });
	};
	for (auto &expr : op.expressions) {
		bump_expression(*expr);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			bump_expression(condition.GetLHS());
			bump_expression(condition.GetRHS());
		}
		break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &condition = op.Cast<LogicalAnyJoin>().condition;
		if (condition) {
			bump_expression(*condition);
		}
		break;
	}
	default:
		break;
	}
	for (auto &child : op.children) {
		CountUses(*child, uses);
	}
}

static idx_t UseCount(const vector<BindingUse> &uses, const ColumnBinding &binding) {
	for (auto &entry : uses) {
		if (entry.binding == binding) {
			return entry.count;
		}
	}
	return 0;
}

static unique_ptr<LogicalOperator> SimplifyMarkerJoin(unique_ptr<LogicalOperator> op,
                                                      const vector<BindingUse> &uses) {
	for (auto &child : op->children) {
		child = SimplifyMarkerJoin(std::move(child), uses);
	}
	if (op->type != LogicalOperatorType::LOGICAL_FILTER || op->expressions.size() != 1 || op->children.size() != 1) {
		return op;
	}
	// The predicate is either the marker itself or its negation.
	bool negated = false;
	const Expression *marker = op->expressions[0].get();
	if (marker->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    marker->GetExpressionType() == ExpressionType::OPERATOR_NOT) {
		auto &not_expr = marker->Cast<BoundOperatorExpression>();
		if (not_expr.GetChildren().size() != 1) {
			return op;
		}
		marker = not_expr.GetChildren()[0].get();
		negated = true;
	}
	if (marker->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return op;
	}
	auto &child = *op->children[0];
	if (child.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return op;
	}
	auto &join = child.Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::MARK) {
		return op;
	}
	auto marker_binding = ColumnBinding(join.mark_index, ProjectionIndex(0));
	if (marker->Cast<BoundColumnRefExpression>().Binding() != marker_binding) {
		return op;
	}
	// Anything else reading the marker needs it to keep existing.
	if (UseCount(uses, marker_binding) != 1) {
		return op;
	}
	// `NOT mark` is anti-join shaped only while the marker is two-valued, which
	// holds exactly when every comparison is NULL-safe: then the marker is never
	// unknown and "no match" coincides with "not true". NOT IN over a list that
	// contains a NULL keeps a three-valued marker and has to stay a MARK join.
	if (negated) {
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			auto type = condition.GetComparisonType();
			if (type != ExpressionType::COMPARE_DISTINCT_FROM &&
			    type != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
				return op;
			}
		}
	}
	// The NULL-safety and the right-side NULL stripping exist only to keep the
	// marker two-valued. A semi/anti join evaluates its condition directly, so
	// plain equality carries the same meaning there, and the stripping filter this
	// join carries becomes redundant. DuckDB's SimplifyNullSafeSemiJoinConditions
	// makes the same trade.
	bool drop_null_filter = false;
	if (join.children[1]->type == LogicalOperatorType::LOGICAL_FILTER) {
		drop_null_filter = true;
		for (auto &expr : join.children[1]->expressions) {
			if (expr->GetExpressionClass() != ExpressionClass::BOUND_OPERATOR ||
			    expr->GetExpressionType() != ExpressionType::OPERATOR_IS_NOT_NULL) {
				drop_null_filter = false;
				break;
			}
		}
	}
	for (auto &condition : join.conditions) {
		if (condition.IsComparison() && condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			condition = JoinCondition(condition.LeftReference()->Copy(), condition.RightReference()->Copy(),
			                          ExpressionType::COMPARE_EQUAL);
		}
	}
	if (drop_null_filter) {
		join.children[1] = std::move(join.children[1]->children[0]);
	}
	join.join_type = negated ? JoinType::ANTI : JoinType::SEMI;
	return std::move(op->children[0]);
}

unique_ptr<LogicalOperator> SimplifyMarkerJoins(unique_ptr<LogicalOperator> plan) {
	if (!plan) {
		return plan;
	}
	vector<BindingUse> uses;
	CountUses(*plan, uses);
	return SimplifyMarkerJoin(std::move(plan), uses);
}

} // namespace duckdb
