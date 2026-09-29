#include "duckdb/cascade/cascade_bindings.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

void RewriteExpressionBindings(unique_ptr<Expression> &expr, const BindingExport &exports) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &) {
		    for (auto &entry : exports) {
			    if (colref.Binding() == entry.first) {
				    colref.BindingMutable() = entry.second;
				    return;
			    }
		    }
	    });
}

void RewriteOperatorBindings(LogicalOperator &op, const BindingExport &exports) {
	for (auto &expr : op.expressions) {
		RewriteExpressionBindings(expr, exports);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// An aggregate keeps its grouping expressions in their own member, away from
		// op.expressions, so they need rewriting too.
		auto &aggregate = op.Cast<LogicalAggregate>();
		for (auto &expr : aggregate.groups) {
			RewriteExpressionBindings(expr, exports);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto &orders = (op.type == LogicalOperatorType::LOGICAL_ORDER_BY) ? op.Cast<LogicalOrder>().orders
		                                                                 : op.Cast<LogicalTopN>().orders;
		for (auto &order : orders) {
			RewriteExpressionBindings(order.expression, exports);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		for (auto &target : distinct.distinct_targets) {
			RewriteExpressionBindings(target, exports);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &condition = op.Cast<LogicalAnyJoin>().condition;
		if (condition) {
			RewriteExpressionBindings(condition, exports);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		// An Apply keeps its ON predicate in a member of its own, and the columns it
		// compares can move like any other expression's.
		auto &condition = op.Cast<LogicalDependentJoin>().condition;
		if (condition) {
			RewriteExpressionBindings(condition, exports);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			RewriteExpressionBindings(condition.LeftReference(), exports);
			RewriteExpressionBindings(condition.RightReference(), exports);
		}
		break;
	}
	default:
		break;
	}
}

void RewriteTreeBindings(LogicalOperator &op, const BindingExport &exports) {
	RewriteOperatorBindings(op, exports);
	for (auto &child : op.children) {
		RewriteTreeBindings(*child, exports);
	}
}

ColumnBinding MapBinding(const ColumnBinding &binding, const BindingExport &exports) {
	for (auto &entry : exports) {
		if (entry.first == binding) {
			return entry.second;
		}
	}
	return binding;
}

bool InBindings(const vector<ColumnBinding> &bindings, const ColumnBinding &binding) {
	for (auto &candidate : bindings) {
		if (candidate == binding) {
			return true;
		}
	}
	return false;
}

bool PassesBindingsThrough(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
