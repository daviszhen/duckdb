#include "duckdb/cascade/cascades/rules/binder_side_invariants.hpp"
#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
namespace duckdb {
namespace {
bool HoldsSubquery(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!found && HoldsSubquery(child)) {
			found = true;
		}
	});
	return found;
}

bool ExpressionsHoldSubquery(const vector<unique_ptr<Expression>> &expressions) {
	for (auto &expression : expressions) {
		if (HoldsSubquery(*expression)) {
			return true;
		}
	}
	return false;
}
} // namespace

bool BinderSideInvariantRule::Matches(GroupExpr &expr) {
	return expr.type == watched;
}

CascadesRulePromise BinderSideInvariantRule::Promise(CascadesOptimizer &, GroupExpr &expr) {
	if (!expr.op) {
		return CascadesRulePromise::NONE;
	}
	string violation;
	switch (invariant) {
	case BinderSideInvariant::HAS_EXPRESSIONS: {
		auto &unnest = expr.op->Cast<LogicalUnnest>();
		if (unnest.expressions.empty()) {
			violation = "an Unnest carries no expression";
		}
		break;
	}
	case BinderSideInvariant::NO_SUBQUERY: {
		if (expr.type == LogicalOperatorType::LOGICAL_FILTER) {
			if (ExpressionsHoldSubquery(expr.op->Cast<LogicalFilter>().expressions)) {
				violation = "a filter predicate still holds a sub-query";
			}
		} else if (expr.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			// A join whose conditions still hold a sub-query is the shape ORCA's SubqJoin2Apply and
			// InnerJoin2IndexGetApply remove, so seeing one means the normalisation did not run.
			auto &join = expr.op->Cast<LogicalComparisonJoin>();
			for (auto &condition : join.conditions) {
				if (HoldsSubquery(condition.GetLHS()) ||
				    (condition.IsComparison() && HoldsSubquery(condition.GetRHS()))) {
					violation = "a join condition still holds a sub-query";
					break;
				}
			}
		} else if (ExpressionsHoldSubquery(expr.op->Cast<LogicalProjection>().expressions)) {
			violation = "a projection expression still holds a sub-query";
		}
		break;
	}
	case BinderSideInvariant::GET_HAS_ACCESS_PATH: {
		// An index or dynamic get only exists once an access path has been chosen for it; a get
		// without one is the invariant broken, not a shape to rewrite.
		auto &get = expr.op->Cast<LogicalGet>();
		if (get.function.name.empty()) {
			violation = "a get has no access path bound";
		}
		break;
	}
	case BinderSideInvariant::TWO_INPUTS:
		if (expr.children.size() != 2) {
			violation = "an Apply does not have its two inputs";
		}
		break;
	}
	if (!violation.empty()) {
		if (CascadeConfig::PrintPlans()) {
			Printer::Print("--- cascade(cascades) rule " + string(Name()) + ": " + violation +
			               " (the binder-side normalisation this rule migrated has not happened)");
		}
		return CascadesRulePromise::NONE;
	}
	return CascadesRulePromise::LOW;
}

bool BinderSideInvariantRule::Apply(CascadesOptimizer &, GroupId, GroupExpr &) {
	// The shape already satisfies what this rule migrated: what is left is that the check runs.
	return false;
}
} // namespace duckdb
