#include "duckdb/cascade/cascades/rules/select_2_filter.hpp"
#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
namespace duckdb {
namespace {
bool ContainsSubquery(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!found && ContainsSubquery(child)) {
			found = true;
		}
	});
	return found;
}
} // namespace
Select2Filter::Select2Filter() : CascadesRule(CascadesRuleKind::EXPLORATION, "select_2_filter") {}
bool Select2Filter::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_FILTER;
}
CascadesRulePromise Select2Filter::Promise(CascadesOptimizer &, GroupExpr &expr) {
	if (!expr.op) {
		return CascadesRulePromise::NONE;
	}
	auto &filter = expr.op->Cast<LogicalFilter>();
	if (filter.expressions.empty()) {
		// The xform's whole point is that the predicate lives in this operator: an empty filter is the
		// invariant broken, so it is reported rather than passed over.
		if (CascadeConfig::PrintPlans()) {
			Printer::Print("--- cascade(cascades) rule " + string(Name()) +
			               ": a filter carries no predicate, which the Select-to-Filter invariant forbids");
		}
		return CascadesRulePromise::NONE;
	}
	for (auto &expression : filter.expressions) {
		if (ContainsSubquery(*expression)) {
			if (CascadeConfig::PrintPlans()) {
				Printer::Print("--- cascade(cascades) rule " + string(Name()) +
				               ": a filter predicate still holds a sub-query, so the Select-to-Filter "
				               "normalisation has not happened");
			}
			return CascadesRulePromise::NONE;
		}
	}
	return CascadesRulePromise::LOW;
}
bool Select2Filter::Apply(CascadesOptimizer &, GroupId, GroupExpr &) {
	// Nothing to rewrite: the invariant this rule migrated is the one the shape already satisfies. The
	// rule exists so that a violation is a check that runs, not a comment that stays true by hope.
	return false;
}
} // namespace duckdb
