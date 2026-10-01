#include "duckdb/cascade/cascades/rules/union_2_union_all.hpp"
#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
namespace duckdb {
Union2UnionAll::Union2UnionAll() : CascadesRule(CascadesRuleKind::EXPLORATION, "union_2_union_all") {}
bool Union2UnionAll::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_UNION && expr.children.size() >= 2;
}
CascadesRulePromise Union2UnionAll::Promise(CascadesOptimizer &, GroupExpr &expr) {
	string reason;
	if (!expr.op) {
		reason = "the set operation has no operator";
	} else if (expr.op->Cast<LogicalSetOperation>().setop_all) {
		reason = "the set operation already keeps duplicates";
	} else {
		return CascadesRulePromise::MEDIUM;
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) rule " + string(Name()) + ": " + reason);
	}
	return CascadesRulePromise::NONE;
}
bool Union2UnionAll::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	if (!expr.op || expr.type != LogicalOperatorType::LOGICAL_UNION) {
		return false;
	}
	auto copy = expr.op->Copy(optimizer.GetContext());
	auto &setop = copy->Cast<LogicalSetOperation>();
	if (setop.setop_all) {
		return false;
	}
	setop.setop_all = true;
	auto &memo = optimizer.GetMemo();
	// The union-all keeps the original's table index, so the columns the parents read are the same
	// ones; the distinct passes them through.
	auto all = memo.AddGroup();
	auto children = expr.children;
	optimizer.AddExpression(all, memo.MakeExpr(std::move(copy), std::move(children)));
	auto distinct = make_uniq<LogicalDistinct>(DistinctType::DISTINCT);
	optimizer.AddExpression(group, memo.MakeExpr(std::move(distinct), {all}));
	return true;
}
} // namespace duckdb
