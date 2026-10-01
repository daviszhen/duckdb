#include "duckdb/cascade/cascades/rules/collapse_project.hpp"
#include "duckdb/cascade/cascade_bindings.hpp"
#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
namespace duckdb {
CollapseProject::CollapseProject() : CascadesRule(CascadesRuleKind::EXPLORATION, "collapse_project") {}
bool CollapseProject::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_PROJECTION && expr.children.size() == 1;
}
namespace {
//! The inner projection, if the group below holds one that is a plain re-numbering.
GroupExpr *FindRenumbering(CascadesOptimizer &optimizer, GroupId below) {
	for (auto &candidate : optimizer.GetMemo().GetGroup(below).exprs) {
		if (candidate->type != LogicalOperatorType::LOGICAL_PROJECTION || !candidate->op ||
		    candidate->children.size() != 1) {
			continue;
		}
		auto &inner = candidate->op->Cast<LogicalProjection>();
		if (inner.expressions.empty()) {
			continue;
		}
		bool plain = true;
		for (auto &e : inner.expressions) {
			if (e->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF || e->IsVolatile()) {
				plain = false;
				break;
			}
		}
		if (!plain) {
			continue;
		}
		// Its own outputs must line up with the references the outer projection makes, which is the
		// map built in Apply; here only the shape is checked.
		if (candidate->bindings.size() != inner.expressions.size()) {
			continue;
		}
		return candidate.get();
	}
	return nullptr;
}
} // namespace
CascadesRulePromise CollapseProject::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	string reason;
	if (!expr.op) {
		reason = "the projection has no operator";
	} else if (!FindRenumbering(optimizer, expr.children[0])) {
		reason = "the child is not a plain re-numbering projection";
	} else {
		return CascadesRulePromise::MEDIUM;
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) rule " + string(Name()) + ": " + reason);
	}
	return CascadesRulePromise::NONE;
}
bool CollapseProject::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	auto inner = FindRenumbering(optimizer, expr.children[0]);
	if (!inner || !expr.op) {
		return false;
	}
	auto &memo = optimizer.GetMemo();
	auto &context = optimizer.GetContext();
	// inner output column i -> the binding its (plain) expression reads.
	BindingExport map;
	auto &inner_projection = inner->op->Cast<LogicalProjection>();
	for (idx_t i = 0; i < inner->bindings.size() && i < inner_projection.expressions.size(); i++) {
		map.emplace_back(inner->bindings[i],
		                 inner_projection.expressions[i]->Cast<BoundColumnRefExpression>().Binding());
	}
	// The outer expressions, re-pointed at the inner child; the inner projection disappears.
	auto &outer = expr.op->Cast<LogicalProjection>();
	vector<unique_ptr<Expression>> expressions;
	for (auto &e : outer.expressions) {
		auto copy = e->Copy();
		RewriteExpressionBindings(copy, map);
		expressions.push_back(std::move(copy));
	}
	auto replacement = make_uniq<LogicalProjection>(outer.table_index, std::move(expressions));
	replacement->ResolveOperatorTypes();
	optimizer.AddExpression(group, memo.MakeExpr(std::move(replacement), {inner->children[0]}));
	return true;
}
} // namespace duckdb
