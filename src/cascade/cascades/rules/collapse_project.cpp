#include "duckdb/cascade/cascades/rules/collapse_project.hpp"
#include "duckdb/cascade/cascade_bindings.hpp"
#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
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
		bool safe = true;
		for (auto &e : inner.expressions) {
			bool has_subquery = false;
			std::function<void(const Expression &)> scan = [&](const Expression &x) {
				if (x.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
					has_subquery = true;
					return;
				}
				ExpressionIterator::EnumerateChildren(x, [&](const Expression &child) { scan(child); });
			};
			scan(*e);
			if (has_subquery) {
				// A sub-query carries its own plan; moving it into the outer projection is not a
				// local rewrite, so decline it.
				safe = false;
				break;
			}
			// A projection maps rows one to one, so evaluating its expressions in the outer
			// projection instead evaluates them exactly as often - unless they are volatile.
			if (e->IsVolatile()) {
				safe = false;
				break;
			}
		}
		if (!safe) {
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
	// inner output column i -> the expression that produces it.
	auto &inner_projection = inner->op->Cast<LogicalProjection>();
	vector<pair<ColumnBinding, const Expression *>> map;
	for (idx_t i = 0; i < inner->bindings.size() && i < inner_projection.expressions.size(); i++) {
		map.emplace_back(inner->bindings[i], inner_projection.expressions[i].get());
	}
	// Substitute recursively: an outer reference to an inner output becomes the inner expression,
	// itself copied (and substituted again, should it read another inner output).
	std::function<void(unique_ptr<Expression> &)> substitute = [&](unique_ptr<Expression> &slot) {
		if (slot->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto binding = slot->Cast<BoundColumnRefExpression>().Binding();
			for (auto &entry : map) {
				if (entry.first == binding) {
					// The slot is replaced, not assigned: Expression has no copy assignment, and
					// replacing the pointer is what the operator tree expects anyway.
					slot = entry.second->Copy();
					substitute(slot);
					return;
				}
			}
			return;
		}
		ExpressionIterator::EnumerateChildren(*slot, [&](unique_ptr<Expression> &child) { substitute(child); });
	};
	auto &outer = expr.op->Cast<LogicalProjection>();
	vector<unique_ptr<Expression>> expressions;
	for (auto &e : outer.expressions) {
		auto copy = e->Copy();
		substitute(copy);
		expressions.push_back(std::move(copy));
	}
	auto replacement = make_uniq<LogicalProjection>(outer.table_index, std::move(expressions));
	replacement->ResolveOperatorTypes();
	optimizer.AddExpression(group, memo.MakeExpr(std::move(replacement), {inner->children[0]}));
	return true;
}
} // namespace duckdb
