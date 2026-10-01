#include "duckdb/cascade/cascades/rules/expand_nary_join.hpp"
#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
namespace duckdb {

namespace {

//! Every column binding an expression reads.
void CollectBindings(const Expression &expr, column_binding_set_t &out) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		out.insert(expr.Cast<BoundColumnRefExpression>().Binding());
		return;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) { CollectBindings(child, out); });
}

//! Which of the children owns a binding, or -1 when none of them does.
int FindChild(const vector<vector<ColumnBinding>> &child_bindings, ColumnBinding binding) {
	for (idx_t i = 0; i < child_bindings.size(); i++) {
		for (auto &candidate : child_bindings[i]) {
			if (candidate == binding) {
				return int(i);
			}
		}
	}
	return -1;
}

//! The shared expansion: children in `order`, each condition on the join that introduces its last
//! input. Declines (returns false) when a condition cannot be placed or a step would be empty.
bool Expand(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr, const vector<idx_t> &order) {
	if (!expr.op || expr.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN || expr.children.size() < 3) {
		return false;
	}
	auto &join = expr.op->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::INNER) {
		// The expansion is only equivalent for an inner join: an outer join's null-extended rows
		// depend on the sides, which a different order would move.
		return false;
	}
	auto &memo = optimizer.GetMemo();
	vector<vector<ColumnBinding>> child_bindings;
	for (auto child : expr.children) {
		auto &below = memo.GetGroup(child);
		if (below.exprs.empty() || !below.exprs[0]->op) {
			return false;
		}
		child_bindings.push_back(below.exprs[0]->bindings);
	}
	// Position of each child within the chosen order.
	vector<idx_t> position(expr.children.size(), 0);
	for (idx_t i = 0; i < order.size(); i++) {
		position[order[i]] = i;
	}
	vector<vector<JoinCondition>> per_step(expr.children.size());
	for (auto &condition : join.conditions) {
		column_binding_set_t refs;
		CollectBindings(condition.GetLHS(), refs);
		if (condition.IsComparison()) {
			CollectBindings(condition.GetRHS(), refs);
		}
		idx_t step = 0;
		for (auto &binding : refs) {
			auto child = FindChild(child_bindings, binding);
			if (child < 0) {
				// A predicate reading something we cannot attribute to a child: leave the plan alone
				// rather than guess where it belongs.
				return false;
			}
			step = MaxValue<idx_t>(step, position[idx_t(child)]);
		}
		if (step == 0) {
			// A predicate on the first input alone: it can only be applied once a join exists.
			step = 1;
		}
		per_step[step].push_back(condition.Copy());
	}
	for (idx_t i = 1; i < order.size(); i++) {
		if (per_step[i].empty()) {
			// This step would join two inputs nothing connects: a cartesian product. Decline.
			return false;
		}
	}
	GroupId current = expr.children[order[0]];
	for (idx_t i = 1; i < order.size(); i++) {
		auto lower = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		lower->conditions = std::move(per_step[i]);
		if (i + 1 == order.size()) {
			optimizer.AddExpression(group, memo.MakeExpr(std::move(lower), {current, expr.children[order[i]]}));
		} else {
			auto fresh = memo.AddGroup();
			optimizer.AddExpression(fresh, memo.MakeExpr(std::move(lower), {current, expr.children[order[i]]}));
			current = fresh;
		}
	}
	return true;
}

//! Ordered indices 0..n-1.
vector<idx_t> NaturalOrder(idx_t n) {
	vector<idx_t> order;
	for (idx_t i = 0; i < n; i++) {
		order.push_back(i);
	}
	return order;
}

//! The children sorted by the rows their first expression reports; uncosted groups keep their place
//! at the end, which makes the order the natural one when nothing has been priced yet.
vector<idx_t> SmallestFirst(CascadesOptimizer &optimizer, GroupExpr &expr) {
	auto &memo = optimizer.GetMemo();
	vector<pair<double, idx_t>> by_rows;
	for (idx_t i = 0; i < expr.children.size(); i++) {
		auto &below = memo.GetGroup(expr.children[i]);
		double rows = (!below.exprs.empty() && below.exprs[0]->costed) ? below.exprs[0]->rows : double(1e18);
		by_rows.emplace_back(rows, i);
	}
	std::stable_sort(by_rows.begin(), by_rows.end());
	vector<idx_t> order;
	for (auto &entry : by_rows) {
		order.push_back(entry.second);
	}
	return order;
}

CascadesRulePromise PromiseForNAry(CascadesOptimizer &, GroupExpr &expr, const char *name) {
	string reason;
	if (!expr.op) {
		reason = "the join has no operator";
	} else if (expr.children.size() < 3) {
		reason = "the join has fewer than three inputs";
	} else if (expr.op->Cast<LogicalComparisonJoin>().join_type != JoinType::INNER) {
		reason = "the join is not an inner join";
	} else {
		return CascadesRulePromise::MEDIUM;
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) rule " + string(name) + ": " + reason);
	}
	return CascadesRulePromise::NONE;
}

} // namespace

// --- CXformExpandNAryJoin (EXformId 1): the inputs in the order they arrive ---------------------
ExpandNAryJoin::ExpandNAryJoin() : CascadesRule(CascadesRuleKind::EXPLORATION, "expand_nary_join") {}
bool ExpandNAryJoin::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN && expr.children.size() >= 3;
}
CascadesRulePromise ExpandNAryJoin::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	return PromiseForNAry(optimizer, expr, Name());
}
bool ExpandNAryJoin::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	return Expand(optimizer, group, expr, NaturalOrder(expr.children.size()));
}

// --- CXformExpandNAryJoinMinCard (EXformId 2): smallest input first -----------------------------
ExpandNAryJoinMinCard::ExpandNAryJoinMinCard()
    : CascadesRule(CascadesRuleKind::EXPLORATION, "expand_nary_join_min_card") {}
bool ExpandNAryJoinMinCard::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN && expr.children.size() >= 3;
}
CascadesRulePromise ExpandNAryJoinMinCard::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	return PromiseForNAry(optimizer, expr, Name());
}
bool ExpandNAryJoinMinCard::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	return Expand(optimizer, group, expr, SmallestFirst(optimizer, expr));
}

// --- CXformExpandNAryJoinDP (EXformId 3): the orders a DP would keep ----------------------------
// Bounded on purpose: every permutation of n inputs is offered up to a cap, so a wide join does not
// fill the memo with orders, and the cap is printed rather than silently applied.
ExpandNAryJoinDP::ExpandNAryJoinDP() : CascadesRule(CascadesRuleKind::EXPLORATION, "expand_nary_join_dp") {}
bool ExpandNAryJoinDP::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN && expr.children.size() >= 3 &&
	       expr.children.size() <= 4;
}
CascadesRulePromise ExpandNAryJoinDP::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	return PromiseForNAry(optimizer, expr, Name());
}
bool ExpandNAryJoinDP::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	auto order = NaturalOrder(expr.children.size());
	const idx_t MAX_ALTERNATIVES = 8;
	idx_t added = 0;
	idx_t generated = 0;
	// next_permutation walks every order; the cap keeps the memo from growing without bound.
	do {
		generated++;
		if (Expand(optimizer, group, expr, order)) {
			added++;
		}
		if (added >= MAX_ALTERNATIVES) {
			break;
		}
	} while (std::next_permutation(order.begin(), order.end()));
	if (CascadeConfig::PrintPlans() && generated > added) {
		Printer::Print("--- cascade(cascades) rule " + string(Name()) + ": offered " + std::to_string(added) +
		               " of " + std::to_string(generated) + " orders (some are not connected, cap " +
		               std::to_string(MAX_ALTERNATIVES) + ")");
	}
	return added > 0;
}

} // namespace duckdb
