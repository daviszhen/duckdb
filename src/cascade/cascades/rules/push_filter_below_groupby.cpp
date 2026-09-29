#include "duckdb/cascade/cascades/rules/push_filter_below_groupby.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

bool PushFilterBelowGroupBy::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_FILTER && expr.children.size() == 1;
}

//! The aggregate this filter sits on, if the child group is one.
static GroupExpr *GroupByBelow(CascadesOptimizer &optimizer, GroupExpr &expr) {
	auto &memo = optimizer.GetMemo();
	if (expr.children.empty()) {
		return nullptr;
	}
	auto &group = memo.GetGroup(expr.children[0]);
	for (auto &candidate : group.exprs) {
		if (candidate->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && !candidate->children.empty()) {
			return candidate.get();
		}
	}
	return nullptr;
}

//! The mapping from the aggregate's output to what the same column is called below it, for the
//! columns a predicate may be moved under. Returns false as soon as the predicate reads
//! something that cannot be expressed below the aggregate - the promise then reports NONE.
static bool BuildPushdownMap(CascadesOptimizer &optimizer, GroupExpr &expr, GroupExpr &aggregate,
                             BindingExport &mapping) {
	if (aggregate.children.size() != 1) {
		return false;
	}
	auto &agg = aggregate.op->Cast<LogicalAggregate>();
	// Every grouping column has to be a plain column below the aggregate, or the predicate cannot
	// be re-expressed there. (The paper allows any column determined by A; this version does not
	// yet know about those.)
	for (idx_t i = 0; i < agg.groups.size(); i++) {
		auto &group = *agg.groups[i];
		if (group.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			mapping.clear();
			return false;
		}
		mapping.emplace_back(ColumnBinding(agg.group_index, ProjectionIndex(i)),
		                     group.Cast<BoundColumnRefExpression>().Binding());
	}
	// Every column the predicate reads has to be one of them.
	bool ok = true;
	for (auto &filter_expr : expr.op->expressions) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *filter_expr, [&](const BoundColumnRefExpression &colref) {
			    for (auto &entry : mapping) {
				    if (entry.first == colref.Binding()) {
					    return;
				    }
			    }
			    ok = false;
		    });
	}
	return ok;
}

CascadesRulePromise PushFilterBelowGroupBy::Promise(GroupExpr &expr) {
	// Only the promise can see the rest of the memo, so it is evaluated in Apply; the queue
	// ordering only needs the cheap match.
	return CascadesRulePromise::MEDIUM;
}

bool PushFilterBelowGroupBy::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	auto aggregate_expr = GroupByBelow(optimizer, expr);
	if (!aggregate_expr) {
		if (CascadeConfig::PrintPlans()) {
			Printer::Print("--- cascade(cascades) rule " + string(Name()) + ": no aggregate below the filter");
		}
		return false;
	}
	BindingExport mapping;
	if (!BuildPushdownMap(optimizer, expr, *aggregate_expr, mapping)) {
		if (CascadeConfig::PrintPlans()) {
			Printer::Print("--- cascade(cascades) rule " + string(Name()) +
			               ": the predicate cannot be expressed below the aggregate");
		}
		return false;
	}
	auto &memo = optimizer.GetMemo();
	auto &agg = aggregate_expr->op->Cast<LogicalAggregate>();

	// The predicate, re-expressed in the columns that exist below the aggregate.
	auto filter = make_uniq<LogicalFilter>();
	// The pushed predicate is the same predicate, so it keeps the estimate the host gave it. A
	// rebuilt operator with no estimate at all would be costed as one row and win for the wrong
	// reason - which is exactly what happened before this line existed.
	filter->estimated_cardinality = expr.op->estimated_cardinality;
	for (auto &filter_expr : expr.op->expressions) {
		auto copy = filter_expr->Copy();
		RewriteExpressionBindings(copy, mapping);
		filter->expressions.push_back(std::move(copy));
	}
	auto filter_group = memo.AddGroup();
	optimizer.AddExpression(filter_group, Memo::MakeExpr(std::move(filter), {aggregate_expr->children[0]}));

	// The aggregate itself is rebuilt rather than copied - the host implements Copy per operator
	// and the aggregate's expressions are what matter - with its indices kept, because an
	// alternative in a group has to expose exactly the group's columns.
	vector<unique_ptr<Expression>> select_list;
	for (auto &aggregate_expression : agg.expressions) {
		select_list.push_back(aggregate_expression->Copy());
	}
	auto pushed = make_uniq<LogicalAggregate>(agg.group_index, agg.aggregate_index, std::move(select_list));
	for (auto &group_expression : agg.groups) {
		pushed->groups.push_back(group_expression->Copy());
	}
	pushed->grouping_sets = agg.grouping_sets;
	pushed->estimated_cardinality = agg.estimated_cardinality;
	optimizer.AddExpression(group, Memo::MakeExpr(std::move(pushed), {filter_group}));
	return true;
}

} // namespace duckdb
