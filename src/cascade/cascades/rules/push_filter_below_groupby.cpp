#include "duckdb/cascade/cascades/rules/push_filter_below_groupby.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascade_bindings.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

bool PushFilterBelowGroupBy::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_FILTER && expr.children.size() == 1;
}

namespace {

//! The shape this rule rewrites: a filter, an optional projection that only re-names the
//! aggregate's columns, and the GroupBy underneath.
struct FilterAboveGroupBy {
	//! The layer between the filter and the aggregate, when there is one. It has to stay: it owns
	//! a table index of its own, so dropping it would change the columns above.
	GroupExpr *projection = nullptr;
	GroupExpr *aggregate = nullptr;
};

bool IsGroupBy(const GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && !expr.children.empty();
}

bool DescribeShape(CascadesOptimizer &optimizer, GroupExpr &filter, FilterAboveGroupBy &shape) {
	auto &memo = optimizer.GetMemo();
	auto &child_group = memo.GetGroup(filter.children[0]);
	for (auto &candidate : child_group.exprs) {
		if (IsGroupBy(*candidate)) {
			shape.aggregate = candidate.get();
			return true;
		}
		if (candidate->type != LogicalOperatorType::LOGICAL_PROJECTION || candidate->children.empty()) {
			continue;
		}
		auto &inner_group = memo.GetGroup(candidate->children[0]);
		for (auto &inner : inner_group.exprs) {
			if (IsGroupBy(*inner)) {
				shape.projection = candidate.get();
				shape.aggregate = inner.get();
				return true;
			}
		}
	}
	return false;
}

//! The rebinding a predicate needs to live below the aggregate: every column it reads has to
//! become a column that exists there. Two hops: the projection (if any) names the aggregate's
//! output, and the aggregate's grouping columns name what is below it.
bool BuildPushdownMap(CascadesOptimizer &optimizer, GroupExpr &filter, const FilterAboveGroupBy &shape,
                      BindingExport &mapping) {
	auto &aggregate = shape.aggregate->op->Cast<LogicalAggregate>();
	if (shape.aggregate->children.size() != 1) {
		return false;
	}
	if (aggregate.grouping_sets.size() > 1) {
		// Grouping sets pad the columns a set does not mention with NULL, so a predicate on such a
		// column is *not* constant within a group and must not move:
		//
		//   SELECT a, count(*) FROM tgrp GROUP BY GROUPING SETS ((a),()) HAVING a IS NOT NULL
		//
		// pushed below the aggregate answers 0 rows instead of 3. The pipeline's
		// groupby_reorder.cpp carries the same guard, and it is the one DuckDB's own filter
		// pushdown carries.
		return false;
	}
	// Hop two: a grouping column's expression below the aggregate has to be a plain column.
	BindingExport below;
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		auto &group = *aggregate.groups[i];
		if (group.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
		below.emplace_back(ColumnBinding(aggregate.group_index, ProjectionIndex(i)),
		                   group.Cast<BoundColumnRefExpression>().Binding());
	}
	// Hop one, *composed* with hop two: the filter reads the projection's columns, the projection
	// names the aggregate's, and the aggregate's grouping columns name what is below it. The two
	// hops have to be folded into one entry per column - the rewriter takes the first match and
	// stops, so a flat two-hop mapping silently stops half way, leaving the predicate pointing at
	// the aggregate's output while it now sits below the aggregate.
	if (shape.projection) {
		auto &projection = shape.projection->op->Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			auto &expr = *projection.expressions[i];
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				// A computed column cannot be re-expressed below the aggregate, and this is the
				// ordinary case in this mode, not an edge case: the mandatory aggregate rewrites
				// put a projection computing CAST(...) between the filter and the GroupBy, so
				//     SELECT * FROM (SELECT k, sum(v) AS s FROM t GROUP BY k) x WHERE k = 1
				// declines here. That is deliberate - the predicate reads a computed column, and
				// assuming the cast is a no-op is exactly the kind of assumption a rewriter must
				// not make. It shows up as no-effect (the task ran), not as rejected (the promise
				// saw the shape), which is how the two are told apart in the statistics.
				return false;
			}
			auto inner = expr.Cast<BoundColumnRefExpression>().Binding();
			// The column the projection reads has to be a *grouping* column: an aggregate's result
			// does not exist below the aggregate, and that is the whole reason the rule has a
			// condition. Without this check
			//
			//   ... JOIN (SELECT k, sum(v) AS t FROM tr GROUP BY k) g ON ts.a = g.k AND g.t > 100
			//
			// moved `g.t > 100` below the aggregate, where `t` is exactly the sum it is computed
			// from - a dangling reference, and the test exists to catch it.
			if (inner.table_index != aggregate.group_index) {
				return false;
			}
			mapping.emplace_back(ColumnBinding(projection.table_index, ProjectionIndex(i)), MapBinding(inner, below));
		}
	}
	for (auto &entry : below) {
		mapping.push_back(entry);
	}
	// Every column the predicate reads has to be covered by the two hops.
	bool ok = true;
	for (auto &filter_expr : filter.op->expressions) {
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

//! The aggregate, rebuilt with its indices kept - an alternative in a group has to expose exactly
//! the group's columns - and with the estimate the host gave the original.
unique_ptr<LogicalOperator> RebuildAggregate(const LogicalAggregate &aggregate, GroupId filter_group) {
	vector<unique_ptr<Expression>> select_list;
	for (auto &expression : aggregate.expressions) {
		select_list.push_back(expression->Copy());
	}
	auto pushed = make_uniq<LogicalAggregate>(aggregate.group_index, aggregate.aggregate_index, std::move(select_list));
	for (auto &group_expression : aggregate.groups) {
		pushed->groups.push_back(group_expression->Copy());
	}
	pushed->grouping_sets = aggregate.grouping_sets;
	pushed->estimated_cardinality = aggregate.estimated_cardinality;
	// children stay empty: the memo attaches them, and MakeExpr clears the field again.
	return std::move(pushed);
}

} // namespace

CascadesRulePromise PushFilterBelowGroupBy::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	// ORCA's Exfp(): answer "not applicable" where the shape is simply not there, so the task is
	// never queued. Whether the predicate can be *rewritten* below the aggregate is a deeper
	// question, answered while building the replacement - that one is a no-effect, not a
	// rejection, and the counters keep the two apart.
	FilterAboveGroupBy shape;
	if (!DescribeShape(optimizer, expr, shape)) {
		return CascadesRulePromise::NONE;
	}
	return CascadesRulePromise::MEDIUM;
}

bool PushFilterBelowGroupBy::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	FilterAboveGroupBy shape;
	if (!DescribeShape(optimizer, expr, shape)) {
		// The promise should have rejected this one before it was queued.
		return false;
	}
	BindingExport mapping;
	if (!BuildPushdownMap(optimizer, expr, shape, mapping)) {
		if (CascadeConfig::PrintPlans()) {
			Printer::Print("--- cascade(cascades) rule " + string(Name()) +
			               ": the predicate cannot be expressed below the aggregate");
		}
		return false;
	}
	auto &memo = optimizer.GetMemo();
	auto &aggregate = shape.aggregate->op->Cast<LogicalAggregate>();

	// The predicate, re-expressed in the columns that exist below the aggregate.
	auto filter = make_uniq<LogicalFilter>();
	filter->estimated_cardinality = expr.op->estimated_cardinality;
	for (auto &filter_expr : expr.op->expressions) {
		auto copy = filter_expr->Copy();
		RewriteExpressionBindings(copy, mapping);
		filter->expressions.push_back(std::move(copy));
	}
	auto filter_group = memo.AddGroup();
	optimizer.AddExpression(filter_group, memo.MakeExpr(std::move(filter), {shape.aggregate->children[0]}));

	auto pushed = RebuildAggregate(aggregate, filter_group);
	if (!shape.projection) {
		// Filter(GroupBy(X)) -> the group can also be built as GroupBy(Filter(X)): the filter passes
		// the aggregate's columns through, so both produce the same ones.
		optimizer.AddExpression(group, memo.MakeExpr(std::move(pushed), {filter_group}));
		return true;
	}
	// With a projection in between the filter cannot simply move: the projection owns a table
	// index of its own, so it stays and the aggregate below it is replaced by
	// GroupBy(Filter(X)). The alternative is then Projection(GroupBy(Filter(X))), which exposes
	// the projection's columns exactly as the original did.
	auto aggregate_group = memo.AddGroup();
	optimizer.AddExpression(aggregate_group, memo.MakeExpr(std::move(pushed), {filter_group}));

	auto &projection = shape.projection->op->Cast<LogicalProjection>();
	vector<unique_ptr<Expression>> select_list;
	for (auto &projection_expression : projection.expressions) {
		select_list.push_back(projection_expression->Copy());
	}
	auto rebuilt = make_uniq<LogicalProjection>(projection.table_index, std::move(select_list));
	rebuilt->estimated_cardinality = projection.estimated_cardinality;
	optimizer.AddExpression(group, memo.MakeExpr(std::move(rebuilt), {aggregate_group}));
	return true;
}

} // namespace duckdb
