// Section 3.1's GroupBy pull-up: the aggregate moves above the join, so the join reduces the
// rows the aggregate reads instead of the aggregate reducing the rows the join reads.
//
//   S |>_p (G_{A,F} R) = G_{A + cols(S), F}(S |>_p R)      S has to be keyed
//
// It removes the global aggregation rather than keeping it, so it is a cost decision and off
// by default. It is also the primitive section 3.4.2 executes:
//
//   (R SA_A E) |>_p T = (R |>_p T) SA_{A + cols(T)} E
//
// Paper: Galindo-Legaria & Joshi (SIGMOD 2001), sections 3.1 and 3.4.2.
// Switch: DUCKDB_CASCADE_AGG_PULLUP (off).
//===----------------------------------------------------------------------===//

#include "duckdb/cascade/aggregate_pullup.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascade_keys.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

namespace {

using PullupBindings = vector<std::pair<ColumnBinding, ColumnBinding>>;

void PullupRemapBindings(unique_ptr<Expression> &expr, const PullupBindings &map) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &) {
		    for (auto &entry : map) {
			    if (colref.Binding() == entry.first) {
				    colref.BindingMutable() = entry.second;
				    return;
			    }
		    }
	    });
}

//! Replace the columns a projection above the GroupBy produces by the expressions that
//! compute them, and the grouping columns by the expressions that computed them. A derived
//! table can be layered - DuckDB's optimizer leaves several projections stacked - so one
//! pass may expose a column the next projection renames; the loop repeats until nothing
//! changes, which takes at most as many passes as there are projections.
void PullupSubstituteThrough(unique_ptr<Expression> &expr, const vector<LogicalProjection *> &projections,
                             TableIndex group_index, const vector<unique_ptr<Expression>> &groups) {
	for (idx_t pass = 0; pass <= projections.size(); pass++) {
		bool changed = false;
		for (auto *projection : projections) {
			ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
			    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &owner) {
				    if (colref.Binding().table_index != projection->table_index) {
					    return;
				    }
				    auto index = colref.Binding().column_index.GetIndexUnsafe();
				    if (index >= projection->expressions.size()) {
					    return;
				    }
				    owner = projection->expressions[index]->Copy();
				    changed = true;
			    });
		}
		ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
		    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &owner) {
			    if (colref.Binding().table_index != group_index) {
				    return;
			    }
			    auto index = colref.Binding().column_index.GetIndexUnsafe();
			    if (index >= groups.size()) {
				    return;
			    }
			    owner = groups[index]->Copy();
			    changed = true;
		    });
		if (!changed) {
			return;
		}
	}
}

//! Does the expression reach one of the aggregate's own results, following the projections
//! that stand between it and the GroupBy? The join predicate may not read those, because
//! below the join they do not exist. Asked before anything is mutated, so that giving up


//! leaves the plan exactly as it was.
bool PullupReadsAggregateResult(const Expression &expr, const vector<LogicalProjection *> &projections,
                                TableIndex aggregate_index) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		if (colref.Binding().table_index == aggregate_index) {
			return true;
		}
		for (auto *projection : projections) {
			if (colref.Binding().table_index != projection->table_index) {
				continue;
			}
			auto index = colref.Binding().column_index.GetIndexUnsafe();
			if (index >= projection->expressions.size()) {
				return false;
			}
			return PullupReadsAggregateResult(*projection->expressions[index], projections, aggregate_index);
		}
		return false;
	}
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    if (PullupReadsAggregateResult(colref, projections, aggregate_index)) {
			    found = true;
		    }
	    });
	return found;
}

//! Section 3.1's premise, in the form the catalog can answer: "the relation being joined has
//! a key". A key is enough on its own, because it makes that relation's rows distinct on the
//! columns the pulled-up grouping carries - the paper states the premise for exactly that
//! reason: with a duplicate there, two rows would fall into one group and the rows it
//! aggregates would be counted twice. (An earlier version also required the predicate to
//! equate the key; that is sufficient too, but narrower than the paper.)
bool PullupKeptSideIsKeyed(LogicalOperator &kept) {
	return !CascadeSideKey(kept).empty();
}

void PullupRewriteOperatorBindings(LogicalOperator &op, const PullupBindings &map) {
	for (auto &expr : op.expressions) {
		PullupRemapBindings(expr, map);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			PullupRemapBindings(condition.LeftReference(), map);
			PullupRemapBindings(condition.RightReference(), map);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &condition = op.Cast<LogicalAnyJoin>().condition;
		if (condition) {
			PullupRemapBindings(condition, map);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		for (auto &group : op.Cast<LogicalAggregate>().groups) {
			PullupRemapBindings(group, map);
		}
		break;
	}
	default:
		break;
	}
	// These keep the expressions that name their child's columns in members of their own,
	// away from op.expressions, and they hand those columns on unchanged - so a rewrite
	// below them stays visible above, and the new bindings have to be written down.
	// (The other rules in this directory rewrite the same way; this one closes the gap
	// because its exports name columns an ancestor can read directly.)
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		for (auto &order : op.Cast<LogicalOrder>().orders) {
			PullupRemapBindings(order.expression, map);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		for (auto &order : op.Cast<LogicalTopN>().orders) {
			PullupRemapBindings(order.expression, map);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		for (auto &target : distinct.distinct_targets) {
			PullupRemapBindings(target, map);
		}
		if (distinct.order_by) {
			for (auto &order : distinct.order_by->orders) {
				PullupRemapBindings(order.expression, map);
			}
		}
		break;
	}
	default:
		break;
	}
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		for (auto &column : op.Cast<LogicalComparisonJoin>().duplicate_eliminated_columns) {
			PullupRemapBindings(column, map);
		}
	}
}

bool PullupPassesBindingsThrough(const LogicalOperator &op) {
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

//! The GroupBy side of a join: the GroupBy itself, the projection that shapes a derived
//! table's output above it, and the selections DuckDB keeps around the two.
struct PullupGroupBySide {
	LogicalAggregate *aggregate = nullptr;
	//! The projections between the join and the GroupBy, outermost first. A derived table can
	//! be layered, and every layer renames the GroupBy's columns for the layer above it.
	vector<LogicalProjection *> projections;
	//! Whose children[0] is the GroupBy, or nothing when the GroupBy is the join's child
	//! itself. That slot is where the GroupBy's own input has to end up.
	optional_ptr<LogicalOperator> holder;
	//! The selections in that chain. They pass bindings through, so they stay where they are
	//! once the GroupBy has moved, and their own expressions are rewritten where they name
	//! the GroupBy's columns (rewriting one that names a projection is a no-op).
	vector<LogicalFilter *> filters;
};

//! Recognise that shape, or report that this side is not a GroupBy at all. Selections and
//! projections can be stacked in any order between the join and the GroupBy - HAVING
//! arrives as a filter below the projection and DuckDB's optimizer moves it above, and the
//! projection that shapes a derived table's output is itself often two deep.
bool PullupDescribeSide(LogicalOperator &op, PullupGroupBySide &side) {
	auto current = &op;
	optional_ptr<LogicalOperator> holder;
	vector<LogicalFilter *> filters;
	vector<LogicalProjection *> projections;
	while (true) {
		if (current->type == LogicalOperatorType::LOGICAL_FILTER && current->children.size() == 1) {
			filters.push_back(&current->Cast<LogicalFilter>());
		} else if (current->type == LogicalOperatorType::LOGICAL_PROJECTION && current->children.size() == 1) {
			projections.push_back(&current->Cast<LogicalProjection>());
		} else {
			break;
		}
		holder = current;
		current = current->children[0].get();
	}
	if (current->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || current->children.size() != 1) {
		return false;
	}
	side.aggregate = &current->Cast<LogicalAggregate>();
	side.projections = std::move(projections);
	side.holder = holder;
	side.filters = std::move(filters);
	return true;
}

} // namespace

AggregatePullup::AggregatePullup(Binder &binder_p, ClientContext &context_p)
    : binder(binder_p), context(context_p) {
}

//! Section 3.1 pull-up:  S |>_p (G_{A,F} R) = G_{A + cols(S), F}(S |>_p R)   (S keyed).
//! It is also the primitive section 3.4.2 executes; the file banner has the details.
unique_ptr<LogicalOperator> AggregatePullup::PullNode(
    unique_ptr<LogicalOperator> op, vector<std::pair<ColumnBinding, ColumnBinding>> &exports) {
	// A materialized CTE is DuckDB's common-subplan sharing: one definition, several
	// references that read it by position. A rewrite inside the definition would have to be
	// reflected in every reference's column list, which this pass does not do - and the
	// optimizer's `__common_subplan_*` nodes are exactly where that used to crash
	// (`Failed to bind column reference ...`, TPC-DS q65). So a CTE is a barrier.
	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
	    op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		return op;
	}
	for (auto &child : op->children) {
		vector<std::pair<ColumnBinding, ColumnBinding>> child_exports;
		child = PullNode(std::move(child), child_exports);
		if (child_exports.empty()) {
			continue;
		}
		PullupRewriteOperatorBindings(*op, child_exports);
		// A join describes its output with *positions* into its children's bindings. One of
		// those children just changed shape (the pull-up replaced a GroupBy with its input and
		// put the new GroupBy above the join), so a map kept from before selects the wrong
		// columns - which is how a column can go missing in the middle of a binding list
		// (`Failed to bind "a" [15.1]`, bindings expose `[15.0]` and `[15.2]`). Dropping the
		// maps only makes the join expose every column of both children, and every reference
		// above it is by binding, so it stays valid.
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
		    op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN ||
		    op->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			auto &join = op->Cast<LogicalJoin>();
			join.left_projection_map.clear();
			join.right_projection_map.clear();
		}
		if (PullupPassesBindingsThrough(*op)) {
			for (auto &entry : child_exports) {
				exports.push_back(entry);
			}
		}
	}
	if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return op;
	}
	auto &join = op->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::INNER || join.children.size() != 2) {
		return op;
	}

	// One side is the GroupBy - possibly under the projection that shapes a derived
	// table's output, and under the selections DuckDB puts there (its join filter pushdown
	// adds one, and HAVING is planned as one) - and the other is the relation whose key
	// makes the pulled-up grouping land on one row per outer row. A selection commutes
	// with the join that moves below it, so it stays exactly where it is, above the
	// GroupBy once that has moved; a LIMIT or an ORDER BY would not commute and so is not
	// matched at all.
	idx_t aggregate_side = DConstants::INVALID_INDEX;
	PullupGroupBySide side;
	for (idx_t candidate_side = 0; candidate_side < 2; candidate_side++) {
		PullupGroupBySide candidate;
		if (!PullupDescribeSide(*join.children[candidate_side], candidate)) {
			continue;
		}
		// Grouping sets enumerate their own members, so an aggregate that has more than one
		// of them is left alone rather than extended.
		if (candidate.aggregate->grouping_sets.size() > 1) {
			continue;
		}
		if (!PullupKeptSideIsKeyed(*join.children[1 - candidate_side])) {
			continue;
		}
		aggregate_side = candidate_side;
		side = candidate;
		break;
	}
	if (aggregate_side == DConstants::INVALID_INDEX) {
		return op;
	}
	auto &old_aggregate = *side.aggregate;

	// Everything below is decided before the tree is touched, so that giving up leaves the
	// plan exactly as it was.
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			return op;
		}
		// The predicate has to be answerable below the GroupBy, so it may not read the
		// aggregate's results: those do not exist there any more.
		if (PullupReadsAggregateResult(condition.GetLHS(), side.projections, old_aggregate.aggregate_index) ||
		    PullupReadsAggregateResult(condition.GetRHS(), side.projections, old_aggregate.aggregate_index)) {
			return op;
		}
	}
	auto &kept = *join.children[1 - aggregate_side];
	kept.ResolveOperatorTypes();
	auto kept_bindings = kept.GetColumnBindings();
	if (kept.types.size() != kept_bindings.size()) {
		return op;
	}
	auto kept_types = kept.types;
	auto old_group_index = old_aggregate.group_index;
	auto old_aggregate_index = old_aggregate.aggregate_index;
	auto group_count = old_aggregate.groups.size();
	auto aggregate_count = old_aggregate.expressions.size();

	// The predicate now compares against the raw inner relation, so the columns the old
	// GroupBy and the projections above it produced are substituted back by the expressions
	// that computed them.
	for (auto &condition : join.conditions) {
		PullupSubstituteThrough(condition.LeftReference(), side.projections, old_group_index, old_aggregate.groups);
		PullupSubstituteThrough(condition.RightReference(), side.projections, old_group_index, old_aggregate.groups);
	}

	// Move what the new GroupBy is built from out of the old one while it is still alive: the
	// next step destroys the operator that holds it.
	vector<unique_ptr<Expression>> old_groups = std::move(old_aggregate.groups);
	vector<unique_ptr<Expression>> old_expressions = std::move(old_aggregate.expressions);
	auto aggregate_input = std::move(old_aggregate.children[0]);

	auto group_index = binder.GenerateTableIndex();
	auto aggregate_index = binder.GenerateTableIndex();
	PullupBindings new_positions;
	for (idx_t i = 0; i < group_count; i++) {
		new_positions.emplace_back(ColumnBinding(old_group_index, ProjectionIndex(i)),
		                           ColumnBinding(group_index, ProjectionIndex(i)));
	}
	for (idx_t i = 0; i < aggregate_count; i++) {
		new_positions.emplace_back(ColumnBinding(old_aggregate_index, ProjectionIndex(i)),
		                           ColumnBinding(aggregate_index, ProjectionIndex(i)));
	}
	// Whatever named the GroupBy's own columns now sits above the new GroupBy, whose columns
	// have moved. Rewriting a layer that names a projection instead is a no-op, because a
	// projection keeps its table index and its output positions - which is also why the
	// layers above it need nothing.
	for (auto *filter : side.filters) {
		for (auto &expr : filter->expressions) {
			PullupRemapBindings(expr, new_positions);
		}
	}
	for (auto *projection : side.projections) {
		for (auto &expr : projection->expressions) {
			PullupRemapBindings(expr, new_positions);
		}
	}

	auto pulled = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(old_expressions));
	for (auto &group : old_groups) {
		pulled->groups.push_back(std::move(group));
	}
	for (idx_t i = 0; i < kept_bindings.size(); i++) {
		pulled->groups.push_back(make_uniq<BoundColumnRefExpression>(kept_types[i], kept_bindings[i]));
	}
	// A grouping set lists its members explicitly, so the columns just added have to join it;
	// left alone, the physical aggregate would pad them with NULL.
	for (auto &set : pulled->grouping_sets) {
		for (idx_t i = 0; i < kept_bindings.size(); i++) {
			set.insert(ProjectionIndex(group_count + i));
		}
	}

	// The join keeps both relations: the GroupBy's place is taken by its own input. The
	// GroupBy's side is held on to first, because the selections and the projection above it
	// survive the move and are re-attached to the new GroupBy below.
	auto side_owner = std::move(join.children[aggregate_side]);
	join.children[aggregate_side] = std::move(aggregate_input);
	join.left_projection_map.clear();
	join.right_projection_map.clear();
	pulled->children.push_back(std::move(op));

	if (!side.holder) {
		// The GroupBy was the join's child: it is the join's input now, and the new GroupBy
		// takes its place above.
		for (auto &entry : new_positions) {
			exports.push_back(entry);
		}
		for (idx_t i = 0; i < kept_bindings.size(); i++) {
			exports.emplace_back(kept_bindings[i], ColumnBinding(group_index, ProjectionIndex(group_count + i)));
		}
		return std::move(pulled);
	}

	if (side.projections.empty()) {
		// Selections above the GroupBy: they keep their own bindings and keep passing the
		// GroupBy's on, which is what the exports record.
		for (auto &entry : new_positions) {
			exports.push_back(entry);
		}
		for (idx_t i = 0; i < kept_bindings.size(); i++) {
			exports.emplace_back(kept_bindings[i], ColumnBinding(group_index, ProjectionIndex(group_count + i)));
		}
		side.holder->children[0] = std::move(pulled);
		return std::move(side_owner);
	}

	// The projections ride above the new GroupBy, innermost first, and each one keeps its own
	// output bindings - so whoever read them still does, including any selection above them.
	// The kept relation's columns have to be carried up through every layer for whoever read
	// those, and the innermost layer is the one the new GroupBy is attached to.
	auto lengths = vector<idx_t>();
	for (auto *projection : side.projections) {
		lengths.push_back(projection->expressions.size());
	}
	vector<ColumnBinding> exposed;
	for (idx_t i = 0; i < kept_bindings.size(); i++) {
		exposed.emplace_back(group_index, ProjectionIndex(group_count + i));
	}
	for (idx_t layer = side.projections.size(); layer > 0; layer--) {
		auto &projection = *side.projections[layer - 1];
		for (idx_t i = 0; i < kept_bindings.size(); i++) {
			projection.expressions.push_back(make_uniq<BoundColumnRefExpression>(kept_types[i], exposed[i]));
			exposed[i] = ColumnBinding(projection.table_index, ProjectionIndex(lengths[layer - 1] + i));
		}
	}
	side.holder->children[0] = std::move(pulled);
	for (idx_t i = 0; i < kept_bindings.size(); i++) {
		exports.emplace_back(kept_bindings[i], exposed[i]);
	}
	return std::move(side_owner);
}

unique_ptr<LogicalOperator> AggregatePullup::Pull(unique_ptr<LogicalOperator> plan) {
	vector<std::pair<ColumnBinding, ColumnBinding>> exports;
	auto result = PullNode(std::move(plan), exports);
	result->ResolveOperatorTypes();
	return result;
}

} // namespace duckdb
