#include "duckdb/cascade/apply_decorrelation.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascade_correlation.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

ApplyDecorrelator::ApplyDecorrelator(Binder &binder_p, ClientContext &context_p)
    : binder(binder_p), context(context_p) {
}

vector<ColumnBinding> ApplyDecorrelator::SideKey(LogicalOperator &side) {
	auto key = CascadeSideKey(side);
	if (!key.empty() || side.type != LogicalOperatorType::LOGICAL_CTE_REF) {
		return key;
	}
	auto entry = cte_key_positions.find(side.Cast<LogicalCTERef>().cte_index.index);
	if (entry == cte_key_positions.end()) {
		return key;
	}
	// A reference exposes the CTE's columns in the order they were materialised, which is the
	// order of the relation the key positions were taken from.
	vector<ColumnBinding> result;
	auto ref_bindings = side.GetColumnBindings();
	for (auto position : entry->second) {
		if (position < ref_bindings.size()) {
			result.push_back(ref_bindings[position]);
		}
	}
	return result;
}

unique_ptr<LogicalOperator> ApplyDecorrelator::DecorrelateApply(unique_ptr<LogicalOperator> op, BindingExport &exports) {
	auto &apply = op->Cast<LogicalDependentJoin>();
	const auto &correlated = apply.correlated_columns;
	auto join_type = apply.join_type;
	auto mark_index = apply.mark_index;
	auto any_join = apply.any_join;

	// Identities (5) and (6) of Figure 4: an Apply distributes over a set operation.
	//     R A_x (E1 u E2) = (R A_x E1) u (R A_x E2)
	//     R A_x (E1 - E2) = (R A_x E1) - (R A_x E2)
	// (and the same with n for intersect). This is the paper's Class 2: each branch needs
	// its own copy of the outer relation, which is exactly the "additional common
	// subexpression" the class is named after - here R is copied, so a plan that keeps both
	// branches scans it twice unless a later optimization shares the subtree.
	if (auto distributed = TryDistributeOverSetOperation(op, exports)) {
		return distributed;
	}
	// Identity (7) of Figure 4: an Apply over a cross product. The two sides are removed
	// independently and matched back per outer row through R's key.
	//     R A_x (E1 x E2) = (R A_x E1) join_{R.key} (R A_x E2)
	if (auto distributed = TryDistributeOverCrossProduct(op, exports)) {
		return distributed;
	}

	vector<unique_ptr<Expression>> extracted;
	auto left = std::move(op->children[0]);
	auto right = std::move(op->children[1]);

	// The sub-query's plan is about to be spliced into the enclosing query, so every
	// reference it makes to that query moves one scope closer. Leaving them at depth 1
	// happens to compute the right answer - once the plan is flat the resolver finds
	// the binding - but the optimizer is entitled to assume a flattened plan has no
	// depth left: FilterPushdown::IsVolatile asserts it before pushing a filter
	// through a projection, and DuckDB's own flattening removes the depth as it goes.
	// Without this, TPC-H Q17 and Q20 plan fine without the optimizer and fail with it.
	DecrementCorrelationDepth(*right);

	// Rule 3: a correlated scalar subquery. Dispatched before the generic lift,
	// because its correlated predicate sits below the aggregate and the generic
	// walk deliberately refuses to look through one.
	if (join_type == JoinType::SINGLE) {
		return DecorrelateScalar(std::move(left), std::move(right), correlated, exports);
	}

	// Identities (3) and (4), applied level by level through the sub-query's body.
	right = LiftCorrelatedPredicates(std::move(right), correlated, extracted);

	// Correlation used in a shape we cannot lift into a join condition yet.
	// Fail loudly rather than emitting a plan that means something else.
	if (SubtreeReferencesCorrelation(*right, correlated)) {
		throw NotImplementedException(
		    "cascade: Apply elimination does not handle this correlated subquery shape yet "
		    "(correlated column used below a non row-preserving operator)");
	}

	// Rule 1: no correlation and an ordinary join type -> cross product.
	if (correlated.empty() && extracted.empty() && !any_join &&
	    (join_type == JoinType::INNER || join_type == JoinType::LEFT || join_type == JoinType::RIGHT ||
	     join_type == JoinType::OUTER)) {
		return make_uniq<LogicalCrossProduct>(std::move(left), std::move(right));
	}

	if (extracted.empty()) {
		throw NotImplementedException(
		    "cascade: Apply elimination without a correlation predicate is not implemented yet "
		    "(uncorrelated semi/anti/mark subquery)");
	}

	if (join_type == JoinType::INNER) {
		// Identities (2) and (3) in the direction that removes the Apply: the body
		// predicate becomes a join condition and the Apply becomes an ordinary inner join.
		// Unlike the marker family it must keep SQL's meaning for an unknown comparison -
		// a NULL correlation is not a match - so the comparison stays a plain equality and
		// the right side is not stripped of NULLs. (A correlated derived table is this
		// shape: `FROM t, (SELECT ... WHERE u.a = t.k) s`.)
		// An inner Apply may carry the join's own ON predicate as well; for inner
		// semantics it is just one more condition on the joined rows.
		vector<unique_ptr<Expression>> conditions;
		for (auto &predicate : extracted) {
			conditions.push_back(std::move(predicate));
		}
		if (apply.condition) {
			if (!CollectComparisons(std::move(apply.condition), conditions)) {
				throw NotImplementedException(
				    "cascade: a correlated cross Apply whose join predicate is not a comparison is not "
				    "implemented yet");
			}
		}
		auto left_bindings = left->GetColumnBindings();
		auto right_bindings = right->GetColumnBindings();
		auto inner_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		for (auto &predicate : conditions) {
			if (!BoundComparisonExpression::IsComparison(*predicate)) {
				throw NotImplementedException(
				    "cascade: a correlated cross Apply whose join predicate is not a comparison is not "
				    "implemented yet");
			}
			auto &comparison = predicate->Cast<BoundFunctionExpression>();
			auto &lhs = BoundComparisonExpression::LeftMutable(comparison);
			auto &rhs = BoundComparisonExpression::RightMutable(comparison);
			// A join condition has a side convention: the left expression may name the left
			// child only, the right expression the right child only. DuckDB's own
			// AddJoinCondition orders the sides by the correlation, which says nothing about a
			// predicate that does not mention the correlation at all - an ON predicate on the
			// sub-query's side used to end up on the wrong side and fail to bind.
			bool lhs_left = ApplyReadsBindings(*lhs, left_bindings);
			bool lhs_right = ApplyReadsBindings(*lhs, right_bindings);
			bool rhs_left = ApplyReadsBindings(*rhs, left_bindings);
			bool rhs_right = ApplyReadsBindings(*rhs, right_bindings);
			if (lhs_right && !lhs_left && !rhs_right) {
				auto moved = std::move(lhs);
				lhs = std::move(rhs);
				rhs = std::move(moved);
				BoundComparisonExpression::FlipType(comparison);
			} else if (lhs_right || rhs_left) {
				throw NotImplementedException(
				    "cascade: a correlated cross Apply's predicate names both sides in a way that is "
				    "not a join condition");
			}
			inner_join->conditions.emplace_back(std::move(lhs), std::move(rhs),
			                                    comparison.GetExpressionType());
		}
		inner_join->children.push_back(std::move(left));
		inner_join->children.push_back(std::move(right));
		return std::move(inner_join);
	}

	// Rule 2 covers the semi/anti/mark family. Only those may have their right
	// sub-tree widened: their output is the left side (plus the mark column), so
	// extra columns on the right stay invisible to the parent.
	if (join_type != JoinType::SEMI && join_type != JoinType::ANTI && join_type != JoinType::MARK) {
		throw NotImplementedException(
		    "cascade: Apply elimination with a correlated predicate is only implemented for "
		    "semi/anti/mark joins so far");
	}
	// x = ANY(subquery) / ALL(subquery) carries a second condition comparing an
	// outer expression with the subquery's output. It becomes one more join
	// condition, and - unlike EXISTS - it keeps its three-valued marker, so it
	// must not receive the NULL-stripping treatment below.
	auto any_condition = std::move(apply.condition);


	// The correlation comparison is always made NULL-safe, and the right side is
	// stripped of NULLs in the correlated columns. Together they keep the
	// correlation behaving like the equality the user wrote (a NULL on either
	// side is not a match) while leaving the marker itself free of the unknown
	// that an equality against NULL would otherwise introduce. Whether the marker
	// ends up two-valued then follows from the remaining conditions: EXISTS has
	// only these, an ANY/IN comparison adds one that can still be NULL.

	// The predicates were re-expressed through every projection they crossed, so the
	// columns they name are the ones the right sub-tree exposes now.
	auto needed = CollectRightColumns(extracted, correlated);

	// EXISTS / NOT EXISTS carry ANY semantics: a NULL comparison counts as "no
	// match", not as "unknown". Dropping the right-side rows whose correlated
	// column is NULL removes the unknown case outright, so a plain MARK join then
	// produces the two-valued marker these subqueries need.
	if (!needed.empty()) {
		vector<unique_ptr<Expression>> not_null;
		for (auto &col : needed) {
			auto colref = make_uniq<BoundColumnRefExpression>(col.type, col.binding);
			auto is_not_null =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
			is_not_null->GetChildrenMutable().push_back(std::move(colref));
			not_null.push_back(std::move(is_not_null));
		}
		if (!not_null.empty()) {
			auto null_filter = make_uniq<LogicalFilter>();
			null_filter->expressions = std::move(not_null);
			null_filter->children.push_back(std::move(right));
			right = std::move(null_filter);
		}
	}

	auto join = make_uniq<LogicalComparisonJoin>(join_type);
	join->mark_index = mark_index;
	join->children.push_back(std::move(left));
	join->children.push_back(std::move(right));
	// A predicate inside the sub-query's body is a WHERE condition: only TRUE lets a row
	// through, so an UNKNOWN behaves as FALSE. An equality keeps its shape - it is the
	// join's hash key, and null_safe makes it two-valued, so it contributes nothing to
	// the marker's third value. Everything else is not a hash key anyway, and is wrapped
	// so that UNKNOWN reads as FALSE.
	vector<unique_ptr<Expression>> body_conditions;
	for (auto &predicate : extracted) {
		bool equality = BoundComparisonExpression::IsComparison(*predicate) &&
		                predicate->GetExpressionType() == ExpressionType::COMPARE_EQUAL;
		if (!equality) {
			auto coalesce =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, LogicalType::BOOLEAN);
			coalesce->GetChildrenMutable().push_back(std::move(predicate));
			coalesce->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(false)));
			predicate = std::move(coalesce);
		}
		if (!any_condition || equality) {
			AddJoinCondition(*join, std::move(predicate), correlated, true);
		} else {
			body_conditions.push_back(std::move(predicate));
		}
	}
	if (any_condition) {
		// An IN/ANY marker is three-valued in one specific way: a NULL in the comparison
		// is unknown, but a row the sub-query's *body* excluded takes no part in it at
		// all. A correlated body predicate cannot be reproduced by a join condition -
		// the marker would either see the body's unknown as its own, or lose it - so this
		// shape is reported instead of answered with the wrong rows. (The paper's own
		// route for booleans is to turn the sub-query into a scalar count first, which
		// brings the body predicate along as the aggregate's input.)
		if (!body_conditions.empty()) {
			throw NotImplementedException(
			    "cascade: a correlated predicate inside an IN/ANY sub-query's body is not implemented yet");
		}
		// It compares an outer expression with the subquery's own output, which the
		// projection already exposes, so it needs no column exposure of its own.
		AddJoinCondition(*join, std::move(any_condition), correlated, false);
	}
	return std::move(join);
}

//! A column of the sub-tree together with the binding the join can reach it by.
struct ExposedColumn {
	ColumnBinding original;
	ColumnBinding visible;
	LogicalType type;
};

//! The GroupBy a scalar sub-query's correlation can be hiding under: a sub-query like
//! `select sum(x) from (select a, max(b) as x from s where s.a = t.a group by a) q`
//! aggregates twice, and the correlated predicate belongs to the inner GroupBy.
static LogicalAggregate *FindNestedGroupedAggregate(LogicalOperator &op) {
	auto current = &op;
	while (current->type == LogicalOperatorType::LOGICAL_PROJECTION && current->children.size() == 1) {
		current = current->children[0].get();
	}
	if (current->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return nullptr;
	}
	auto &aggregate = current->Cast<LogicalAggregate>();
	if (aggregate.groups.empty()) {
		return nullptr;
	}
	return &aggregate;
}

//! Identity (8) of Galindo-Legaria & Joshi, for the scalar case where the correlated
//! predicate sits below a GroupBy of the sub-query's own:
//!     R A_x (G_{A,F} E)  =  G_{A ∪ columns(R), F}( R A_x E )
//! The sub-query's own GroupBy cannot be aggregated around - the correlation has to be
//! removed where it lives - so the Apply moves below it and the outer columns join its
//! grouping. Each original group then splits by outer row, and `F` over such a
//! sub-group is exactly the aggregate over the rows that outer row matched.
//!
//! Three details make that work for a *scalar* sub-query, and each of them was a bug
//! before it was a design:
//!
//!  * The outer side is deduplicated first. The original Apply evaluates the sub-query
//!    once per distinct correlation value, so two identical outer rows share one result;
//!    grouping the joined rows by `columns(R)` instead folds them together and, for a
//!    combination like `sum(sum(x))`, counts their rows twice.
//!  * The join under the GroupBy is an inner join. A left outer join pads an outer
//!    value with no match with a NULL row, and the GroupBy materialises that padding
//!    into a group of its own - so the derived table above it would hold one row where
//!    SQL says it is empty (making `count(*)` return 1 instead of 0).
//!  * The multiplicity is handed back afterwards, by joining the aggregated sub-query
//!    to the *original* outer rows on those columns (NULL-safe, so an outer row whose
//!    correlation column is NULL still finds its group). An outer value with no match
//!    is padded, which is exactly what an empty aggregate input returns - except for
//!    count, which pi_c repairs with a constant.
unique_ptr<LogicalOperator> ApplyDecorrelator::DecorrelateNestedScalar(
    unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right, const vector<LogicalOperator *> &projections,
    LogicalAggregate &top, LogicalAggregate &nested, vector<unique_ptr<Expression>> &extracted,
    const CorrelatedColumns &correlated, BindingExport &exports, const ColumnBinding &value_binding) {
	nested_scalar = true;
	auto needed = CollectRightColumns(extracted, correlated);
	if (needed.empty()) {
		throw NotImplementedException("cascade: a correlated scalar subquery with no sub-query column");
	}

	auto left_bindings = left->GetColumnBindings();
	left->ResolveOperatorTypes();
	if (left->types.size() != left_bindings.size()) {
		throw NotImplementedException("cascade: cannot resolve the outer column types required by identity (8)");
	}
	vector<LogicalType> outer_types = left->types;

	// The projections between the outer aggregate and the nested one, bottom first.
	vector<LogicalOperator *> between;
	auto current = top.children[0].get();
	while (current != &nested) {
		if (current->type != LogicalOperatorType::LOGICAL_PROJECTION || current->children.size() != 1) {
			throw NotImplementedException("cascade: a correlated scalar subquery whose grouping is not directly "
			                              "below another aggregate is not implemented yet");
		}
		between.push_back(current);
		current = current->children[0].get();
	}

	// (1) The distinct outer values drive the grouping; the original relation is kept
	// for the join that hands the rows back at the end.
	auto dedup_group_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> no_aggregates;
	auto dedup = make_uniq<LogicalAggregate>(dedup_group_index, binder.GenerateTableIndex(),
	                                         std::move(no_aggregates));
	BindingExport dedup_export;
	for (idx_t i = 0; i < left_bindings.size(); i++) {
		dedup->groups.push_back(make_uniq<BoundColumnRefExpression>(outer_types[i], left_bindings[i]));
		dedup_export.emplace_back(left_bindings[i], ColumnBinding(dedup_group_index, ProjectionIndex(i)));
	}
	dedup->children.push_back(left->Copy(context));

	// (2) The Apply moves below the sub-query's GroupBy, as an inner join so that no
	// padding row can turn into a group.
	auto inner = std::move(nested.children[0]);
	vector<std::pair<ColumnBinding, ColumnBinding>> mapping;
	if (inner->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		ExposeRightColumns(*inner, needed, mapping);
	}
	for (auto &predicate : extracted) {
		RewriteExpressionBindings(predicate, mapping);
	}
	if (SubtreeReferencesCorrelation(*inner, correlated)) {
		throw NotImplementedException("cascade: identity (8) does not handle this correlated subquery shape yet "
		                              "(correlated column used below a non row-preserving operator)");
	}
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	join->children.push_back(std::move(dedup));
	join->children.push_back(std::move(inner));
	for (auto &predicate : extracted) {
		AddJoinCondition(*join, std::move(predicate), correlated, false);
	}
	// The predicate is oriented by looking for the correlation domain, so the outer
	// side is repointed at the deduplicated keys only once it has run.
	RewriteOperatorBindings(*join, dedup_export);
	nested.children[0] = std::move(join);

	// (3) The outer keys join the inner grouping, and every projection on the way up
	// has to carry them: a projection does not pass a column it was not asked for.
	vector<ColumnBinding> carry;
	for (idx_t i = 0; i < left_bindings.size(); i++) {
		auto position = nested.groups.size();
		nested.groups.push_back(make_uniq<BoundColumnRefExpression>(
		    outer_types[i], ColumnBinding(dedup_group_index, ProjectionIndex(i))));
		// The grouping sets have to grow with the groups: a set that does not mention a
		// grouping column makes the physical aggregate pad that column with NULL, which
		// silently collapses every row into one group whose key is NULL.
		for (auto &set : nested.grouping_sets) {
			set.insert(ProjectionIndex(position));
		}
		carry.push_back(ColumnBinding(nested.group_index, ProjectionIndex(position)));
	}
	auto thread = [&](const vector<LogicalOperator *> &chain) {
		for (auto *op : chain) {
			auto &projection = op->Cast<LogicalProjection>();
			for (idx_t c = 0; c < carry.size(); c++) {
				auto position = projection.expressions.size();
				projection.expressions.push_back(make_uniq<BoundColumnRefExpression>(outer_types[c], carry[c]));
				carry[c] = ColumnBinding(projection.table_index, ProjectionIndex(position));
			}
		}
	};
	thread(between);
	top.groups.clear();
	for (idx_t c = 0; c < carry.size(); c++) {
		top.groups.push_back(make_uniq<BoundColumnRefExpression>(outer_types[c], carry[c]));
	}
	for (auto &set : top.grouping_sets) {
		for (idx_t c = 0; c < carry.size(); c++) {
			set.insert(ProjectionIndex(c));
		}
	}
	carry.clear();
	for (idx_t c = 0; c < left_bindings.size(); c++) {
		carry.push_back(ColumnBinding(top.group_index, ProjectionIndex(c)));
	}
	auto above = projections;
	thread(above);

	// (4) Hand the outer rows their multiplicity back. The sub-query plan above holds
	// one row per distinct outer value, so joining it to the original relation restores
	// the duplicates, and an outer value with no match is padded with NULLs.
	auto expanded = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
	expanded->children.push_back(std::move(left));
	expanded->children.push_back(std::move(right));
	for (idx_t i = 0; i < left_bindings.size(); i++) {
		expanded->conditions.emplace_back(
		    make_uniq<BoundColumnRefExpression>(outer_types[i], left_bindings[i]),
		    make_uniq<BoundColumnRefExpression>(outer_types[i], carry[i]),
		    ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	}

	// pi_c: an empty aggregate input is what an unmatched outer value gets from the
	// padding, and count is the aggregate whose answer there is not NULL.
	auto count_at_top = false;
	for (auto &expr : top.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			continue;
		}
		auto &name = expr->Cast<BoundAggregateExpression>().Function().GetName();
		if (name == "count" || name == "count_star") {
			count_at_top = true;
		}
	}
	if (!count_at_top) {
		return std::move(expanded);
	}
	auto projection_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> select_list;
	auto outputs = expanded->GetColumnBindings();
	auto types = expanded->types;
	if (types.size() != outputs.size()) {
		expanded->ResolveOperatorTypes();
		types = expanded->types;
	}
	for (idx_t i = 0; i < outputs.size(); i++) {
		if (outputs[i] == value_binding) {
			auto coalesce = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, types[i]);
			coalesce->GetChildrenMutable().push_back(make_uniq<BoundColumnRefExpression>(types[i], outputs[i]));
			coalesce->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
			select_list.push_back(std::move(coalesce));
			exports.emplace_back(value_binding, ColumnBinding(projection_index, ProjectionIndex(i)));
			continue;
		}
		select_list.push_back(make_uniq<BoundColumnRefExpression>(types[i], outputs[i]));
		exports.emplace_back(outputs[i], ColumnBinding(projection_index, ProjectionIndex(i)));
	}
	auto fixup = make_uniq<LogicalProjection>(projection_index, std::move(select_list));
	fixup->children.push_back(std::move(expanded));
	return std::move(fixup);
}

unique_ptr<LogicalOperator> ApplyDecorrelator::DecorrelateScalar(unique_ptr<LogicalOperator> left,
                                                                unique_ptr<LogicalOperator> right,
                                                                const CorrelatedColumns &correlated,
                                                                BindingExport &exports) {
	// Identity (9) of Galindo-Legaria & Joshi, "Orthogonal Optimization of
	// Subqueries and Aggregation":
	//     R A_x (G_{F1} E)  =  G_{columns(R), F'}( R LOJ E )
	scalar_aggregate = true;
	scalar_subqueries++;
	// It holds because SQL aggregates satisfy agg(empty) = agg({null}): a left outer
	// join hands an outer row with no match a single NULL-padded row, and the
	// aggregate over that row is exactly the aggregate over an empty input.
	//
	// Grouping the outer side - rather than grouping the sub-query side and joining
	// one group back per key - is what makes count work. count over the padded row
	// is 1, so F' re-expresses it over a compared sub-query column, which is NULL
	// there.
	vector<LogicalOperator *> projections;
	auto node = right.get();
	while (node->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		projections.push_back(node);
		node = node->children[0].get();
	}
	if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || node->children.size() != 1) {
		throw NotImplementedException(
		    "cascade: a correlated scalar subquery is only decorrelated when it aggregates");
	}
	auto &aggregate = node->Cast<LogicalAggregate>();
	if (!aggregate.groups.empty()) {
		throw NotImplementedException(
		    "cascade: a correlated scalar subquery that already groups is not implemented yet");
	}
	if (aggregate.expressions.size() != 1 ||
	    aggregate.expressions[0]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
		throw NotImplementedException(
		    "cascade: a correlated scalar subquery over anything but one aggregate is not implemented yet");
	}

	// What the parent reads as the sub-query's value, captured before the sub-tree
	// is replaced by the group-by.
	auto value_binding = right->GetColumnBindings().back();

	vector<unique_ptr<Expression>> extracted;
	node->children[0] = ExtractCorrelatedPredicates(std::move(node->children[0]), correlated, extracted);
	LogicalAggregate *nested = nullptr;
	if (extracted.empty()) {
		// No predicate could be lifted because the correlation sits below a GroupBy of
		// the sub-query's own. That is identity (8)'s case: the Apply has to move below
		// that GroupBy first.
		nested = FindNestedGroupedAggregate(*node->children[0]);
		if (nested) {
			nested->children[0] = ExtractCorrelatedPredicates(std::move(nested->children[0]), correlated, extracted);
		}
	}
	if (extracted.empty()) {
		throw NotImplementedException(
		    "cascade: a correlated scalar subquery without a correlated predicate is not implemented yet");
	}
	if (nested) {
		return DecorrelateNestedScalar(std::move(left), std::move(right), projections, aggregate, *nested, extracted,
		                               correlated, exports, value_binding);
	}
	if (SubtreeReferencesCorrelation(*node->children[0], correlated)) {
		throw NotImplementedException(
		    "cascade: Apply elimination does not handle this correlated subquery shape yet "
		    "(correlated column used below a non row-preserving operator)");
	}
	// The predicate may take any form, not only equality: identity (9) groups the
	// outer side, so the predicate merely decides which inner rows each group sees.
	// A NULL comparison is never true, so every column the predicate compares is
	// non-NULL on the rows the join keeps - which is what the count rewrite relies
	// on.
	auto needed = CollectRightColumns(extracted, correlated);
	if (needed.empty()) {
		throw NotImplementedException("cascade: a correlated scalar subquery with no sub-query column");
	}

	// Read what F' needs out of the aggregate before the sub-tree is released: the
	// aggregate and the projections above it are replaced by an aggregate below the
	// join, and the projections themselves are re-attached at the end.
	auto value_expression = aggregate.expressions[0]->Copy();
	auto value_type = aggregate.expressions[0]->GetReturnType();
	auto &aggregate_expression = aggregate.expressions[0]->Cast<BoundAggregateExpression>();
	auto &name = aggregate_expression.Function().GetName();
	// count is the aggregate whose value on an empty input is not NULL, so it is the
	// one that needs the compensating project pi_c of section 3.2.
	const bool count_rewrite = name == "count" || name == "count_star";

	const auto old_aggregate_index = aggregate.aggregate_index;
	auto inner = std::move(node->children[0]);

	// The lifted predicates are evaluated at the join, which sits above the body's
	// own projections, so every column they name has to be projected out first.
	vector<std::pair<ColumnBinding, ColumnBinding>> mapping;
	if (inner->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		ExposeRightColumns(*inner, needed, mapping);
	}
	for (auto &predicate : extracted) {
		RewriteExpressionBindings(predicate, mapping);
	}

	auto left_bindings = left->GetColumnBindings();
	if (left->types.size() != left_bindings.size()) {
		left->ResolveOperatorTypes();
	}
	if (left->types.size() != left_bindings.size()) {
		throw NotImplementedException("cascade: cannot resolve the outer column types required by identity (9)");
	}
	vector<LogicalType> outer_types = left->types;

	// Section 3.2 of the paper moves the GroupBy below the outer join:
	//     G_{A,F}( S LOJ_p R ) = pi_c( S LOJ_p ( G_{A-columns(S),F} R ) )
	// The trade is that an outer row matching nothing no longer hands the aggregate a
	// NULL-padded row: it produces no group at all, and the outerjoin hands the
	// caller NULL, which pi_c repairs wherever NULL is not the empty-input answer
	// (count). That is both cheaper and more faithful than identity (9) - count and
	// list come out right by construction, instead of by the fiction that
	// agg(empty) = agg({null}).
	//
	// The rule needs the predicate's sub-query columns to be functionally determined
	// by the grouping columns, which here are the outer columns themselves. With
	// s.c = <outer expression> the outer row fixes s.c, so at most one group can
	// match and each outer row still contributes exactly one output row - which is
	// also what makes this form correct when the outer relation has duplicate rows,
	// where identity (9) folds them into a single group. A predicate such as
	// s.a + s.b = t.a fixes neither s.a nor s.b, so it stays with identity (9).
	//
	// It also needs the aggregate functions to read only the sub-query's own columns:
	// below the outerjoin the outer columns are not in scope. Identity (9) has no such
	// restriction, since its GroupBy sits above the join - which is how
	// (select sum(t.a) from s where s.a = t.a) is handled.
	vector<ColumnBinding> inner_keys;
	bool pushdown = !ReferencesCorrelation(*aggregate.expressions[0], correlated);
	for (auto &column : needed) {
		auto binding = MapBinding(column.binding, mapping);
		bool determined = false;
		for (auto &predicate : extracted) {
			if (!BoundComparisonExpression::IsComparison(*predicate) ||
			    predicate->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			auto &comparison = predicate->Cast<BoundFunctionExpression>();
			auto &lhs = BoundComparisonExpression::Left(comparison);
			auto &rhs = BoundComparisonExpression::Right(comparison);
			for (idx_t side = 0; side < 2; side++) {
				auto &operand = side == 0 ? lhs : rhs;
				auto &other = side == 0 ? rhs : lhs;
				if (operand.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
				    operand.Cast<BoundColumnRefExpression>().Binding() != binding) {
					continue;
				}
				if (!ReferencesInner(other, correlated)) {
					determined = true;
				}
			}
		}
		if (!determined) {
			pushdown = false;
			break;
		}
		inner_keys.push_back(binding);
	}

	// Both strategies end in the same shape: a node that exposes the outer columns and
	// the sub-query's value, plus a note of where those two have moved to.
	vector<unique_ptr<Expression>> aggregate_list;
	aggregate_list.push_back(std::move(value_expression));
	unique_ptr<LogicalOperator> body;
	vector<ColumnBinding> outer_now;
	ColumnBinding value_now;
	//! Whether the outer rows still have to be handed back their multiplicity.
	bool reexpand = false;

	if (pushdown) {
		auto inner_group_index = binder.GenerateTableIndex();
		auto inner_aggregate_index = binder.GenerateTableIndex();
		auto pushed = make_uniq<LogicalAggregate>(inner_group_index, inner_aggregate_index, std::move(aggregate_list));
		BindingExport key_export;
		for (idx_t i = 0; i < needed.size(); i++) {
			pushed->groups.push_back(make_uniq<BoundColumnRefExpression>(needed[i].type, inner_keys[i]));
			key_export.emplace_back(inner_keys[i], ColumnBinding(inner_group_index, ProjectionIndex(i)));
		}
		pushed->children.push_back(std::move(inner));

		auto join = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
		join->children.push_back(std::move(left));
		join->children.push_back(std::move(pushed));
		for (auto &predicate : extracted) {
			// The predicate now compares the outer side against the group keys; the
			// sub-query's own columns are no longer exposed by the aggregate below.
			// Plain comparison, not IS NOT DISTINCT FROM: the outer row has to mean
			// exactly what the user wrote, so a NULL outer value matches nothing and
			// receives the empty-input answer from pi_c.
			RewriteExpressionBindings(predicate, key_export);
			AddJoinCondition(*join, std::move(predicate), correlated, false);
		}

		// pi_c is only needed where NULL is not the empty-input answer. For every other
		// aggregate the outerjoin already hands the caller exactly what agg(empty)
		// returns - the paper's own example notes that "no computing projects are
		// required here as the aggregate expression sum(...) does result in NULL when
		// calculated on a singleton NULL" - so the join is the whole answer and the
		// outer columns stay where they were on its left side.
		value_now = ColumnBinding(inner_aggregate_index, ProjectionIndex(0));
		if (!count_rewrite) {
			for (idx_t i = 0; i < left_bindings.size(); i++) {
				outer_now.push_back(left_bindings[i]);
			}
			body = std::move(join);
		} else {
			auto projection_index = binder.GenerateTableIndex();
			vector<unique_ptr<Expression>> select_list;
			for (idx_t i = 0; i < left_bindings.size(); i++) {
				select_list.push_back(make_uniq<BoundColumnRefExpression>(outer_types[i], left_bindings[i]));
				outer_now.push_back(ColumnBinding(projection_index, ProjectionIndex(i)));
			}
			value_now = ColumnBinding(projection_index, ProjectionIndex(select_list.size()));
			auto coalesce = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, value_type);
			coalesce->GetChildrenMutable().push_back(
			    make_uniq<BoundColumnRefExpression>(value_type, ColumnBinding(inner_aggregate_index, ProjectionIndex(0))));
			coalesce->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
			select_list.push_back(std::move(coalesce));
			auto fixup = make_uniq<LogicalProjection>(projection_index, std::move(select_list));
			fixup->children.push_back(std::move(join));
			body = std::move(fixup);
		}
	} else {
		// Identity (9), for the predicates section 3.2 cannot push: group the outer
		// side over a left outer join, so every outer row has a group and the
		// aggregate always sees at least a NULL-padded row.
		//
		// For count that padded row must contribute 0 rather than 1, so the count is
		// re-expressed over a compared sub-query column. Every other SQL aggregate
		// already agrees with the NULL the padded row carries, so it is carried over.
		if (count_rewrite) {
			auto count_binding = MapBinding(needed[0].binding, mapping);
			FunctionBinder function_binder(context);
			vector<LogicalType> argument_types {needed[0].type};
			auto count_function = GetBuiltinAggregateFunction(context, Identifier("count"), argument_types);
			vector<unique_ptr<Expression>> arguments;
			arguments.push_back(make_uniq<BoundColumnRefExpression>(needed[0].type, count_binding));
			aggregate_list[0] = function_binder.BindAggregateFunction(std::move(count_function), std::move(arguments),
			                                                          nullptr, AggregateType::NON_DISTINCT);
		}

		auto join = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
		// Identity (9) is stated for an outer relation that contains a key: it folds
		// every outer row sharing a grouping value into one group, so duplicate outer
		// rows would lose both their rows and - because that one group then sees the
		// matches of all of them - their value. Instead of leaning on the
		// precondition, the outer side is deduplicated before it is joined and its
		// rows are handed back afterwards, which is what DuckDB's delimited join does
		// with the keys it materialises. Repointing the predicate's outer side at the
		// deduplicated keys needs every predicate to be a comparison; any other shape
		// keeps the plan the paper describes, precondition included.
		bool can_dedup = true;
		for (auto &predicate : extracted) {
			if (!BoundComparisonExpression::IsComparison(*predicate)) {
				can_dedup = false;
			}
		}
		BindingExport outer_export;
		unique_ptr<LogicalOperator> outer_child;
		if (can_dedup) {
			auto dedup_index = binder.GenerateTableIndex();
			vector<unique_ptr<Expression>> no_aggregates;
			auto dedup = make_uniq<LogicalAggregate>(dedup_index, binder.GenerateTableIndex(),
			                                          std::move(no_aggregates));
			for (idx_t i = 0; i < left_bindings.size(); i++) {
				dedup->groups.push_back(make_uniq<BoundColumnRefExpression>(outer_types[i], left_bindings[i]));
				outer_export.emplace_back(left_bindings[i], ColumnBinding(dedup_index, ProjectionIndex(i)));
			}
			dedup->children.push_back(left->Copy(context));
			outer_child = std::move(dedup);
		} else {
			outer_child = left->Copy(context);
			for (idx_t i = 0; i < left_bindings.size(); i++) {
				outer_export.emplace_back(left_bindings[i], left_bindings[i]);
			}
		}

		join->children.push_back(std::move(outer_child));
		join->children.push_back(std::move(inner));
		for (auto &predicate : extracted) {
			// Plain comparison, not IS NOT DISTINCT FROM. The scalar path has no
			// marker to keep two-valued, and the predicate has to mean exactly what
			// the user wrote: a NULL outer value must match nothing, so that the left
			// outer join supplies the padded row and the aggregate sees an empty
			// input. A NULL-safe comparison would instead let a NULL outer value
			// match a NULL inner row - which is how max/sum over a NULL outer value
			// wrongly returned the inner row's value while count happened to stay
			// right.
			AddJoinCondition(*join, std::move(predicate), correlated, false);
		}
		// AddJoinCondition orients the predicate by looking for the correlation
		// domain, so the keys are substituted only once it has run.
		RewriteOperatorBindings(*join, outer_export);

		auto group_index = binder.GenerateTableIndex();
		auto aggregate_index = binder.GenerateTableIndex();
		auto group_by = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregate_list));
		for (idx_t i = 0; i < left_bindings.size(); i++) {
			group_by->groups.push_back(make_uniq<BoundColumnRefExpression>(outer_types[i], outer_export[i].second));
			outer_now.push_back(ColumnBinding(group_index, ProjectionIndex(i)));
		}
		group_by->children.push_back(std::move(join));
		// A sub-query aggregate may read the outer columns itself -
		// (select sum(s.b + t.a) ...) - and those are now the deduplicated keys.
		RewriteOperatorBindings(*group_by, outer_export);
		value_now = ColumnBinding(aggregate_index, ProjectionIndex(0));
		body = std::move(group_by);
		reexpand = can_dedup;
	}

	// The sub-query's own projections are kept rather than discarded: they may
	// compute on top of the aggregate - TPC-H Q20 uses 0.5 * sum(...) - so dropping
	// them would silently change the value the parent reads. They are re-attached
	// above the aggregate, and only the bottom one names the aggregate, so only that
	// reference has to move. Keeping them also means the parent's binding survives
	// untouched, since the topmost projection keeps its table index.
	// Where the outer columns can be read once the shape above is in place: identity
	// (9) has replaced them with its group keys, section 3.2 leaves them where they
	// were on the outerjoin's left side.
	vector<ColumnBinding> pass;
	unique_ptr<LogicalOperator> result;
	if (!projections.empty()) {
		auto &bottom = projections.back()->Cast<LogicalProjection>();
		// Below the aggregate the outer columns and the sub-query value were named by
		// the left child's bindings and the old aggregate index; above it they are the
		// group keys (or the join's columns) and the new aggregate. The bottom
		// projection is the one that straddles that change.
		BindingExport aggregate_export;
		for (idx_t i = 0; i < left_bindings.size(); i++) {
			aggregate_export.emplace_back(left_bindings[i], outer_now[i]);
		}
		aggregate_export.emplace_back(ColumnBinding(old_aggregate_index, ProjectionIndex(0)), value_now);
		for (auto &expr : bottom.expressions) {
			RewriteExpressionBindings(expr, aggregate_export);
		}
		bottom.children[0] = std::move(body);

		// The parent of the Apply also reads the outer columns, and the sub-query's
		// own projection does not carry them. Thread them up the chain, appending
		// after the existing expressions so the sub-query value keeps its position -
		// which is what keeps the parent's reference to that value valid.
		pass = outer_now;
		for (idx_t i = projections.size(); i-- > 0;) {
			auto &projection = projections[i]->Cast<LogicalProjection>();
			for (idx_t c = 0; c < pass.size(); c++) {
				auto position = projection.expressions.size();
				projection.expressions.push_back(make_uniq<BoundColumnRefExpression>(outer_types[c], pass[c]));
				pass[c] = ColumnBinding(projection.table_index, ProjectionIndex(position));
			}
		}
		result = std::move(right);
	} else {
		pass = outer_now;
		result = std::move(body);
	}

	if (reexpand) {
		// Identity (9) is stated for an outer relation that contains a key, because it
		// folds every outer row sharing a grouping value into one group. Rather than
		// rely on that precondition, hand the outer rows back their multiplicity by
		// joining the aggregated sub-query to them again - which is what DuckDB's
		// delimited join achieves by materialising the distinct keys. The comparison
		// is NULL-safe so an outer row whose correlation column is NULL still finds
		// its own group instead of being dropped by the inner join.
		auto expanded = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		expanded->children.push_back(std::move(left));
		expanded->children.push_back(std::move(result));
		for (idx_t i = 0; i < left_bindings.size(); i++) {
			expanded->conditions.emplace_back(
			    make_uniq<BoundColumnRefExpression>(outer_types[i], left_bindings[i]),
			    make_uniq<BoundColumnRefExpression>(outer_types[i], pass[i]),
			    ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		}
		result = std::move(expanded);
		// The outer columns keep the bindings they already had, so only the sub-query
		// value has to be repointed - and only when no sub-query projection carried it.
		if (projections.empty()) {
			exports.emplace_back(value_binding, value_now);
		}
		return result;
	}

	for (idx_t c = 0; c < pass.size(); c++) {
		exports.emplace_back(left_bindings[c], pass[c]);
	}
	if (projections.empty()) {
		exports.emplace_back(value_binding, value_now);
	}
	return result;
}

unique_ptr<LogicalOperator> ApplyDecorrelator::DecorrelateNode(unique_ptr<LogicalOperator> op, BindingExport &exports) {
	// What this operator's left side moved, so an Apply can hand the mapping on to its parent.
	BindingExport left_exports;
	for (idx_t child_index = 0; child_index < op->children.size(); child_index++) {
		auto &child = op->children[child_index];
		BindingExport child_exports;
		child = DecorrelateNode(std::move(child), child_exports);
		if (child_index == 0 && op->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
			left_exports = child_exports;
		}
		if (child_exports.empty()) {
			continue;
		}
		RewriteOperatorBindings(*op, child_exports);
		if (op->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
			// The sub-query correlates to this operator's left side, and that side has just
			// been rewritten: both the metadata and every reference inside the sub-query have
			// to follow, or the lift builds a join condition against bindings its left child
			// no longer exposes (`Failed to bind column reference "a" [0.0]`, two scalar
			// sub-queries in one SELECT).
			auto &apply = op->Cast<LogicalDependentJoin>();
			for (auto &info : apply.correlated_columns) {
				info.binding = MapBinding(info.binding, child_exports);
			}
			for (idx_t other = 1; other < op->children.size(); other++) {
				RewriteTreeBindings(*op->children[other], child_exports);
			}
		}
		if (PassesBindingsThrough(*op)) {
			for (auto &entry : child_exports) {
				exports.push_back(entry);
			}
		}
	}
	if (op->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		// This Apply's own exports describe what its *left* side exposes, but the parent still
		// names the outer columns the way they were bound before that left side was rewritten.
		// A second Apply stacked on the same outer relation is exactly that case, so the two
		// mappings have to be composed - otherwise the parent keeps asking for a binding that
		// no longer exists (`Failed to bind column reference "a" [0.0]`, two scalar sub-queries
		// in one SELECT).
		auto own_from = exports.size();
		auto result = DecorrelateApply(std::move(op), exports);
		if (!left_exports.empty()) {
			BindingExport own(exports.begin() + static_cast<ptrdiff_t>(own_from), exports.end());
			for (auto &entry : left_exports) {
				auto mapped = MapBinding(entry.second, own);
				if (mapped != entry.first) {
					exports.emplace_back(entry.first, mapped);
				}
			}
		}
		return result;
	}
	return op;
}

unique_ptr<LogicalOperator> ApplyDecorrelator::Decorrelate(unique_ptr<LogicalOperator> plan) {
	if (!plan) {
		return plan;
	}
	BindingExport exports;
	return DecorrelateNode(std::move(plan), exports);
}

} // namespace duckdb
