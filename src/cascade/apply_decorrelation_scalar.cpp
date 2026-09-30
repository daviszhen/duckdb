// The correlated scalar sub-query: identities (8) and (9) of Figure 4, together with the
// move section 3.2 describes. See ApplyDecorrelator::DecorrelateScalar.
//
//   (9)   R A_x (G_{F1} E)  = G_{cols(R),F'}(R A_LOJ E)
//         group the outer side over a left outer join, so an outer row with no match still
//         has a group - and SQL's agg(empty) = agg({NULL}) makes that group's value right.
//   (8)   R A_x (G_{A,F} E) = G_{A + cols(R),F}(R A_x E)
//         the correlation sits below a GroupBy of the sub-query's own; the Apply slides
//         under it and the outer columns join its grouping.
//   (3.2) G_{A,F}(S LOJ_p R) = pi_c(S LOJ_p (G_{A - cols(S),F} R))
//         the strategy that pushes the GroupBy below the outer join; pi_c is the
//         compensating projection that gives an unmatched outer row the aggregate over an
//         empty input.
//
// Both strategies end in the same shape and are chosen by whether the predicate can be
// answered below the GroupBy. count is the aggregate whose empty-input answer is not NULL,
// so it needs pi_c; every other SQL aggregate already returns what agg(empty) does.
// Identity (9) folds outer rows that share a grouping value into one group, so the outer
// rows are handed back afterwards by joining the result to the original relation - which is
// why it needs no key on the outer side.
//
// Paper: Galindo-Legaria & Joshi (SIGMOD 2001), sections 2.5 and 3.2.
//===----------------------------------------------------------------------===//

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
		// The template for the missing rewrite is three lines up: `dedup`, an aggregate whose groups
		// are the outer columns, plus `dedup_export`, the mapping from them to the columns it exposes.
		// That is ORCA's identity (8) - the correlated columns become a group key. What is missing here
		// is doing the same to `inner` when the correlation sits inside it, instead of giving up: wrap
		// the inner side in an aggregate grouped by the correlated columns, rewrite every reference
		// inside it through the export mapping (join conditions included), left-outer-join the result,
		// and assert that what comes out exposes exactly what the apply exposed before returning it.
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
		// This is the largest single bucket of the remaining failures (19 of 55 files, measured): a
		// scalar sub-query whose root is not an aggregate. ORCA has a separate transformation for it
		// (ExfScalarSubquery), and it is the easier of the two: there is no grouping to add, only a left
		// outer join - the correlated predicate becomes the join condition, so an outer row with no
		// match gets NULL, which is exactly what a scalar sub-query returns. The pieces are all here:
		// the projections above were collected already, ExtractCorrelatedPredicates fills `extracted`
		// (as the aggregate branch below does), and the orientation helpers are the ones the rule uses.
		// What must not be done is deleting this guard: the code after it assumes the node is an
		// aggregate and casts it. The new path has to build the join, hang it under `projections`,
		// return that as the new right side, and check that what it exposes is what the apply exposed
		// before returning - otherwise keep refusing.
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

} // namespace duckdb
