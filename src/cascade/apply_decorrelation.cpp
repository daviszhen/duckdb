// Apply elimination: section 2 of Galindo-Legaria & Joshi, "Orthogonal Optimization of
// Subqueries and Aggregation" (SIGMOD 2001) - the nine identities of Figure 4.
//
//   R A_f E  ->  an ordinary join, by whichever identity matches:
//     (1) E unrelated to R          R A_x E          = R x_true E
//     (2) ... with a predicate      R A_x (s_p E)    = R x_p E
//     (3) predicate into the Apply  R A_x (s_p E)    = s_p (R A_x E)
//     (4) projection widened        R A_x (pi_v E)   = pi_{v + cols(R)}(R A_x E)
//     (5) union all                 R A_x (E1 u E2)  = (R A_x E1) u (R A_x E2)      [class 2]
//     (6) except all                R A_x (E1 - E2)  = (R A_x E1) - (R A_x E2)      [class 2]
//     (7) cross product             R A_x (E1 x E2)  = (R A_x E1) |> R.key (R A_x E2)  [class 2]
//     (8) aggregate in the body     R A_x (G_{A,F} E)  = G_{A + cols(R),F}(R A_x E)
//     (9) scalar aggregate          R A_x (G_{F1} E)   = G_{cols(R),F'}(R A_LOJ E)
//
// Sections 2.3-2.5: (1)-(4) and (8)/(9) are class 1; (5)-(7) are class 2, removed by
// introducing the "additional common expression" the class is named after (here a
// materialised CTE); class 3 (Max1row, conditional Apply) is refused rather than guessed
// at, as the paper itself does not remove it.
//
// This file is the framework - the recursion over the plan and the dispatch. The
// identities live in apply_decorrelation_{setop,crossproduct,scalar}.cpp.
// Switches: DUCKDB_CASCADE, and DUCKDB_CASCADE_KEEP_APPLY to keep the Apply operator.
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

ApplyDecorrelator::ApplyDecorrelator(Binder &binder_p, ClientContext &context_p)
    : binder(binder_p), context(context_p) {
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
		// The correlation is not in a predicate that can be lifted into a join condition - it is used
		// somewhere else in the sub-query (an aggregate key, a projection, below a non row-preserving
		// operator). The earlier wording here said "uncorrelated", which sent more than one diagnosis
		// down the wrong path: an uncorrelated semi/anti/mark apply is handled above, and this branch
		// means the opposite - there *is* a correlation and it cannot be lifted.
		throw NotImplementedException(
		    "cascade: Apply elimination cannot lift this correlation into a join condition "
		    "(the correlated column is used where a predicate cannot be lifted from)");
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
// Measured gap in that model: it cannot express "empty right side means false". For
//   SELECT i = ANY(SELECT i FROM integers WHERE i = i1.i) FROM integers i1;
// the outer value NULL makes the sub-query empty, and SQL says the result is false - but with the
// outer comparison folded into the join an empty side only shows up as "no match", so the mark is
// NULL and the answer comes out NULL (cascade NULL vs the host's false, measured). Three ways out,
// in ascending order of agreement with the host: keep an explicit bool_or-style aggregate for ANY;
// mark the comparison so the consumer can COALESCE the mark to false; or express ANY through the
// delim/CTE machinery the host uses - which is the same thing the join-order problem needs.
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
