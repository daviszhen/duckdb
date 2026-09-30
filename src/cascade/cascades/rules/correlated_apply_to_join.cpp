#include "duckdb/cascade/cascades/rules/correlated_apply_to_join.hpp"

// OPEN: what this rewrite still needs, and what has been measured. Kept here because the
// measurements are the expensive part and they must not be rediscovered.
//
// The rewrite below replaces the Apply's own group with a MARK join. That is *not* what DuckDB's
// own decorrelation does: comparing the two plans for decorrelation.test:27 shows the enforcer
// producing a SEMI join that keeps the projection and has no mark filter above it, while this rule
// produces a MARK join, drops the projection, and leaves the parent's "SUBQUERY" filter in place.
// Three attempts at the correct shape (build SEMI/ANTI, splice the absorbed pair out of the
// parents) were measured and reverted; what they showed:
//
//   * The pair to splice is (this Apply's group, the mark filter's group above it). Memo::ParentsOf
//     and Memo::ReplaceExpression/Reschedule exist for that, and Memo::FindMarkConsumer already
//     answers SEMI versus ANTI - the two Apply nodes are identical in every field, the negation
//     lives only in the consumer.
//   * With the splice in place the rule did work - "spliced group 5, join SEMI in group 8",
//     enforced 30 -> 25 - and then failed to bind:
//         Failed to bind column reference "b" [0.0] (bindings: {#[7.0], #[7.1]})
//     The available set is a *correlated copy* of the left table at another table index. Moving
//     the condition is not enough: the decorrelator's own contract says why. CollectRightColumns
//     gathers the columns a lifted predicate references, and ExposeRightColumns requires the right
//     sub-tree to be a projection, *appends* whatever that projection does not already expose, and
//     reports the old -> new binding mapping so the conditions can be rewritten through it. It
//     refuses (rather than emitting a plan) when the column is not produced by the projection's
//     own child - the case of a column sitting below a second projection - which is the same
//     decline this rule needs. In memo terms that is: rebuild the right-side projection with the
//     needed columns appended, map the condition's bindings onto the new ones, and refuse when the
//     group below the projection does not expose them.
//   * Where the missing reference actually lives, established by the last attempt: the available set
//     at the failure is the *inner* table and the reference is the *outer* table, so the expression
//     that fails to bind sits below the join's right child - the right sub-tree still names the
//     outer columns at their original indices, because in the unflattened plan the Apply's machinery
//     resolved them. DuckDB's flattened plan for the same query shows what has to be built instead:
//     the right side carries a projection that *exposes the correlated copies* (`Projection(1,
//     #[7.0])` for the self-correlated case), and the condition reads those. So the rewrite is not
//     finished by splicing: the right child has to be a projection that carries the correlated
//     columns, and the conditions have to read them there. Two guards were tried - the conditions
//     and the re-pointed parents against what the children expose - and neither refuses this case,
//     which is consistent with the reference being inside the right sub-tree rather than at either
//     of those places.
//   * The next thing that has to exist before the splice can work: the right side needs a
//     projection that passes the filter's child's columns through *and* appends the correlated
//     ones, and the conditions then read that projection. Building it needs the *types* of the
//     pass-through columns, and the memo only records bindings (GroupExpr::bindings) - so a rule
//     cannot construct that projection today. Recording the types next to the bindings when a
//     group expression is built is the enabling change, and it is a small one. A DELIM join
//     carrying duplicate_eliminated_columns was also tried and is not enough on its own: the
//     columns have to be exposed by the right side's projection, which is the part that needs the
//     types.
//   * The furthest attempt so far, and where it stops: building the right side as a projection that
//     passes the inner columns through and appends the correlated ones, with a DELIM join carrying
//     duplicate_eliminated_columns, brought the enforcer down to *zero* on decorrelation.test
//     (`enforced=0`, where the pre-pass used to be needed for 30 statements). It then failed to
//     bind: `Failed to bind column reference "a" [0.0] (bindings: {#[7.0]})` - the appended
//     correlated references were written with the *outer* binding, while the projection's child
//     exposes only its own columns. So the appended references need a table index of their own,
//     assigned by the delim machinery (`LogicalDelimJoin`'s delim_types in DuckDB's own flattening),
//     rather than the outer binding. That is the one thing left: how the right side names the
//     columns the join carries to it.
//   * The delim-get form is the right machinery and nearly binds. Following DuckDB's own
//     flattening (flatten_dependent_join.cpp): the correlated columns are carried by a
//     LogicalDelimGet at a table index of its own, cross-producted with the left input, with
//     duplicate_eliminated_columns describing what to de-duplicate from the left; the conditions
//     read the carried columns at that index. Measured: the enforcer goes to zero on
//     decorrelation.test (`enforced=0`) and the generated index shows up in the plan - and then
//         Failed to bind column reference "a" [7.0] (bindings: {#[0.0], #[15.0]})
//     i.e. an *inner* column is read where only the left input plus the carried columns are
//     available. So what is left is the orientation of the conditions: with the mapping applied the
//     correlated references sit at (delim_index, i) and the inner ones at the inner table's index,
//     and AddJoinCondition's correlation-list heuristic puts one of them on the wrong side. The two
//     children's bindings are known (the cross product's and the filter's), so the split can be done
//     directly instead of heuristically - that is the next step, not a new structure.
//   * Conclusion about the approach, after the delim-get form was built and measured: the enforcer
//     goes to zero (enforced=0 on decorrelation.test) and then the statement *segfaults*. A delim
//     join cannot be hand-built inside a memo rule: LogicalDelimGet and duplicate_eliminated_columns
//     are only valid with the binder-side bookkeeping that FlattenDependentJoins sets up - the
//     generated table index registered as a delim join, its delim_types, and the CTE/delim rewriter
//     that fills the delim scan. None of that exists for a rule, and there is no way to invoke it on
//     a memo subtree either, because operator-level Copy is unavailable (rules rebuild expressions,
//     not operators).
//
//     What that means for identity (4) in this host: the correlated case cannot be finished by
//     splicing a join in place of the Apply. The options that remain are (a) keep the Apply and let
//     the enforcer decorrelate the chosen plan, which is what this rule's declinations already do
//     and what the matrix has been verifying all along, or (b) run the host's own flattening on the
//     sub-plan after the memo has chosen it, which needs the plan-taking API rather than a rule.
//     Recording it rather than leaving it to be rediscovered: the earlier attempts (MARK join, semi
//     without exposure, exposure by projection) each failed for their own reason, and this one fails
//     for a structural one.
//   * The same rule body spliced twice in one build and not at all in another (applied=4 but
//     ParentsOf(consumer_group) empty) is *not* nondeterminism: the splice rewrites the parents'
//     child ids, so after the first one the consumer group has no parents left and later
//     applications find nothing to re-point. Derived state again - ParentsOf answers from the
//     current children - and it has a consequence for the real rewrite: the second application on
//     the same group is expected to do nothing, and must not be read as a failure. A consumer with
//     no parents at the *first* application is a different case: that is the memo's root, and a
//     SEMI join cannot stand in for it because it exposes the left side only.
//
// Both matrices are green with this rule as it stands (it is inert on the corpus), so the shape
// above is a design note, not a description of what the code does today.

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascade_correlation.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

bool CorrelatedApplyToJoin::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && expr.children.size() == 2;
}

namespace {

bool FindCorrelatedShape(CascadesOptimizer &optimizer, GroupExpr &expr, GroupExpr *&projection, GroupExpr *&filter) {
	auto &memo = optimizer.GetMemo();
	for (auto &candidate : memo.GetGroup(expr.children[1]).exprs) {
		if (candidate->type != LogicalOperatorType::LOGICAL_PROJECTION || candidate->children.size() != 1) {
			continue;
		}
		for (auto &below : memo.GetGroup(candidate->children[0]).exprs) {
			if (below->type == LogicalOperatorType::LOGICAL_FILTER && below->children.size() == 1) {
				projection = candidate.get();
				filter = below.get();
				return true;
			}
		}
	}
	return false;
}

bool ReplacesWithCorrelatedJoin(JoinType type) {
	return type == JoinType::SEMI || type == JoinType::ANTI || type == JoinType::MARK;
}

bool PredicatesPairBothSides(GroupExpr &filter, const vector<ColumnBinding> &left_bindings,
                            const vector<ColumnBinding> &right_bindings) {
	if (filter.op->expressions.empty()) {
		return false;
	}
	for (auto &predicate : filter.op->expressions) {
		if (!BoundComparisonExpression::IsComparison(*predicate)) {
			return false;
		}
		auto &comparison = predicate->Cast<BoundFunctionExpression>();
		auto &lhs = BoundComparisonExpression::Left(comparison);
		auto &rhs = BoundComparisonExpression::Right(comparison);
		bool lhs_left = ApplyReadsBindings(lhs, left_bindings);
		bool lhs_right = ApplyReadsBindings(lhs, right_bindings);
		bool rhs_left = ApplyReadsBindings(rhs, left_bindings);
		bool rhs_right = ApplyReadsBindings(rhs, right_bindings);
		if (!((lhs_left && rhs_right) || (lhs_right && rhs_left))) {
			return false;
		}
	}
	return true;
}

} // namespace

CascadesRulePromise CorrelatedApplyToJoin::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	if (apply.condition || apply.correlated_columns.empty()) {
		return CascadesRulePromise::NONE;
	}
	if (!ReplacesWithCorrelatedJoin(apply.join_type)) {
		return CascadesRulePromise::NONE;
	}
	GroupExpr *projection = nullptr;
	GroupExpr *filter = nullptr;
	if (!FindCorrelatedShape(optimizer, expr, projection, filter)) {
		return CascadesRulePromise::NONE;
	}
	auto &memo = optimizer.GetMemo();
	auto &left_group = memo.GetGroup(expr.children[0]);
	auto &right_group = memo.GetGroup(filter->children[0]);
	if (left_group.exprs.empty() || right_group.exprs.empty()) {
		return CascadesRulePromise::NONE;
	}
	if (!PredicatesPairBothSides(*filter, left_group.exprs[0]->bindings, right_group.exprs[0]->bindings)) {
		return CascadesRulePromise::NONE;
	}
	for (idx_t candidate_group = 0; candidate_group < memo.GroupCount(); candidate_group++) {
		for (auto &candidate : memo.GetGroup(candidate_group).exprs) {
			if (!candidate->op) {
				continue;
			}
			for (auto &expression : candidate->op->expressions) {
				bool negated = false;
				ExpressionIterator::VisitExpression<BoundOperatorExpression>(
				    *expression, [&](const BoundOperatorExpression &node) {
					    if (node.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
						    negated = true;
					    }
				    });
				ExpressionIterator::VisitExpression<BoundFunctionExpression>(
				    *expression, [&](const BoundFunctionExpression &node) {
					    if (node.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
						    negated = true;
					    }
				    });
				if (negated) {
					if (CascadeConfig::PrintPlans()) {
						Printer::Print("--- cascade(cascades) rule " + string(Name()) +
						               " refused: the plan negates the mark consumer");
					}
					return CascadesRulePromise::NONE;
				}
			}
		}
	}
	return CascadesRulePromise::HIGH;
}

bool CorrelatedApplyToJoin::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	GroupExpr *projection = nullptr;
	GroupExpr *filter = nullptr;
	if (!FindCorrelatedShape(optimizer, expr, projection, filter)) {
		return false;
	}
	auto &memo = optimizer.GetMemo();
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	auto &left_group = memo.GetGroup(expr.children[0]);
	auto &right_group = memo.GetGroup(filter->children[0]);
	if (left_group.exprs.empty() || right_group.exprs.empty()) {
		return false;
	}

	{
		// The consumer decides SEMI versus ANTI: the two Apply nodes are identical, the negation
		// lives above them. Printed (and thus checked) here because this is where the group id is
		// known, and because the rewrite that follows has to take the consumer with it.
		GroupId consumer_group = INVALID_GROUP_ID;
		GroupExpr *consumer = nullptr;
		bool negated = false;
		if (memo.FindMarkConsumer(group, consumer_group, consumer, negated) && CascadeConfig::PrintPlans()) {
			Printer::Print(StringUtil::Format(
			    "--- cascade(cascades)   mark consumer: filter in group %llu negated=%d -> would become %s",
			    (unsigned long long)consumer_group, (int)negated, negated ? "ANTI" : "SEMI"));
		}
	}
	{
		// Live check of the two inputs the correct rewrite needs, printed where the group id is
		// known. It is here rather than in the note because a print that never runs tells nothing:
		// this one runs whenever the rule is applicable.
		GroupId consumer_group = INVALID_GROUP_ID;
		GroupExpr *consumer = nullptr;
		bool negated = false;
		auto found = memo.FindMarkConsumer(group, consumer_group, consumer, negated);
		if (CascadeConfig::PrintPlans()) {
			// Types as well: a rule that builds a projection over a child's columns needs them, and
			// the print is the check that they are there during the search rather than only after it.
			idx_t left_types = memo.GetGroup(expr.children[0]).exprs.empty()
			                       ? 0
			                       : memo.GetGroup(expr.children[0]).exprs[0]->types.size();
			idx_t right_types = memo.GetGroup(expr.children[1]).exprs.empty()
			                        ? 0
			                        : memo.GetGroup(expr.children[1]).exprs[0]->types.size();
			Printer::Print(StringUtil::Format(
			    "--- cascade(cascades) rule %s: consumer found=%d group=%llu negated=%d parents=%llu "
			    "| left types=%llu right types=%llu",
			    Name(), (int)found, (unsigned long long)consumer_group, (int)negated,
			    (unsigned long long)(found ? memo.ParentsOf(consumer_group).size() : 0),
			    (unsigned long long)left_types, (unsigned long long)right_types));
		}
	}
	auto join = make_uniq<LogicalComparisonJoin>(apply.join_type);
	if (apply.join_type == JoinType::MARK) {
		join->mark_index = apply.mark_index;
	}
	for (auto &predicate : filter->op->expressions) {
		auto copy = predicate->Copy();
		auto &comparison = copy->Cast<BoundFunctionExpression>();
		auto &lhs = BoundComparisonExpression::LeftMutable(comparison);
		auto &rhs = BoundComparisonExpression::RightMutable(comparison);
		bool lhs_left = ApplyReadsBindings(*lhs, left_group.exprs[0]->bindings);
		bool lhs_right = ApplyReadsBindings(*lhs, right_group.exprs[0]->bindings);
		bool rhs_right = ApplyReadsBindings(*rhs, right_group.exprs[0]->bindings);
		if (lhs_right && !lhs_left && !rhs_right) {
			auto moved = std::move(lhs);
			lhs = std::move(rhs);
			rhs = std::move(moved);
			BoundComparisonExpression::FlipType(comparison);
		}
		join->conditions.emplace_back(std::move(lhs), std::move(rhs), comparison.GetExpressionType());
	}
	join->estimated_cardinality = expr.op->estimated_cardinality;
	join->has_estimated_cardinality = expr.op->has_estimated_cardinality;

	auto &projection_op = projection->op->Cast<LogicalProjection>();
	vector<unique_ptr<Expression>> select_list;
	for (auto &projection_expression : projection_op.expressions) {
		select_list.push_back(projection_expression->Copy());
	}
	auto rebuilt = make_uniq<LogicalProjection>(projection_op.table_index, std::move(select_list));
	rebuilt->estimated_cardinality = projection_op.estimated_cardinality;
	rebuilt->has_estimated_cardinality = projection_op.has_estimated_cardinality;

	auto right_without_filter = memo.AddGroup();
	optimizer.AddExpression(right_without_filter, memo.MakeExpr(std::move(rebuilt), {filter->children[0]}));
	optimizer.AddExpression(group, memo.MakeExpr(std::move(join), {expr.children[0], right_without_filter}));
	return true;
}

} // namespace duckdb
