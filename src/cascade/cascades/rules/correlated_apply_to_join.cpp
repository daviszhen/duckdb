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
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

// What has been measured on this rule's remaining shapes, so it does not have to be rediscovered.
//
// test/sql/subquery/exists/test_correlated_exists.test: the rule declines, the enforcer produces the
// plan, and the answer is wrong. The chain, each step measured: the promise's old "one child per
// side" predicate heuristic declined it (removing that check is safe - the apply path orients every
// condition against the bindings the inputs expose and refuses when it cannot); with the check gone
// the rule enters Apply and leaves at FindMarkConsumer, which only recognises a mark read by a
// FILTER - this statement reads it in the select list, i.e. through a PROJECTION.
//
// "Consumer B" - replacing the apply in its own group with a MARK join that keeps the mark index,
// so the projection above still resolves what it reads - compiles and takes the work over from the
// enforcer, but it is a net loss: the subquery sweep goes from 33/88 to 32/88 while the cascade
// matrix reads 7/8 three runs in a row. An earlier attempt of the same patch measured 8/8 ten
// times per file and was taken as evidence it was harmless; that reading is not reproducible and is
// not trusted. Lesson: judge such a change by the sweep (it is stable), not by the matrix alone.
// Next step there: find which statement it costs and which it gains, then narrow the condition.
//
// The other route - exposing the correlated columns in the decorrelator (apply_decorrelation.cpp,
// before the "cannot lift this correlation" guard) - was tried three times: on its own, with
// RewriteEveryBinding over the sub-tree's expressions, and with the join conditions rewritten too.
// All three gave 5/8 (and one run of the first gave 8/8: a false green). Conclusion: widening a
// sub-tree's output needs exposure + a rewrite of every reference + a rebinding of the parents, or it
// belongs in the memo layer instead, the way identity (4) was done.
//
// Gate: the cascade matrix takes about a second, and single runs have produced false greens twice.
// Run it three times and require the same result.
// Where the remaining failures live and which layer has to fix them (all measured):
//
// The subquery suite fails in 55 of 88 files. Of those, 50 report "Not implemented", and running one
// with DUCKDB_CASCADE_PRINT shows the error is raised AFTER the memo's summary line - so the memo's
// rules declined the shape, an Apply was still in the chosen plan, and the ENFORCER threw. The layer
// that can fix them is therefore this one, not the decorrelator: exposing columns in the decorrelator
// was tried in three local forms (Rule 2 unconditionally, and the scalar path with and without the
// projection-only guard) and every one either changed nothing in the sweep or cost a matrix file -
// the wrong layer cannot help, because it only changes what the enforcer does.
//
// The shapes, counted over the failing files (the line the probe prints is `right=[...] below=[...]`):
//
//   15  right=[LOGICAL_PROJECTION] below=[LOGICAL_AGGREGATE_AND_GROUP_BY]   correlated column under
//                                                                          an aggregate - ORCA's
//                                                                          ExfScalarAggSubquery family,
//                                                                          paper identity (8)
//   14  right=[LOGICAL_PROJECTION] below=[LOGICAL_DUMMY_SCAN]              a sub-query with no table
//                                                                          at all: nothing to lift,
//                                                                          only the column itself
//    6  right=[LOGICAL_MATERIALIZED_CTE] below=[...]                       CTE / recursive CTE
//    4  right=[LOGICAL_PROJECTION] below=[LOGICAL_FILTER]
//    3  right=[LOGICAL_PROJECTION] below=[LOGICAL_UNNEST]
//    2  right=[LOGICAL_PROJECTION] below=[LOGICAL_WINDOW]
//    2  right=[LOGICAL_PROJECTION] below=[LOGICAL_PROJECTION]
//
// The first two classes are 29 of the 50 and are where to start.
// The rule that should carry these shapes, and how the pieces map. The goal is to express sub-query
// unnesting in the memo as transformations, not to grow the apply-based decorrelation - so the work
// belongs in a new rule, with these as its inputs:
//
//   class (count)                  apply type   ORCA transform to mirror        rule shape
//   Projection<-Aggregate  (15)    SINGLE (8)   ExfScalarAggSubquery            group the left side
//                                                                              by the correlated
//                                                                              columns, LEFT JOIN
//                                                                              the aggregate result,
//                                                                              project what the
//                                                                              query needs
//   Projection<-DummyScan  (14)    SINGLE/MARK  ExfScalarSubquery / no-         nothing to lift; the
//                                              correlations variant            correlated column is
//                                                                              the whole problem
//   Distinct<-Union                 MARK (7)    ExfPushJoinBelowUnionAll       push the correlation
//                                                                              into the branches
//   Materialized CTE        (6)    SINGLE/MARK  the CTE family                 needs the CTE
//                                                                              machinery, later
//   Filter / Unnest / Window / Projection (11) SINGLE  each its own            one at a time
//
// A rule is written the way push_filter_below_groupby is: Matches on LOGICAL_DEPENDENT_JOIN, a Promise
// that decides applicability read-only (the shape checks the census came from), and an Apply that
// builds the replacement. Two output paths, and which one is legal depends on the column set: if the
// replacement exposes what the Apply exposed, replace the expression in its own group; if it changes
// the columns, its parents have to be rebound as well - the session's own measurements are blunt about
// that (a rule that widened a sub-tree without rebinding its parents cost three matrix files).
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

//! Point a comparison at one child per side. The two inputs' bindings are known (measured for the
//! EXISTS shape: left child {#[0.0]}, filter child {#[7.0]}, predicate reading 7.0 on its left and
//! 0.0 on its right), so the sides are decided here rather than by a correlation-list heuristic -
//! that heuristic put an inner column on the left input and left a side empty, which is where the
//! NULL dereference inside the join-condition helper came from. Anything that cannot be put one side
//! per child is refused, and refused before the memo is touched.
bool OrientCondition(Expression &condition, const vector<ColumnBinding> &left_side,
                     const vector<ColumnBinding> &right_side) {
	if (!BoundComparisonExpression::IsComparison(condition) ||
	    condition.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &comparison = condition.Cast<BoundFunctionExpression>();
	auto &lhs = BoundComparisonExpression::LeftMutable(comparison);
	auto &rhs = BoundComparisonExpression::RightMutable(comparison);
	auto left_only = [&](const Expression &expr) {
		return ApplyReadsBindings(expr, left_side) && !ApplyReadsBindings(expr, right_side);
	};
	auto right_only = [&](const Expression &expr) {
		return ApplyReadsBindings(expr, right_side) && !ApplyReadsBindings(expr, left_side);
	};
	if (left_only(*lhs) && right_only(*rhs)) {
		return true;
	}
	if (right_only(*lhs) && left_only(*rhs)) {
		auto moved = std::move(lhs);
		lhs = std::move(rhs);
		rhs = std::move(moved);
		BoundComparisonExpression::FlipType(comparison);
		return true;
	}
	return false;
}

//! Phase one, read-only: is every correlated reference in this sub-tree inside a filter predicate,
//! and is each such predicate orientable against the two inputs? Those can become join conditions - a
//! condition is evaluated over both children, so it can name the outer columns - while a reference
//! anywhere else cannot, because a rule has no way to carry a column to the right side (a hand-built
//! delimiter segfaults, measured). Checking before moving keeps a declined rewrite from touching the
//! memo: an earlier version extracted as it walked and left the sub-tree without its predicates when
//! it then declined.
bool LiftableCorrelatedInMemo(Memo &memo, GroupId group, const CorrelatedColumns &correlated,
                              const vector<ColumnBinding> &left_side, const vector<ColumnBinding> &right_side,
                              unordered_set<GroupId> &visited) {
	if (!visited.insert(group).second) {
		return true;
	}
	bool liftable = true;
	for (auto &expr : memo.GetGroup(group).exprs) {
		if (!expr->op) {
			continue;
		}
		for (auto &expression : expr->op->expressions) {
			bool reads = false;
			ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
			    *expression, [&](const BoundColumnRefExpression &colref) {
				    for (auto &entry : correlated) {
					    if (entry.binding == colref.Binding()) {
						    reads = true;
					    }
				    }
			    });
			if (!reads) {
				continue;
			}
			if (expr->type != LogicalOperatorType::LOGICAL_FILTER) {
				liftable = false;
				continue;
			}
			auto copy = expression->Copy();
			if (!OrientCondition(*copy, left_side, right_side)) {
				liftable = false;
			}
		}
		for (auto child : expr->children) {
			if (!LiftableCorrelatedInMemo(memo, child, correlated, left_side, right_side, visited)) {
				liftable = false;
			}
		}
	}
	return liftable;
}

//! Phase two: move them. A filter's columns pass through, so one left with fewer predicates - or with
//! none - changes nothing about the columns its group exposes.
void MoveCorrelatedInMemo(Memo &memo, GroupId group, const CorrelatedColumns &correlated,
                          vector<unique_ptr<Expression>> &lifted, unordered_set<GroupId> &visited) {
	if (!visited.insert(group).second) {
		return;
	}
	for (auto &expr : memo.GetGroup(group).exprs) {
		if (!expr->op) {
			continue;
		}
		if (expr->type == LogicalOperatorType::LOGICAL_FILTER && !expr->op->expressions.empty()) {
			vector<unique_ptr<Expression>> local;
			for (auto &predicate : expr->op->expressions) {
				if (ReferencesCorrelation(*predicate, correlated)) {
					lifted.push_back(std::move(predicate));
				} else {
					local.push_back(std::move(predicate));
				}
			}
			expr->op->expressions = std::move(local);
		}
		for (auto child : expr->children) {
			MoveCorrelatedInMemo(memo, child, correlated, lifted, visited);
		}
	}
}

//! Does anything in this sub-tree read one of the correlated columns? The predicates that move into
//! the join condition are read from the filter itself, not from its child, so a reference below the
//! filter is one the condition cannot account for - and carrying it to the right side needs the
//! delim machinery, which a rule cannot construct (measured: a hand-built delim join segfaults).
bool SubtreeReadsCorrelated(Memo &memo, GroupId group, const CorrelatedColumns &correlated,
                            unordered_set<GroupId> &visited) {
	if (!visited.insert(group).second) {
		return false;
	}
	for (auto &expr : memo.GetGroup(group).exprs) {
		if (!expr->op) {
			continue;
		}
		for (auto &expression : expr->op->expressions) {
			bool reads = false;
			ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
			    *expression, [&](const BoundColumnRefExpression &colref) {
				    for (auto &entry : correlated) {
					    if (entry.binding == colref.Binding()) {
						    reads = true;
					    }
				    }
			    });
			if (reads) {
				return true;
			}
		}
		for (auto child : expr->children) {
			if (SubtreeReadsCorrelated(memo, child, correlated, visited)) {
				return true;
			}
		}
	}
	return false;
}

} // namespace

CascadesRulePromise CorrelatedApplyToJoin::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	auto decline = [](int line) -> CascadesRulePromise {
		if (CascadeConfig::PrintPlans()) {
			Printer::Print(StringUtil::Format(
			    "--- cascade(cascades) rule correlated_apply_to_join declined at line %d", line));
		}
		return CascadesRulePromise::NONE;
	};

	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	if (CascadeConfig::PrintPlans()) {
		Printer::Print(StringUtil::Format(
		    "--- cascade(cascades) apply seen: join_type=%d condition=%s correlated=%llu", (int)apply.join_type,
		    apply.condition ? "yes" : "no", (unsigned long long)apply.correlated_columns.size()));
	}
	if (apply.condition || apply.correlated_columns.empty()) {
		return decline(__LINE__);
	}
	if (!ReplacesWithCorrelatedJoin(apply.join_type)) {
		return decline(__LINE__);
	}
	GroupExpr *projection = nullptr;
	GroupExpr *filter = nullptr;
	if (!FindCorrelatedShape(optimizer, expr, projection, filter)) {
		return decline(__LINE__);
	}
	auto &memo = optimizer.GetMemo();
	auto &left_group = memo.GetGroup(expr.children[0]);
	auto &right_group = memo.GetGroup(filter->children[0]);
	if (left_group.exprs.empty() || right_group.exprs.empty()) {
		return decline(__LINE__);
	}
	if (!PredicatesPairBothSides(*filter, left_group.exprs[0]->bindings, right_group.exprs[0]->bindings)) {
		return decline(__LINE__);
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
				// A negated consumer is no longer a refusal: Memo::FindMarkConsumer reports the
				// negation and the replacement becomes an anti join, which is what NOT EXISTS means -
				// a row with no matching row, NULL comparisons included. The refusal that used to
				// stand here belonged to the MARK-join version, which kept the consumer in place and
				// therefore got the NULL case wrong.
				(void)negated;
			}
		}
	}
	return CascadesRulePromise::HIGH;
}

bool CorrelatedApplyToJoin::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	if (!expr.op) {
		return false;
	}
	GroupExpr *projection = nullptr;
	GroupExpr *filter = nullptr;
	if (!FindCorrelatedShape(optimizer, expr, projection, filter)) {
		return false;
	}
	auto &memo = optimizer.GetMemo();
	auto &dependent = expr.op->Cast<LogicalDependentJoin>();
	auto &filter_child = memo.GetGroup(filter->children[0]);
	if (filter_child.exprs.empty()) {
		return false;
	}
	GroupId consumer_group = INVALID_GROUP_ID;
	GroupExpr *consumer = nullptr;
	bool negated = false;
	if (!memo.FindMarkConsumer(group, consumer_group, consumer, negated)) {
		return false;
	}
	auto consumers = memo.ParentsOf(consumer_group);
	if (consumers.empty()) {
		return false;
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) STEP sides");
	}
	auto &left_group = memo.GetGroup(expr.children[0]);
	if (left_group.exprs.empty()) {
		return false;
	}
	auto &left_side = left_group.exprs[0]->bindings;
	auto &right_side = filter_child.exprs[0]->bindings;

	// Collect and orient every condition first; nothing below runs unless every reason to decline has
	// been ruled out, so a refused rewrite leaves the memo exactly as it was.
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) STEP collect");
	}
	vector<unique_ptr<Expression>> conditioned;
	for (auto &predicate : filter->op->expressions) {
		auto copy = predicate->Copy();
		if (!OrientCondition(*copy, left_side, right_side)) {
			if (CascadeConfig::PrintPlans()) {
				Printer::Print("--- cascade(cascades) rule " + string(Name()) +
				               ": declined, a predicate cannot be put one side per input");
			}
			return false;
		}
		conditioned.push_back(std::move(copy));
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) STEP check-liftable");
	}
	{
		unordered_set<GroupId> visited;
		if (!LiftableCorrelatedInMemo(memo, filter->children[0], dependent.correlated_columns, left_side, right_side,
		                              visited)) {
			if (CascadeConfig::PrintPlans()) {
				Printer::Print("--- cascade(cascades) rule " + string(Name()) +
				               ": declined, the correlation is read where it cannot be lifted from");
			}
			return false;
		}
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) STEP move");
	}
	{
		unordered_set<GroupId> visited;
		MoveCorrelatedInMemo(memo, filter->children[0], dependent.correlated_columns, conditioned, visited);
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) STEP reorient");
	}
	for (auto &condition : conditioned) {
		if (!OrientCondition(*condition, left_side, right_side)) {
			return false;
		}
	}

	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) STEP build-join");
	}
	auto join = make_uniq<LogicalComparisonJoin>(negated ? JoinType::ANTI : JoinType::SEMI);
	for (auto &condition : conditioned) {
		auto &comparison = condition->Cast<BoundFunctionExpression>();
		auto &lhs = BoundComparisonExpression::LeftMutable(comparison);
		auto &rhs = BoundComparisonExpression::RightMutable(comparison);
		join->conditions.emplace_back(std::move(lhs), std::move(rhs), comparison.GetExpressionType());
	}
	join->estimated_cardinality = expr.op->estimated_cardinality;
	join->has_estimated_cardinality = expr.op->has_estimated_cardinality;
	auto join_group = memo.AddGroup();
	optimizer.AddExpression(join_group, memo.MakeExpr(std::move(join), {expr.children[0], filter->children[0]}));
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) STEP splice");
	}
	for (auto parent : consumers) {
		bool touched = false;
		for (auto &candidate : memo.GetGroup(parent).exprs) {
			for (auto &child : candidate->children) {
				if (child == consumer_group) {
					child = join_group;
					touched = true;
				}
			}
		}
		if (touched) {
			optimizer.Reschedule(parent);
		}
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print(StringUtil::Format("--- cascade(cascades) rule %s: spliced group %llu, %s join in group %llu",
		                                  Name(), (unsigned long long)consumer_group, negated ? "ANTI" : "SEMI",
		                                  (unsigned long long)join_group));
	}
	return true;
}

} // namespace duckdb
