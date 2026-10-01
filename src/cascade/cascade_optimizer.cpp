// The cascade pipeline, in the order it runs, and where each stage comes from in
// Galindo-Legaria & Joshi, "Orthogonal Optimization of Subqueries and Aggregation"
// (SIGMOD 2001):
//
//   ApplyDecorrelator              section 2, Figure 4 identities (1)-(9); classes in 2.5
//   SimplifyMarkerJoins            section 2, marker -> semijoin/antijoin
//   ReorderGroupBy                 section 3.1, rules (A) and (D)
//   -- DuckDB's own optimizer, only with DUCKDB_CASCADE_OPTIMIZE=1 --
//   AggregatePullup                section 3.1 pull-up (the primitive 3.4.2 executes)
//   AggregatePushdown              section 3.1 push-down
//   LocalAggregatePusher           section 3.3, local/global split
//   BuildSegmentApplyAlternatives  section 3.4.1 and 3.4.2 (Figure 6/7)
//   -- or, with the optimizer off, DuckDB's mandatory aggregate rewrites --
//
// Two things are worth knowing when reading a plan produced here. Our rewrite runs before
// DuckDB's optimizer, so the optimizer sees plans it did not build; PASS_GUARDS below lists
// the passes that mis-rewrite the plans particular identities produce, each with its repro.
// And every rule is behind a switch (see cascade_config.hpp), off unless the switch is set,
// so the default path is never touched.
//===----------------------------------------------------------------------===//

#include "duckdb/cascade/cascade_optimizer.hpp"

#include "duckdb/cascade/aggregate_pullup.hpp"
#include "duckdb/cascade/aggregate_pushdown.hpp"
#include "duckdb/cascade/apply_decorrelation.hpp"
#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/groupby_reorder.hpp"
#include "duckdb/cascade/local_aggregate.hpp"
#include "duckdb/cascade/segment_apply.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/subquery/flatten_dependent_join.hpp"

namespace duckdb {

CascadeOptimizer::CascadeOptimizer(Binder &binder_p, ClientContext &context_p)
    : context(context_p), binder(binder_p), duck(binder_p, context_p) {
}

ClientContext &CascadeOptimizer::GetContext() {
	return context;
}

Binder &CascadeOptimizer::GetBinder() {
	return binder;
}

namespace {

//! A DuckDB optimizer pass that must not look at a plan one of the identities below built,
//! together with the shape that trips it. Every entry is a bug report: the pass rewrites the
//! plan we hand it in a way that leaves a stale binding behind (or silently changes the answer),
//! and the plan we build on its own is verified correct by the sqllogictests. Keeping the repro
//! next to the guard is the point of the table - a guard without one is not a bug fix.
struct PassGuard {
	//! Which decorrelator outcome brings the shape about.
	bool (ApplyDecorrelator::*triggers)() const;
	//! The pass that cannot see it.
	OptimizerType pass;
};

const PassGuard PASS_GUARDS[] = {
    // Identity (5)/(6) - an Apply distributed over a set operation - builds a set operation
    // whose branches each read the shared outer relation through a materialised CTE.
    // FilterPullup pulls a filter out of the left branch up above the EXCEPT and rewrites its
    // column references to the set operation's positions, which changes the multiset
    // difference. Repro (cascade + KEEP_APPLY + OPTIMIZE):
    //     SELECT s.a FROM tt, (SELECT a FROM tu WHERE tu.a = tt.k
    //                          EXCEPT ALL SELECT b FROM tv WHERE tv.b = tt.k) s;
    // answers 1,1 without the optimizer and 1,1,1,1,2,2,2,2,2,2,5,5,5,5 with it.
    {&ApplyDecorrelator::DistributedSetOperation, OptimizerType::FILTER_PULLUP},
    // Identity (9) - a correlated scalar sub-query - groups over a left outer join.
    // CompressedMaterialization narrows that group-by key and expects to compensate above it,
    // but on this shape it leaves a reference to a column that is no longer in scope
    // (`Failed to bind column reference "" [18.0]`, `SELECT a, (SELECT count(*) FROM s
    // WHERE s.a < t.a) FROM t`). The equality forms happen to survive; the guard covers the
    // whole identity, because a lost narrowing pass costs speed and a wrong plan costs
    // correctness.
    {&ApplyDecorrelator::ScalarAggregate, OptimizerType::COMPRESSED_MATERIALIZATION},
    // Identity (8) - a correlated scalar sub-query whose body has a GroupBy of its own.
    // StatisticsPropagator derives a filter (e.g. `a = 1` from the WHERE clause) and pushes
    // it below the group-by the identity builds, where the group keys do not satisfy it: the
    // answer comes back empty instead of `1|2, 1|2` - a silent wrong answer, the worst mode.
    {&ApplyDecorrelator::NestedScalarAggregate, OptimizerType::STATISTICS_PROPAGATION},
    // Two correlated sub-queries sharing the outer relation. CommonSubplanOptimizer
    // materialises the shared part into `__common_subplan_N` and leaves the group keys of the
    // aggregates we built pointing at columns it moved (`Failed to bind column reference "a"
    // [30.0]`, two scalar sub-queries with non-equality correlations).
    {&ApplyDecorrelator::SharedSubQueries, OptimizerType::COMMON_SUBPLAN},
};

} // namespace

unique_ptr<LogicalOperator> CascadeOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade: bound logical plan (pre-optimization) ---");
		Printer::Print(plan->ToString(&context));
	}

	// Apply elimination: ours, in place of FlattenDependentJoins.
	ApplyDecorrelator decorrelator(binder, context);
	// A shape this pass cannot handle must not end the query here: the memo's rules are the
	// place those shapes are meant to be handled, and this pass runs *before* them, so
	// throwing would mean the rules never see the Apply at all. Keep the plan as it is and let
	// the pipeline try; if nothing there handles it either, the enforcer runs this same
	// decorrelator and raises the same exception - so an unhandled shape still fails, with the
	// same error, but only after the rules had their chance. Measured motivation: with the pass
	// throwing first, the memo reported `rules applied=0` and its `enforced` counter stayed 0,
	// i.e. no rule could ever be observed to take any of this work.
	auto untransformed = plan->Copy(context);
	try {
		plan = decorrelator.Decorrelate(std::move(plan));
	} catch (const NotImplementedException &) {
		plan = std::move(untransformed);
	}

	// Then turn the marker joins that produced into the semi/anti joins they mean.
	plan = SimplifyMarkerJoins(std::move(plan));

	// Section 3.1: an aggregate should see as few rows as the predicates allow, so
	// the predicates that are constant within a group move below the GroupBy - which
	// also covers the semijoin/antijoin consuming an aggregate, since the paper
	// treats those as filters.
	if (CascadeConfig::ReorderGroupBy()) {
		plan = ReorderGroupBy(std::move(plan), CascadeConfig::ReorderSemijoins());
	}

	if (CascadeConfig::RunDuckOptimizers()) {
		// Fair comparison: our rewrite, then DuckDB's own downstream passes over it - except for
		// the passes in PASS_GUARDS below, which mis-rewrite the plan an identity built. Each
		// entry carries its own repro; the guard is per plan and per shape, so every other plan
		// still runs the whole optimizer.
		auto &disabled = DBConfig::GetConfig(context).options.disabled_optimizers;
		vector<OptimizerType> added;
		for (auto &pass_guard : PASS_GUARDS) {
			if (!(decorrelator.*(pass_guard.triggers))()) {
				continue;
			}
			if (disabled.insert(pass_guard.pass).second) {
				added.push_back(pass_guard.pass);
			}
		}
		Optimizer optimizer(binder, context);
		plan = optimizer.Optimize(std::move(plan));
		for (auto type : added) {
			disabled.erase(type);
		}
	} else {
		// A plan must still satisfy the mandatory rewrites DuckDB applies even with
		// the optimizer disabled, otherwise the physical planner rejects it.
		plan = duck.LowerMandatoryAggregateRewrites(std::move(plan));
	}

	// Section 3.1's other direction, and the primitive section 3.4.2 executes: an
	// aggregate below an inner join moves above it, so the join reduces the rows the
	// aggregate reads instead of the aggregate reducing the rows the join reads. It is
	// only sound when the relation being joined has a key - with a duplicate there, one
	// outer row would match twice and its rows would be aggregated twice - and it removes
	// the global aggregation rather than keeping it, so it is a cost decision.
	if (CascadeConfig::PullUpAggregates()) {
		AggregatePullup pullup(binder, context);
		plan = pullup.Pull(std::move(plan));
	}

	// Section 3.1 in the other direction, and the paper's "aggregate, then join" strategy:
	// an aggregate above an inner join moves below it and disappears, so the join reads one
	// row per group instead of one per row. It is the dual of the pull-up above - there the
	// aggregate moves up to let the join reduce what is aggregated, here it moves down to
	// remove the global aggregation - so the two switches are alternatives, not a pipeline.
	if (CascadeConfig::PushDownAggregates()) {
		AggregatePushdown pushdown(binder, context);
		plan = pushdown.Push(std::move(plan));
	}

	// Section 3.3: reduce the rows that reach a join by aggregating one side first.
	// Keeping the global aggregation above the join is what makes this move
	// unconditional - the plain GroupBy pushdown of section 3.1 needs the other side
	// to be keyed, because it removes the global aggregation entirely.
	//
	// This runs last on purpose. It needs an aggregate whose child really is a join
	// with conditions; the plan the binder produces for an implicit join is a cross
	// product with a filter above it, and turning that into a join is DuckDB's
	// filter pushdown, which the cascade-only mode does not run.
	if (CascadeConfig::PushLocalAggregates()) {
		LocalAggregatePusher pusher(binder, context);
		plan = pusher.Push(std::move(plan));
	}

	// Section 3.4.1: introduce the SegmentApply alternatives. Rows whose value in the
	// segmenting column differs can never match, so the relation can be partitioned and the
	// parameterized side evaluated once per segment instead of once for everything.
	if (CascadeConfig::BuildSegmentApply()) {
		plan = BuildSegmentApplyAlternatives(std::move(plan), binder);
	}

	// ... and report the alternatives that are left, i.e. the shapes this rule did not take
	// (it is off by default, so by default this is all of them). They are the
	// shapes where correlation removal left two instances of the same expression joined
	// on the same column of the same table, which is what a SegmentApply would partition
	// on. This runs after the optimizer, because that is when an implicit join is a join
	// at all rather than a filter over a cross product. DuckDB's executor has no operator
	// that evaluates a sub-plan per segment, so the alternative is reported rather than
	// built - and the reordering the paper gains from it (an aggregate moving below the
	// join that filters its input) shows up as an ordinary rewrite.
	if (CascadeConfig::PrintPlans()) {
		auto alternatives = DescribeSegmentApplyAlternatives(*plan);
		if (!alternatives.empty()) {
			Printer::Print("--- cascade: SegmentApply alternatives (section 3.4.1): " +
			               StringUtil::Join(alternatives, ", "));
		}
	}

	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade: plan handed to PhysicalPlanGenerator ---");
		Printer::Print(plan->ToString(&context));
	}

	return plan;
}

} // namespace duckdb
