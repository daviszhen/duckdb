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

unique_ptr<LogicalOperator> CascadeOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade: bound logical plan (pre-optimization) ---");
		Printer::Print(plan->ToString(&context));
	}

	// Apply elimination: ours, in place of FlattenDependentJoins.
	ApplyDecorrelator decorrelator(binder, context);
	plan = decorrelator.Decorrelate(std::move(plan));

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
		// Fair comparison: our rewrite, then DuckDB's own downstream passes over it.
		//
		// FIXME: one of those passes, FilterPullup, is wrong on the plan identity (5)/(6) builds.
		// That plan is a set operation whose branches each read the shared outer relation through
		// the materialised CTE, and FilterPullup pulls a filter out of the left branch up above
		// the EXCEPT - rewriting its column references to the set operation's positions - which
		// changes the multiset difference. A four-line repro (cascade + KEEP_APPLY + OPTIMIZE):
		//
		//     SELECT s.a FROM tt, (SELECT a FROM tu WHERE tu.a = tt.k
		//                          EXCEPT ALL SELECT b FROM tv WHERE tv.b = tt.k) s;
		//
		// answers 1,1 without the optimizer and 1,1,1,1,2,2,2,2,2,2,5,5,5,5 with it; disabling
		// `filter_pullup` by hand restores 1,1. Until the pass is fixed, keep it away from this
		// shape - the plan we build is verified correct on its own (the sqllogictest pins it).
		// ... CompressedMaterialization is wrong on the plan identity (9) builds: it narrows a
		// group-by key and expects to compensate above, but on this shape it leaves a reference to
		// a column that is no longer in scope:
		//
		//     SELECT a, (SELECT count(*) FROM s WHERE s.a < t.a) FROM t;
		//
		// `Failed to bind column reference "" [18.0] (bindings: {#[0.0]})`; disabling the pass by
		// hand answers it. The equality forms happen to survive, but the guard covers the whole
		// identity - a lost narrowing pass costs speed, a wrong plan costs correctness.
		//
		// Both guards are per plan: every other plan keeps the passes.
		auto &disabled = DBConfig::GetConfig(context).options.disabled_optimizers;
		vector<OptimizerType> added;
		auto guard = [&](OptimizerType type) {
			if (disabled.insert(type).second) {
				added.push_back(type);
			}
		};
		if (decorrelator.DistributedSetOperation()) {
			guard(OptimizerType::FILTER_PULLUP);
		}
		if (decorrelator.ScalarAggregate()) {
			guard(OptimizerType::COMPRESSED_MATERIALIZATION);
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
