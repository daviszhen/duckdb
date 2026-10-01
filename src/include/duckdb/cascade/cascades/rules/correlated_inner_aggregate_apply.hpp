//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/rules/correlated_inner_aggregate_apply.hpp
//
// Probe stage of the inner-apply family that the corpus is made of: a correlated sub-query in
// the FROM clause that has a table of its own AND aggregates over the outer row:
//
//     SELECT * FROM integers i1, LATERAL (SELECT SUM(i + i1.i) FROM integers) t(sum) ORDER BY i;
//
// This file deliberately only *recognises* the shape and explains itself: Apply() returns false,
// so nothing in the plan can change. It exists because two earlier attempts failed for reasons
// that were not visible in the output:
//   * a rule that matched the CONSUMER (a projection above the Apply) was never accepted at all -
//     measured: its promise reported "no INNER Apply ... in the consumer's child group", i.e. the
//     Apply is not reachable from the consumer the rule was given. Matching the Apply itself, the
//     way CorrelatedApplyToJoin does, removes that whole class of failure.
//   * the whole-plan ApplyDecorrelator call in CascadeOptimizer::Optimize used to run before the
//     memo and threw on this shape, so no rule saw it at all (fixed by deferring that pass).
//
// Once the promise's reasons say the shape is understood, Apply() is where the rewrite goes:
//   expose (outer_refs -> ExposeRightColumns) -> equality conditions from the mapping -> INNER
//   join -> Aggregate grouped by the outer columns -> ReplaceExpression + Reschedule.
// A cross product is NOT the rewrite: the body reads the binder's materialisation of the outer
// columns, one row per outer row, so a product would sum over every outer row's values.
//
// Kind: SUBSTITUTION (once it replaces the Apply).
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/cascade/cascades/rule.hpp"

namespace duckdb {

class CorrelatedInnerAggregateApply : public CascadesRule {
public:
	CorrelatedInnerAggregateApply();

public:
	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};

} // namespace duckdb
