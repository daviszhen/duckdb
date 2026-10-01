//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/rules/group_apply_by_outer_columns.hpp
//
// Section 3.2 of Galindo-Legaria & Joshi (SIGMOD 2001):
//
//     G_{A,F}( S LOJ_p R ) = pi_c( S LOJ_p ( G_{A-columns(S),F} R ) )
//
// A correlated scalar sub-query whose body aggregates is decorrelated by pushing
// that GroupBy below the outer join: the sub-query is grouped by the columns the
// correlated predicate pins down (`s.c = <outer expression>` makes s.c one of the
// group keys), the outer side is LEFT JOINed to the resulting aggregate, and the
// value is repaired with pi_c - COALESCE(..., 0) - exactly where NULL is not the
// empty-input answer, i.e. for count and count_star.
//
// The preconditions, all of them checked in Promise so that a task which could
// not bind is never queued:
//
//   * the Apply is the scalar one (JoinType::SINGLE) with no ON condition and at
//     least one correlated column;
//   * the sub-query's root is a projection chain over one aggregate;
//   * every sub-query column the correlated predicate names is functionally
//     determined by the grouping columns, which after the push are the outer
//     columns: `s.c = <outer expression>` qualifies, `s.a + s.b = t.a` does not,
//     and any other correlated predicate declines the whole rule;
//   * the aggregate functions read only the sub-query's own columns: below the
//     outer join the outer columns are not in scope, so `sum(s.b + t.a)` stays
//     with identity (9), whose GroupBy sits above the join;
//   * the two guards section 3.1 (A)'s pushdown carries, copied here: no more
//     than one grouping set (GROUPING SETS pad the columns a set does not mention
//     with NULL, so a predicate on such a column is not constant within a group),
//     and every grouping column's expression below the aggregate a plain
//     BOUND_COLUMN_REF;
//   * the aggregate has no grouping of its own - the recipe at
//     apply_decorrelation_scalar.cpp:438-458 implements the scalar half, and an
//     aggregate that already groups is identity (8)'s case, not this one.
//
// This is the scalar decorrelator's pushdown branch
// (apply_decorrelation_scalar.cpp:438-458) moved onto the memo. It is more
// faithful than identity (9): count and list come out right by construction,
// instead of through the fiction that agg(empty) = agg({NULL}).
//
// ORCA counterpart: there is none as a registered xform. ORCA decorrelates this
// shape inside CSubqueryHandler / CDecorrelator, i.e. as handler logic rather
// than as a memo rule. ExfScalarAggSubquery is NOT an ORCA rule id - against
// ORCA's CXform.h, 152 rules are registered and EXformId names 153 values
// (ExfInvalid included) - so it is deliberately not cited here.
//
// Kind: EXPLORATION - it offers an alternative for the Apply's equivalence class
// rather than replacing it. Whether the pushdown wins is the cost model's
// decision, and the paper leaves it to the optimizer for the same reason: the
// aggregate below the join is not free, and only pays when the outer side
// repeats fewer keys than the sub-query has rows.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/cascade/cascades/rule.hpp"

namespace duckdb {

class GroupApplyByOuterColumns : public CascadesRule {
public:
	GroupApplyByOuterColumns() : CascadesRule(CascadesRuleKind::EXPLORATION, "group_apply_by_outer_columns") {
	}

	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};

} // namespace duckdb
