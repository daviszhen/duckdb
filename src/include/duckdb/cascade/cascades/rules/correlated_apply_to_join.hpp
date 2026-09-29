//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/rules/correlated_apply_to_join.hpp
//
// Identity (4) of Galindo-Legaria & Joshi (SIGMOD 2001): the predicate that made an
// Apply dependent becomes a condition of the join that replaces it.
//
//   Apply(L, Projection(Filter(p)))  ->  Join(L, Projection(F.child))
//
// p reads both sides; that is the correlation, and it is the join condition. The
// counterparts are ORCA's ExfLeftSemiApply2LeftSemiJoin, ExfLeftAntiSemiApply2Left-
// AntiSemiJoin, and the mark join an existence sub-query takes in DuckDB - which is
// what this corpus produces (join_type=7, MARK; SINGLE=8 for the scalar case, which
// this rule does not handle because its right side holds an aggregate).
//
// The mark column is why mark_index matters: a MARK join exposes the left side plus
// that column, so the replacement carries the Apply's mark_index or the parent's
// reference to it stops binding.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/cascade/cascades/rule.hpp"

namespace duckdb {

class CorrelatedApplyToJoin : public CascadesRule {
public:
	CorrelatedApplyToJoin() : CascadesRule(CascadesRuleKind::SUBSTITUTION, "correlated_apply_to_join") {
	}

	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};

} // namespace duckdb
