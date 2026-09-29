//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/rules/semi_apply_to_join.hpp
//
// The correlated case of identity (4) of Galindo-Legaria & Joshi (SIGMOD 2001), for
// the shape the corpus actually contains: the correlated predicate of an existence
// sub-query becomes the condition of a semi or anti join.
//
//   Apply_semi(L, Projection(Filter(p)))  ->  Join_semi(L, Projection(F.child))
//
// p reads both sides; that is what made the Apply dependent, and it is what a semi
// join condition is. The counterpart is ORCA's ExfLeftSemiApply2LeftSemiJoin /
// ExfLeftAntiSemiApply2LeftAntiSemiJoin.
//
// Same narrowness as the other rules: the predicates have to be comparisons pairing
// the two sides (all of them), the Apply must carry no condition of its own, and the
// projection must sit directly on the filter. Anything else declines, and says so.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/cascade/cascades/rule.hpp"

namespace duckdb {

class SemiApplyToJoin : public CascadesRule {
public:
	SemiApplyToJoin() : CascadesRule(CascadesRuleKind::SUBSTITUTION, "semi_apply_to_join") {
	}

	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};

} // namespace duckdb
