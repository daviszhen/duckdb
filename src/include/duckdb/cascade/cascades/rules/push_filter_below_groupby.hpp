//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/rules/push_filter_below_groupby.hpp
//
// Section 3.1 (A) of Galindo-Legaria & Joshi (SIGMOD 2001):
//
//     sigma_p(G_{A,F} R) = G_{A,F}(sigma_p R)
//
// A predicate that is constant within a group moves below the GroupBy, so the
// aggregate sees fewer rows. The condition is that every column p reads is
// determined by the grouping columns A; this first version implements the plain
// half of that - p reads grouping columns whose expression below the aggregate is
// a column reference - and declines everything else.
//
// This is the pipeline's `groupby_reorder.cpp` moved onto the memo, and the
// counterpart of ORCA's ExfPushGbBelowJoin / ExfPushGbWithHavingBelowJoin: read
// its Exfp() for the condition, i.e. when the rule reports "not applicable"
// rather than producing an expression it would have to throw away.
//
// Kind: EXPLORATION - it offers an alternative for the same group rather than
// replacing anything. Which of the two wins is the cost model's decision, and the
// paper leaves it to the cost-based optimizer for exactly that reason: the filter
// is extra work, and only pays when it keeps enough rows out of the aggregate.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/cascade/cascades/rule.hpp"

namespace duckdb {

class PushFilterBelowGroupBy : public CascadesRule {
public:
	PushFilterBelowGroupBy() : CascadesRule(CascadesRuleKind::EXPLORATION, "push_filter_below_groupby") {
	}

	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};

} // namespace duckdb
