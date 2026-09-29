//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/rules/lift_local_predicate.hpp
//
// Section 2, identity (3) of Galindo-Legaria & Joshi (SIGMOD 2001), in the half
// that needs no bookkeeping:
//
//   R A_x (sigma_p E) = sigma_p (R A_x E)      (p reads only E)
//
// A predicate that reads only the sub-query's own columns moves above the Apply.
// The counterpart is ORCA's ExfSelect2Apply/ExfInnerApply2InnerJoin family; the
// paper uses this direction to make the correlation the only thing left inside the
// Apply, which is what the correlated rule then has to deal with.
//
// The half that is implemented here is the one where nothing else has to move: a
// right-only predicate does not touch the correlation list (no depth to decrement,
// no columns to expose), and an inner or cross Apply exposes both sides' columns
// either way, so the group keeps its columns. What it deliberately does not do is
// the semi/anti case, where lifting is simply wrong: "some matching row satisfies
// p" is not "the row satisfies p".
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/cascade/cascades/rule.hpp"

namespace duckdb {

class LiftLocalPredicate : public CascadesRule {
public:
	LiftLocalPredicate() : CascadesRule(CascadesRuleKind::SUBSTITUTION, "lift_local_predicate") {
	}

	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};

} // namespace duckdb
