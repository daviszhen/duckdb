//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/rules/apply_to_join.hpp
//
// Identity (1)/(2) of Galindo-Legaria & Joshi (SIGMOD 2001): an Apply whose right
// side has no correlation *is* a join.
//
//   R A_× E = R x E            (E does not reference R)
//
// The counterpart is ORCA's ExfInnerApply2InnerJoin in its NoCorrelations case.
// This is the rule that makes an Apply *disappear*, which is the only thing that
// can lower the enforcer counter - and it is why it comes before identity (3)/(4):
// those move predicates around an Apply, this one removes it.
//
// This first version takes the case that needs no side convention and no column
// exposure: inner, no correlated columns, no condition. That it fires at all is a
// measurable question - see the note in the commit - and the answer decides which
// rule is needed next.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/cascade/cascades/rule.hpp"

namespace duckdb {

class ApplyToJoin : public CascadesRule {
public:
	ApplyToJoin() : CascadesRule(CascadesRuleKind::SUBSTITUTION, "apply_to_join") {
	}

	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};

} // namespace duckdb
