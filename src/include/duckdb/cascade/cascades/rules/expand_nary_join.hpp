//! ORCA's NAry join expansion family - the join order entry point of the authoritative list:
//!   CXformExpandNAryJoin        (EXformId 1)
//!   CXformExpandNAryJoinMinCard (EXformId 2)
//!   CXformExpandNAryJoinDP      (EXformId 3)
//!
//! A join of more than two inputs is what ORCA sorts: it first expands the n-ary join into a binary
//! tree and then reorders it. The three rules differ only in the order they pick, and each offers
//! its tree as another expression of the same group, so the cost model chooses between them - which
//! is why they are migrated together.
//!
//! Conditions are placed on the join that introduces their last input, so no step is left without a
//! condition and the expansion never turns into a cartesian product: if some step would have to be
//! condition-free, the rule declines instead (a cross product in a chosen plan is a regression).
//! Kind: EXPLORATION.
#pragma once
#include "duckdb/cascade/cascades/rule.hpp"
namespace duckdb {
class ExpandNAryJoin : public CascadesRule {
public:
	ExpandNAryJoin();
	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};

class ExpandNAryJoinMinCard : public CascadesRule {
public:
	ExpandNAryJoinMinCard();
	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};

class ExpandNAryJoinDP : public CascadesRule {
public:
	ExpandNAryJoinDP();
	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};
}
