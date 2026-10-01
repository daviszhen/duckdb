//! ORCA CXformUnion2UnionAll, EXformId 65.
//!
//! A UNION is a UNION ALL with the duplicates removed, and ORCA states it that way so that the
//! cheaper rules for UNION ALL (pushing work below the set operation) apply to both. The rule offers
//! exactly that alternative: the same children, the same table index, with a distinct on top.
//! Kind: EXPLORATION.
#pragma once
#include "duckdb/cascade/cascades/rule.hpp"
namespace duckdb {
class Union2UnionAll : public CascadesRule {
public:
	int OrcaId() const override {
		return 65;
	}
	Union2UnionAll();
	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};
}
