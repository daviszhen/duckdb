//! ORCA CXformLeftOuterApply2LeftOuterJoinNoCorrelations, EXformId 34.
//!
//! An outer Apply whose right side reads none of the left side's columns is a left outer join over
//! the same two inputs: every left row survives, matched or null-extended. apply_to_join covers the
//! inner counterpart (EXformId 31); this is the outer one, which that rule refuses on purpose.
//! Kind: SUBSTITUTION.
#pragma once
#include "duckdb/cascade/cascades/rule.hpp"
namespace duckdb {
class LeftOuterApplyToJoin : public CascadesRule {
public:
	int OrcaId() const override {
		return 34;
	}
	LeftOuterApplyToJoin();
	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};
}
