//! ORCA ExfCollapseProject (CXform::EXformId 139): two adjacent projections become one. Only the
//! safe half is migrated here - the inner projection must be a plain re-numbering (every expression
//! a column reference), so the outer expressions can be re-pointed at the inner child by a binding
//! remap. Composing arbitrary computed expressions would change how often they are evaluated.
//! Kind: EXPLORATION.
#pragma once
#include "duckdb/cascade/cascades/rule.hpp"
namespace duckdb {
class CollapseProject : public CascadesRule {
public:
	CollapseProject();
	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};
}
