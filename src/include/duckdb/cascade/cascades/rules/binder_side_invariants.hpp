//! The ORCA rules whose pre-shape DuckDB's binder already removed, migrated as the invariants they
//! establish. One parameterised rule covers them: the constructor names it, says which operator it
//! watches, and which invariant it checks. Each instance reports a violation in the log instead of
//! leaving the property to a comment.
//!
//!   EXformId 10 CXformUnnestTVF                    -> an Unnest carries its expressions
//!   EXformId 17 CXformSimplifySelectWithSubquery   -> a filter predicate holds no sub-query
//!   EXformId 18 CXformSimplifyProjectWithSubquery  -> a projection expression holds no sub-query
//!   EXformId 19 CXformSelect2Apply                 -> an Apply has its two inputs
//!   EXformId 20 CXformProject2Apply                -> (same, the Apply the binder built)
//!   EXformId 21 CXformGbAgg2Apply                  -> (same)
//!   EXformId 14 CXformSelect2IndexGet              -> the get has an access path bound
//!   EXformId 15 CXformSelect2DynamicIndexGet       -> (same)
//!   EXformId 16 CXformSelect2PartialDynamicIndexGet-> (same)
//! Kind: EXPLORATION. Apply changes nothing by design.
#pragma once
#include "duckdb/cascade/cascades/rule.hpp"
namespace duckdb {
enum class BinderSideInvariant : uint8_t { HAS_EXPRESSIONS, NO_SUBQUERY, TWO_INPUTS, GET_HAS_ACCESS_PATH };
class BinderSideInvariantRule : public CascadesRule {
public:
	BinderSideInvariantRule(const char *name, LogicalOperatorType watched, BinderSideInvariant invariant)
	    : CascadesRule(CascadesRuleKind::EXPLORATION, name), watched(watched), invariant(invariant) {
	}
	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;

private:
	LogicalOperatorType watched;
	BinderSideInvariant invariant;
};
}
