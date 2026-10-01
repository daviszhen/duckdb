//! ORCA CXformSelect2Filter, EXformId 13.
//!
//! ORCA keeps a Select (one predicate) apart from a Filter and this xform wraps the predicate into
//! the Filter operator. DuckDB has no Select node: its binder produces LogicalFilter directly, so
//! there is no pre-shape in a cascade input to transform. What can be migrated is the invariant the
//! xform establishes - a filter that carries its predicates and no sub-query expression - and that is
//! what this rule checks, reporting a violation through the log instead of assuming it away.
//! It changes no plan; its kind is EXPLORATION so the search may run it anywhere a filter appears.
#pragma once
#include "duckdb/cascade/cascades/rule.hpp"
namespace duckdb {
class Select2Filter : public CascadesRule {
public:
	Select2Filter();
	bool Matches(GroupExpr &expr) override;
	CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) override;
	bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) override;
};
}
