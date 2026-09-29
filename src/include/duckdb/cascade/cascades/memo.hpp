//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/memo.hpp
//
// The memo of the cascade optimizer: the set of logical expressions known to be
// equivalent, plus the plan each equivalence class has chosen.
//
// Structure follows ORCA's CMemo / CGroup / CGroupExpression, trimmed to a
// single-node host:
//
//   Group      one logical equivalence class - expressions that produce the same
//              columns and can replace each other
//   GroupExpr  one expression in it: the operator, plus its children *by GroupId*
//              (never by pointer, or a rule could not splice in an alternative)
//   context    the cache unit is (group, required properties), i.e. ORCA's
//              COptimizationContext. With no physical rules yet the property set
//              is empty, so one context per group - but the shape is in place.
//
// Reference: Graefe, "The Cascades Framework for Query Optimization" (1995);
// Soliman et al., "Orca" (SIGMOD 2014); see CASCADE_OPTIMIZER_REFS.md section 10.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ClientContext;

//! A logical equivalence class.
using GroupId = idx_t;
static constexpr GroupId INVALID_GROUP_ID = DConstants::INVALID_INDEX;

//! What a consumer requires of the plan it gets (ORCA's CReqdPropPlan). Empty for now: the
//! skeleton has no physical rules, so nothing can be required beyond "a plan for this group".
//! Physical properties (order, and the build/probe choice) land here in the next stage.
struct RequiredProperties {
	bool operator==(const RequiredProperties &other) const {
		return true;
	}
	bool operator!=(const RequiredProperties &other) const {
		return !(*this == other);
	}
	string ToString() const {
		return "<any>";
	}
};

//! ORCA's COptimizationContext: the unit a winner is cached under.
struct OptimizationContext {
	GroupId group = INVALID_GROUP_ID;
	RequiredProperties props;
};

//! One expression of a group: the operator with its children detached, recorded as group ids.
struct GroupExpr {
	LogicalOperatorType type = LogicalOperatorType::LOGICAL_INVALID;
	//! The operator itself, with `children` empty - they live in `children` below.
	unique_ptr<LogicalOperator> op;
	//! Child groups, in the operator's own child order.
	vector<GroupId> children;
	//! The columns this expression exposes. Captured while the operator still has its children -
	//! GetColumnBindings() walks them, and in the memo they are gone - so that invariant 2 can be
	//! checked at all: two expressions of one group have to expose the same columns.
	vector<ColumnBinding> bindings;
	//! Set once an implementation rule produced it; the skeleton has none, so a logical
	//! expression is also its own chosen plan (that is what "no physical rules" means).
	bool physical = false;
	//! Estimated cost, filled when the expression is optimised.
	double cost = 0;
	//! Estimated output rows, also filled when the expression is optimised: a parent's cost is
	//! charged on the rows that reach it, so every expression has to report what it emits.
	double rows = 0;
	//! Which rule produced it (0 = came from the input plan). For the stats printout.
	idx_t rule_id = 0;
};

struct Group {
	vector<unique_ptr<GroupExpr>> exprs;
	//! Exploration ran to a fixed point (ORCA: group explored).
	bool explored = false;
	idx_t logical_count = 0;
	idx_t physical_count = 0;
};

class Memo {
public:
	explicit Memo(ClientContext &context) : context(context) {
	}

	//! Copy `op` into the memo - children detached and memoised bottom-up - and return its group.
	GroupId Add(unique_ptr<LogicalOperator> op);

	//! A fresh, empty equivalence class. A rule that needs a place for an intermediate
	//! expression - the filter it pushed below the aggregate - creates one, then adds to it.
	GroupId AddGroup();

	//! Wrap an operator and the groups of its children into a memo expression. The operator's
	//! own `children` are left empty: in the memo they are group ids. The bindings are derived
	//! from the operator and the children's groups, since they cannot be asked of the operator.
	unique_ptr<GroupExpr> MakeExpr(unique_ptr<LogicalOperator> op, vector<GroupId> children);

	//! Check the invariants a memo has to satisfy, and say which one broke. They are the safety
	//! net under every rule: a rule that produces a group expression with a missing child, or one
	//! that exposes different columns than its group, is caught here rather than three passes
	//! later as a binding error.
	bool Validate(string &error) const;

	//! The expression the given context settled on, or nullptr when it has not been optimised.
	GroupExpr *WinnerOf(const OptimizationContext &context) const;
	void SetWinner(const OptimizationContext &context, GroupExpr *expr);

	//! Rebuild a plan from the winners below this group. The memo is consumed by this call:
	//! a plan is a tree in this host, so every group is extracted exactly once.
	unique_ptr<LogicalOperator> ExtractPlan(GroupId root);

	idx_t GroupCount() const {
		return groups.size();
	}
	idx_t ExprCount() const;
	idx_t PhysicalCount() const;
	Group &GetGroup(GroupId id) {
		return *groups[id];
	}
	const vector<unique_ptr<Group>> &Groups() const {
		return groups;
	}

private:
	ClientContext &context;
	vector<unique_ptr<Group>> groups;
	//! Winners by optimization context. A vector, not a map: the memo is small, contexts are
	//! few, and the empty property set makes hashing pointless until properties are real.
	vector<std::pair<OptimizationContext, GroupExpr *>> winners;
};

} // namespace duckdb
