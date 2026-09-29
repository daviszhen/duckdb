//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascade_bindings.hpp
//
// The binding bookkeeping every cascade rewrite shares.
//
// A rewrite replaces one sub-tree with another, and the replacement rarely
// exposes the same columns under the same names: an aggregate re-binds its
// groups, a set operation re-binds by position, a join that loses a side
// renumbers. Whoever performed the rewrite reports the old -> new mapping, and
// the callers use these helpers to repoint the expressions they still hold.
//
// They live here rather than in one of the passes because both the Apply
// elimination and section 3.4's SegmentApply need them, and keeping two copies
// had already drifted: the segment one remapped ORDER BY and DISTINCT
// expressions, the decorrelation one did not, and only one of them knew about an
// Apply's own `condition` member.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class Expression;
class LogicalOperator;

//! A rewrite can change the bindings a node exposes. The caller gets the old ->
//! new mapping back so it can repoint its own expressions; without it, replacing
//! a sub-tree with an aggregate (which re-binds its groups) would strand every
//! reference the parent holds.
//!
//! A vector rather than a map on purpose: the callers build it in a meaningful
//! order (the outer columns, then the sub-query's value) and every rewrite takes
//! the first match, so the order is part of the contract.
using BindingExport = vector<std::pair<ColumnBinding, ColumnBinding>>;

//! Repoint every column reference the expression holds. References at any depth
//! are rewritten: a sub-query's correlation to the outer relation is exactly such
//! a reference, and it is the one that strands the parent when it is missed.
void RewriteExpressionBindings(unique_ptr<Expression> &expr, const BindingExport &exports);

//! The same for one operator's own expressions - `expressions`, a join's
//! conditions, an aggregate's grouping expressions, an order's keys, a distinct's
//! targets, and an Apply's `condition`. It does *not* descend into the children;
//! use RewriteTreeBindings for that.
void RewriteOperatorBindings(LogicalOperator &op, const BindingExport &exports);

//! RewriteOperatorBindings for a whole sub-tree. Needed when a mapping has to be
//! applied where the references live *inside* the node rather than above it.
void RewriteTreeBindings(LogicalOperator &op, const BindingExport &exports);

//! Follow one binding through a mapping, or return it unchanged.
ColumnBinding MapBinding(const ColumnBinding &binding, const BindingExport &exports);

//! Whether a binding list contains a binding.
bool InBindings(const vector<ColumnBinding> &bindings, const ColumnBinding &binding);

//! True for operators that expose their child's bindings unchanged - so a rewrite
//! below one of them is still visible to its own parent and the mapping has to
//! keep travelling upwards.
bool PassesBindingsThrough(const LogicalOperator &op);

} // namespace duckdb
