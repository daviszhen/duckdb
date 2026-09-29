//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/apply_decorrelation.hpp
//
// Apply elimination: the classic rule set that rewrites the Apply operator
// (LogicalDependentJoin) into ordinary joins, run as an optimizer pass.
//
// DuckDB decorrelates during planning, in FlattenDependentJoins. Keeping the
// same work here instead lets one query be planned both ways and compared.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class ClientContext;
class Expression;
class LogicalAggregate;
class LogicalOperator;

//! A rewrite can change the bindings a node exposes. The caller gets the old ->
//! new mapping back so it can repoint its own expressions; without it, replacing
//! a sub-tree with an aggregate (which re-binds its groups) would strand every
//! reference the parent holds.
using BindingExport = vector<std::pair<ColumnBinding, ColumnBinding>>;

//! Rewrite the marker joins that Apply elimination produces into the semi/anti
//! joins they actually mean, whenever the marker is consumed only as a
//! predicate. A marker is dead weight the executor has to carry and that keeps
//! the join off the semi-join fast path; DuckDB's Deliminator does the same
//! cleanup after FlattenDependentJoins.
unique_ptr<LogicalOperator> SimplifyMarkerJoins(unique_ptr<LogicalOperator> plan);

class ApplyDecorrelator {
public:
	ApplyDecorrelator(Binder &binder, ClientContext &context);

	//! Eliminate every Apply in the plan.
	unique_ptr<LogicalOperator> Decorrelate(unique_ptr<LogicalOperator> plan);

	//! Whether identity (5)/(6) distributed an Apply over a set operation, i.e. whether the plan
	//! now shares the outer relation between the branches through a materialised CTE. DuckDB's
	//! pull-up passes mis-rewrite that shape (see CascadeOptimizer::Optimize), so the caller has
	//! to know it afterwards.
	bool DistributedSetOperation() const {
		return distributed_set_operation;
	}

	//! Whether identity (9) turned a correlated scalar sub-query into the group-by over a left
	//! outer join. DuckDB's CompressedMaterialization is wrong on that shape (it narrows the
	//! group-by key and leaves a stale reference behind), see CascadeOptimizer::Optimize.
	bool ScalarAggregate() const {
		return scalar_aggregate;
	}

	//! Whether more than one correlated sub-query was decorrelated. Their plans then share the
	//! outer relation, and DuckDB's CommonSubplanOptimizer materialises that shared part into a
	//! CTE and leaves the group keys of our aggregates pointing at bindings it moved.
	bool SharedSubQueries() const {
		return scalar_subqueries > 1;
	}

private:
	bool distributed_set_operation = false;
	bool scalar_aggregate = false;
	idx_t scalar_subqueries = 0;
	unique_ptr<LogicalOperator> DecorrelateNode(unique_ptr<LogicalOperator> op, BindingExport &exports);
	unique_ptr<LogicalOperator> DecorrelateApply(unique_ptr<LogicalOperator> op, BindingExport &exports);
	//! Identities (5) and (6): an Apply over a set operation becomes a set operation of
	//! Applies, one per branch, each with its own copy of the outer relation. Returns
	//! nothing (leaving `op` alone) when the shape is not one of those identities.
	unique_ptr<LogicalOperator> TryDistributeOverSetOperation(unique_ptr<LogicalOperator> &op, BindingExport &exports);
	//! Identity (7): an Apply over a cross product becomes the two branches matched back
	//! through a key of the outer relation. Returns nothing when the shape is not that one.
	unique_ptr<LogicalOperator> TryDistributeOverCrossProduct(unique_ptr<LogicalOperator> &op, BindingExport &exports);
	//! The key of a plan side. A side that is one of the materialised CTEs this pass
	//! introduced still stands for the keyed relation that went into it, and the catalog can
	//! no longer see that, so the key is remembered here. Without it identity (7) only fires
	//! once per query: the second application looks at a CTE reference.
	vector<ColumnBinding> SideKey(LogicalOperator &side);
	//! Correlated scalar subquery, by identity (9) of Galindo-Legaria & Joshi:
	//! group by the outer columns over a left outer join, so an outer row with no
	//! match still has a group and the aggregate sees a NULL-padded row.
	unique_ptr<LogicalOperator> DecorrelateScalar(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
	                                              const CorrelatedColumns &correlated, BindingExport &exports);
	//! Correlated scalar sub-query whose correlation sits below a GroupBy of its own,
	//! by identity (8): the Apply moves below that GroupBy (over a deduplicated outer
	//! side) and the outer columns join its grouping, with the outer rows' multiplicity
	//! restored afterwards.
	unique_ptr<LogicalOperator> DecorrelateNestedScalar(unique_ptr<LogicalOperator> left,
	                                                    unique_ptr<LogicalOperator> right,
	                                                    const vector<LogicalOperator *> &projections,
	                                                    LogicalAggregate &top, LogicalAggregate &nested,
	                                                    vector<unique_ptr<Expression>> &extracted,
	                                                    const CorrelatedColumns &correlated, BindingExport &exports,
	                                                    const ColumnBinding &value_binding);

private:
	Binder &binder;
	ClientContext &context;
	//! Key positions of the relations this pass materialised, by CTE index.
	unordered_map<idx_t, vector<idx_t>> cte_key_positions;
};

} // namespace duckdb
