//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascade_correlation.hpp
//
// What every cascade rewrite asks about a correlation: which expressions name a
// column of the correlation domain, how to lift a correlated predicate out of a
// sub-query's body one row-preserving operator at a time, and how to hand the
// right sub-tree the columns the lifted predicate reads.
//
// These lived as file-local statics in apply_decorrelation.cpp until the
// identities were split into their own files - every one of them needs this, so
// they are a unit of their own now.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/cascade/cascade_bindings.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

class Expression;
class LogicalComparisonJoin;
class LogicalOperator;

//! True if any sub-expression references a column of the correlation domain.
bool ReferencesCorrelation(const Expression &expr, const CorrelatedColumns &correlated);

//! True if any sub-expression references a column of the sub-query's own side.
bool ReferencesInner(const Expression &expr, const CorrelatedColumns &correlated);

//! The sub-query's references to the outer relation move one scope closer once the
//! Apply is flattened.
void DecrementCorrelationDepth(Expression &expr);
void DecrementCorrelationDepth(LogicalOperator &op);

//! Whether a list of expressions, or a whole sub-tree, still references the outer relation.
bool ListReferencesCorrelation(const vector<unique_ptr<Expression>> &exprs, const CorrelatedColumns &correlated);
bool SubtreeReferencesCorrelation(const LogicalOperator &op, const CorrelatedColumns &correlated);

//! Identity (3): a filter inside the sub-query contributes its correlated conjuncts to
//! the Apply, and disappears when nothing local is left of it.
unique_ptr<LogicalOperator> ExtractCorrelatedPredicates(unique_ptr<LogicalOperator> op,
                                                        const CorrelatedColumns &correlated,
                                                        vector<unique_ptr<Expression>> &extracted);

//! A column of the right sub-tree that a lifted predicate needs to see.
struct NeededColumn {
	ColumnBinding binding;
	LogicalType type;
};

//! The columns those predicates read on the sub-query's side, and the way to expose them
//! through the projection on top of it (identity (4)).
vector<NeededColumn> CollectRightColumns(const vector<unique_ptr<Expression>> &predicates,
                                         const CorrelatedColumns &correlated);
void ExposeRightColumns(LogicalOperator &right, const vector<NeededColumn> &needed, BindingExport &mapping);

//! Identities (3) and (4) as a framework: the correlated predicates travel up through the
//! row-preserving operators of the sub-query's body until they reach the Apply.
unique_ptr<LogicalOperator> LiftCorrelatedPredicates(unique_ptr<LogicalOperator> op,
                                                     const CorrelatedColumns &correlated,
                                                     vector<unique_ptr<Expression>> &pending);

//! Turn an extracted predicate into a join condition. A NULL comparison is never true, so
//! only an equality may be made NULL-safe - it can be a hash key, a `<` cannot.
void AddJoinCondition(LogicalComparisonJoin &join, unique_ptr<Expression> predicate,
                      const CorrelatedColumns &correlated, bool null_safe);

//! Whether the expression names any column of the given side, and a splitter for an ON
//! predicate that is a conjunction of comparisons.
bool ApplyReadsBindings(const Expression &expr, const vector<ColumnBinding> &side);
bool CollectComparisons(unique_ptr<Expression> predicate, vector<unique_ptr<Expression>> &result);

} // namespace duckdb
