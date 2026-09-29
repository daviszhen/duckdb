//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/cost.hpp
//
// Costing. ORCA keeps statistics and the cost model in separate libraries
// (libnaucrates/statistics vs libgpdbcost) so both are pluggable; the interface here
// is what has to stay pluggable, the coefficients are not real yet.
//
// The shape of the model matters more than its numbers, and it took a wrong turn to
// get here: costing a plan by "rows it produces plus what its children cost" makes an
// operator's price independent of how many rows reach it, so moving a filter below a
// GroupBy - which exists *only* to keep rows out of the aggregate - costs the same and
// the choice comes down to which candidate happened to be costed first. What an
// operator pays for is its *input*:
//
//     cost(expr) = sum(cost(children)) + sum(rows(children)) * row cost(op)
//
// With the aggregate's per-row cost above the filter's, the pushdown wins exactly when
// it should: when it keeps enough rows out of the aggregate to pay for itself.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"

namespace duckdb {

class LogicalOperator;
class GroupExpr;

class CostModel {
public:
	//! Rows this operator is expected to produce. The host fills `estimated_cardinality` in
	//! during optimization; 1 is the safe answer when it did not.
	static double Cardinality(const LogicalOperator &op);
	//! What one row costs to push through an operator of this type. Placeholder coefficients -
	//! they only have to rank the operators sensibly for now: an aggregate is dear per row, a
	//! filter and a projection are cheap.
	static double RowCost(LogicalOperatorType type);
	//! How many rows an expression is expected to emit. The host's estimate is used whenever there
	//! is one; when there is not - an in-memory table has no statistics, and the estimates the
	//! host computes live in the optimizer this mode bypasses - the type decides, and the
	//! coefficients below are placeholders. They only have to rank plans the way a real
	//! selectivity would: a filter removes most rows, an aggregate does not invent them.
	static double OutputRows(const GroupExpr &expr, const vector<double> &child_rows);
	//! Cost of an expression given each child's total cost and its output rows.
	static double Cost(const GroupExpr &expr, const vector<double> &child_costs, const vector<double> &child_rows);
};

} // namespace duckdb
