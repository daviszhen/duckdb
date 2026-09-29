//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/cost.hpp
//
// Costing, deliberately trivial in the skeleton: a plan costs the rows it produces,
// and the host already estimates those per operator. ORCA keeps statistics and the
// cost model in separate libraries (libnaucrates/statistics vs libgpdbcost) so both
// are pluggable; the interface here is what has to stay pluggable, the formula is
// not.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class LogicalOperator;
class GroupExpr;
class Memo;

class CostModel {
public:
	//! Rows this operator is expected to produce. The host fills `estimated_cardinality` in
	//! during optimization; 1 is the safe answer when it did not.
	static double Cardinality(const LogicalOperator &op);
	//! Cost of one expression, given its children's costs.
	static double Cost(const GroupExpr &expr, const vector<double> &child_costs);
};

} // namespace duckdb
