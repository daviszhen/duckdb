#include "duckdb/cascade/cascades/cost.hpp"

#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

double CostModel::Cardinality(const LogicalOperator &op) {
	// The host fills this in while optimizing; when it did not, one row is the honest answer.
	return op.estimated_cardinality == 0 ? 1.0 : static_cast<double>(op.estimated_cardinality);
}

double CostModel::Cost(const GroupExpr &expr, const vector<double> &child_costs) {
	// Skeleton cost: the rows this expression produces plus what its children cost. Good enough
	// to keep the winners stable; a real model (ORCA keeps it in libgpdbcost, apart from the
	// statistics in libnaucrates) comes once there is more than one candidate per group.
	double cost = expr.op ? Cardinality(*expr.op) : 1.0;
	for (auto child_cost : child_costs) {
		cost += child_cost;
	}
	return cost;
}

} // namespace duckdb
