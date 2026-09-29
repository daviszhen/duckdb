#include "duckdb/cascade/cascades/cost.hpp"

#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

double CostModel::Cardinality(const LogicalOperator &op) {
	// The host fills this in while optimizing; when it did not, one row is the honest answer.
	return op.estimated_cardinality == 0 ? 1.0 : static_cast<double>(op.estimated_cardinality);
}

double CostModel::RowCost(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		// Grouping and aggregating is what costs; this is the coefficient the pushdown has to
		// earn its extra filter against.
		return 10.0;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
		return 5.0;
	default:
		// A scan, a filter, a projection: work proportional to the rows, and cheap per row.
		return 1.0;
	}
}

double CostModel::Cost(const GroupExpr &expr, const vector<double> &child_costs, const vector<double> &child_rows) {
	double cost = 0;
	for (auto child_cost : child_costs) {
		cost += child_cost;
	}
	auto row_cost = expr.op ? RowCost(expr.type) : 1.0;
	for (auto child_row : child_rows) {
		// What this operator pays is the rows it has to look at, not the rows it emits.
		cost += child_row * row_cost;
	}
	return cost;
}

} // namespace duckdb
