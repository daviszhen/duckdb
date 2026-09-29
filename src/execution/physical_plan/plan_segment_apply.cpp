#include "duckdb/execution/operator/join/physical_segment_apply.hpp"
#include "duckdb/execution/operator/join/physical_segment_collector.hpp"
#include "duckdb/execution/operator/scan/physical_segment_parameter_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_segment_apply.hpp"
#include "duckdb/planner/operator/logical_segment_parameter_get.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalSegmentApply &op) {
	D_ASSERT(op.children.size() == 2);
	// R: the relation that is segmented.
	auto &segmented = CreatePlan(*op.children[0]);
	// E: the parameterized expression. It reads the segment through its parameter scans,
	// which the new operator binds before every evaluation.
	auto &segment_plan = CreatePlan(*op.children[1]);
	// E's sink: buffers E's output for the segment that is being evaluated.
	auto &collector = Make<PhysicalSegmentCollector>(segment_plan.types, op.estimated_cardinality);
	collector.children.push_back(segment_plan);
	return Make<PhysicalSegmentApply>(op.types, segmented, segment_plan, collector, op.segment_positions,
	                                  op.segment_types, op.estimated_cardinality);
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalSegmentParameterGet &op) {
	D_ASSERT(op.children.empty());
	return Make<PhysicalSegmentParameterScan>(op.types, op.estimated_cardinality);
}

} // namespace duckdb
