//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_segment_parameter_scan.hpp
//
// The table-valued parameter of section 3.4's SegmentApply: it reads the segment the
// driver (PhysicalSegmentApply) is currently on.
//
//     R SA_A E = union over a of ( {a} x E(sigma_{A=a} R) )
//
// The collection is handed over *directly*, one segment at a time, by the driver - there
// is no key registry, no duplicate elimination and no delim index involved. That is the
// difference between this and a delim scan: a delim scan reads every parameter value that
// was collected once, so the parameterized side runs once for all of them, while here E is
// re-evaluated for each segment with only that segment visible.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalSegmentParameterScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::SEGMENT_PARAMETER_SCAN;

public:
	PhysicalSegmentParameterScan(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::SEGMENT_PARAMETER_SCAN, std::move(types),
	                       estimated_cardinality) {
	}

	//! The segment the driver is currently evaluating. Mutated by PhysicalSegmentApply before
	//! it runs E, so this operator never decides on its own which rows to expose.
	optional_ptr<const ColumnDataCollection> segment;

public:
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
