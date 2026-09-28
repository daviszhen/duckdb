//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_segment_apply.hpp
//
// Section 3.4 of Galindo-Legaria & Joshi, "Orthogonal Optimization of Subqueries and
// Aggregation" (SIGMOD 2001): SegmentApply is Apply whose parameter is a *set* of rows -
// a segment - rather than a single row.
//
//     R SA_A E = union over a of ( {a} x E(sigma_{A=a} R) )
//
// `children[0]` is the segmented relation R, `children[1]` is E, and E reads the segment
// through a LogicalSegmentParameterGet. The output leads with the segmenting columns A
// (the paper's `{a}`) followed by E's columns; the rows of R are not part of the output
// on their own - they reach the consumer through E, exactly as in Figure 7 of the paper,
// where the LINEITEM instance inside the SegmentApply is the segment.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalSegmentApply : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_SEGMENT_APPLY;

public:
	//! `segment_positions` are positions in children[0]'s output: the columns that partition
	//! R. `segment_types` are their types, which are also the types of the output's prefix.
	LogicalSegmentApply(vector<idx_t> segment_positions, vector<LogicalType> segment_types)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_SEGMENT_APPLY),
	      segment_positions(std::move(segment_positions)), segment_types(std::move(segment_types)) {
	}

	//! Positions of the segmenting columns A within children[0]'s output.
	vector<idx_t> segment_positions;
	//! Types of the segmenting columns.
	vector<LogicalType> segment_types;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	string GetName() const override;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
