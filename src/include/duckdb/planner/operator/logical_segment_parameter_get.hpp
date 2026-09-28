//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_segment_parameter_get.hpp
//
// The table-valued parameter of section 3.4's SegmentApply. It exposes one *segment* -
// the rows of `sigma_{A=a} R` for the segment the executor is currently working on:
//
//     R SA_A E = union over a of ( {a} x E(sigma_{A=a} R) )
//
// The node always sits *inside* E, never as a child of the SegmentApply itself, and the
// executor fills it in before E is evaluated for the segment. That is what makes the
// parameter a set of rows rather than a single row, which is the only difference between
// SegmentApply and Apply in the paper.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalSegmentParameterGet : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_SEGMENT_PARAMETER_GET;

public:
	LogicalSegmentParameterGet(TableIndex table_index, vector<LogicalType> types)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_SEGMENT_PARAMETER_GET), table_index(table_index) {
		D_ASSERT(!types.empty());
		chunk_types = std::move(types);
	}

	//! The table index in the current bind context
	TableIndex table_index;
	//! The types of the segment: the columns of the relation being segmented
	vector<LogicalType> chunk_types;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, chunk_types.size());
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	vector<TableIndex> GetTableIndex() const override;
	string GetName() const override;

protected:
	void ResolveTypes() override {
		// types are resolved in the constructor
		this->types = chunk_types;
	}
};

} // namespace duckdb
