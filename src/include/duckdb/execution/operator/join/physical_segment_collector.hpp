//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_segment_collector.hpp
//
// The sink that ends the parameterized child (E) of a PhysicalSegmentApply. It simply
// buffers E's output for the segment that is currently being evaluated; the driver reads
// that buffer after E's pipelines have finished and prefixes it with the segment's key.
//
// It exists so that E stays an ordinary physical plan whose sink can be reset for the next
// segment - the collector is the only part of E that knows about segments at all.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

class ColumnDataCollection;
class ColumnDataAppendState;

//! The buffer E writes into: E's output for the segment that is being evaluated. The driver
//! reads it after E's pipelines finished and clears it before the next segment.
class SegmentCollectorGlobalState : public GlobalSinkState {
public:
	SegmentCollectorGlobalState(ClientContext &context, const vector<LogicalType> &types);

	ColumnDataCollection collection;
	ColumnDataAppendState append_state;

	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ClientContext &context) override;
};

class PhysicalSegmentCollector : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::SEGMENT_COLLECTOR;

public:
	PhysicalSegmentCollector(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::SEGMENT_COLLECTOR, std::move(types),
	                       estimated_cardinality) {
	}

public:
	//! Sink interface: buffer E's chunks for the current segment.
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	//! The segment is evaluated by one thread; E's own operators still use their own parallelism.
	bool ParallelSink() const override {
		return false;
	}
	bool SinkOrderDependent() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
