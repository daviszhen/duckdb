//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_segment_apply.hpp
//
// Section 3.4 of Galindo-Legaria & Joshi, "Orthogonal Optimization of Subqueries and
// Aggregation" (SIGMOD 2001): segmented execution, as a physical operator.
//
//     R SA_A E = union over a of ( {a} x E(sigma_{A=a} R) )
//
// The sink partitions R by the segmenting columns A; the source then walks the segments,
// and for each one binds that segment to the parameter scans inside E and *re-executes E's
// pipelines*. Nothing here is shared with the delim join: there is no duplicate
// elimination, no key registry and no delim index - E is genuinely evaluated once per
// segment, which is precisely what the paper's operator is for (the right subtree can use
// the segment to prune what it reads), and what DuckDB's delim join does not do.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

class ColumnDataCollection;
class PhysicalSegmentParameterScan;

class PhysicalSegmentApply : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::SEGMENT_APPLY;

public:
	PhysicalSegmentApply(PhysicalPlan &physical_plan, vector<LogicalType> types, PhysicalOperator &segmented,
	                     PhysicalOperator &segment_plan, PhysicalOperator &collector, vector<idx_t> segment_positions,
	                     vector<LogicalType> segment_types, idx_t estimated_cardinality);

	//! The relation that is segmented (children[0] of the logical operator).
	//! The parameterized child E. It is built into `segment_meta_pipeline`, which the
	//! executor never schedules: PhysicalSegmentApply runs it once per segment.
	PhysicalOperator &segment_plan;
	//! The sink that ends E and buffers its output for the current segment.
	PhysicalOperator &collector;
	//! Positions of the segmenting columns A within the segmented relation's output.
	vector<idx_t> segment_positions;
	//! Their types: the output leads with them (`{a}` in the formula).
	vector<LogicalType> segment_types;
	//! E's own pipelines, in dependency order, as worked out in BuildPipelines.
	shared_ptr<MetaPipeline> segment_meta_pipeline;
	vector<shared_ptr<Pipeline>> segment_pipelines;
	//! The parameter scans inside E, i.e. the operators the driver binds a segment to.
	vector<reference<PhysicalSegmentParameterScan>> parameter_scans;

public:
	//! Sink interface: partition the segmented relation by A.
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	//! The partitioning itself is single-threaded; the segments are what can be parallel.
	bool ParallelSink() const override {
		return false;
	}
	bool SinkOrderDependent() const override {
		return true;
	}

	//! Source interface: run E once per segment and emit `{a} x E(segment)`.
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return false;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
	vector<const_reference<PhysicalOperator>> GetChildren() const override;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

private:
	//! Bind one segment and evaluate E for it, by running E's pipelines to completion.
	void EvaluateSegment(ExecutionContext &context, ColumnDataCollection &segment, idx_t segment_index) const;
};

} // namespace duckdb
