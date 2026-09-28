#include "duckdb/execution/operator/join/physical_segment_apply.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <execinfo.h>

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/join/physical_segment_collector.hpp"
#include "duckdb/execution/operator/scan/physical_segment_parameter_scan.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/pipeline_finish_event.hpp"

namespace duckdb {

//! Per-segment tracing, for the case where a nested pipeline fails and the framework
//! swallows the error (DUCKDB_CASCADE_SEGMENT_DEBUG=1). Without it, a failure inside E
//! surfaces as an empty result, which is the one shape of bug this operator must never have
//! silently.
static bool SegmentDebug() {
	static const bool enabled = std::getenv("DUCKDB_CASCADE_SEGMENT_DEBUG") != nullptr;
	return enabled;
}

PhysicalSegmentApply::PhysicalSegmentApply(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                           PhysicalOperator &segmented, PhysicalOperator &segment_plan_p,
                                           PhysicalOperator &collector_p, vector<idx_t> segment_positions_p,
                                           vector<LogicalType> segment_types_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::SEGMENT_APPLY, std::move(types), estimated_cardinality),
      segment_plan(segment_plan_p), collector(collector_p), segment_positions(std::move(segment_positions_p)),
      segment_types(std::move(segment_types_p)) {
	children.push_back(segmented);
}

//===--------------------------------------------------------------------===//
// Segment: the rows of R for one value of A
//===--------------------------------------------------------------------===//
class SegmentApplySegment {
public:
	SegmentApplySegment(ClientContext &context, const vector<LogicalType> &types) : rows(context, types) {
		rows.InitializeAppend(append_state);
	}

	ColumnDataCollection rows;
	ColumnDataAppendState append_state;
};

//===--------------------------------------------------------------------===//
// Sink: partition the segmented relation by A
//===--------------------------------------------------------------------===//
class SegmentApplyGlobalSinkState : public GlobalSinkState {
public:
	SegmentApplyGlobalSinkState(ClientContext &context_p, const PhysicalSegmentApply &op_p)
	    : context(context_p), op(op_p) {
	}

	ClientContext &context;
	const PhysicalSegmentApply &op;
	//! One entry per distinct value of A, in the order the segments were first seen.
	vector<unique_ptr<SegmentApplySegment>> segments;
	//! hash(A) -> candidate segments. A hash collision can leave more than one candidate;
	//! the key values decide, and NULLs compare equal here because a NULL key is a segment
	//! of its own (the `=` inside E is what keeps NULL keys from matching anything).
	unordered_map<hash_t, vector<idx_t>> candidates;
	//! The key of every segment, for comparisons and for the output prefix.
	vector<vector<Value>> keys;

	idx_t FindOrCreate(DataChunk &chunk, idx_t row, hash_t hash) {
		auto &bucket = candidates[hash];
		for (auto index : bucket) {
			bool equal = true;
			for (idx_t k = 0; k < op.segment_positions.size(); k++) {
				if (!Value::NotDistinctFrom(chunk.GetValue(op.segment_positions[k], row), keys[index][k])) {
					equal = false;
					break;
				}
			}
			if (equal) {
				return index;
			}
		}
		auto index = segments.size();
		segments.push_back(make_uniq<SegmentApplySegment>(context, op.children[0].get().types));
		vector<Value> key;
		for (auto position : op.segment_positions) {
			key.push_back(chunk.GetValue(position, row));
		}
		keys.push_back(std::move(key));
		bucket.push_back(index);
		return index;
	}
};

class SegmentApplyLocalSinkState : public LocalSinkState {
public:
	explicit SegmentApplyLocalSinkState(Allocator &allocator, const vector<LogicalType> &types) {
		row_segments.resize(STANDARD_VECTOR_SIZE);
		slice_offsets.resize(STANDARD_VECTOR_SIZE);
		slice.Initialize(allocator, types);
	}

	//! Per row of the incoming chunk: which segment it went to.
	vector<idx_t> row_segments;
	//! Rows grouped by segment, so each run can be appended as one slice.
	vector<idx_t> slice_offsets;
	Vector hashes {LogicalType::HASH};
	DataChunk slice;
};

unique_ptr<GlobalSinkState> PhysicalSegmentApply::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<SegmentApplyGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalSegmentApply::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<SegmentApplyLocalSinkState>(Allocator::Get(context.client), children[0].get().types);
}

SinkResultType PhysicalSegmentApply::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<SegmentApplyGlobalSinkState>();
	auto &lstate = input.local_state.Cast<SegmentApplyLocalSinkState>();
	auto count = chunk.size();
	if (count == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	D_ASSERT(count <= STANDARD_VECTOR_SIZE);

	// Hash the segmenting columns, then put every row into its segment.
	VectorOperations::Hash(chunk.data[segment_positions[0]], lstate.hashes, count);
	for (idx_t k = 1; k < segment_positions.size(); k++) {
		VectorOperations::CombineHash(lstate.hashes, chunk.data[segment_positions[k]], count);
	}
	auto hashes = FlatVector::GetData<hash_t>(lstate.hashes);
	lstate.slice_offsets.resize(count);
	for (idx_t row = 0; row < count; row++) {
		lstate.row_segments[row] = gstate.FindOrCreate(chunk, row, hashes[row]);
		lstate.slice_offsets[row] = row;
	}

	// Group the rows by segment (the ids are small and the chunk is short) so that each
	// segment's rows are appended as contiguous slices instead of one row at a time.
	std::sort(lstate.slice_offsets.begin(), lstate.slice_offsets.begin() + NumericCast<idx_t>(count),
	          [&](idx_t left, idx_t right) { return lstate.row_segments[left] < lstate.row_segments[right]; });
	idx_t run_start = 0;
	while (run_start < count) {
		auto segment = lstate.row_segments[lstate.slice_offsets[run_start]];
		idx_t run_end = run_start + 1;
		while (run_end < count && lstate.row_segments[lstate.slice_offsets[run_end]] == segment) {
			run_end++;
		}
		// A contiguous run of the *sorted* offsets is not contiguous in the chunk, so the
		// rows are gathered through a selection vector.
		SelectionVector selection(NumericCast<idx_t>(run_end - run_start));
		for (idx_t i = run_start; i < run_end; i++) {
			selection.set_index(i - run_start, lstate.slice_offsets[i]);
		}
		lstate.slice.Slice(chunk, selection, run_end - run_start);
		auto &target = *gstate.segments[segment];
		target.rows.Append(target.append_state, lstate.slice);
		run_start = run_end;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalSegmentApply::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	// The sink is single-threaded, so there is nothing to combine.
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalSegmentApply::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source: evaluate E once per segment
//===--------------------------------------------------------------------===//
class SegmentApplyGlobalSourceState : public GlobalSourceState {
public:
	//! The next segment to evaluate; segments are visited in the order they were created,
	//! which keeps the output deterministic for a given input.
	idx_t next_segment = 0;

	idx_t MaxThreads() override {
		// One evaluator; the parallelism inside a segment is E's own business (E's pipelines
		// are executed by this operator, one segment at a time).
		return 1;
	}
};

class SegmentApplyLocalSourceState : public LocalSourceState {
public:
	SegmentApplyLocalSourceState(Allocator &allocator, const vector<LogicalType> &segment_plan_types) {
		scan_chunk.Initialize(allocator, segment_plan_types);
	}

	//! Scan of the collector's buffer: E's output for the segment being emitted.
	ColumnDataScanState scan_state;
	bool scanning = false;
	//! Which segment is being emitted (its key leads the output).
	idx_t current_segment = 0;
	DataChunk scan_chunk;
};

unique_ptr<GlobalSourceState> PhysicalSegmentApply::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<SegmentApplyGlobalSourceState>();
}

unique_ptr<LocalSourceState> PhysicalSegmentApply::GetLocalSourceState(ExecutionContext &context,
                                                                      GlobalSourceState &gstate) const {
	return make_uniq<SegmentApplyLocalSourceState>(Allocator::Get(context.client), segment_plan.types);
}

//! Run one pipeline of E to completion, the way the normal schedule would: execute it,
//! then finalize its sink through a PipelineFinishEvent. Tasks are drained here, so this
//! works with a single thread as well.
static void ExecuteSegmentPipeline(Executor &executor, Pipeline &pipeline) {
	auto signal_state = make_shared_ptr<InterruptDoneSignalState>();
	{
		PipelineExecutor pipeline_executor(executor.context, pipeline);
		pipeline_executor.SetInterruptState(InterruptState(weak_ptr<InterruptDoneSignalState>(signal_state)));
		while (true) {
			auto result = pipeline_executor.Execute();
			if (result == PipelineExecuteResult::FINISHED) {
				break;
			}
			if (result == PipelineExecuteResult::INTERRUPTED) {
				signal_state->Await();
				continue;
			}
			throw InternalException("segment apply: E's pipeline did not run to completion");
		}
	}
	pipeline.PrepareFinalize();
	auto finish_event = make_shared_ptr<PipelineFinishEvent>(pipeline.shared_from_this());
	auto complete_event = make_shared_ptr<PipelineCompleteEvent>(executor, false);
	complete_event->AddDependency(*finish_event);
	finish_event->Schedule();
	while (!complete_event->IsFinished()) {
		if (!executor.WorkOnTasks()) {
			executor.WaitForTask();
		}
		if (executor.HasError()) {
			executor.ThrowException();
		}
	}
}

void PhysicalSegmentApply::EvaluateSegment(ExecutionContext &context, ColumnDataCollection &segment,
                                           idx_t segment_index) const {
	// 1. The segment becomes the parameter E reads.
	for (auto &scan_ref : parameter_scans) {
		auto &scan = scan_ref.get();
		const_cast<PhysicalSegmentParameterScan &>(scan).segment = &segment;
	}
	auto &executor = segment_meta_pipeline->GetExecutor();
	// 2. E is evaluated for this segment, by running its pipelines to completion. Three
	//    things about that are worth spelling out, because each of them was a bug first:
	//
	//    * The pipelines are run in dependency order (worked out once, in BuildPipelines),
	//      not in the order the framework happened to create them: a join's build pipeline
	//      reads the aggregate, so it has to run after the aggregate's own pipeline.
	//    * Every pipeline's operators and source state are refreshed just before it runs.
	//      Refreshing them all up front would build the build-pipeline's source state against
	//      an aggregate that has not been filled yet.
	//    * The production sinks (the aggregate, the join) are re-created for every segment
	//      instead of reset: a hash join's probe tears down the table it built, so a "reset"
	//      hands the next segment a state whose table is already gone. They are re-created
	//      only after the consumers' source states have been refreshed (phase 1a), because
	//      those states point straight into the build side's table.
	// Phase 1a: refresh every pipeline's operators and source state, consumers before
	// producers, so that no consumer still points at the build states that phase 1b replaces.
	// (A probe pipeline's source state points straight into the build side's table.)
	for (auto pipeline = segment_pipelines.rbegin(); pipeline != segment_pipelines.rend(); ++pipeline) {
		// Drop the source state outright before anything is torn down: a source state can point
		// straight into a build state (a join's or an aggregate's), and those are replaced in
		// phase 1b. Clearing is what lets a *nested* join inside E be re-run - TPC-H Q17 has one.
		(*pipeline)->ClearSource();
	}
	// Phase 1b: this segment's production sinks start empty. They are re-created rather than
	// reset: a hash join's probe tears the table it built down again, so a "reset" would hand
	// the next segment a state whose table is already gone (that is exactly the null pointer
	// this used to crash on).
	reference_set_t<PhysicalOperator> fresh_sinks;
	for (auto &pipeline : segment_pipelines) {
		auto sink = pipeline->GetSink();
		if (!sink || !fresh_sinks.insert(*sink).second) {
			continue;
		}
		sink->sink_state = sink->GetGlobalSinkState(context.client);
	}
	if (!collector.sink_state) {
		throw InternalException("segment apply: E's collector has no sink state");
	}
	// Phase 2: run them in dependency order. Each pipeline refreshes its own operators and
	// source just before it runs, which is what lets a pipeline read the table the previous
	// pipeline of *this* segment just built.
	for (auto &pipeline : segment_pipelines) {
		try {
			pipeline->ResetForReschedule(false);
			// ... and force the source state itself to be rebuilt. A source state can support
			// re-use, in which case the reset above keeps its pointers into a build state that
			// phase 1b has since replaced - which is exactly the null hash table this used to
			// crash on once E contained a join of its own (TPC-H Q17's part join).
			pipeline->ResetSource(true);
			ExecuteSegmentPipeline(executor, *pipeline);
		} catch (std::exception &ex) {
			if (SegmentDebug()) {
				fprintf(stderr, "[segment apply] segment %llu failed in the pipeline whose source is %s: %s\n",
				        (unsigned long long)segment_index,
				        pipeline->GetSource() ? pipeline->GetSource()->GetName().c_str() : "(none)", ex.what());
			}
			throw;
		}
	}
}

SourceResultType PhysicalSegmentApply::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<SegmentApplyGlobalSourceState>();
	auto &lstate = input.local_state.Cast<SegmentApplyLocalSourceState>();
	if (!sink_state) {
		throw InternalException("segment apply: the segmented relation was never materialized");
	}
	auto &sink = sink_state->Cast<SegmentApplyGlobalSinkState>();
	if (SegmentDebug()) {
		fprintf(stderr, "[segment apply] %llu segments, next=%llu\n", (unsigned long long)sink.segments.size(),
		        (unsigned long long)gstate.next_segment);
	}

	while (true) {
		if (!lstate.scanning) {
			if (gstate.next_segment >= sink.segments.size()) {
				return SourceResultType::FINISHED;
			}
			auto index = gstate.next_segment++;
			// The buffer is fetched after the evaluation: evaluating a segment creates the
			// collector's sink state on the first segment and empties it on every later one.
			EvaluateSegment(context, sink.segments[index]->rows, index);
			if (!collector.sink_state) {
				throw InternalException("segment apply: E's collector has no sink state");
			}
			lstate.current_segment = index;
			lstate.scan_state = ColumnDataScanState();
			collector.sink_state->Cast<SegmentCollectorGlobalState>().collection.InitializeScan(lstate.scan_state);
			lstate.scanning = true;
		}
		auto &collected = collector.sink_state->Cast<SegmentCollectorGlobalState>();
		if (!collected.collection.Scan(lstate.scan_state, lstate.scan_chunk)) {
			lstate.scanning = false;
			continue;
		}
		// 3. `{a} x E(segment_a)`: the key leads, E's output follows. The key is constant
		//    within the segment, so it is a reference to the value rather than a copy.
		auto count = lstate.scan_chunk.size();
		if (count == 0) {
			// A collection can hold an empty chunk (the collector appends whatever E emits);
			// nothing to hand out for it.
			continue;
		}
		chunk.Reset();
		for (idx_t k = 0; k < segment_positions.size(); k++) {
			chunk.data[k].Reference(sink.keys[lstate.current_segment][k], count_t(count));
		}
		for (idx_t column = 0; column < lstate.scan_chunk.ColumnCount(); column++) {
			chunk.data[segment_positions.size() + column].Reference(lstate.scan_chunk.data[column]);
		}
		chunk.SetCardinality(count);
		if (SegmentDebug()) {
			fprintf(stderr, "[segment apply] segment %llu -> %llu rows\n",
			        (unsigned long long)lstate.current_segment, (unsigned long long)count);
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
}

//===--------------------------------------------------------------------===//
// Pipeline construction
//===--------------------------------------------------------------------===//
static void GatherParameterScans(PhysicalOperator &op,
                                 vector<reference<PhysicalSegmentParameterScan>> &result) {
	if (op.type == PhysicalOperatorType::SEGMENT_PARAMETER_SCAN) {
		result.push_back(op.Cast<PhysicalSegmentParameterScan>());
	}
	for (auto &child : op.children) {
		GatherParameterScans(child.get(), result);
	}
}

//! E's pipelines have to run in dependency order. The executor normally gets this from its
//! event graph; here the driver runs them itself, so the order is worked out once.
static void OrderPipelinesByDependency(vector<shared_ptr<Pipeline>> &pipelines) {
	vector<shared_ptr<Pipeline>> ordered;
	ordered.reserve(pipelines.size());
	unordered_set<Pipeline *> pending;
	for (auto &pipeline : pipelines) {
		pending.insert(pipeline.get());
	}
	while (!pending.empty()) {
		bool progressed = false;
		for (auto &pipeline : pipelines) {
			if (pending.find(pipeline.get()) == pending.end()) {
				continue;
			}
			bool ready = true;
			for (auto &dependency : pipeline->GetAllDependencies()) {
				if (pending.find(dependency.get()) != pending.end()) {
					ready = false;
					break;
				}
			}
			if (!ready) {
				continue;
			}
			pending.erase(pipeline.get());
			ordered.push_back(pipeline);
			progressed = true;
		}
		if (!progressed) {
			throw InternalException("segment apply: cyclic dependency between E's pipelines");
		}
	}
	pipelines = std::move(ordered);
}

void PhysicalSegmentApply::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();
	segment_meta_pipeline.reset();
	parameter_scans.clear();
	segment_pipelines.clear();

	auto &state = meta_pipeline.GetState();
	// This operator is the source of the output pipeline ...
	state.SetPipelineSource(current, *this);
	// ... and the segmented relation ends in its sink.
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.Build(children[0].get());

	// E gets a MetaPipeline of its own. The executor never schedules it - the driver runs it
	// once per segment, which is the whole point of the operator. This is the same
	// construction PhysicalRecursiveCTE uses for its recursive part.
	// E's pipelines end in the collector, not in this operator: the collector is what buffers
	// one segment's worth of E's output for the driver to read. So the collector is the sink
	// of this MetaPipeline and E is what gets built into it - building the collector itself
	// would add a pipeline from the collector to the collector, which is not a pipeline at
	// all (and the framework rejects it: the source has no IsSource set).
	auto &executor = meta_pipeline.GetExecutor();
	segment_meta_pipeline = make_shared_ptr<MetaPipeline>(executor, state, &collector);
	segment_meta_pipeline->Build(segment_plan);
	segment_meta_pipeline->Ready();

	segment_meta_pipeline->GetPipelines(segment_pipelines, true);
	OrderPipelinesByDependency(segment_pipelines);
	if (segment_pipelines.empty()) {
		throw InternalException("segment apply: E produced no pipelines");
	}
	GatherParameterScans(segment_plan, parameter_scans);
}

//===--------------------------------------------------------------------===//
// Introspection
//===--------------------------------------------------------------------===//
InsertionOrderPreservingMap<string> PhysicalSegmentApply::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string columns;
	for (auto position : segment_positions) {
		if (!columns.empty()) {
			columns += ",";
		}
		columns += std::to_string(position);
	}
	result["Segment Columns"] = columns;
	result["Segments"] = "one evaluation of E per segment";
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

vector<const_reference<PhysicalOperator>> PhysicalSegmentApply::GetChildren() const {
	// For the plan display (and the profiler) E hangs off the collector, which is its sink -
	// that is the tree the driver executes, even though E is not one of this operator's
	// ordinary children.
	vector<const_reference<PhysicalOperator>> result;
	result.push_back(children[0].get());
	result.push_back(collector);
	return result;
}

} // namespace duckdb
