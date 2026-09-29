#include "duckdb/execution/operator/join/physical_segment_collector.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

SegmentCollectorGlobalState::SegmentCollectorGlobalState(ClientContext &context, const vector<LogicalType> &types)
    : collection(context, types) {
	collection.InitializeAppend(append_state);
}

//! The buffer is reused for every segment, so a query that runs E for thousands of segments
//! does not allocate a collection each time.
void SegmentCollectorGlobalState::Reset(ClientContext &context) {
	collection.Reset();
	collection.InitializeAppend(append_state);
	GlobalSinkState::Reset(context);
}

class SegmentCollectorLocalState : public LocalSinkState {
public:
	explicit SegmentCollectorLocalState(ClientContext &context, const vector<LogicalType> &types)
	    : collection(context, types) {
		collection.InitializeAppend(append_state);
	}

	//! The sink is single-threaded, but a local collection keeps the (possibly parallel)
	//! producers' chunks separate until Combine, which is the shape the framework expects.
	ColumnDataCollection collection;
	ColumnDataAppendState append_state;
};

unique_ptr<GlobalSinkState> PhysicalSegmentCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<SegmentCollectorGlobalState>(context, types);
}

unique_ptr<LocalSinkState> PhysicalSegmentCollector::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<SegmentCollectorLocalState>(context.client, types);
}

SinkResultType PhysicalSegmentCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<SegmentCollectorLocalState>();
	lstate.collection.Append(lstate.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalSegmentCollector::Combine(ExecutionContext &context,
                                                       OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<SegmentCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<SegmentCollectorLocalState>();
	gstate.collection.Combine(lstate.collection);
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalSegmentCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                    OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

InsertionOrderPreservingMap<string> PhysicalSegmentCollector::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Role"] = "segment output";
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
