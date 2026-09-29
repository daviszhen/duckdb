#include "duckdb/execution/operator/scan/physical_segment_parameter_scan.hpp"

namespace duckdb {

class SegmentParameterGlobalState : public GlobalSourceState {
public:
	ColumnDataScanState scan_state;
	//! The segment scan_state was initialized for. The state is re-used when the framework
	//! says so, and a scan state belongs to exactly one collection - so the collection it was
	//! built for is what decides whether it has to be re-initialized.
	optional_ptr<const ColumnDataCollection> bound;

	idx_t MaxThreads() override {
		// One segment is one small table, and the driver runs E single-threaded per segment.
		return 1;
	}
};

class SegmentParameterLocalState : public LocalSourceState {
public:
	SegmentParameterLocalState(Allocator &allocator, const vector<LogicalType> &types) {
		scan_chunk.Initialize(allocator, types);
	}

	DataChunk scan_chunk;
};

unique_ptr<GlobalSourceState> PhysicalSegmentParameterScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<SegmentParameterGlobalState>();
}

unique_ptr<LocalSourceState> PhysicalSegmentParameterScan::GetLocalSourceState(ExecutionContext &context,
                                                                               GlobalSourceState &) const {
	return make_uniq<SegmentParameterLocalState>(Allocator::Get(context.client), types);
}

SourceResultType PhysicalSegmentParameterScan::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                               OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<SegmentParameterGlobalState>();
	if (!segment) {
		// No segment bound: the driver has not started this segment yet.
		return SourceResultType::FINISHED;
	}
	if (gstate.bound != segment) {
		segment->InitializeScan(gstate.scan_state);
		gstate.bound = segment;
	}
	if (!segment->Scan(gstate.scan_state, chunk)) {
		return SourceResultType::FINISHED;
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

InsertionOrderPreservingMap<string> PhysicalSegmentParameterScan::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Segment Rows"] = segment ? StringUtil::Format("%llu", segment->Count()) : "unbound";
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
