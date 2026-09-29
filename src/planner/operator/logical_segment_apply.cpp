#include "duckdb/planner/operator/logical_segment_apply.hpp"

#include "duckdb/planner/operator/logical_segment_parameter_get.hpp"

namespace duckdb {

vector<ColumnBinding> LogicalSegmentApply::GetColumnBindings() {
	// The paper's `{a}` - the segmenting columns - lead the output; E's columns follow.
	auto left_bindings = children[0]->GetColumnBindings();
	vector<ColumnBinding> result;
	for (auto position : segment_positions) {
		result.push_back(left_bindings[position]);
	}
	auto right_bindings = children[1]->GetColumnBindings();
	result.insert(result.end(), right_bindings.begin(), right_bindings.end());
	return result;
}

string LogicalSegmentApply::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		string columns;
		for (auto position : segment_positions) {
			if (!columns.empty()) {
				columns += ",";
			}
			columns += std::to_string(position);
		}
		return LogicalOperator::GetName() + StringUtil::Format(" segment#%s", columns);
	}
#endif
	return LogicalOperator::GetName();
}

void LogicalSegmentApply::ResolveTypes() {
	types = segment_types;
	for (auto &child_type : children[1]->types) {
		types.push_back(child_type);
	}
}

} // namespace duckdb
