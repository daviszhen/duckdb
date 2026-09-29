#include "duckdb/planner/operator/logical_segment_parameter_get.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

vector<TableIndex> LogicalSegmentParameterGet::GetTableIndex() const {
	return vector<TableIndex> {table_index};
}

string LogicalSegmentParameterGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index.index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
