#include "duckdb/cascade/cascade_keys.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

//! The base-table scan a side reduces to, skipping filters, or nothing.
optional_ptr<LogicalOperator> CascadeBaseTable(LogicalOperator &op) {
	auto current = &op;
	while (current->type == LogicalOperatorType::LOGICAL_FILTER && current->children.size() == 1) {
		current = current->children[0].get();
	}
	if (current->type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	return current;
}

} // namespace

vector<ColumnBinding> CascadeSideKey(LogicalOperator &side) {
	auto base = CascadeBaseTable(side);
	if (!base) {
		return {};
	}
	auto &get = base->Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table) {
		return {};
	}
	auto &columns = table->GetColumns();
	auto &scan_columns = get.GetColumnIds();
	auto side_bindings = side.GetColumnBindings();

	// The scan reads more columns than it exposes: the optimizer pushes filters into it and
	// projects the rest away, and an exposed binding's column index is its position in the
	// scan's column list. The two lists are therefore not parallel, and a key column only
	// counts when the scan exposes it.
	auto exposed_position = [&](idx_t logical_index) {
		for (idx_t position = 0; position < side_bindings.size(); position++) {
			auto scan_position = side_bindings[position].column_index.GetIndexUnsafe();
			if (scan_position >= scan_columns.size()) {
				continue;
			}
			auto &column = scan_columns[scan_position];
			if (column.HasPrimaryIndex() && column.ToLogical().index == logical_index) {
				return position;
			}
		}
		return DConstants::INVALID_INDEX;
	};

	// A candidate is the set of positions of one key's columns in the side's output.
	vector<vector<idx_t>> candidates;
	for (auto &constraint : table->GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto indexes = constraint->Cast<UniqueConstraint>().GetLogicalIndexes(columns);
		if (indexes.empty()) {
			continue;
		}
		vector<idx_t> candidate;
		for (auto &index : indexes) {
			auto position = exposed_position(index.index);
			if (position == DConstants::INVALID_INDEX) {
				break;
			}
			candidate.push_back(position);
		}
		if (candidate.size() == indexes.size()) {
			candidates.push_back(std::move(candidate));
		}
	}
	for (idx_t position = 0; position < side_bindings.size(); position++) {
		auto scan_position = side_bindings[position].column_index.GetIndexUnsafe();
		if (scan_position >= scan_columns.size() || !scan_columns[scan_position].HasPrimaryIndex()) {
			continue;
		}
		auto logical = scan_columns[scan_position].ToLogical();
		if (logical.index >= columns.LogicalColumnCount()) {
			continue;
		}
		auto &column = columns.GetColumn(LogicalIndex(logical.index));
		if (CascadeConfig::IsDeclaredKey(table->name.GetIdentifierName(), column.Name().GetIdentifierName())) {
			candidates.push_back(vector<idx_t> {position});
		}
	}
	if (candidates.empty()) {
		return {};
	}
	vector<ColumnBinding> key;
	for (auto position : candidates[0]) {
		key.push_back(side_bindings[position]);
	}
	return key;
}

} // namespace duckdb
