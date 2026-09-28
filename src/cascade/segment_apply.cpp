#include "duckdb/cascade/segment_apply.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascade_keys.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_segment_apply.hpp"
#include "duckdb/planner/operator/logical_segment_parameter_get.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

//! Follow a binding down to the base-table column it comes from, through the projections
//! and GroupBy keys that merely rename it. A computed column, and an aggregate's own
//! result, have no base column and stop the trace - which is what keeps the equality in
//! the join predicate honest about comparing two instances of the same *table* column.
static bool TraceToBaseColumn(LogicalOperator &op, const ColumnBinding &binding, string &table, idx_t &column) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		auto bindings = get.GetColumnBindings();
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (bindings[i] != binding) {
				continue;
			}
			auto entry = get.GetTable();
			table = entry ? entry->name.GetIdentifierName() : get.GetName();
			// A scan may project a subset of the table's columns, so the scan's output
			// position and the table's column are not the same thing.
			auto &column_ids = get.GetColumnIds();
			column = i < column_ids.size() ? column_ids[i].GetPrimaryIndex() : i;
			return true;
		}
		return false;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			if (ColumnBinding(projection.table_index, ProjectionIndex(i)) != binding) {
				continue;
			}
			auto &expr = *projection.expressions[i];
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return false;
			}
			return TraceToBaseColumn(*projection.children[0], expr.Cast<BoundColumnRefExpression>().Binding(), table,
			                         column);
		}
		return false;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			if (ColumnBinding(aggregate.group_index, ProjectionIndex(i)) != binding) {
				continue;
			}
			auto &expr = *aggregate.groups[i];
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return false;
			}
			return TraceToBaseColumn(*aggregate.children[0], expr.Cast<BoundColumnRefExpression>().Binding(), table,
			                         column);
		}
		// an aggregate's own result is not a column of any table
		return false;
	}
	default:
		break;
	}
	// A filter, an order by, a limit ... pass their child's bindings through unchanged,
	// and a join exposes both of its inputs - so the binding names whichever child
	// produces it. That child is the one to follow, which is what lets the aggregated
	// side of a two-instance join be recognized when it sits inside another join.
	for (auto &child : op.children) {
		for (auto &candidate : child->GetColumnBindings()) {
			if (candidate == binding) {
				return TraceToBaseColumn(*child, binding, table, column);
			}
		}
	}
	return false;
}

static string ColumnName(LogicalOperator &op, const string &table, idx_t column) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return table + ".#" + to_string(column);
	}
	auto entry = op.Cast<LogicalGet>().GetTable();
	if (!entry || column >= entry->GetColumns().PhysicalColumnCount()) {
		return table + ".#" + to_string(column);
	}
	return table + "." + entry->GetColumns().GetColumn(PhysicalIndex(column)).Name();
}

static void CollectSegmentingColumns(LogicalOperator &op, vector<string> &found) {
	if ((op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	     op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) &&
	    op.children.size() == 2) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison() ||
			    condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			auto &lhs = condition.GetLHS();
			auto &rhs = condition.GetRHS();
			if (lhs.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
			    rhs.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				// An equality between arbitrary expressions is not necessarily an
				// equality of two instances of the same column.
				continue;
			}
			auto &left_ref = lhs.Cast<BoundColumnRefExpression>();
			auto &right_ref = rhs.Cast<BoundColumnRefExpression>();
			string left_table;
			string right_table;
			idx_t left_column = 0;
			idx_t right_column = 0;
			if (!TraceToBaseColumn(*op.children[0], left_ref.Binding(), left_table, left_column)) {
				continue;
			}
			if (!TraceToBaseColumn(*op.children[1], right_ref.Binding(), right_table, right_column)) {
				continue;
			}
			// The same column of the same table on both sides: rows whose value differs
			// can never match, so the column partitions the relation.
			if (left_table != right_table || left_column != right_column) {
				continue;
			}
			auto name = ColumnName(*op.children[0], left_table, left_column);
			bool already = false;
			for (auto &existing : found) {
				if (existing == name) {
					already = true;
					break;
				}
			}
			if (!already) {
				found.push_back(std::move(name));
			}
		}
	}
	for (auto &child : op.children) {
		CollectSegmentingColumns(*child, found);
	}
}

vector<string> DescribeSegmentApplyAlternatives(LogicalOperator &plan) {
	vector<string> found;
	CollectSegmentingColumns(plan, found);
	sort(found.begin(), found.end());
	return found;
}


//===----------------------------------------------------------------------===//
// Section 3.4.1: building the alternative
//
// The paper's criterion is a join predicate with a conjunct that compares "two instances
// of the same column from the two expressions". For such a pair we know that rows whose
// value differs can never match, so the column can partition the relation - and the paper
// turns that into a SegmentApply whose inner child evaluates once per segment:
//
//     R SA_A E = union over a of ( {a} x E(sigma_{A=a} R) )
//
// That is sound only if the aggregate inside E sees exactly the rows of the segment: the
// aggregate's input has to be the same base table as R, restricted by the same filters.
// Otherwise E(segment) would aggregate over a different set of rows than the join it
// replaces. Both conditions are checked below, and anything else is left alone.
//===----------------------------------------------------------------------===//

namespace {

//! Why the rule declined (DUCKDB_CASCADE_SEGMENT_DEBUG=1). A rewrite that silently does
//! not fire is the hardest kind to notice, so every reason is reported.
static bool SegmentDebug() {
	static const bool enabled = std::getenv("DUCKDB_CASCADE_SEGMENT_DEBUG") != nullptr;
	return enabled;
}

bool InBindings(const vector<ColumnBinding> &bindings, const ColumnBinding &binding);

static void Declined(const string &reason) {
	if (SegmentDebug()) {
		fprintf(stderr, "[segment apply] declined: %s\n", reason.c_str());
	}
}

struct BaseColumnIdentity {
	bool found = false;
	string table;
	idx_t column = 0;

	bool operator==(const BaseColumnIdentity &other) const {
		return found && other.found && table == other.table && column == other.column;
	}
};

BaseColumnIdentity IdentityOf(LogicalOperator &op, const ColumnBinding &binding) {
	BaseColumnIdentity identity;
	identity.found = TraceToBaseColumn(op, binding, identity.table, identity.column);
	return identity;
}

//! The scan a side reduces to through filters only, i.e. without a projection in between:
//! adding a column to such a scan is visible to the join, adding one below a projection is
//! not.
bool StripToScanWithoutProjection(LogicalOperator &op, vector<const Expression *> &filters,
                                  optional_ptr<LogicalGet> &scan) {
	auto current = &op;
	while (current->type == LogicalOperatorType::LOGICAL_FILTER && current->children.size() == 1) {
		for (auto &expr : current->expressions) {
			filters.push_back(expr.get());
		}
		current = current->children[0].get();
	}
	if (current->type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	scan = current->Cast<LogicalGet>();
	return true;
}

//! The scan a side reduces to through filters and projections, plus the filters on the way.
//! Returns false when the side is not a filtered base-table scan - those sides are not
//! compared, because "the same rows" is then not something this rule can establish.
bool StripToScan(LogicalOperator &op, vector<const Expression *> &filters, optional_ptr<LogicalGet> &scan) {
	auto current = &op;
	while (true) {
		if (current->type == LogicalOperatorType::LOGICAL_FILTER && current->children.size() == 1) {
			for (auto &expr : current->expressions) {
				filters.push_back(expr.get());
			}
			current = current->children[0].get();
			continue;
		}
		if (current->type == LogicalOperatorType::LOGICAL_PROJECTION && current->children.size() == 1) {
			current = current->children[0].get();
			continue;
		}
		break;
	}
	if (current->type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	scan = current->Cast<LogicalGet>();
	return true;
}

bool SameBaseTable(const LogicalGet &left, const LogicalGet &right) {
	auto left_table = left.GetTable();
	auto right_table = right.GetTable();
	if (left_table && right_table) {
		return left_table == right_table;
	}
	return left.GetName() == right.GetName();
}

bool SameFilterSet(const vector<const Expression *> &left, const vector<const Expression *> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	vector<bool> used(right.size(), false);
	for (auto &expr : left) {
		bool found = false;
		for (idx_t i = 0; i < right.size(); i++) {
			if (used[i] || !expr->Equals(*right[i])) {
				continue;
			}
			used[i] = true;
			found = true;
			break;
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

//! Every base column the aggregate's own expressions read.
void CollectAggregateIdentities(LogicalAggregate &aggregate, vector<BaseColumnIdentity> &result) {
	auto collect = [&](const Expression &expr) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    expr, [&](const BoundColumnRefExpression &colref) {
			    auto identity = IdentityOf(*aggregate.children[0], colref.Binding());
			    for (auto &existing : result) {
				    if (existing == identity) {
					    return;
				    }
			    }
			    result.push_back(identity);
		    });
	};
	for (auto &expr : aggregate.groups) {
		collect(*expr);
	}
	for (auto &expr : aggregate.expressions) {
		collect(*expr);
	}
}

//! Make the segmented side expose every base column the aggregate reads, by adding the
//! missing columns to the scan it reduces to. The binder narrows a scan to the columns the
//! query references *above* it, so the side that gets segmented often does not expose a
//! column the aggregate needs - but the rows the segment holds are the same either way, so
//! the column can simply be added. With `apply = false` this only reports whether that is
//! possible (a projection in between, or a computed column, makes it impossible).
bool ExposeAggregateColumns(LogicalOperator &relation, LogicalAggregate &aggregate, bool apply) {
	vector<const Expression *> filters;
	optional_ptr<LogicalGet> relation_scan;
	if (!StripToScanWithoutProjection(relation, filters, relation_scan)) {
		return false;
	}
	vector<const Expression *> aggregate_filters;
	optional_ptr<LogicalGet> aggregate_scan;
	if (!StripToScanWithoutProjection(*aggregate.children[0], aggregate_filters, aggregate_scan)) {
		return false;
	}
	vector<BaseColumnIdentity> wanted;
	CollectAggregateIdentities(aggregate, wanted);
	auto relation_bindings = relation.GetColumnBindings();
	bool added = false;
	for (auto &identity : wanted) {
		bool exposed = false;
		for (auto &binding : relation_bindings) {
			if (IdentityOf(relation, binding) == identity) {
				exposed = true;
				break;
			}
		}
		if (exposed) {
			continue;
		}
		// The column is read by the aggregate but not exposed by the segmented side: take its
		// column id from the aggregate's own scan of the same table.
		bool found = false;
		for (auto &column_id : aggregate_scan->GetColumnIds()) {
			if (identity.found && column_id.GetPrimaryIndex() == identity.column) {
				if (apply) {
					// The scan's column ids are logical column indexes in the table
					// (ColumnIndex::ToLogical is the primary index), which is what
					// AddColumnId expects.
					relation_scan->AddColumnId(column_id.GetPrimaryIndex());
				}
				added = true;
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	if (apply && added) {
		relation.ResolveOperatorTypes();
	}
	return true;
}

//! The aggregate a side's output comes from, through projections and filters.
optional_ptr<LogicalAggregate> FindAggregate(LogicalOperator &op) {
	auto current = &op;
	while (current->children.size() == 1 &&
	       (current->type == LogicalOperatorType::LOGICAL_PROJECTION ||
	        current->type == LogicalOperatorType::LOGICAL_FILTER)) {
		current = current->children[0].get();
	}
	if (current->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return current->Cast<LogicalAggregate>();
	}
	// The aggregate can also sit one join away: TPC-H Q17's decorrelated plan keeps `part`
	// inside the same sub-tree as the aggregate, and the join with it is exactly what the
	// paper pushes below the SegmentApply. E then contains that whole sub-tree.
	if (current->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    current->Cast<LogicalComparisonJoin>().join_type == JoinType::INNER) {
		for (auto &child : current->children) {
			if (auto nested = FindAggregate(*child)) {
				return nested;
			}
		}
	}
	return nullptr;
}

//! Whether the aggregate's own grouping is the given base column - i.e. the segment key.
bool AggregateGroupedOn(LogicalAggregate &aggregate, const BaseColumnIdentity &identity) {
	vector<BaseColumnIdentity> groups;
	CollectAggregateIdentities(aggregate, groups);
	for (auto &group : groups) {
		if (group == identity) {
			return true;
		}
	}
	return false;
}

//! Section 3.4.1 asks for a conjunct that compares two instances of the same column. DuckDB's
//! decorrelated TPC-H Q17 does not have one: `lineitem.l_partkey = part.p_partkey` and
//! `part.p_partkey = l2_partkey` only equate the two lineitem instances *through* part. So
//! when the outer equality compares different base columns, the aggregate's side is searched
//! for the hop that leads back to the segmented relation's column.
bool FindIndirectKey(LogicalOperator &aggregate_side, const ColumnBinding &through,
                     const BaseColumnIdentity &relation_identity, ColumnBinding &key, LogicalType &key_type) {
	if (aggregate_side.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &inner = aggregate_side.Cast<LogicalComparisonJoin>();
	if (inner.join_type != JoinType::INNER) {
		return false;
	}
	for (auto &condition : inner.conditions) {
		if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
			continue;
		}
		auto &lhs = condition.GetLHS();
		auto &rhs = condition.GetRHS();
		if (lhs.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    rhs.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		auto &left = lhs.Cast<BoundColumnRefExpression>();
		auto &right = rhs.Cast<BoundColumnRefExpression>();
		for (idx_t side = 0; side < 2; side++) {
			auto &same = (side == 0 ? left : right).Binding();
			auto &candidate = (side == 0 ? right : left);
			if (!(same == through)) {
				continue;
			}
			if (IdentityOf(aggregate_side, candidate.Binding()) == relation_identity) {
				key = candidate.Binding();
				key_type = candidate.GetReturnType();
				return true;
			}
		}
	}
	return false;
}

//! Every base column the aggregate's own expressions read, mapped to the position of the
//! same base column in the segmented relation's output. An empty result means the relation
//! does not expose everything E needs, which makes the rewrite impossible.
bool MapAggregateColumns(LogicalOperator &relation, LogicalAggregate &aggregate,
                         const vector<ColumnBinding> &target_bindings, BindingExport &output) {
	// The positions of the relation's own columns, by base-column identity.
	vector<BaseColumnIdentity> identities;
	auto relation_bindings = relation.GetColumnBindings();
	relation.ResolveOperatorTypes();
	for (auto &binding : relation_bindings) {
		identities.push_back(IdentityOf(relation, binding));
	}
	if (identities.empty()) {
		return false;
	}
	D_ASSERT(target_bindings.size() == relation_bindings.size());

	vector<ColumnBinding> referenced;
	auto collect = [&](const Expression &expr) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    expr, [&](const BoundColumnRefExpression &colref) {
			    for (auto &existing : referenced) {
				    if (existing == colref.Binding()) {
					    return;
				    }
			    }
			    referenced.push_back(colref.Binding());
		    });
	};
	for (auto &expr : aggregate.groups) {
		collect(*expr);
	}
	for (auto &expr : aggregate.expressions) {
		collect(*expr);
	}
	// The aggregate reads its input: every referenced binding has to be produced by it.
	auto input_bindings = aggregate.children[0]->GetColumnBindings();
	for (auto &binding : referenced) {
		auto identity = IdentityOf(*aggregate.children[0], binding);
		bool exposed = false;
		for (idx_t position = 0; position < identities.size(); position++) {
			if (identities[position] == identity) {
				// The parameter exposes the relation's columns in order, so the position
				// carries over; the aggregate now reads the parameter, not the old input.
				output.emplace_back(binding, target_bindings[position]);
				exposed = true;
				break;
			}
		}
		if (!exposed) {
			if (SegmentDebug()) {
				fprintf(stderr, "[segment apply]   column [%llu.%llu] -> %s.%llu not exposed; candidates:\n",
				        (unsigned long long)binding.table_index.index,
				        (unsigned long long)binding.column_index.GetIndexUnsafe(), identity.table.c_str(),
				        (unsigned long long)identity.column);
				for (idx_t c = 0; c < identities.size(); c++) {
					fprintf(stderr, "[segment apply]     [%llu] %s.%llu\n", (unsigned long long)c,
					        identities[c].table.c_str(), (unsigned long long)identities[c].column);
				}
			}
			return false;
		}
	}
	return true;
}

//! The shape section 3.4.1 looks for, resolved to the two sides of the join.
//! Whether BuildSegmentApply will push a relation below the SegmentApply for this aggregate
//! side: the aggregate sits in one child of an inner join and the other child is the relation
//! in between (TPC-H Q17's part).
bool WillPushRelationBelow(LogicalOperator &aggregate_side) {
	if (aggregate_side.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &inner = aggregate_side.Cast<LogicalComparisonJoin>();
	if (inner.join_type != JoinType::INNER || inner.children.size() != 2) {
		return false;
	}
	auto first_holds = FindAggregate(*inner.children[0]) != nullptr;
	auto second_holds = FindAggregate(*inner.children[1]) != nullptr;
	return first_holds != second_holds;
}

struct SegmentShape {
	//! Which child of the join is the segmented relation, and which holds the aggregate.
	idx_t relation_child = 0;
	idx_t aggregate_child = 1;
	//! Position of the segmenting column in the segmented relation's output.
	idx_t relation_key_position = 0;
	LogicalType relation_key_type;
	//! The aggregate side's output binding for the same column, and its type.
	ColumnBinding aggregate_key;
	LogicalType aggregate_key_type;
	//! The index of the condition that established the equality.
	idx_t key_condition = 0;
};

bool FindSegmentShape(LogicalComparisonJoin &join, SegmentShape &shape) {
	if (join.join_type != JoinType::INNER || join.children.size() != 2) {
		Declined("not an inner join of two children");
		return false;
	}
	for (idx_t condition_index = 0; condition_index < join.conditions.size(); condition_index++) {
		auto &condition = join.conditions[condition_index];
		if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
			continue;
		}
		auto &lhs = condition.GetLHS();
		auto &rhs = condition.GetRHS();
		if (lhs.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    rhs.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		for (idx_t side = 0; side < 2; side++) {
			auto relation_index = side;
			auto aggregate_index = 1 - side;
			auto &relation_ref = (side == 0 ? lhs : rhs).Cast<BoundColumnRefExpression>();
			auto &aggregate_ref = (side == 0 ? rhs : lhs).Cast<BoundColumnRefExpression>();

			auto relation_identity = IdentityOf(*join.children[relation_index], relation_ref.Binding());
			auto aggregate_identity = IdentityOf(*join.children[aggregate_index], aggregate_ref.Binding());
			auto key_binding = aggregate_ref.Binding();
			auto key_type = aggregate_ref.GetReturnType();
			if (!(relation_identity == aggregate_identity)) {
				// Not compared directly - look for the instance one hop away, inside the
				// aggregate's side (TPC-H Q17's part join).
				ColumnBinding indirect_key;
				LogicalType indirect_type;
				auto indirect_aggregate = FindAggregate(*join.children[aggregate_index]);
				if (!indirect_aggregate ||
				    !FindIndirectKey(*join.children[aggregate_index], aggregate_ref.Binding(), relation_identity,
				                     indirect_key, indirect_type)) {
					Declined("not the same base column on both sides");
					continue;
				}
				key_binding = indirect_key;
				key_type = indirect_type;
			}
			// The aggregate side has to be an aggregate, grouped by that same column.
			auto aggregate = FindAggregate(*join.children[aggregate_index]);
			if (!aggregate) {
				Declined("the other side is not an aggregate");
				continue;
			}
			if (aggregate->groups.empty()) {
				Declined("the aggregate has no grouping columns");
				continue;
			}
			if (!AggregateGroupedOn(*aggregate, relation_identity)) {
				Declined("the aggregate is not grouped by the segmented relation's key");
				continue;
			}
			// ... and it has to read the same rows as the segmented relation does.
			vector<const Expression *> relation_filters;
			optional_ptr<LogicalGet> relation_scan;
			if (!StripToScan(*join.children[relation_index], relation_filters, relation_scan)) {
				Declined("the segmented side is not a filtered base-table scan");
				continue;
			}
			vector<const Expression *> aggregate_filters;
			optional_ptr<LogicalGet> aggregate_scan;
			if (!StripToScan(*aggregate->children[0], aggregate_filters, aggregate_scan)) {
				Declined("the aggregate's input is not a filtered base-table scan");
				continue;
			}
			if (!SameBaseTable(*relation_scan, *aggregate_scan)) {
				Declined("the two sides read different base tables");
				continue;
			}
			if (!SameFilterSet(relation_filters, aggregate_filters)) {
				Declined("the two sides have different filters");
				continue;
			}
			// An aggregate side that is itself a join is only handled by the pushed-down
			// variant; if that does not apply, the join keeps a relation E cannot see.
			if (join.children[aggregate_index]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
			    !WillPushRelationBelow(*join.children[aggregate_index])) {
				Declined("the aggregate's side is a join the pushdown does not cover");
				continue;
			}
			// Unless the pushed-down variant applies (the aggregate's side is a join that keeps
			// the relation in between), every other condition has to be evaluable above the
			// segment's key join: it may only read the segmented relation or what the
			// aggregate's side exposes. A condition reaching *into* that side for a relation it
			// does not expose would otherwise be left pointing at a binding that is gone.
			if (!WillPushRelationBelow(*join.children[aggregate_index])) {
				auto relation_side_bindings = join.children[relation_index]->GetColumnBindings();
				auto aggregate_side_bindings = join.children[aggregate_index]->GetColumnBindings();
				bool evaluable = true;
				for (idx_t other = 0; other < join.conditions.size() && evaluable; other++) {
					if (other == condition_index || !join.conditions[other].IsComparison()) {
						continue;
					}
					auto check = [&](const Expression &expr) {
						ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
						    expr, [&](const BoundColumnRefExpression &colref) {
							    if (!InBindings(relation_side_bindings, colref.Binding()) &&
							        !InBindings(aggregate_side_bindings, colref.Binding())) {
								    evaluable = false;
							    }
						    });
					};
					check(join.conditions[other].GetLHS());
					check(join.conditions[other].GetRHS());
				}
				if (!evaluable) {
					Declined("a join condition reads a relation the aggregate's side does not expose");
					continue;
				}
			}
			// ... and the relation has to expose every base column the aggregate reads.
			BindingExport aggregate_input_map;
			if (!ExposeAggregateColumns(*join.children[relation_index], *aggregate, false)) {
				Declined("the segmented side does not expose every column the aggregate reads, and its scan "
				         "cannot be widened to do so");
				continue;
			}
			// Whether the segmented side has to be widened is decided here; the widening
			// itself happens in BuildSegmentApply, once the shape has been accepted.
			// The segmenting column's position in the relation's output.
			auto relation_bindings = join.children[relation_index]->GetColumnBindings();
			join.children[relation_index]->ResolveOperatorTypes();
			idx_t key_position = DConstants::INVALID_INDEX;
			for (idx_t position = 0; position < relation_bindings.size(); position++) {
				if (IdentityOf(*join.children[relation_index], relation_bindings[position]) == relation_identity) {
					key_position = position;
					break;
				}
			}
			if (key_position == DConstants::INVALID_INDEX) {
				Declined("the segmenting column is not in the segmented side's output");
				continue;
			}
			shape.relation_child = relation_index;
			shape.aggregate_child = aggregate_index;
			shape.relation_key_position = key_position;
			shape.relation_key_type = join.children[relation_index]->types[key_position];
			shape.aggregate_key = key_binding;
			shape.aggregate_key_type = key_type;
			shape.key_condition = condition_index;
			return true;
		}
	}
	return false;
}

void SegmentRewriteExpressionBindings(unique_ptr<Expression> &expr, const BindingExport &exports) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &) {
		    for (auto &entry : exports) {
			    if (colref.Binding() == entry.first) {
				    colref.BindingMutable() = entry.second;
				    return;
			    }
		    }
	    });
}

void SegmentRewriteOperatorBindings(LogicalOperator &op, const BindingExport &exports) {
	for (auto &expr : op.expressions) {
		SegmentRewriteExpressionBindings(expr, exports);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		for (auto &expr : aggregate.groups) {
			SegmentRewriteExpressionBindings(expr, exports);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto &orders = (op.type == LogicalOperatorType::LOGICAL_ORDER_BY)
		                   ? op.Cast<LogicalOrder>().orders
		                   : op.Cast<LogicalTopN>().orders;
		for (auto &order : orders) {
			SegmentRewriteExpressionBindings(order.expression, exports);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		for (auto &target : distinct.distinct_targets) {
			SegmentRewriteExpressionBindings(target, exports);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &any_join = op.Cast<LogicalAnyJoin>();
		SegmentRewriteExpressionBindings(any_join.condition, exports);
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			SegmentRewriteExpressionBindings(condition.LeftReference(), exports);
			SegmentRewriteExpressionBindings(condition.RightReference(), exports);
		}
		break;
	}
	default:
		break;
	}
}

bool SegmentPassesBindingsThrough(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return true;
	default:
		return false;
	}
}

//! Build the SegmentApply for a shape that FindSegmentShape accepted.
//! The bindings a parameter leaf of the given width exposes.
vector<ColumnBinding> ParameterBindings(TableIndex table_index, idx_t column_count) {
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < column_count; i++) {
		result.emplace_back(table_index, ProjectionIndex(i));
	}
	return result;
}

unique_ptr<LogicalOperator> BuildSegmentApply(LogicalComparisonJoin &join, const SegmentShape &shape, Binder &binder,
                                             BindingExport &exports) {
	auto relation = std::move(join.children[shape.relation_child]);
	auto aggregate_side = std::move(join.children[shape.aggregate_child]);
	auto aggregate = FindAggregate(*aggregate_side);
	D_ASSERT(aggregate);

	// Figure 7 of the paper: when the two instances are equated only *through* another relation
	// (TPC-H Q17's part), that relation is pushed below the SegmentApply - E then reads only the
	// rows the join kept. It also leaves E with a single join, which is the shape the driver can
	// run: this fork plans a join whose output feeds another join's build, and re-running that
	// per segment is not something the driver can order.
	unique_ptr<LogicalOperator> pushed_side;
	vector<JoinCondition> pushed_conditions;
	if (aggregate_side->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &inner = aggregate_side->Cast<LogicalComparisonJoin>();
		if (inner.join_type == JoinType::INNER && inner.children.size() == 2) {
			auto first_holds_aggregate = FindAggregate(*inner.children[0]) != nullptr;
			auto second_holds_aggregate = FindAggregate(*inner.children[1]) != nullptr;
			if (first_holds_aggregate != second_holds_aggregate) {
				idx_t kept = first_holds_aggregate ? 0 : 1;
				pushed_side = std::move(inner.children[1 - kept]);
				auto kept_side = std::move(inner.children[kept]);
				for (auto &condition : inner.conditions) {
					pushed_conditions.push_back(condition.Copy());
				}
				aggregate_side = std::move(kept_side);
			}
		}
	}

	// The segmented side may have to be widened first: the binder narrows its scan to what the
	// query references above, and E reads the segment through that same column list.
	if (!ExposeAggregateColumns(*relation, *aggregate, true)) {
		throw InternalException("cascade: SegmentApply could not expose the aggregate's columns");
	}
	relation->ResolveOperatorTypes();
	auto relation_bindings = relation->GetColumnBindings();
	auto relation_ptr = relation.get();

	// The parameter is a table-valued one - a *set* of rows - and E reads it twice: once as the
	// aggregate's input and once as the segment whose rows are joined with that aggregate's
	// result, exactly as Figure 7 of the paper shows LINEITEM in both places. Each use is its
	// own scan of the same materialized segment, so each gets its own bindings; the driver binds
	// the segment to all of them before E runs.
	auto aggregate_index = binder.GenerateTableIndex();
	auto segment_index = binder.GenerateTableIndex();

	// Push the intermediate relation below, if the shape came with one: the segmented relation
	// becomes R joined with it, and the segmenting columns grow by its key - one row of R can
	// match several rows of that relation, and each of those copies needs a segment of its own.
	unique_ptr<LogicalOperator> segmented = std::move(relation);
	vector<idx_t> pushed_key_positions;
	vector<LogicalType> pushed_key_types;
	vector<unique_ptr<Expression>> pushed_filters;
	if (pushed_side) {
		pushed_side->ResolveOperatorTypes();
		auto pushed_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		pushed_join->children.push_back(std::move(segmented));
		pushed_join->children.push_back(std::move(pushed_side));
		pushed_join->ResolveOperatorTypes();
		idx_t first_width = pushed_join->children[0]->GetColumnBindings().size();
		auto other_bindings = pushed_join->children[1]->GetColumnBindings();
		auto other_key = CascadeSideKey(*pushed_join->children[1]);
		if (other_key.empty()) {
			throw InternalException("cascade: the relation pushed below a SegmentApply has no key");
		}
		for (auto &key_binding : other_key) {
			for (idx_t i = 0; i < other_bindings.size(); i++) {
				if (other_bindings[i] == key_binding) {
					pushed_key_positions.push_back(first_width + i);
					pushed_key_types.push_back(pushed_join->children[1]->types[i]);
					break;
				}
			}
		}
		// The conditions of the join being replaced that only mention R and the pushed side are
		// enforced by this new join; the one that ties R to the aggregate is implied by the
		// segment's key join below, and anything else moves into E.
		for (idx_t i = 0; i < join.conditions.size(); i++) {
			if (i == shape.key_condition) {
				continue;
			}
			auto &condition = join.conditions[i];
			if (!condition.IsComparison()) {
				throw InternalException("cascade: SegmentApply on a non-comparison join condition");
			}
			auto left = condition.GetLHS().Copy();
			auto right = condition.GetRHS().Copy();
			vector<ColumnBinding> allowed = relation_bindings;
			for (auto &binding : other_bindings) {
				allowed.push_back(binding);
			}
			bool local = true;
			auto check = [&](const Expression &expr) {
				ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
				    expr, [&](const BoundColumnRefExpression &colref) {
					    if (!InBindings(allowed, colref.Binding())) {
						    local = false;
					    }
				    });
			};
			check(*left);
			check(*right);
			if (local) {
				pushed_join->conditions.push_back(
				    JoinCondition(std::move(left), std::move(right), condition.GetComparisonType()));
			} else {
				pushed_filters.push_back(
				    BoundComparisonExpression::Create(condition.GetComparisonType(), std::move(left), std::move(right)));
			}
		}
		pushed_join->ResolveOperatorTypes();
		segmented = std::move(pushed_join);
	}
	segmented->ResolveOperatorTypes();
	auto parameter_types = segmented->types;
	auto aggregate_parameter_bindings = ParameterBindings(aggregate_index, parameter_types.size());
	auto segment_bindings = ParameterBindings(segment_index, parameter_types.size());

	// Repoint the aggregate at the segment. Everything it reads has to be a column of R, which
	// MapAggregateColumns checks; the segment may be R joined with the pushed side, and R's
	// columns keep their positions, so nothing inside the aggregate changes.
	BindingExport aggregate_input_map;
	if (!MapAggregateColumns(*relation_ptr, *aggregate, aggregate_parameter_bindings, aggregate_input_map)) {
		throw InternalException("cascade: SegmentApply lost the relation's columns");
	}
	for (auto &expr : aggregate->groups) {
		SegmentRewriteExpressionBindings(expr, aggregate_input_map);
	}
	for (auto &expr : aggregate->expressions) {
		SegmentRewriteExpressionBindings(expr, aggregate_input_map);
	}
	auto aggregate_parameter = make_uniq<LogicalSegmentParameterGet>(aggregate_index, parameter_types);
	aggregate->children[0] = std::move(aggregate_parameter);

	// E is `sigma_{q'}( S join_{A} AGG[S] )`: the key equality keeps NULL keys from matching
	// (which is what `=` did in the join being replaced) and the rest of the predicate is a
	// filter over it - the paper's Figure 7 has both inside the SegmentApply.
	BindingExport relation_map;
	for (idx_t i = 0; i < relation_bindings.size(); i++) {
		relation_map.emplace_back(relation_bindings[i], segment_bindings[i]);
	}
	if (pushed_side) {
		// ... and the pushed side's columns are read from the segment as well.
		auto pushed_bindings = segmented->children[1]->GetColumnBindings();
		idx_t first_width = segmented->children[0]->GetColumnBindings().size();
		for (idx_t i = 0; i < pushed_bindings.size(); i++) {
			relation_map.emplace_back(pushed_bindings[i], segment_bindings[first_width + i]);
		}
	}
	auto key_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	auto key_lhs = make_uniq<BoundColumnRefExpression>(shape.relation_key_type,
	                                                  segment_bindings[shape.relation_key_position]);
	auto key_rhs = make_uniq<BoundColumnRefExpression>(shape.aggregate_key_type, shape.aggregate_key);
	key_join->conditions.emplace_back(std::move(key_lhs), std::move(key_rhs), ExpressionType::COMPARE_EQUAL);
	key_join->children.push_back(make_uniq<LogicalSegmentParameterGet>(segment_index, parameter_types));
	key_join->children.push_back(std::move(aggregate_side));
	key_join->ResolveOperatorTypes();

	unique_ptr<LogicalOperator> segment_body = std::move(key_join);
	vector<unique_ptr<Expression>> remaining;
	// The join that was pushed below keeps its own predicates: they compare the pushed side (now
	// a column of the segment) with the aggregate.
	for (auto &condition : pushed_conditions) {
		if (!condition.IsComparison()) {
			throw InternalException("cascade: SegmentApply on a non-comparison join condition");
		}
		auto left = condition.GetLHS().Copy();
		auto right = condition.GetRHS().Copy();
		SegmentRewriteExpressionBindings(left, relation_map);
		SegmentRewriteExpressionBindings(right, relation_map);
		remaining.push_back(
		    BoundComparisonExpression::Create(condition.GetComparisonType(), std::move(left), std::move(right)));
	}
	for (auto &filter : pushed_filters) {
		SegmentRewriteExpressionBindings(filter, relation_map);
		remaining.push_back(std::move(filter));
	}
	for (idx_t i = 0; i < join.conditions.size(); i++) {
		if (i == shape.key_condition || pushed_side) {
			// With a pushed side, this join's own conditions are the pushed join's conditions.
			continue;
		}
		auto &condition = join.conditions[i];
		if (!condition.IsComparison()) {
			throw InternalException("cascade: SegmentApply on a non-comparison join condition");
		}
		auto left = condition.GetLHS().Copy();
		auto right = condition.GetRHS().Copy();
		SegmentRewriteExpressionBindings(left, relation_map);
		SegmentRewriteExpressionBindings(right, relation_map);
		remaining.push_back(
		    BoundComparisonExpression::Create(condition.GetComparisonType(), std::move(left), std::move(right)));
	}
	if (!remaining.empty()) {
		auto filter = make_uniq<LogicalFilter>();
		filter->expressions = std::move(remaining);
		filter->children.push_back(std::move(segment_body));
		segment_body = std::move(filter);
	}

	// The SegmentApply itself: R (joined with the pushed side, if there was one) is segmented, E
	// is evaluated once per segment, and the output leads with the segmenting columns.
	vector<idx_t> segment_positions {shape.relation_key_position};
	vector<LogicalType> segment_types {shape.relation_key_type};
	for (idx_t i = 0; i < pushed_key_positions.size(); i++) {
		segment_positions.push_back(pushed_key_positions[i]);
		segment_types.push_back(pushed_key_types[i]);
	}
	auto segment_apply = make_uniq<LogicalSegmentApply>(std::move(segment_positions), std::move(segment_types));
	segment_apply->children.push_back(std::move(segmented));
	segment_apply->children.push_back(std::move(segment_body));
	segment_apply->ResolveOperatorTypes();

	// The columns of R (and of the pushed side) now come from the segment E joins with, which is
	// the copy that E exposes; the aggregate reads the other copy.
	for (idx_t i = 0; i < relation_bindings.size(); i++) {
		exports.emplace_back(relation_bindings[i], segment_bindings[i]);
	}
	if (segment_apply->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &pushed_join = segment_apply->children[0]->Cast<LogicalComparisonJoin>();
		auto pushed_bindings = pushed_join.children[1]->GetColumnBindings();
		idx_t first_width = pushed_join.children[0]->GetColumnBindings().size();
		for (idx_t i = 0; i < pushed_bindings.size(); i++) {
			exports.emplace_back(pushed_bindings[i], segment_bindings[first_width + i]);
		}
	}
	return std::move(segment_apply);
}

//===----------------------------------------------------------------------===//
// Section 3.4.2: pushing a join below the SegmentApply
//
//     (R SA_A E) join_p T = (R join_p T) SA_{A + columns(T)} E
//                                        iff columns(p) subset of A + columns(T)
//
// "All or nothing": if the predicate can only look at the segmenting columns or at T, then
// either every row of a segment matches a row of T or none does - so pushing the join below
// cannot split a segment, which is what would change what E sees. Pushing it below is what
// makes segmented execution pay off: E then only reads the rows that survived the join
// (TPC-H Q17's part filter is the paper's example).
//
// One row of R can match several rows of T, and then the joined row appears several times.
// The paper's fix is to add T's key to the segmenting columns: each of those copies is then
// a segment of its own, so E still sees a row once per segment.
//===----------------------------------------------------------------------===//

//! Every column the join predicate reads, so that the "all or nothing" condition can be
//! checked against the segmenting columns and T's columns.
void CollectConditionBindings(LogicalComparisonJoin &join, vector<ColumnBinding> &result) {
	auto add = [&](const Expression &expr) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    expr, [&](const BoundColumnRefExpression &colref) {
			    for (auto &existing : result) {
				    if (existing == colref.Binding()) {
					    return;
				    }
			    }
			    result.push_back(colref.Binding());
		    });
	};
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			continue;
		}
		add(condition.GetLHS());
		add(condition.GetRHS());
	}
}

bool InBindings(const vector<ColumnBinding> &bindings, const ColumnBinding &binding) {
	for (auto &candidate : bindings) {
		if (candidate == binding) {
			return true;
		}
	}
	return false;
}

//! The bindings a plan produces whose value is the same for every row of a segment. Seeded
//! with the segmenting columns the parameter exposes, and propagated only through plain
//! column references: an aggregate's grouping column that is the segmenting column is
//! constant within a segment, a computed expression is not known to be.
void CollectSegmentStableBindings(LogicalOperator &op, const vector<idx_t> &segment_positions,
                                  vector<ColumnBinding> &stable, vector<idx_t> &source_positions) {
	if (op.type == LogicalOperatorType::LOGICAL_SEGMENT_PARAMETER_GET) {
		auto bindings = op.GetColumnBindings();
		for (auto position : segment_positions) {
			if (position < bindings.size() && !InBindings(stable, bindings[position])) {
				stable.push_back(bindings[position]);
				source_positions.push_back(position);
			}
		}
		return;
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		CollectSegmentStableBindings(*op.children[0], segment_positions, stable, source_positions);
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			auto &expr = *projection.expressions[i];
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			auto &input = expr.Cast<BoundColumnRefExpression>().Binding();
			for (idx_t s = 0; s < stable.size(); s++) {
				if (stable[s] == input && !InBindings(stable, ColumnBinding(projection.table_index, ProjectionIndex(i)))) {
					stable.push_back(ColumnBinding(projection.table_index, ProjectionIndex(i)));
					source_positions.push_back(source_positions[s]);
					break;
				}
			}
		}
		return;
	}
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &aggregate = op.Cast<LogicalAggregate>();
		CollectSegmentStableBindings(*op.children[0], segment_positions, stable, source_positions);
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			auto &expr = *aggregate.groups[i];
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			auto &input = expr.Cast<BoundColumnRefExpression>().Binding();
			for (idx_t s = 0; s < stable.size(); s++) {
				if (stable[s] == input && !InBindings(stable, ColumnBinding(aggregate.group_index, ProjectionIndex(i)))) {
					stable.push_back(ColumnBinding(aggregate.group_index, ProjectionIndex(i)));
					source_positions.push_back(source_positions[s]);
					break;
				}
			}
		}
		return;
	}
	for (auto &child : op.children) {
		CollectSegmentStableBindings(*child, segment_positions, stable, source_positions);
	}
}

//! The parameter scan E joins the segment with (the aggregate reads another copy of it).
optional_ptr<LogicalSegmentParameterGet> RowParameter(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		for (auto &child : op.children) {
			if (child->type == LogicalOperatorType::LOGICAL_SEGMENT_PARAMETER_GET) {
				return child->Cast<LogicalSegmentParameterGet>();
			}
		}
	}
	for (auto &child : op.children) {
		if (auto found = RowParameter(*child)) {
			return found;
		}
	}
	return nullptr;
}

//! Every parameter scan inside E, so that they can all be widened together.
void CollectParameters(LogicalOperator &op, vector<reference<LogicalSegmentParameterGet>> &result) {
	if (op.type == LogicalOperatorType::LOGICAL_SEGMENT_PARAMETER_GET) {
		result.push_back(op.Cast<LogicalSegmentParameterGet>());
	}
	for (auto &child : op.children) {
		CollectParameters(*child, result);
	}
}

unique_ptr<LogicalOperator> PushJoinBelowSegmentApply(LogicalComparisonJoin &join, idx_t segment_side, Binder &binder,
                                                      BindingExport &exports) {
	if (join.join_type != JoinType::INNER) {
		return nullptr;
	}
	auto &segment_apply = join.children[segment_side]->Cast<LogicalSegmentApply>();
	auto &other = *join.children[1 - segment_side];
	// T needs a key, and every column of the predicate has to come from T or from the
	// segmenting columns.
	auto other_key = CascadeSideKey(other);
	if (other_key.empty()) {
		Declined("3.4.2: the other side of the join has no key to add to the segmenting columns");
		return nullptr;
	}
	vector<ColumnBinding> predicate_bindings;
	CollectConditionBindings(join, predicate_bindings);
	auto segment_apply_bindings = segment_apply.GetColumnBindings();
	vector<ColumnBinding> segment_columns;
	for (idx_t i = 0; i < segment_apply.segment_positions.size() && i < segment_apply_bindings.size(); i++) {
		segment_columns.push_back(segment_apply_bindings[i]);
	}
	auto other_bindings = other.GetColumnBindings();
	// The predicate may read T's columns, the segmenting columns, and anything E produces that
	// is constant within a segment (the aggregate's grouping column, for instance - which is
	// how the paper's TPC-H example refers to the segment key).
	vector<ColumnBinding> stable;
	vector<idx_t> stable_positions;
	CollectSegmentStableBindings(*segment_apply.children[1], segment_apply.segment_positions, stable,
	                             stable_positions);
	for (idx_t i = 0; i < segment_columns.size(); i++) {
		if (!InBindings(stable, segment_columns[i])) {
			stable.push_back(segment_columns[i]);
			stable_positions.push_back(segment_apply.segment_positions[i]);
		}
	}
	// A predicate that names one of those columns names the segmenting column itself: inside
	// the pushed join it is replaced by the column of R + T it is equal to.
	vector<std::pair<ColumnBinding, idx_t>> condition_positions;
	for (auto &binding : predicate_bindings) {
		if (InBindings(other_bindings, binding)) {
			continue;
		}
		bool mapped = false;
		for (idx_t i = 0; i < stable.size(); i++) {
			if (stable[i] == binding) {
				condition_positions.emplace_back(binding, stable_positions[i]);
				mapped = true;
				break;
			}
		}
		if (!mapped) {
			Declined("3.4.2: the predicate reads a column that is not fixed within a segment");
			return nullptr;
		}
	}
	auto row_parameter = RowParameter(*segment_apply.children[1]);
	if (!row_parameter) {
		Declined("3.4.2: E has no segment parameter to widen");
		return nullptr;
	}
	// Everything checks out: build `(R join_p T) SA_{A + key(T)} E`.
	auto segmented = std::move(segment_apply.children[0]);
	auto segment_body = std::move(segment_apply.children[1]);
	auto other_plan = std::move(join.children[1 - segment_side]);
	other_plan->ResolveOperatorTypes();

	auto pushed_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	for (auto &condition : join.conditions) {
		pushed_join->conditions.push_back(condition.Copy());
	}
	pushed_join->children.push_back(std::move(segmented));
	pushed_join->children.push_back(std::move(other_plan));
	pushed_join->ResolveOperatorTypes();
	auto pushed_bindings = pushed_join->GetColumnBindings();
	idx_t segmented_width = pushed_join->children[0]->GetColumnBindings().size();

	// The predicate's references to columns that are fixed within a segment name the
	// segmenting column itself, so inside the pushed join they become references to the
	// column of R + T they are equal to.
	if (!condition_positions.empty()) {
		BindingExport condition_map;
		for (auto &entry : condition_positions) {
			if (entry.second < pushed_bindings.size()) {
				condition_map.emplace_back(entry.first, pushed_bindings[entry.second]);
			}
		}
		for (auto &condition : pushed_join->conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			SegmentRewriteExpressionBindings(condition.LeftReference(), condition_map);
			SegmentRewriteExpressionBindings(condition.RightReference(), condition_map);
		}
	}

	// E reads the segment through its parameter scans; the segment is now the joined rows, so
	// every one of them sees R's columns followed by T's. R's columns keep their positions,
	// which is why E's own expressions do not have to change.
	vector<reference<LogicalSegmentParameterGet>> parameters;
	CollectParameters(*segment_body, parameters);
	auto joined_types = pushed_join->types;
	for (auto &parameter : parameters) {
		parameter.get().chunk_types = joined_types;
		parameter.get().ResolveOperatorTypes();
	}

	// The segmenting columns are the old ones plus T's key.
	vector<idx_t> positions = segment_apply.segment_positions;
	vector<LogicalType> types = segment_apply.segment_types;
	BindingExport parameter_map;
	auto parameter_bindings = row_parameter->GetColumnBindings();
	for (idx_t i = 0; i < other_key.size(); i++) {
		idx_t position = DConstants::INVALID_INDEX;
		for (idx_t candidate = 0; candidate < other_bindings.size(); candidate++) {
			if (other_bindings[candidate] == other_key[i]) {
				position = candidate;
				break;
			}
		}
		if (position == DConstants::INVALID_INDEX) {
			throw InternalException("cascade: 3.4.2 lost a key column of the other side");
		}
		positions.push_back(segmented_width + position);
		types.push_back(joined_types[segmented_width + position]);
	}
	// T now lives inside the segment, and the parent still refers to its columns: they are
	// read from the parameter E joins the segment with.
	for (idx_t i = 0; i < other_bindings.size(); i++) {
		parameter_map.emplace_back(other_bindings[i], parameter_bindings[segmented_width + i]);
	}

	auto rewritten = make_uniq<LogicalSegmentApply>(std::move(positions), std::move(types));
	rewritten->children.push_back(std::move(pushed_join));
	rewritten->children.push_back(std::move(segment_body));
	rewritten->ResolveOperatorTypes();
	for (auto &entry : parameter_map) {
		exports.push_back(entry);
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade: section 3.4.2 - join pushed below the SegmentApply, so E reads the rows "
		               "the join kept (the other side's key joins the segmenting columns)");
	}
	return std::move(rewritten);
}

unique_ptr<LogicalOperator> RewriteNode(unique_ptr<LogicalOperator> op, Binder &binder, BindingExport &exports) {
	for (auto &child : op->children) {
		BindingExport child_exports;
		child = RewriteNode(std::move(child), binder, child_exports);
		if (child_exports.empty()) {
			continue;
		}
		SegmentRewriteOperatorBindings(*op, child_exports);
		if (SegmentPassesBindingsThrough(*op)) {
			for (auto &entry : child_exports) {
				exports.push_back(entry);
			}
		}
	}
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		// Section 3.4.2 first: a join that consumes a SegmentApply and whose predicate cannot
		// split a segment moves below it, so E only sees the rows the join kept.
		for (idx_t side = 0; side < 2; side++) {
			if (join.children[side]->type != LogicalOperatorType::LOGICAL_SEGMENT_APPLY) {
				continue;
			}
			if (auto pushed = PushJoinBelowSegmentApply(join, side, binder, exports)) {
				return pushed;
			}
		}
		SegmentShape shape;
		if (FindSegmentShape(join, shape)) {
			auto result = BuildSegmentApply(join, shape, binder, exports);
			if (SegmentDebug()) {
				fprintf(stderr, "[segment apply] rewritten plan:\n%s\n", result->ToString(&binder.context).c_str());
			}
			if (CascadeConfig::PrintPlans()) {
				Printer::Print("--- cascade: section 3.4.1 - SegmentApply introduced; E is evaluated per "
				               "segment of the joined relation");
			}
			return result;
		}
	}
	return op;
}

} // namespace

unique_ptr<LogicalOperator> BuildSegmentApplyAlternatives(unique_ptr<LogicalOperator> plan, Binder &binder) {
	if (!plan) {
		return plan;
	}
	BindingExport exports;
	return RewriteNode(std::move(plan), binder, exports);
}

} // namespace duckdb
