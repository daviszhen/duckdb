// Class 2 of section 2.5, identity (7) of Figure 4, plus the SideKey helper it needs:
//
//   R A_x (E1 x E2) = (R A_x E1) |>_{R.key} (R A_x E2)
//
// The two branches are removed independently and then matched back through a key of the
// outer relation, so one outer row is paired with its own branch results exactly once -
// that key is the paper's precondition, and without one the rule declines instead of
// guessing. The comparison is IS NOT DISTINCT FROM so an outer row whose key is NULL
// still finds its rows.
//
// Paper: Galindo-Legaria & Joshi (SIGMOD 2001), section 2.5, Figure 4 identity (7).
//===----------------------------------------------------------------------===//

#include "duckdb/cascade/apply_decorrelation.hpp"

#include "duckdb/cascade/cascade_bindings.hpp"
#include "duckdb/cascade/cascade_correlation.hpp"
#include "duckdb/cascade/cascade_keys.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {


unique_ptr<LogicalOperator> ApplyDecorrelator::TryDistributeOverCrossProduct(unique_ptr<LogicalOperator> &op,
                                                                               BindingExport &exports) {
	auto &apply = op->Cast<LogicalDependentJoin>();
	if (apply.join_type != JoinType::INNER) {
		return nullptr;
	}
	// The cross product usually arrives under the projection that shapes a derived table's
	// output; that projection is recomputed above the key join, because its expressions name
	// columns from both branches.
	optional_ptr<LogicalProjection> shaping;
	auto *cross_node = op->children[1].get();
	if (cross_node->type == LogicalOperatorType::LOGICAL_PROJECTION && cross_node->children.size() == 1 &&
	    cross_node->children[0]->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		shaping = &cross_node->Cast<LogicalProjection>();
		cross_node = cross_node->children[0].get();
	}
	if (cross_node->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return nullptr;
	}
	auto &cross = cross_node->Cast<LogicalCrossProduct>();
	if (cross.children.size() != 2) {
		return nullptr;
	}
	// Identity (7) is the one that needs a key of the *outer* relation: the two branches are
	// matched back by it, and because a key is unique that match is exactly one outer row
	// rather than a product of two.
	auto outer_key = SideKey(*op->children[0]);
	if (outer_key.empty()) {
		throw NotImplementedException(
		    "cascade: a correlated Apply over a cross product needs a key on the outer relation "
		    "(identity (7) of the paper)");
	}
	auto outer_bindings = op->children[0]->GetColumnBindings();
	vector<idx_t> key_positions;
	for (auto &binding : outer_key) {
		idx_t position = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < outer_bindings.size(); i++) {
			if (outer_bindings[i] == binding) {
				position = i;
				break;
			}
		}
		if (position == DConstants::INVALID_INDEX) {
			throw InternalException("cascade: the key of the outer relation is not in its output");
		}
		key_positions.push_back(position);
	}

	// The outer relation is materialised once and read by both branches, exactly as in the
	// set-operation case.
	op->children[0]->ResolveOperatorTypes();
	auto outer_types = op->children[0]->types;
	vector<Identifier> outer_names;
	for (idx_t i = 0; i < outer_types.size(); i++) {
		outer_names.emplace_back("col" + std::to_string(i));
	}
	auto cte_index = binder.GenerateTableIndex();
	// Remember the key across the CTE this rewrite is about to introduce, so that a branch
	// that is itself a correlated cross product can match its own branches on the same key.
	cte_key_positions[cte_index.index] = key_positions;

	auto e1_bindings = cross.children[0]->GetColumnBindings();
	auto e2_bindings = cross.children[1]->GetColumnBindings();
	vector<vector<ColumnBinding>> own_bindings_of_branch = {e1_bindings, e2_bindings};
	vector<unique_ptr<LogicalOperator>> branch_plans;
	vector<vector<ColumnBinding>> ref_bindings_of_branch;
	vector<BindingExport> branch_exports_of_branch;
	for (idx_t branch_index = 0; branch_index < 2; branch_index++) {
		auto &branch = cross.children[branch_index];
		auto &own_bindings = own_bindings_of_branch[branch_index];
		// A branch that never reads the outer relation is simply crossed with it - there is
		// nothing to match on a key. This happens as soon as only one side of the user's cross
		// product is correlated.
		auto branch_correlated = SubtreeReferencesCorrelation(*branch, apply.correlated_columns);
		auto ref_index = binder.GenerateTableIndex();
		auto ref = make_uniq<LogicalCTERef>(ref_index, cte_index, outer_types, outer_names);
		auto ref_bindings = ref->GetColumnBindings();
		ColumnBindingReplacer replacer;
		for (idx_t i = 0; i < outer_bindings.size(); i++) {
			replacer.replacement_bindings.emplace_back(outer_bindings[i], ref_bindings[i]);
		}
		replacer.VisitOperator(*branch);
		if (!branch_correlated) {
			auto plain_cross = make_uniq<LogicalCrossProduct>(std::move(ref), std::move(branch));
			BindingExport identity_map;
			for (idx_t i = 0; i < ref_bindings.size(); i++) {
				identity_map.emplace_back(ref_bindings[i], ref_bindings[i]);
			}
			for (idx_t i = 0; i < own_bindings.size(); i++) {
				identity_map.emplace_back(own_bindings[i], own_bindings[i]);
			}
			branch_plans.push_back(std::move(plain_cross));
			ref_bindings_of_branch.push_back(std::move(ref_bindings));
			branch_exports_of_branch.push_back(std::move(identity_map));
			continue;
		}

		auto branch_apply = make_uniq<LogicalDependentJoin>(JoinType::INNER);
		branch_apply->correlated_columns = apply.correlated_columns;
		for (auto &info : branch_apply->correlated_columns) {
			for (idx_t i = 0; i < outer_bindings.size(); i++) {
				if (info.binding == outer_bindings[i]) {
					info.binding = ref_bindings[i];
					break;
				}
			}
		}
		branch_apply->children.push_back(std::move(ref));
		branch_apply->children.push_back(std::move(branch));
		BindingExport branch_exports;
		branch_plans.push_back(DecorrelateApply(std::move(branch_apply), branch_exports));
		ref_bindings_of_branch.push_back(std::move(ref_bindings));
		branch_exports_of_branch.push_back(std::move(branch_exports));
	}
	// A decorrelated branch need not expose its columns in the order the Apply did, so every
	// column this rewrite reads is located through the branch's export map instead of by
	// position.
	auto resolve_in_branch = [&](LogicalOperator &plan, const BindingExport &branch_exports,
	                             const ColumnBinding &binding) {
		auto mapped = MapBinding(binding, branch_exports);
		auto branch_bindings = plan.GetColumnBindings();
		for (idx_t i = 0; i < branch_bindings.size(); i++) {
			if (branch_bindings[i] == mapped) {
				return i;
			}
		}
		throw InternalException("cascade: identity (7) cannot find a column the branch has to expose");
	};

	// Match the two branches on the outer key. The comparison is NULL-safe: a nullable unique
	// constraint may leave a NULL key, and the cross product it stands for must still pair
	// that outer row with both branches rather than dropping it.
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	idx_t first_branch_width = outer_bindings.size() + e1_bindings.size();
	for (idx_t i = 0; i < key_positions.size(); i++) {
		auto &type = outer_types[key_positions[i]];
		auto left_position = resolve_in_branch(*branch_plans[0], branch_exports_of_branch[0],
		                                      ref_bindings_of_branch[0][key_positions[i]]);
		auto right_position = resolve_in_branch(*branch_plans[1], branch_exports_of_branch[1],
		                                       ref_bindings_of_branch[1][key_positions[i]]);
		auto lhs = make_uniq<BoundColumnRefExpression>(
		    type, branch_plans[0]->GetColumnBindings()[left_position]);
		auto rhs = make_uniq<BoundColumnRefExpression>(
		    type, branch_plans[1]->GetColumnBindings()[right_position]);
		join->conditions.emplace_back(std::move(lhs), std::move(rhs), ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	}
	join->children.push_back(std::move(branch_plans[0]));
	join->children.push_back(std::move(branch_plans[1]));
	join->ResolveOperatorTypes();

	// The join carries both branches' copies of the outer relation; the second one is dropped,
	// because the parent expects the outer columns once, followed by E1's and then E2's.
	auto projection_index = binder.GenerateTableIndex();
	auto join_bindings = join->GetColumnBindings();
	idx_t first_child_width = join->children[0]->GetColumnBindings().size();
	auto branch_column = [&](idx_t branch_index, const ColumnBinding &binding) {
		return resolve_in_branch(*join->children[branch_index], branch_exports_of_branch[branch_index], binding);
	};
	vector<unique_ptr<Expression>> select_list;
	// The outer relation as the first branch provides it, then E1, then E2.
	for (idx_t i = 0; i < outer_bindings.size(); i++) {
		auto position = branch_column(0, ref_bindings_of_branch[0][i]);
		select_list.push_back(
		    make_uniq<BoundColumnRefExpression>(join->children[0]->types[position], join_bindings[position]));
	}
	for (idx_t i = 0; i < e1_bindings.size(); i++) {
		auto position = branch_column(0, e1_bindings[i]);
		select_list.push_back(
		    make_uniq<BoundColumnRefExpression>(join->children[0]->types[position], join_bindings[position]));
	}
	for (idx_t i = 0; i < e2_bindings.size(); i++) {
		auto position = branch_column(1, e2_bindings[i]);
		select_list.push_back(make_uniq<BoundColumnRefExpression>(join->children[1]->types[position],
		                                                         join_bindings[first_child_width + position]));
	}
	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(select_list));
	projection->children.push_back(std::move(join));
	projection->ResolveOperatorTypes();
	auto dropped_bindings = projection->GetColumnBindings();

	unique_ptr<LogicalOperator> result_plan;
	bool has_shaped_top = false;
	TableIndex shaped_top_index = TableIndex(0);
	if (!shaping) {
		// The parent reads the outer columns, then E1's, then E2's - which is the layout of
		// the projection that dropped the second copy of the outer relation.
		for (idx_t i = 0; i < outer_bindings.size(); i++) {
			exports.emplace_back(outer_bindings[i], dropped_bindings[i]);
		}
		for (idx_t i = 0; i < e1_bindings.size(); i++) {
			exports.emplace_back(e1_bindings[i], dropped_bindings[outer_bindings.size() + i]);
		}
		for (idx_t i = 0; i < e2_bindings.size(); i++) {
			exports.emplace_back(e2_bindings[i], dropped_bindings[first_branch_width + i]);
		}
		result_plan = std::move(projection);
	} else {
		// The derived table's own projection is recomputed above, over the columns the two
		// branches now provide, and the outer relation's columns are carried alongside it.
		auto top_index = binder.GenerateTableIndex();
		vector<unique_ptr<Expression>> top_list;
		for (idx_t i = 0; i < outer_bindings.size(); i++) {
			top_list.push_back(
			    make_uniq<BoundColumnRefExpression>(projection->types[i], dropped_bindings[i]));
		}
		BindingExport branch_map;
		for (idx_t i = 0; i < e1_bindings.size(); i++) {
			branch_map.emplace_back(e1_bindings[i], dropped_bindings[outer_bindings.size() + i]);
		}
		for (idx_t i = 0; i < e2_bindings.size(); i++) {
			branch_map.emplace_back(e2_bindings[i], dropped_bindings[first_branch_width + i]);
		}
		for (auto &expr : shaping->expressions) {
			auto copy = expr->Copy();
			RewriteExpressionBindings(copy, branch_map);
			top_list.push_back(std::move(copy));
		}
		has_shaped_top = true;
		shaped_top_index = top_index;
		auto top = make_uniq<LogicalProjection>(top_index, std::move(top_list));
		top->children.push_back(std::move(projection));
		for (idx_t i = 0; i < outer_bindings.size(); i++) {
			exports.emplace_back(outer_bindings[i], ColumnBinding(top_index, ProjectionIndex(i)));
		}
		for (idx_t i = 0; i < shaping->expressions.size(); i++) {
			exports.emplace_back(ColumnBinding(shaping->table_index, ProjectionIndex(i)),
			                     ColumnBinding(top_index, ProjectionIndex(outer_bindings.size() + i)));
		}
		result_plan = std::move(top);
	}

	// An ON predicate compares the outer relation with the derived table's columns; above the
	// rewrite it is a filter on those same columns.
	if (apply.condition) {
		auto filter = make_uniq<LogicalFilter>();
		auto condition = apply.condition->Copy();
		BindingExport filter_map;
		for (idx_t i = 0; i < outer_bindings.size(); i++) {
			filter_map.emplace_back(
			    outer_bindings[i], has_shaped_top ? ColumnBinding(shaped_top_index, ProjectionIndex(i))
			                                      : dropped_bindings[i]);
		}
		if (shaping) {
			for (idx_t i = 0; i < shaping->expressions.size(); i++) {
				filter_map.emplace_back(
				    ColumnBinding(shaping->table_index, ProjectionIndex(i)),
				    ColumnBinding(shaped_top_index, ProjectionIndex(outer_bindings.size() + i)));
			}
		} else {
			for (idx_t i = 0; i < e1_bindings.size(); i++) {
				filter_map.emplace_back(e1_bindings[i], dropped_bindings[outer_bindings.size() + i]);
			}
			for (idx_t i = 0; i < e2_bindings.size(); i++) {
				filter_map.emplace_back(e2_bindings[i], dropped_bindings[first_branch_width + i]);
			}
		}
		RewriteExpressionBindings(condition, filter_map);
		filter->expressions.push_back(std::move(condition));
		filter->children.push_back(std::move(result_plan));
		result_plan = std::move(filter);
	}

	auto result = make_uniq<LogicalMaterializedCTE>(Identifier("__cascade_class2_cross"), cte_index,
	                                              outer_bindings.size(), std::move(op->children[0]),
	                                              std::move(result_plan), CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade: section 2.5 class 2 - Apply distributed over a cross product "
		               "(identity (7)); the branches are matched back on the outer key");
	}
	return std::move(result);
}

vector<ColumnBinding> ApplyDecorrelator::SideKey(LogicalOperator &side) {
	auto key = CascadeSideKey(side);
	if (!key.empty() || side.type != LogicalOperatorType::LOGICAL_CTE_REF) {
		return key;
	}
	auto entry = cte_key_positions.find(side.Cast<LogicalCTERef>().cte_index.index);
	if (entry == cte_key_positions.end()) {
		return key;
	}
	// A reference exposes the CTE's columns in the order they were materialised, which is the
	// order of the relation the key positions were taken from.
	vector<ColumnBinding> result;
	auto ref_bindings = side.GetColumnBindings();
	for (auto position : entry->second) {
		if (position < ref_bindings.size()) {
			result.push_back(ref_bindings[position]);
		}
	}
	return result;
}

} // namespace duckdb
