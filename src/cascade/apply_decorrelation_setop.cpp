// Class 2 of section 2.5, identities (5) and (6) of Figure 4: an Apply over a set operation
// becomes one Apply per branch, and the outer relation is materialised once as a CTE so
// every branch reads the same rows. That shared subplan is exactly the "additional common
// expression" class 2 is named after, and the reason the paper removes this class during
// cost-based optimization rather than during normalization.
//
//   R A_x (E1 u_all E2) = (R A_x E1) u_all (R A_x E2)
//   R A_x (E1 -_all E2) = (R A_x E1) -_all (R A_x E2)
//
// Paper: Galindo-Legaria & Joshi (SIGMOD 2001), section 2.5, Figure 4 identities (5)/(6).
// The outer columns stay part of each row, which is what keeps EXCEPT ALL correct per
// outer row: two outer rows with the same value remain two rows in each branch.
//===----------------------------------------------------------------------===//

#include "duckdb/cascade/apply_decorrelation.hpp"

#include "duckdb/cascade/cascade_bindings.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {


unique_ptr<LogicalOperator> ApplyDecorrelator::TryDistributeOverSetOperation(unique_ptr<LogicalOperator> &op,
                                                                              BindingExport &exports) {
	auto &apply = op->Cast<LogicalDependentJoin>();
	// The identities are stated for the cross form (`A_x`); the marker family needs its marks
	// combined and the outer-join forms cannot distribute at all, so those keep their rules.
	if (apply.join_type != JoinType::INNER || apply.correlated_columns.empty()) {
		return nullptr;
	}
	auto &right = *op->children[1];
	if (right.type != LogicalOperatorType::LOGICAL_UNION && right.type != LogicalOperatorType::LOGICAL_EXCEPT &&
	    right.type != LogicalOperatorType::LOGICAL_INTERSECT) {
		return nullptr;
	}
	auto &setop = right.Cast<LogicalSetOperation>();
	if (setop.children.size() < 2) {
		return nullptr;
	}
	// The outer relation is materialised once and read by every branch. That shared subplan is
	// the "additional common subexpression" the paper's Class 2 is named after: the alternative,
	// copying the sub-tree per branch, would leave two operators exposing the same bindings -
	// a logical copy keeps the table indexes - and would evaluate the outer relation twice.
	auto outer_bindings = op->children[0]->GetColumnBindings();
	auto inner_bindings = right.GetColumnBindings();
	op->children[0]->ResolveOperatorTypes();
	auto outer_types = op->children[0]->types;
	if (outer_types.size() != outer_bindings.size()) {
		return nullptr;
	}
	vector<Identifier> outer_names;
	for (idx_t i = 0; i < outer_types.size(); i++) {
		outer_names.emplace_back("col" + std::to_string(i));
	}
	auto cte_index = binder.GenerateTableIndex();

	vector<unique_ptr<LogicalOperator>> branches;
	for (auto &branch : setop.children) {
		auto ref_index = binder.GenerateTableIndex();
		auto ref = make_uniq<LogicalCTERef>(ref_index, cte_index, outer_types, outer_names);
		auto ref_bindings = ref->GetColumnBindings();
		// Inside a branch, the outer relation is the CTE reference.
		ColumnBindingReplacer replacer;
		for (idx_t i = 0; i < outer_bindings.size(); i++) {
			replacer.replacement_bindings.emplace_back(outer_bindings[i], ref_bindings[i]);
		}
		replacer.VisitOperator(*branch);

		// The Apply's own ON predicate named the set operation's columns; inside a branch
		// those are the branch's columns at the same positions. Applying it per branch is the
		// same as applying it to the union's rows for an inner join.
		auto branch_bindings = branch->GetColumnBindings();
		auto branch_condition = unique_ptr<Expression>();
		if (apply.condition) {
			branch_condition = apply.condition->Copy();
			BindingExport condition_map;
			// Both sides move: the outer relation is now the CTE reference, and the set
			// operation's columns are this branch's.
			for (idx_t i = 0; i < outer_bindings.size() && i < ref_bindings.size(); i++) {
				condition_map.emplace_back(outer_bindings[i], ref_bindings[i]);
			}
			for (idx_t i = 0; i < inner_bindings.size() && i < branch_bindings.size(); i++) {
				condition_map.emplace_back(inner_bindings[i], branch_bindings[i]);
			}
			RewriteExpressionBindings(branch_condition, condition_map);
		}

		auto branch_apply = make_uniq<LogicalDependentJoin>(JoinType::INNER);
		branch_apply->correlated_columns = apply.correlated_columns;
		// The correlation now names the CTE reference's columns, so the metadata has to follow
		// it - otherwise the predicate lift would look for bindings the branch no longer has.
		for (auto &info : branch_apply->correlated_columns) {
			for (idx_t i = 0; i < outer_bindings.size(); i++) {
				if (info.binding == outer_bindings[i]) {
					info.binding = ref_bindings[i];
					break;
				}
			}
		}
		branch_apply->condition = std::move(branch_condition);
		branch_apply->children.push_back(std::move(ref));
		branch_apply->children.push_back(std::move(branch));
		// The branch's own export map is dropped on purpose: the set operation re-binds by
		// position, so what the branch did to its bindings is invisible above it.
		BindingExport branch_exports;
		branches.push_back(DecorrelateApply(std::move(branch_apply), branch_exports));
	}

	auto main_plan = make_uniq<LogicalSetOperation>(setop.table_index, outer_bindings.size() + inner_bindings.size(),
	                                               std::move(branches), setop.type, setop.setop_all,
	                                               setop.allow_out_of_order);
	auto result = make_uniq<LogicalMaterializedCTE>(Identifier("__cascade_class2"), cte_index,
	                                              outer_bindings.size(), std::move(op->children[0]),
	                                              std::move(main_plan), CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	// The parent read the Apply's output: the outer relation's columns, then the sub-query's.
	// The CTE exposes exactly the latter's plan (the set operation), positionally.
	for (idx_t i = 0; i < outer_bindings.size(); i++) {
		exports.emplace_back(outer_bindings[i], ColumnBinding(setop.table_index, ProjectionIndex(i)));
	}
	for (idx_t i = 0; i < inner_bindings.size(); i++) {
		exports.emplace_back(inner_bindings[i],
		                     ColumnBinding(setop.table_index, ProjectionIndex(outer_bindings.size() + i)));
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade: section 2.5 class 2 - Apply distributed over a set operation "
		               "(identity (5)/(6)); the outer relation is shared through a materialised CTE");
	}
	distributed_set_operation = true;
	return std::move(result);
}

} // namespace duckdb
