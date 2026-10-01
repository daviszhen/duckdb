#include "duckdb/cascade/cascades/rules/group_apply_by_outer_columns.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascade_correlation.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

bool GroupApplyByOuterColumns::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && expr.children.size() == 2;
}

namespace {

//! A sub-query column the correlated predicate pins to an outer expression.
struct GroupApplyKey {
	ColumnBinding binding;
	LogicalType type;
};

//! Everything the precondition looks at: the projection chain over the aggregate,
//! the filter the correlated predicates live in, and the columns they pin.
struct GroupApplyShape {
	//! The projections between the Apply and the aggregate, the top one first.
	vector<GroupExpr *> projections;
	GroupExpr *aggregate = nullptr;
	//! The filter directly below the aggregate, when the correlated predicate is there.
	GroupExpr *filter = nullptr;
	//! The sub-query columns the correlated predicate pins, in first-seen order.
	vector<GroupApplyKey> keys;
};

bool GroupApplyIsCorrelatedBinding(const ColumnBinding &binding, const CorrelatedColumns &correlated) {
	for (auto &entry : correlated) {
		if (entry.binding == binding) {
			return true;
		}
	}
	return false;
}

bool GroupApplyIsProjection(const GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_PROJECTION && expr.children.size() == 1;
}

bool GroupApplyIsAggregate(const GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && expr.children.size() == 1;
}

//! Walk down from `group` through whatever projections sit between the Apply and the
//! sub-query's aggregate. A rule cannot assume the binder put exactly one there: the
//! mandatory aggregate rewrites insert projections of their own.
bool GroupApplyDescribeFrom(CascadesOptimizer &optimizer, GroupId group, idx_t depth,
                            vector<GroupExpr *> &projections, GroupExpr *&aggregate) {
	if (depth > 8) {
		return false;
	}
	auto &memo = optimizer.GetMemo();
	for (auto &candidate : memo.GetGroup(group).exprs) {
		if (!GroupApplyIsProjection(*candidate)) {
			continue;
		}
		vector<GroupExpr *> inner;
		GroupExpr *found = nullptr;
		if (GroupApplyDescribeFrom(optimizer, candidate->children[0], depth + 1, inner, found)) {
			inner.insert(inner.begin(), candidate.get());
			projections = std::move(inner);
			aggregate = found;
			return true;
		}
	}
	for (auto &candidate : memo.GetGroup(group).exprs) {
		if (GroupApplyIsAggregate(*candidate)) {
			projections.clear();
			aggregate = candidate.get();
			return true;
		}
	}
	return false;
}

//! Is this comparison one the push can express: an equality with a sub-query column on one
//! side and an outer-only expression on the other? Returns that sub-query column.
//!
//! `s.c = <outer expression>` qualifies because the outer row fixes s.c, so the pushed
//! aggregate's group for s.c is exactly the rows that outer row matched, and at most one
//! group can match - which is also what keeps the LEFT join from multiplying the outer
//! rows. `s.a + s.b = t.a` fixes neither s.a nor s.b, so it is refused rather than
//! approximated; the recorded rule for that shape is identity (9).
bool GroupApplyPinnedColumn(Expression &comparison, const CorrelatedColumns &correlated, ColumnBinding &binding,
                            LogicalType &type) {
	if (!BoundComparisonExpression::IsComparison(comparison) ||
	    comparison.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	if (comparison.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
		return false;
	}
	auto &function = comparison.Cast<BoundFunctionExpression>();
	auto &lhs = BoundComparisonExpression::Left(function);
	auto &rhs = BoundComparisonExpression::Right(function);
	for (idx_t side = 0; side < 2; side++) {
		auto &operand = side == 0 ? lhs : rhs;
		auto &other = side == 0 ? rhs : lhs;
		if (operand.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		auto &colref = operand.Cast<BoundColumnRefExpression>();
		if (GroupApplyIsCorrelatedBinding(colref.Binding(), correlated)) {
			// The pinned column has to be the sub-query's own; a correlated column can never be
			// a group key of the pushed aggregate, because below the outer join it is not there.
			continue;
		}
		// The other side may name correlated columns - those the outer row provides - but not
		// the sub-query's own: with `s.c = s.d` the outer row fixes neither.
		if (ReferencesInner(other, correlated)) {
			continue;
		}
		binding = colref.Binding();
		type = colref.GetReturnType();
		return true;
	}
	return false;
}

//! The read-only precondition, and the shape `Apply` builds from. Everything is settled
//! here, so Promise can answer NONE for a shape whose replacement could not bind.
bool GroupApplyDescribe(CascadesOptimizer &optimizer, GroupExpr &expr, GroupApplyShape &shape) {
	if (expr.type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN || expr.children.size() != 2 || !expr.op) {
		return false;
	}
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	if (apply.join_type != JoinType::SINGLE || apply.condition || apply.correlated_columns.empty()) {
		return false;
	}
	if (!GroupApplyDescribeFrom(optimizer, expr.children[1], 0, shape.projections, shape.aggregate)) {
		return false;
	}
	auto &aggregate = shape.aggregate->op->Cast<LogicalAggregate>();
	if (shape.aggregate->children.size() != 1) {
		return false;
	}
	// Section 3.1 (A)'s two guards, copied: a grouping set that does not mention a column pads
	// it with NULL, so a predicate on that column is not constant within a group, and a
	// grouping column computed from something else is not a column the pushed aggregate can
	// name. There is a third here: section 3.2's recipe covers the scalar aggregate only, so
	// an aggregate that already groups is declined and left to identity (8).
	if (aggregate.grouping_sets.size() > 1) {
		return false;
	}
	for (auto &group : aggregate.groups) {
		if (group->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
	}
	if (!aggregate.groups.empty()) {
		return false;
	}
	if (aggregate.expressions.size() != 1 ||
	    aggregate.expressions[0]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
		return false;
	}
	if (ReferencesCorrelation(*aggregate.expressions[0], apply.correlated_columns)) {
		// Below the outer join the outer columns are not in scope, so an aggregate that reads
		// one cannot be pushed. (select sum(s.b + t.a) ...) stays with identity (9).
		return false;
	}
	{
		// count is the one aggregate whose empty-input answer is not NULL, so it is the one that
		// needs section 3.2's pi_c. pi_c is a projection *above* the outer join - below it the
		// join's NULL-padding would overwrite the repair, which is exactly the bug this guard
		// exists for (measured: count returned NULL instead of 0 for an outer row with no match).
		// A projection above the join renumbers the outer columns, so it is not an alternative for
		// the Apply's equivalence class but a replacement that has to rebind every parent group.
		// Until that is built, count stays with identity (9)'s enforcer, which already answers it
		// correctly; this rule takes the aggregates whose answer on an empty input is NULL.
		auto &name = aggregate.expressions[0]->Cast<BoundAggregateExpression>().Function().GetName();
		if (name == "count" || name == "count_star") {
			return false;
		}
	}
	auto &memo = optimizer.GetMemo();
	GroupExpr *filter = nullptr;
	for (auto &candidate : memo.GetGroup(shape.aggregate->children[0]).exprs) {
		if (candidate->type == LogicalOperatorType::LOGICAL_FILTER && candidate->children.size() == 1 && candidate->op) {
			filter = candidate.get();
			break;
		}
	}
	if (!filter) {
		// No liftable correlated predicate: the correlation is somewhere the push cannot reach.
		return false;
	}
	auto &body = memo.GetGroup(filter->children[0]);
	if (body.exprs.empty()) {
		return false;
	}
	for (auto &predicate : filter->op->expressions) {
		if (!ReferencesCorrelation(*predicate, apply.correlated_columns)) {
			continue;
		}
		// A conjunctive predicate is split, and each conjunct judged on its own: a local
		// conjunct stays below the aggregate, a correlated one has to be an equality the push
		// can state as the join condition.
		vector<unique_ptr<Expression>> comparisons;
		if (!CollectComparisons(predicate->Copy(), comparisons)) {
			return false;
		}
		for (auto &comparison : comparisons) {
			if (!ReferencesCorrelation(*comparison, apply.correlated_columns)) {
				continue;
			}
			ColumnBinding binding;
			LogicalType type;
			if (!GroupApplyPinnedColumn(*comparison, apply.correlated_columns, binding, type)) {
				return false;
			}
			bool known = false;
			for (auto &key : shape.keys) {
				if (key.binding == binding) {
					known = true;
				}
			}
			if (!known) {
				shape.keys.push_back(GroupApplyKey {binding, type});
			}
			// The pushed aggregate groups by this column, so the body below the filter has to
			// expose it - otherwise there is nothing to group by.
			if (!InBindings(body.exprs[0]->bindings, binding)) {
				return false;
			}
		}
	}
	if (shape.keys.empty()) {
		return false;
	}
	shape.filter = filter;
	return true;
}

//! Point a comparison at one child per side: the outer expression names the LEFT join's left
//! child, the group key the right one. Anything that cannot be put one side per child is
//! refused rather than emitted, because the resolver would look for it in the wrong scope.
bool GroupApplyOrient(Expression &condition, const vector<ColumnBinding> &left_side,
                      const vector<ColumnBinding> &key_side) {
	if (!BoundComparisonExpression::IsComparison(condition) ||
	    condition.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &comparison = condition.Cast<BoundFunctionExpression>();
	auto &lhs = BoundComparisonExpression::LeftMutable(comparison);
	auto &rhs = BoundComparisonExpression::RightMutable(comparison);
	auto left_only = [&](const Expression &e) {
		return ApplyReadsBindings(e, left_side) && !ApplyReadsBindings(e, key_side);
	};
	auto key_only = [&](const Expression &e) {
		return ApplyReadsBindings(e, key_side) && !ApplyReadsBindings(e, left_side);
	};
	if (left_only(*lhs) && key_only(*rhs)) {
		return true;
	}
	if (key_only(*lhs) && left_only(*rhs)) {
		auto moved = std::move(lhs);
		lhs = std::move(rhs);
		rhs = std::move(moved);
		BoundComparisonExpression::FlipType(comparison);
		return true;
	}
	return false;
}

} // namespace

CascadesRulePromise GroupApplyByOuterColumns::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	GroupApplyShape shape;
	if (!GroupApplyDescribe(optimizer, expr, shape)) {
		// ORCA's Exfp(): the precondition does not hold, so no task is queued. A shape that
		// passes here but cannot be built is possible in principle, but not on this rule's
		// path: Apply redoes exactly these checks before it builds anything.
		return CascadesRulePromise::NONE;
	}
	return CascadesRulePromise::MEDIUM;
}

bool GroupApplyByOuterColumns::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	GroupApplyShape shape;
	if (!GroupApplyDescribe(optimizer, expr, shape)) {
		// The promise should have rejected this one before it was queued.
		return false;
	}
	auto &memo = optimizer.GetMemo();
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	auto &aggregate = shape.aggregate->op->Cast<LogicalAggregate>();
	auto &filter = shape.filter->op->Cast<LogicalFilter>();
	auto &left_group = memo.GetGroup(expr.children[0]);
	if (left_group.exprs.empty()) {
		return false;
	}

	// Split the filter below the aggregate into the predicates the push states as the join
	// condition and the local ones that stay where they are.
	vector<unique_ptr<Expression>> lifted;
	vector<unique_ptr<Expression>> local;
	for (auto &predicate : filter.expressions) {
		if (!ReferencesCorrelation(*predicate, apply.correlated_columns)) {
			local.push_back(predicate->Copy());
			continue;
		}
		vector<unique_ptr<Expression>> comparisons;
		if (!CollectComparisons(predicate->Copy(), comparisons)) {
			return false;
		}
		for (auto &comparison : comparisons) {
			if (ReferencesCorrelation(*comparison, apply.correlated_columns)) {
				lifted.push_back(std::move(comparison));
			} else {
				local.push_back(std::move(comparison));
			}
		}
	}
	if (lifted.empty()) {
		return false;
	}

	// The body below the filter. The filter disappears when nothing local is left of it, and is
	// rebuilt over the same child otherwise, so the bindings the aggregate's expressions read
	// are the ones that were there before either way.
	GroupId body_group = shape.filter->children[0];
	if (!local.empty()) {
		auto rebuilt = make_uniq<LogicalFilter>();
		for (auto &predicate : local) {
			rebuilt->expressions.push_back(std::move(predicate));
		}
		rebuilt->estimated_cardinality = filter.estimated_cardinality;
		rebuilt->has_estimated_cardinality = filter.has_estimated_cardinality;
		body_group = memo.AddGroup();
		optimizer.AddExpression(body_group, memo.MakeExpr(std::move(rebuilt), {shape.filter->children[0]}));
	}

	// The pushed aggregate. It keeps the sub-query aggregate's own group and aggregate indices,
	// so the value binding the parent holds - and every expression of the projections above -
	// keeps meaning exactly what it did: only where the value comes from changes.
	//
	// The grouping sets are deliberately dropped. The original aggregate has no grouping of its
	// own (checked in Describe), so a single empty set is a grand total over the rows a
	// predicate match hands it, and that is what grouping by the pinned columns produces here.
	vector<unique_ptr<Expression>> pushed_list;
	for (auto &expression : aggregate.expressions) {
		pushed_list.push_back(expression->Copy());
	}
	auto pushed = make_uniq<LogicalAggregate>(aggregate.group_index, aggregate.aggregate_index, std::move(pushed_list));
	for (auto &key : shape.keys) {
		pushed->groups.push_back(make_uniq<BoundColumnRefExpression>(key.type, key.binding));
	}
	pushed->estimated_cardinality = aggregate.estimated_cardinality;
	pushed->has_estimated_cardinality = aggregate.has_estimated_cardinality;
	auto pushed_group = memo.AddGroup();
	optimizer.AddExpression(pushed_group, memo.MakeExpr(std::move(pushed), {body_group}));

	// pi_c: count is the aggregate whose empty-input answer is not NULL, so it is the one that
	// needs the compensating projection. It sits directly on the aggregate, below the sub-query's
	// own projections - which is what lets a projection that computes on the value (TPC-H Q20's
	// 0.5 * sum(...)) still see the repaired one.
	GroupId current = pushed_group;
	ColumnBinding value_from = ColumnBinding(aggregate.aggregate_index, ProjectionIndex(0));
	vector<ColumnBinding> key_bindings;
	for (idx_t i = 0; i < shape.keys.size(); i++) {
		key_bindings.emplace_back(aggregate.group_index, ProjectionIndex(i));
	}
	auto &aggregate_expression = aggregate.expressions[0]->Cast<BoundAggregateExpression>();
	auto &name = aggregate_expression.Function().GetName();
	if (name == "count" || name == "count_star") {
		auto fixup_index = optimizer.GetBinder().GenerateTableIndex();
		vector<unique_ptr<Expression>> select_list;
		auto coalesce = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE,
		                                                   aggregate_expression.GetReturnType());
		coalesce->GetChildrenMutable().push_back(make_uniq<BoundColumnRefExpression>(
		    aggregate_expression.GetReturnType(), value_from));
		coalesce->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
		select_list.push_back(std::move(coalesce));
		for (idx_t k = 0; k < shape.keys.size(); k++) {
			select_list.push_back(make_uniq<BoundColumnRefExpression>(shape.keys[k].type, key_bindings[k]));
		}
		auto fixup = make_uniq<LogicalProjection>(fixup_index, std::move(select_list));
		fixup->estimated_cardinality = aggregate.estimated_cardinality;
		fixup->has_estimated_cardinality = aggregate.has_estimated_cardinality;
		auto fixup_group = memo.AddGroup();
		optimizer.AddExpression(fixup_group, memo.MakeExpr(std::move(fixup), {pushed_group}));
		current = fixup_group;
		value_from = ColumnBinding(fixup_index, ProjectionIndex(0));
		for (idx_t k = 0; k < shape.keys.size(); k++) {
			key_bindings[k] = ColumnBinding(fixup_index, ProjectionIndex(1 + k));
		}
	}

	// The projections between the Apply and the aggregate, rebuilt bottom-up. Each keeps its own
	// table index, so every column it exposed is still exposed under the same binding and the
	// parent needs no rebinding at all. The pinned columns are appended to every one of them, so
	// the join condition can read them off the top one; the value is repointed at pi_c where
	// there is one.
	BindingExport value_export;
	value_export.emplace_back(ColumnBinding(aggregate.aggregate_index, ProjectionIndex(0)), value_from);
	GroupId top_projection = current;
	for (idx_t i = shape.projections.size(); i-- > 0;) {
		auto &projection = shape.projections[i]->op->Cast<LogicalProjection>();
		vector<unique_ptr<Expression>> select_list;
		for (auto &projection_expression : projection.expressions) {
			auto copy = projection_expression->Copy();
			if (i == shape.projections.size() - 1) {
				// Only the bottom projection names the aggregate; the ones above it name it.
				RewriteExpressionBindings(copy, value_export);
			}
			select_list.push_back(std::move(copy));
		}
		for (idx_t k = 0; k < key_bindings.size(); k++) {
			select_list.push_back(make_uniq<BoundColumnRefExpression>(shape.keys[k].type, key_bindings[k]));
		}
		auto rebuilt = make_uniq<LogicalProjection>(projection.table_index, std::move(select_list));
		rebuilt->estimated_cardinality = projection.estimated_cardinality;
		rebuilt->has_estimated_cardinality = projection.has_estimated_cardinality;
		auto rebuilt_group = memo.AddGroup();
		optimizer.AddExpression(rebuilt_group, memo.MakeExpr(std::move(rebuilt), {current}));
		for (idx_t k = 0; k < key_bindings.size(); k++) {
			key_bindings[k] =
			    ColumnBinding(projection.table_index, ProjectionIndex(projection.expressions.size() + k));
		}
		current = rebuilt_group;
		top_projection = rebuilt_group;
	}

	// The predicate becomes the LEFT join's condition, with the sub-query column repointed at the
	// copy of it the top projection exposes. DecrementCorrelationDepth is what the decorrelator
	// does to the whole sub-tree before it lifts anything: the condition is now evaluated at the
	// Apply's own level, so a reference to the enclosing query is no longer one scope out.
	BindingExport key_export;
	for (idx_t k = 0; k < shape.keys.size(); k++) {
		key_export.emplace_back(shape.keys[k].binding, key_bindings[k]);
	}
	auto &left_side = memo.GetGroup(expr.children[0]).exprs[0]->bindings;
	vector<unique_ptr<Expression>> conditions;
	for (auto &condition : lifted) {
		RewriteExpressionBindings(condition, key_export);
		DecrementCorrelationDepth(*condition);
		if (!GroupApplyOrient(*condition, left_side, key_bindings)) {
			if (CascadeConfig::PrintPlans()) {
				Printer::Print("--- cascade(cascades) rule " + string(Name()) +
				               ": the predicate cannot be put one side per join child");
			}
			return false;
		}
		conditions.push_back(std::move(condition));
	}

	auto join = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
	join->estimated_cardinality = expr.op->estimated_cardinality;
	join->has_estimated_cardinality = expr.op->has_estimated_cardinality;
	for (auto &condition : conditions) {
		auto &comparison = condition->Cast<BoundFunctionExpression>();
		auto &lhs = BoundComparisonExpression::LeftMutable(comparison);
		auto &rhs = BoundComparisonExpression::RightMutable(comparison);
		join->conditions.emplace_back(std::move(lhs), std::move(rhs), comparison.GetExpressionType());
	}
	optimizer.AddExpression(group, memo.MakeExpr(std::move(join), {expr.children[0], top_projection}));
	return true;
}

} // namespace duckdb
