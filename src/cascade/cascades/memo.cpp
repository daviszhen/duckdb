#include "duckdb/cascade/cascades/memo.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

GroupId Memo::Add(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op);
	auto expr = make_uniq<GroupExpr>();
	expr->type = op->type;
	expr->op = std::move(op);
	// Ask the operator for its columns while it still has children to walk.
	expr->bindings = expr->op->GetColumnBindings();
	// The children are memoised bottom-up and taken out of the operator: inside the memo a
	// group expression refers to its children by group id, which is what lets a rule splice an
	// alternative child in without touching the operator above it.
	auto taken = std::move(expr->op->children);
	expr->op->children.clear();
	for (auto &child : taken) {
		expr->children.push_back(Add(std::move(child)));
	}
	auto group = make_uniq<Group>();
	group->logical_count = 1;
	group->exprs.push_back(std::move(expr));
	groups.push_back(std::move(group));
	return groups.size() - 1;
}

GroupId Memo::AddGroup() {
	groups.push_back(make_uniq<Group>());
	return groups.size() - 1;
}

unique_ptr<GroupExpr> Memo::MakeExpr(unique_ptr<LogicalOperator> op, vector<GroupId> children) {
	D_ASSERT(op);
	auto expr = make_uniq<GroupExpr>();
	expr->type = op->type;
	expr->op = std::move(op);
	expr->op->children.clear();
	expr->children = std::move(children);
	// The bindings have to be derived, not asked for: the children are group ids now. Only what a
	// rule can actually build is covered - a pass-through operator keeps its child's columns, a
	// projection and an aggregate generate their own - and anything else is left empty, which
	// Validate treats as "not known" rather than as a mismatch.
	switch (expr->type) {
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_DISTINCT:
		// A child id that is not a group is left alone here on purpose: deriving bindings must not
		// be the thing that fails first, or the invariant check never gets to report it - it would
		// be an out-of-range read instead. Validate is what rejects it.
		if (expr->children.size() == 1 && expr->children[0] < groups.size()) {
			auto &child_group = *groups[expr->children[0]];
			if (!child_group.exprs.empty()) {
				expr->bindings = child_group.exprs[0]->bindings;
			}
		}
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		// Only an inner join exposes both sides; a semi or anti join exposes the left one, and
		// deriving the wrong columns there would be worse than not deriving any.
		if (expr->op->Cast<LogicalComparisonJoin>().join_type != JoinType::INNER) {
			break;
		}
		[[fallthrough]];
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		for (auto child : expr->children) {
			if (child >= groups.size() || groups[child]->exprs.empty()) {
				expr->bindings.clear();
				break;
			}
			for (auto &binding : groups[child]->exprs[0]->bindings) {
				expr->bindings.push_back(binding);
			}
		}
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = expr->op->Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			expr->bindings.emplace_back(projection.table_index, ProjectionIndex(i));
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = expr->op->Cast<LogicalAggregate>();
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			expr->bindings.emplace_back(aggregate.group_index, ProjectionIndex(i));
		}
		for (idx_t i = 0; i < aggregate.expressions.size(); i++) {
			expr->bindings.emplace_back(aggregate.aggregate_index, ProjectionIndex(i));
		}
		break;
	}
	default:
		break;
	}
	return expr;
}

bool Memo::Validate(string &error) const {
	for (idx_t group = 0; group < groups.size(); group++) {
		auto &data = *groups[group];
		D_ASSERT(!data.exprs.empty());
		for (auto &expr : data.exprs) {
			// 1. every child has to be a group that exists. A rule that invents an id, or reads a
			//    group that was never added, breaks here.
			for (auto child : expr->children) {
				if (child >= groups.size()) {
					error = StringUtil::Format("group %llu has an expression whose child %llu does not exist",
					                           (unsigned long long)group, (unsigned long long)child);
					return false;
				}
			}
			// 2. every expression of a group has to expose the group's columns: the whole point of
			//    an equivalence class is that its members can replace each other. An expression
			//    whose bindings could not be derived is skipped rather than reported as a
			//    mismatch.
			if (!expr->bindings.empty() && !data.exprs[0]->bindings.empty() &&
			    expr->bindings != data.exprs[0]->bindings) {
				error = StringUtil::Format(
				    "group %llu has expressions with different columns (%s vs %s)", (unsigned long long)group,
				    LogicalOperator::ColumnBindingsToString(data.exprs[0]->bindings),
				    LogicalOperator::ColumnBindingsToString(expr->bindings));
				return false;
			}
			// 3. properties: nothing to check yet - no rule produces a physical expression, and the
			//    property set is empty, so every expression satisfies it. The check belongs here
			//    when implementation rules arrive.
			if (expr->physical && expr->op && expr->op->children.size() != expr->children.size()) {
				error = StringUtil::Format("group %llu has a physical expression with %llu children but %llu groups",
				                           (unsigned long long)group, (unsigned long long)expr->op->children.size(),
				                           (unsigned long long)expr->children.size());
				return false;
			}
		}
		// 4. a winner has to be one of the group's expressions, and no costed expression of the
		//    group may be cheaper than it - that is what "the winner is the best so far" means.
		OptimizationContext context;
		context.group = group;
		auto winner = WinnerOf(context);
		if (!winner) {
			continue;
		}
		bool in_group = false;
		for (auto &expr : data.exprs) {
			if (expr.get() == winner) {
				in_group = true;
			} else if (expr->rows > 0 && expr->cost < winner->cost) {
				error = StringUtil::Format("group %llu kept a winner costing %.2f while a %.2f expression is in it",
				                           (unsigned long long)group, winner->cost, expr->cost);
				return false;
			}
		}
		if (!in_group) {
			error = StringUtil::Format("group %llu has a winner that is not one of its expressions",
			                           (unsigned long long)group);
			return false;
		}
	}
	return true;
}

GroupExpr *Memo::WinnerOf(const OptimizationContext &context) const {
	for (auto &entry : winners) {
		if (entry.first.group == context.group && entry.first.props == context.props) {
			return entry.second;
		}
	}
	return nullptr;
}

void Memo::SetWinner(const OptimizationContext &context, GroupExpr *expr) {
	for (auto &entry : winners) {
		if (entry.first.group == context.group && entry.first.props == context.props) {
			entry.second = expr;
			return;
		}
	}
	winners.emplace_back(context, expr);
}

idx_t Memo::ExprCount() const {
	idx_t count = 0;
	for (auto &group : groups) {
		count += group->exprs.size();
	}
	return count;
}

idx_t Memo::PhysicalCount() const {
	idx_t count = 0;
	for (auto &group : groups) {
		count += group->physical_count;
	}
	return count;
}

unique_ptr<LogicalOperator> Memo::ExtractPlan(GroupId root) {
	OptimizationContext context;
	context.group = root;
	auto winner = WinnerOf(context);
	if (!winner || !winner->op) {
		throw InternalException("cascade(cascades): group %llu has no chosen plan", (unsigned long long)root);
	}
	// The winner owns the operator; a plan is a tree in this host, so each group is extracted
	// exactly once and the memo is consumed by this call. Sharing (a group read twice) is what
	// rules will introduce, and it needs LogicalOperator::Copy - noted in search.cpp.
	auto result = std::move(winner->op);
	result->children.clear();
	for (auto child : winner->children) {
		result->children.push_back(ExtractPlan(child));
	}
	return result;
}

} // namespace duckdb
