#include <algorithm>
#include "duckdb/cascade/cascades/memo.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

GroupId Memo::Add(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op);
	auto expr = make_uniq<GroupExpr>();
	expr->type = op->type;
	expr->op = std::move(op);
	// Ask the operator for its columns while it still has children to walk.
	expr->bindings = expr->op->GetColumnBindings();
	// The operator's own resolved types; the host computes them while optimizing, so the plan is
	// resolved before the memo is built (Optimize does that) and they are available here.
	expr->types = expr->op->types;
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
	//! ORCA's derived property: an Apply resolves exactly the outer references it was built to
	//! consume, so those are what it provides to whatever needs them further up. Placed here, right
	//! after the children are taken over and before the switch, because `op` has been moved into
	//! expr->op by now and the switch is about bindings, not properties.
	if (expr->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		auto &dependent = expr->op->Cast<LogicalDependentJoin>();
		for (auto &column : dependent.correlated_columns) {
			expr->outer_refs.push_back(column.binding);
		}
	}
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
				expr->types = child_group.exprs[0]->types;
				// A pass-through operator resolves nothing by itself, so what its child provides it provides.
				expr->provides = child_group.exprs[0]->provides;
				// A pass-through operator resolves nothing, so whatever its input still has to have brought in
				// from outside is still outstanding above it.
				expr->outer_refs = child_group.exprs[0]->outer_refs;
			}
		}
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	// A join resolves no outer reference itself: it carries what both inputs provide, and that is
	// what a rule above it may rely on. Duplicates are dropped so the property stays canonical.
	for (auto child : expr->children) {
		if (child < groups.size() && !groups[child]->exprs.empty()) {
			for (auto &provided : groups[child]->exprs[0]->provides) {
				if (std::find(expr->provides.begin(), expr->provides.end(), provided) == expr->provides.end()) {
					expr->provides.push_back(provided);
				}
			}
		}
	}
		// Only an inner join exposes both sides; a semi or anti join exposes the left one, and
		// deriving the wrong columns there would be worse than not deriving any.
	{
			auto join_type = expr->op->Cast<LogicalComparisonJoin>().join_type;
			if (join_type == JoinType::MARK) {
				// The left side plus the mark column, as LogicalJoin does for MARK. Verified against
				// what an Apply actually exposes by the probe in search.cpp.
				if (!expr->children.empty() && expr->children[0] < groups.size() &&
				    !groups[expr->children[0]]->exprs.empty()) {
					expr->bindings = groups[expr->children[0]]->exprs[0]->bindings;
					expr->bindings.emplace_back(expr->op->Cast<LogicalComparisonJoin>().mark_index,
					                            ProjectionIndex(0));
				}
				break;
			}
			if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
				// A semi or anti join keeps the left side's columns and nothing else; deriving that
				// is worth it, because the second invariant then has something to check instead of
				// treating these expressions as unknown.
				if (!expr->children.empty() && expr->children[0] < groups.size() &&
				    !groups[expr->children[0]]->exprs.empty()) {
					expr->bindings = groups[expr->children[0]]->exprs[0]->bindings;
				}
				break;
			}
			if (join_type != JoinType::INNER) {
				break;
			}
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
	// A projection computes expressions but resolves no outer reference on its own: whatever its
	// input provides is visible above it, and the references it reads without resolving are what the
	// required side has to say (that is what the outer_refs property is for).
	if (!expr->children.empty() && expr->children[0] < groups.size() &&
	    !groups[expr->children[0]]->exprs.empty()) {
		expr->provides = groups[expr->children[0]]->exprs[0]->provides;
	}
		auto &projection = expr->op->Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			expr->bindings.emplace_back(projection.table_index, ProjectionIndex(i));
			expr->types.push_back(projection.expressions[i]->GetReturnType());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
	// An aggregate groups and computes, but resolves no outer reference itself either: what its input
	// provides is visible above it. The correlated column it *reads* as a group key is a required
	// property, not a provided one - which is the case this property exists for.
	if (!expr->children.empty() && expr->children[0] < groups.size() &&
	    !groups[expr->children[0]]->exprs.empty()) {
		expr->provides = groups[expr->children[0]]->exprs[0]->provides;
	}
		auto &aggregate = expr->op->Cast<LogicalAggregate>();
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			expr->bindings.emplace_back(aggregate.group_index, ProjectionIndex(i));
			expr->types.push_back(aggregate.groups[i]->GetReturnType());
		}
		for (idx_t i = 0; i < aggregate.expressions.size(); i++) {
			expr->bindings.emplace_back(aggregate.aggregate_index, ProjectionIndex(i));
			expr->types.push_back(aggregate.expressions[i]->GetReturnType());
		}
		break;
	}
	default:
		break;
	}
	return expr;
}

vector<GroupId> Memo::ParentsOf(GroupId group) const {
	vector<GroupId> parents;
	for (idx_t candidate = 0; candidate < groups.size(); candidate++) {
		for (auto &expr : groups[candidate]->exprs) {
			for (auto child : expr->children) {
				if (child == group) {
					parents.push_back(candidate);
					break;
				}
			}
		}
	}
	return parents;
}

bool Memo::ReplaceExpression(GroupId group, const GroupExpr *old_expression, unique_ptr<GroupExpr> replacement) {
	auto &data = *groups[group];
	for (idx_t i = 0; i < data.exprs.size(); i++) {
		if (data.exprs[i].get() != old_expression) {
			continue;
		}
		data.exprs[i] = std::move(replacement);
		// The replacement has to be explored and costed like any other expression, and it has to be
		// costed *after* its children have winners.
		data.explored = false;
		return true;
	}
	return false;
}

bool Memo::FindMarkConsumer(GroupId apply_group, GroupId &filter_group, GroupExpr *&filter, bool &negated) const {
	// The mark column is the extra binding the Apply exposes beyond its left side; measured as
	// {#[0.0], #[14.0]} for an existence sub-query, i.e. the last one.
	if (groups[apply_group]->exprs.empty() || groups[apply_group]->exprs[0]->bindings.empty()) {
		return false;
	}
	auto mark_binding = groups[apply_group]->exprs[0]->bindings.back();
	for (auto parent : ParentsOf(apply_group)) {
		for (auto &expr : groups[parent]->exprs) {
			if (expr->type != LogicalOperatorType::LOGICAL_FILTER || !expr->op) {
				continue;
			}
			for (auto &expression : expr->op->expressions) {
				bool found_negation = false;
				bool found_subquery = false;
				ExpressionIterator::VisitExpression<BoundSubqueryExpression>(
				    *expression, [&](const BoundSubqueryExpression &subquery) {
					    found_subquery = true;
					    if (subquery.GetSubqueryType() == SubqueryType::NOT_EXISTS) {
						    found_negation = true;
					    }
				    });
				ExpressionIterator::VisitExpression<BoundOperatorExpression>(
				    *expression, [&](const BoundOperatorExpression &node) {
					    if (node.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
						    found_negation = true;
					    }
				    });
				ExpressionIterator::VisitExpression<BoundFunctionExpression>(
				    *expression, [&](const BoundFunctionExpression &node) {
					    if (node.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
						    found_negation = true;
					    }
				    });
				// Reading the mark column counts as consuming it - for EXISTS the consumer is an
				// ordinary column reference, only the negated form has a NOT around it, which is why
				// the two are detected separately.
				bool reads_mark = false;
				ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
				    *expression, [&](const BoundColumnRefExpression &colref) {
					    if (colref.Binding() == mark_binding) {
						    reads_mark = true;
					    }
				    });
				if (found_subquery || reads_mark) {
					filter_group = parent;
					filter = expr.get();
					negated = found_negation;
					return true;
				}
			}
		}
	}
	return false;
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
			} else if (expr->costed && expr->cost < winner->cost) {
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
