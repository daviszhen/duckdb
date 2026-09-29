#include "duckdb/cascade/cascades/memo.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

GroupId Memo::Add(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op);
	auto expr = make_uniq<GroupExpr>();
	expr->type = op->type;
	expr->op = std::move(op);
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
	return expr;
}

GroupExpr *Memo::WinnerOf(const OptimizationContext &context) {
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
