#include "duckdb/cascade/cascades/search.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascades/rules/push_filter_below_groupby.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

static const char *PromiseName(CascadesRulePromise promise) {
	switch (promise) {
	case CascadesRulePromise::NONE:
		return "none";
	case CascadesRulePromise::LOW:
		return "low";
	case CascadesRulePromise::MEDIUM:
		return "medium";
	case CascadesRulePromise::HIGH:
		return "high";
	}
	return "?";
}

void CascadesOptimizer::RegisterRules() {
	// Stage 1 of CASCADE_OPTIMIZER_REFS.md section 11.4: the skeleton only. The rules arrive
	// one commit each, and each names the ORCA xform it was written against:
	//
	//   identity (3)/(4)  predicate into the Apply, projection widened
	//                     -> ORCA ExfInnerApply2InnerJoin / ExfSelect2Apply
	//   section 3.1 (A)   predicate below the GroupBy
	//                     -> ORCA ExfPushGbBelowJoin / ExfPushGbWithHavingBelowJoin
	//
	// Section 3.1 (A): a predicate constant within a group moves below the GroupBy.
	AddRule(make_uniq<PushFilterBelowGroupBy>());
}

unique_ptr<LogicalOperator> CascadesOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	if (!plan) {
		return plan;
	}
	RegisterRules();
	// Children are added before their parents, so the root is the last group and a group id is
	// always greater than the ids of its children.
	auto root = memo.Add(std::move(plan));
	if (memo.GroupCount() > 0) {
		CascadesTask task;
		task.kind = CascadesTaskKind::OPTIMIZE_GROUP;
		task.group = root;
		task.promise = CascadesRulePromise::HIGH;
		tasks.Push(task);
	}
	RunTasks();
	auto result = memo.ExtractPlan(root);
	result->ResolveOperatorTypes();
	if (CascadeConfig::PrintPlans()) {
		Printer::Print(StringUtil::Format("--- cascade(cascades): groups=%llu exprs=%llu physical=%llu | "
		                                  "explored=%llu rules applied=%llu no-effect=%llu rejected=%llu alternatives=%llu",
		                                  (unsigned long long)memo.GroupCount(), (unsigned long long)memo.ExprCount(),
		                                  (unsigned long long)memo.PhysicalCount(),
		                                  (unsigned long long)groups_explored, (unsigned long long)rules_applied,
		                                  (unsigned long long)rules_no_effect,
		                                  (unsigned long long)rules_rejected,
		                                  (unsigned long long)expressions_added));
	}
	return result;
}

void CascadesOptimizer::RunTasks() {
	CascadesTask task;
	while (tasks.Pop(task)) {
		switch (task.kind) {
		case CascadesTaskKind::OPTIMIZE_GROUP:
			// Explore first: an equivalence class should know all of its logical expressions
			// before one of them is chosen (ORCA's exploration phase before implementation).
			ExploreGroup(task.group);
			[[fallthrough]];
		case CascadesTaskKind::EXPLORE_GROUP: {
			auto &group = memo.GetGroup(task.group);
			// Push in reverse so the first expression is optimized first; `exprs` grows while
			// rules run, and pushing a pointer is safe because the group owns the objects.
			for (idx_t i = group.exprs.size(); i-- > 0;) {
				CascadesTask next;
				next.kind = CascadesTaskKind::OPTIMIZE_EXPR;
				next.group = task.group;
				next.expr = group.exprs[i].get();
				next.promise = CascadesRulePromise::HIGH;
				tasks.Push(next);
			}
			break;
		}
		case CascadesTaskKind::OPTIMIZE_EXPR:
			OptimizeExpr(task.group, *task.expr);
			break;
		case CascadesTaskKind::OPTIMIZE_INPUTS: {
			auto &expr = *task.expr;
			if (task.input >= expr.children.size()) {
				// Every child has a winner now, so this expression can be costed. Calling
				// OptimizeExpr here would schedule the inputs again and loop forever.
				FinishExpr(task.group, expr);
				break;
			}
			// Come back for the next input once this one has a winner.
			CascadesTask next = task;
			next.input = task.input + 1;
			tasks.Push(next);
			CascadesTask child;
			child.kind = CascadesTaskKind::OPTIMIZE_GROUP;
			child.group = expr.children[task.input];
			child.promise = CascadesRulePromise::HIGH;
			tasks.Push(child);
			break;
		}
		case CascadesTaskKind::APPLY_RULE:
			ApplyRule(task.group, *task.expr, *task.rule);
			break;
		}
	}
}

void CascadesOptimizer::ExploreGroup(GroupId group) {
	auto &data = memo.GetGroup(group);
	if (data.explored) {
		return;
	}
	data.explored = true;
	groups_explored++;
	// The list grows while rules run, so iterate by index until it stops growing: that is the
	// fixed point. The rule memory inside ApplyRule keeps a rule from firing twice on the same
	// group, which is what terminates it.
	for (idx_t i = 0; i < data.exprs.size(); i++) {
		for (auto &rule : rules) {
			if (rule->Kind() == CascadesRuleKind::IMPLEMENTATION) {
				// No physical rules in stage 1: a logical expression is its own chosen plan.
				continue;
			}
			if (!rule->Matches(*data.exprs[i])) {
				continue;
			}
			CascadesTask task;
			task.kind = CascadesTaskKind::APPLY_RULE;
			task.group = group;
			task.expr = data.exprs[i].get();
			task.rule = rule.get();
			task.promise = rule->Promise(*data.exprs[i]);
			tasks.Push(task);
		}
	}
}

void CascadesOptimizer::OptimizeExpr(GroupId group, GroupExpr &expr) {
	if (!expr.children.empty()) {
		// Costing needs the children's winners, so schedule them first and come back.
		CascadesTask task;
		task.kind = CascadesTaskKind::OPTIMIZE_INPUTS;
		task.group = group;
		task.expr = &expr;
		task.input = 0;
		task.promise = CascadesRulePromise::HIGH;
		tasks.Push(task);
		return;
	}
	FinishExpr(group, expr);
}

void CascadesOptimizer::FinishExpr(GroupId group, GroupExpr &expr) {
	auto cost = CostOf(expr);
	OptimizationContext context;
	context.group = group;
	auto current = memo.WinnerOf(context);
	if (!current || cost < current->cost) {
		expr.cost = cost;
		memo.SetWinner(context, &expr);
	}
}

double CascadesOptimizer::CostOf(GroupExpr &expr) {
	vector<double> child_costs;
	for (auto child : expr.children) {
		OptimizationContext context;
		context.group = child;
		auto winner = memo.WinnerOf(context);
		child_costs.push_back(winner ? winner->cost : 0.0);
	}
	return CostModel::Cost(expr, child_costs);
}

void CascadesOptimizer::ApplyRule(GroupId group, GroupExpr &expr, CascadesRule &rule) {
	// CascadesRule memory (ORCA keeps the same): one application per (rule, group).
	for (auto &entry : rule_memory) {
		if (entry.first == &rule && entry.second == group) {
			return;
		}
	}
	rule_memory.emplace_back(&rule, group);
	auto promise = rule.Promise(expr);
	if (promise == CascadesRulePromise::NONE) {
		// The precondition does not hold here - ORCA's Exfp() contract. Counting these is how
		// "the rule ran but declined" is told apart from "the rule never saw the shape".
		rules_rejected++;
		return;
	}
	if (rule.Apply(*this, group, expr)) {
		rules_applied++;
	} else {
		// Matched and applied, but produced nothing (the shape was not there after all).
		rules_no_effect++;
	}
}

void CascadesOptimizer::AddExpression(GroupId group, unique_ptr<GroupExpr> expr) {
	auto &data = memo.GetGroup(group);
	expressions_added++;
	if (expr->physical) {
		data.physical_count++;
	} else {
		data.logical_count++;
	}
	data.exprs.push_back(std::move(expr));
	// A new expression has to be explored too, or the rules would never see what a rule produced.
	if (data.explored) {
		data.explored = false;
		CascadesTask task;
		task.kind = CascadesTaskKind::EXPLORE_GROUP;
		task.group = group;
		task.promise = CascadesRulePromise::MEDIUM;
		tasks.Push(task);
	}
}

} // namespace duckdb
