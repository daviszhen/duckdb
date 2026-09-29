//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/search.hpp
//
// The cascade optimizer itself: take the bound logical plan, build the memo, run the
// rules over it with a task queue, hand back the plan the winners select.
//
// This is stage 1 of the plan in CASCADE_OPTIMIZER_REFS.md section 8 (design A): the
// memo covers the *logical* layer only and the host's PhysicalPlanGenerator still
// instantiates physical operators, so nothing here needs to know about pipelines.
// It is hooked behind DUCKDB_CASCADE_MEMO, and with no rules registered it must
// reproduce the input plan exactly - which is the first thing worth verifying.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/cascade/apply_decorrelation.hpp"
#include "duckdb/cascade/cascades/cost.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/rule.hpp"
#include "duckdb/cascade/cascades/task.hpp"

namespace duckdb {

class ClientContext;
class Binder;

class CascadesOptimizer {
public:
	//! The binder is needed by the enforcer, which reuses the decorrelator.
	explicit CascadesOptimizer(Binder &binder, ClientContext &context)
	    : optimizer_binder(binder), context(context), memo(context) {
	}

	//! Optimize `plan` and return the chosen plan.
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	//! Register a rule; the optimizer owns it.
	void AddRule(unique_ptr<CascadesRule> rule) {
		rules.push_back(std::move(rule));
	}

	Memo &GetMemo() {
		return memo;
	}
	ClientContext &GetContext() {
		return context;
	}
	//! Add an expression to a group (called by rules).
	void AddExpression(GroupId group, unique_ptr<GroupExpr> expr);

	idx_t GroupsExplored() const {
		return groups_explored;
	}
	idx_t RulesApplied() const {
		return rules_applied;
	}
	//! How many alternative expressions the rules put into the memo. This is the number that
	//! says a rule did something, independently of whether its alternative won on cost.
	idx_t ExpressionsAdded() const {
		return expressions_added;
	}
	idx_t RulesRejected() const {
		return rules_rejected;
	}
	idx_t RulesSkipped() const {
		return rules_skipped;
	}
	idx_t Enforced() const {
		return enforced;
	}

private:
	void RegisterRules();
	//! Test hook behind DUCKDB_CASCADE_MEMO_SELFTEST: break the memo the way invariant `which`
	//! is supposed to catch, so that Validate has something to reject.
	void InjectSelfTestFault(idx_t which);
	void RunTasks();
	void ExploreGroup(GroupId group);
	void OptimizeExpr(GroupId group, GroupExpr &expr);
	//! Cost the expression and make it the group's winner when it is the best so far. Reached
	//! once every child has a winner - the terminal step of OptimizeInputs.
	void FinishExpr(GroupId group, GroupExpr &expr);
	void ApplyRule(GroupId group, GroupExpr &expr, CascadesRule &rule);
	//! Cost an expression from its children's winners (they are already chosen).
	double CostOf(GroupExpr &expr);

	Binder &optimizer_binder;
	ClientContext &context;
	Memo memo;
	vector<unique_ptr<CascadesRule>> rules;
	CascadesTaskQueue tasks;
	//! (rule, group) pairs already applied - ORCA's rule memory, without which the same
	//! exploration rule would keep re-firing on the expressions it produced.
	vector<std::pair<const CascadesRule *, GroupId>> rule_memory;
	//! (rule, expression) pairs for the rules that may only run once per expression.
	vector<std::pair<const CascadesRule *, GroupExpr *>> once_memory;
	idx_t groups_explored = 0;
	idx_t rules_applied = 0;
	idx_t expressions_added = 0;
	idx_t rules_no_effect = 0;
	//! Rules whose ApplyOnce() refused a second application on the same expression.
	idx_t rules_skipped = 0;
	//! How often the required property had to be enforced after the search instead of being
	//! provided by a rule. The smaller this gets, the more of the decorrelation lives in rules.
	idx_t enforced = 0;
	idx_t rules_rejected = 0;
};

} // namespace duckdb
