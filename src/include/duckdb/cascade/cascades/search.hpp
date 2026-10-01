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
#include <functional>

#include "duckdb/cascade/cascades/rule.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
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
	//! The binder, for a rule that has to mint a table index. An aggregate a rule pushes down
	//! keeps the index it already had (so the bindings above it do not move); a compensating
	//! projection is a new operator and needs a new one, and taking it from the binder is what
	//! keeps it from colliding with an index the plan already uses.
	Binder &GetBinder() {
		return optimizer_binder;
	}
	//! Add an expression to a group (called by rules).
	void AddExpression(GroupId group, unique_ptr<GroupExpr> expr);
	//! The parameterisation path (ORCA's correlated-columns-as-parameters): how many of the Applies
	//! in this plan the path could take, given which shapes it has been taught so far. Everything it
	//! cannot take is counted by the enforcer as the remaining backlog. Step one of the design in
	//! cascade-orca-notes/NEXT_framework_capability_design.md: measurement before implementation, so
	//! the interface exists without changing a single plan.
	idx_t ParameterizableApplies(const LogicalOperator &op) const {
		// The first shape the path will be taught: an inner Apply whose right side reads exactly one
		// outer column. Counting it before implementing it says how much of the backlog that one
		// shape is worth, which is what decides whether it is the shape to start with.
		idx_t count = 0;
		if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
			auto &apply = op.Cast<LogicalDependentJoin>();
			// Widened from "inner + one column" to "one column, any join type": the count says which
			// dimension splits the backlog, and the wider bucket is the honest first one if the join
			// type turns out not to matter for the shapes the corpus has.
			if (apply.correlated_columns.size() == 1) {
				count++;
			}
		}
		for (auto &child : op.children) {
			count += ParameterizableApplies(*child);
		}
		return count;
	}

	//! The same traversal, one line per bucket: how many Applies carry 1, 2 or 3+ correlated
	//! columns. The count decides which shape the rewrite starts with; the buckets that are left
	//! decide the order of everything after it. Counting only, never applied.
	string ParameterisableBreakdown(const LogicalOperator &op) const {
		vector<idx_t> buckets(4, 0);
		std::function<void(const LogicalOperator &)> walk = [&](const LogicalOperator &node) {
			if (node.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
				auto &apply = node.Cast<LogicalDependentJoin>();
				auto n = apply.correlated_columns.size();
				buckets[n < 3 ? n : 3]++;
			}
			for (auto &child : node.children) {
				walk(*child);
			}
		};
		walk(op);
		return "one column=" + std::to_string(buckets[1]) + " two=" + std::to_string(buckets[2]) +
		       " three_or_more=" + std::to_string(buckets[3]);
	}
	//! Replace an expression of a group in place, for the rules that have to rewrite what the parent
	//! reads as well as the expression itself (see Memo::ReplaceExpression), then re-schedule it.
	void ReplaceExpression(GroupId group, const GroupExpr *old_expression, unique_ptr<GroupExpr> replacement);
	//! Re-explore a group whose expressions a rule changed in place.
	void Reschedule(GroupId group);

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
	//! The required properties come from the task that reached this group: the search descends by
	//! scheduling tasks, so ORCA's optimization context travels with the job rather than being looked up.
	void FinishExpr(GroupId group, GroupExpr &expr, const RequiredProperties &required = RequiredProperties());
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
