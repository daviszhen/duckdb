#include <algorithm>
#include "duckdb/cascade/cascades/search.hpp"

#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascades/rules/apply_to_join.hpp"
#include "duckdb/cascade/cascades/rules/correlated_apply_to_join.hpp"
#include "duckdb/cascade/cascades/rules/lift_local_predicate.hpp"
#include "duckdb/cascade/cascades/rules/semi_apply_to_join.hpp"
#include "duckdb/cascade/cascades/rules/push_filter_below_groupby.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
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
	if (!CascadeConfig::MemoRules()) {
		// The verified state is the skeleton: the memo builds, the tasks run and the plan comes
		// back unchanged. The rules are development work - see CascadeConfig::MemoRules.
		return;
	}
	// Identity (1)/(2): an inner Apply without correlation is a join. This is the rule that can
	// lower the enforcer counter, so it goes first.
	AddRule(make_uniq<ApplyToJoin>());
	// Section 2, identity (3): a predicate reading only the sub-query's own columns moves above
	// the Apply.
	AddRule(make_uniq<LiftLocalPredicate>());
	// Identity (4), the shape the corpus has: the correlation of an existence sub-query becomes
	// the condition of a semi or anti join.
	AddRule(make_uniq<SemiApplyToJoin>());
	// Identity (4), the shape this corpus has: the correlation of an existence sub-query becomes
	// the condition of the semi, anti or mark join that replaces the Apply.
	AddRule(make_uniq<CorrelatedApplyToJoin>());
	// Section 3.1 (A): a predicate constant within a group moves below the GroupBy.
	AddRule(make_uniq<PushFilterBelowGroupBy>());
}

//! Does this plan still contain an Apply? The host has no physical operator for one, so this is
//! what the required property amounts to in practice.
static bool PlanHasApply(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		return true;
	}
	for (auto &child : op.children) {
		if (PlanHasApply(*child)) {
			return true;
		}
	}
	return false;
}

namespace {

//! Replace a MARK join's non-comparison condition with a constant comparison. The physical MARK
//! implementation needs a left/right comparison and fails to plan otherwise, and a plan the memo
//! passed through can carry a condition that is a plain expression (measured: the host plans the same
//! statement with `cmp=40` and no column references - a constant comparison - while this path carried
//! one that is not a comparison at all).
//!
//! MARK only: the semi and anti joins the rules build carry conditions that mean something, and
//! replacing those broke two matrix files (measured). A MARK join with such a condition, on the other
//! hand, is a stream the rules declined, so the condition it holds is a filter inside the right
//! sub-tree already - which is where it belongs, and the join only has to say "has a row". The whole
//! condition is replaced, not rewritten: the single-expression form has no accessor for its
//! expression.
void NormalizeMarkJoinConditions(LogicalOperator &op) {
	for (auto &child : op.children) {
		NormalizeMarkJoinConditions(*child);
	}
	auto is_join = op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	               op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
	               op.type == LogicalOperatorType::LOGICAL_ANY_JOIN;
	if (!is_join) {
		return;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::MARK) {
		return;
	}
	for (auto &condition : join.conditions) {
		if (condition.IsComparison()) {
			continue;
		}
		if (CascadeConfig::PrintPlans()) {
			Printer::Print("--- cascade(cascades) replacing a non-comparison MARK condition");
		}
		condition = JoinCondition(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)),
		                          make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)),
		                          ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	}
}

} // namespace

unique_ptr<LogicalOperator> CascadesOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	if (!plan) {
		return plan;
	}
	RegisterRules();
	// Types are resolved before the memo is built rather than only after it is taken apart: a rule
	// that builds a projection over a child's columns needs to know their types, and deciding
	// whether two expressions in one group agree on their columns wants them too.
	plan->ResolveOperatorTypes();
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
	// The invariants are checked before the plan is taken apart, so a rule that broke the memo is
	// reported as such rather than as a binding error three passes later.
	if (CascadeConfig::PrintPlans()) {
		// This inspection has to happen *before* ExtractPlan, because ExtractPlan moves each
		// winner's operator out of the memo: afterwards every winner expression has a null `op`,
		// and a dump that dereferences it dies with "unique_ptr that is NULL" - which is what kept
		// this reconnaissance from producing anything for several rounds. Any later reader of the
		// memo has the same constraint.
		for (idx_t group = 0; group < memo.GroupCount(); group++) {
			for (auto &expr : memo.GetGroup(group).exprs) {
				if (expr->type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN || !expr->op) {
					continue;
				}
				auto &apply = expr->op->Cast<LogicalDependentJoin>();
				string right_types;
				string right_below;
				for (auto &candidate : memo.GetGroup(expr->children[1]).exprs) {
					right_types += (right_types.empty() ? "" : ",") + EnumUtil::ToString(candidate->type);
					// One level deeper: where the correlated predicate actually is. The right side is
					// a projection, so the predicate cannot be on it.
					for (auto child : candidate->children) {
						for (auto &below : memo.GetGroup(child).exprs) {
							right_below += (right_below.empty() ? "" : ",") + EnumUtil::ToString(below->type);
						}
					}
				}
				// What the correlated rule's promise would see: the left side's columns, and for each
				// predicate of the filter under the projection, which sides it reads.
				auto &left_probe = memo.GetGroup(expr->children[0]);
				idx_t left_cols = left_probe.exprs.empty() ? 0 : left_probe.exprs[0]->bindings.size();
				string predicate_probe;
				for (auto child : expr->children) {
					(void)child;
				}
				for (auto &right_expr : memo.GetGroup(expr->children[1]).exprs) {
					if (right_expr->type != LogicalOperatorType::LOGICAL_PROJECTION || right_expr->children.empty()) {
						continue;
					}
					for (auto &below : memo.GetGroup(right_expr->children[0]).exprs) {
						if (below->type != LogicalOperatorType::LOGICAL_FILTER || below->children.empty()) {
							continue;
						}
						auto &right_probe = memo.GetGroup(below->children[0]);
						idx_t right_cols = right_probe.exprs.empty() ? 0 : right_probe.exprs[0]->bindings.size();
						&left_probe;
						for (auto &predicate : below->op->expressions) {
							predicate_probe += StringUtil::Format(" [left=%llu right=%llu cmp=%s]",
							                                      (unsigned long long)left_cols,
							                                      (unsigned long long)right_cols,
							                                      BoundComparisonExpression::IsComparison(*predicate) ? "y" : "n");
						}
					}
				}
				Printer::Print(StringUtil::Format(
				    "--- cascade(cascades) apply in group %llu: children=%llu join_type=%d condition=%s "
				    "correlated=%llu any=%d delim=%d nulls=%d right=[%s] below=[%s]",
				    (unsigned long long)group, (unsigned long long)expr->children.size(), (int)apply.join_type,
				    apply.condition ? "yes" : "no", (unsigned long long)apply.correlated_columns.size(),
				    (int)apply.any_join, (int)apply.perform_delim, (int)apply.propagate_null_values,
				    right_types.c_str(), right_below.c_str()));
				Printer::Print(StringUtil::Format("--- cascade(cascades)   predicate probe:%s",
				                                  predicate_probe.c_str()));
				// What the Apply itself exposes, captured when the memo was built: the replacement has
				// to match this exactly, and invariant 2 is what says so.
				Printer::Print(StringUtil::Format("--- cascade(cascades)   apply bindings: %s",
				                                  LogicalOperator::ColumnBindingsToString(expr->bindings).c_str()));
				// Demonstrate (and check) the parent lookup the cross-level rewrite needs: the mark
				// column this Apply exposes is read by the expression above it.
				{
					string used_by;
					for (auto parent : memo.ParentsOf(group)) {
						used_by += StringUtil::Format(" %llu", (unsigned long long)parent);
					}
					Printer::Print("--- cascade(cascades)   used by groups:" + used_by);
				}
				// The same thing derived the way MakeExpr would: left side plus the mark column.
				{
					vector<ColumnBinding> derived;
					auto &left_probe_group = memo.GetGroup(expr->children[0]);
					if (!left_probe_group.exprs.empty()) {
						derived = left_probe_group.exprs[0]->bindings;
					}
					derived.emplace_back(apply.mark_index, ProjectionIndex(0));
					for (auto &right_expr : memo.GetGroup(expr->children[1]).exprs) {
						if (right_expr->type != LogicalOperatorType::LOGICAL_PROJECTION) {
							continue;
						}
						Printer::Print(StringUtil::Format(
						    "--- cascade(cascades)   projection table_index=%llu bindings=%s",
						    (unsigned long long)right_expr->op->Cast<LogicalProjection>().table_index.index,
						    LogicalOperator::ColumnBindingsToString(right_expr->bindings).c_str()));
					}
					Printer::Print(StringUtil::Format(
					    "--- cascade(cascades)   derived bindings: %s | mark_index=%llu | match=%s",
					    LogicalOperator::ColumnBindingsToString(derived).c_str(),
					    (unsigned long long)apply.mark_index.index,
					    derived == expr->bindings ? "yes" : "NO"));
				}
			}
		}
	}
	string violation;
	if (!memo.Validate(violation)) {
		// The counters go into the failure. A statement that throws inside Optimize never reaches the
		// summary line, so its work is *missing* from the statistics rather than reported as zero -
		// and reading "no counters" as "the rule never fired" is a mistake that has been made here
		// more than once. An invariant violation caused by a rule is exactly the case where the
		// counters matter most.
		throw InternalException(StringUtil::Format(
		    "cascade(cascades): memo invariant broken: %s | groups=%llu exprs=%llu explored=%llu applied=%llu "
		    "no-effect=%llu rejected=%llu alternatives=%llu enforced=%llu",
		    violation, (unsigned long long)memo.GroupCount(), (unsigned long long)memo.ExprCount(),
		    (unsigned long long)groups_explored, (unsigned long long)rules_applied,
		    (unsigned long long)rules_no_effect, (unsigned long long)rules_rejected,
		    (unsigned long long)expressions_added, (unsigned long long)enforced));
	}
	auto result = memo.ExtractPlan(root);
	// The enforcer: the memo was asked for a decorrelated plan, and if the rules could not provide
	// one, the decorrelation that used to be a pre-pass is applied here instead. This is ORCA's
	// enforcer pattern - a required property no rule satisfies is enforced physically rather than
	// declared impossible - and the counter is the scoreboard for moving the job into rules: every
	// Apply the rules remove makes this number smaller.
	if (PlanHasApply(*result)) {
		enforced++;
		ApplyDecorrelator decorrelator(optimizer_binder, context);
		result = decorrelator.Decorrelate(std::move(result));
		result = SimplifyMarkerJoins(std::move(result));
	}
	// Measured limit of the normalization above, recorded here so the next step does not have to
	// rediscover it: the single-expression condition can be the outer predicate of an EXISTS
	// (`i1.i > 2`), and replacing it with a constant drops that filter - the default path answers
	// `1|false 2|false 3|true NULL|false` for test_correlated_exists and this answers true for every
	// row. The predicate cannot be recovered at this point (JoinCondition's members are private, the
	// single-expression form has no accessor - a compile error settled that). The host keeps the
	// semantics because its own optimizer turns this shape into CTE/delim machinery (`CTE / CTE N /
	// CTE S / CTE I` in its plan), which does not run here: this path replaces the host optimizer.
	// So the normalization is the backstop that turns an unbuildable plan into a plan, and the real
	// fix is earlier - rewrite the no-correlation MARK apply (ORCA's ExfLeftSemiApply2LeftSemiJoin
	// without correlations) while the predicate is still readable.
	NormalizeMarkJoinConditions(*result);
	result->ResolveOperatorTypes();
	if (auto self_test = CascadeConfig::MemoSelfTest()) {
		// The plan is reconstructed first, so the injected corruption cannot affect the answer -
		// what is being tested is the validator, not the query.
		InjectSelfTestFault(self_test);
		string selftest_violation;
		if (memo.Validate(selftest_violation)) {
			throw InternalException("cascade(cascades): selftest injected violation %llu and the memo still validated",
			                        (unsigned long long)self_test);
		}
		// Throw rather than print: the self-test has to exercise the path a real rule bug takes, and
		// that path is where the counters have to appear, because a statement that throws inside
		// Optimize never reaches the summary line - its work is missing from the statistics rather
		// than reported as zero, which is a reading mistake that has been made here more than once.
		throw InternalException(StringUtil::Format(
		    "cascade(cascades) selftest: invariant %llu rejected as expected: %s | applied=%llu alternatives=%llu",
		    (unsigned long long)self_test, selftest_violation, (unsigned long long)rules_applied,
		    (unsigned long long)expressions_added));
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print("--- cascade(cascades) chosen plan:\n" + result->ToString(&context));
	}
	if (CascadeConfig::PrintPlans()) {
		for (idx_t group = 0; group < memo.GroupCount(); group++) {
			auto &data = memo.GetGroup(group);
			OptimizationContext context;
			context.group = group;
			auto winner = memo.WinnerOf(context);
			Printer::Print(StringUtil::Format("--- cascade(cascades) group %llu: exprs=%llu logical=%llu physical=%llu "
			                                  "winner=%s cost=%.2f",
			                                  (unsigned long long)group, (unsigned long long)data.exprs.size(),
			                                  (unsigned long long)data.logical_count,
			                                  (unsigned long long)data.physical_count,
			                                  winner ? EnumUtil::ToString(winner->type).c_str() : "(none)",
			                                  winner ? winner->cost : 0.0));
		}
		Printer::Print(StringUtil::Format("--- cascade(cascades): groups=%llu exprs=%llu physical=%llu | "
		                                  "explored=%llu rules applied=%llu no-effect=%llu rejected=%llu alternatives=%llu enforced=%llu",
		                                  (unsigned long long)memo.GroupCount(), (unsigned long long)memo.ExprCount(),
		                                  (unsigned long long)memo.PhysicalCount(),
		                                  (unsigned long long)groups_explored, (unsigned long long)rules_applied,
		                                  (unsigned long long)rules_no_effect,
		                                  (unsigned long long)rules_rejected,
		                                  (unsigned long long)expressions_added,
		                                  (unsigned long long)enforced));
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
			if (!data.exprs[i]->op) {
				continue;
			}
			if (rule->Kind() == CascadesRuleKind::IMPLEMENTATION) {
				// No physical rules in stage 1: a logical expression is its own chosen plan.
				continue;
			}
			if (!rule->Matches(*data.exprs[i])) {
				continue;
			}
			// The promise is asked once, here, and a NONE promise means the task is never queued:
			// that is the point of ORCA's Exfp() - a rule whose precondition does not hold should
			// cost a check, not a task.
			auto promise = rule->Promise(*this, *data.exprs[i]);
			if (promise == CascadesRulePromise::NONE) {
				if (CascadeConfig::PrintPlans()) {
					Printer::Print("--- cascade(cascades) rule " + string(rule->Name()) +
					               ": promise says the precondition does not hold, not queued");
				}
				rules_rejected++;
				continue;
			}
			CascadesTask task;
			task.kind = CascadesTaskKind::APPLY_RULE;
			task.group = group;
			task.expr = data.exprs[i].get();
			task.rule = rule.get();
			task.promise = promise;
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
	// Deterministic tie-break. "The first one costed wins a tie" makes the winner depend on the
	// order the task queue happened to reach the expressions in, and the same binary then picks
	// different plans between runs - it did, which is what made a failing test file appear to move
	// from one file to another. Ties go to the expression that comes first in its group instead.
	auto index_of = [&](GroupExpr *candidate) -> idx_t {
		if (!candidate) {
			return DConstants::INVALID_INDEX;
		}
		auto &data = memo.GetGroup(group);
		for (idx_t i = 0; i < data.exprs.size(); i++) {
			if (data.exprs[i].get() == candidate) {
				return i;
			}
		}
		return DConstants::INVALID_INDEX;
	};
	bool wins;
	if (!current) {
		wins = true;
	} else if (cost < current->cost) {
		wins = true;
	} else if (cost == current->cost) {
		wins = index_of(&expr) < index_of(current);
	} else {
		wins = false;
	}
	if (CascadeConfig::PrintPlans()) {
		Printer::Print(StringUtil::Format("--- cascade(cascades) candidate: group=%llu type=%s cost=%.2f%s",
		                                  (unsigned long long)group, EnumUtil::ToString(expr.type).c_str(), cost,
		                                  wins ? " <- winner" : ""));
	}
	if (wins) {
		expr.cost = cost;
		memo.SetWinner(context, &expr);
	}
}

double CascadesOptimizer::CostOf(GroupExpr &expr) {
	vector<double> child_costs;
	vector<double> child_rows;
	for (auto child : expr.children) {
		OptimizationContext context;
		context.group = child;
// The required side, read for the first time: what this expression still needs from outside is
// what its inputs need, minus what they resolve themselves. The setter side does not record the
// required side yet, so a narrow lookup that finds nothing falls back to the plain one - the
// behaviour is unchanged, but the property is consulted instead of merely stored.
context.props.outer_refs = expr.outer_refs;
for (auto &provided : expr.provides) {
	context.props.outer_refs.erase(
	    std::remove(context.props.outer_refs.begin(), context.props.outer_refs.end(), provided),
	    context.props.outer_refs.end());
}
auto winner = memo.WinnerOf(context);
if (!winner && !context.props.outer_refs.empty()) {
	context.props.outer_refs.clear();
	winner = memo.WinnerOf(context);
}
		child_costs.push_back(winner ? winner->cost : 0.0);
		// The rows a child produces are what this operator pays to look at.
		child_rows.push_back(winner && winner->rows >= 1 ? winner->rows : 1.0);
	}
	// Every expression has to report its own output rows too, or its parent cannot be costed.
	expr.rows = CostModel::OutputRows(expr, child_rows);
	// The property is derived the same way, bottom-up: no Apply here, and none below.
	expr.decorrelated = expr.type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
	for (auto child : expr.children) {
		OptimizationContext context;
		context.group = child;
		auto winner = memo.WinnerOf(context);
		if (!winner || !winner->decorrelated) {
			expr.decorrelated = false;
			break;
		}
	}
	return CostModel::Cost(expr, child_costs, child_rows);
}

void CascadesOptimizer::ApplyRule(GroupId group, GroupExpr &expr, CascadesRule &rule) {
	// CascadesRule memory (ORCA keeps the same): one application per (rule, group).
	for (auto &entry : rule_memory) {
		if (entry.first == &rule && entry.second == group) {
			return;
		}
	}
	rule_memory.emplace_back(&rule, group);
	// Apply-once (ORCA's IsApplyOnce): on a deep expression a rule can be reached many times, and
	// some rules must only run once per expression or the memo explodes. No rule sets it yet, so
	// the counter below stays zero until one needs it - the mechanism is what is being wired.
	if (rule.ApplyOnce()) {
		for (auto &entry : once_memory) {
			if (entry.first == &rule && entry.second == &expr) {
				rules_skipped++;
				return;
			}
		}
		once_memory.emplace_back(&rule, &expr);
	}
	if (rule.Apply(*this, group, expr)) {
		if (CascadeConfig::PrintPlans()) {
			Printer::Print(StringUtil::Format("--- cascade(cascades) rule applied: %s in group %llu", rule.Name(),
			                                  (unsigned long long)group));
		}
		rules_applied++;
	} else {
		// Matched and applied, but produced nothing (the shape was not there after all).
		rules_no_effect++;
	}
}

void CascadesOptimizer::ReplaceExpression(GroupId group, const GroupExpr *old_expression,
                                         unique_ptr<GroupExpr> replacement) {
	if (!memo.ReplaceExpression(group, old_expression, std::move(replacement))) {
		throw InternalException("cascade(cascades): rule asked to replace an expression that is not in group %llu",
		                        (unsigned long long)group);
	}
	CascadesTask task;
	task.kind = CascadesTaskKind::EXPLORE_GROUP;
	task.group = group;
	task.promise = CascadesRulePromise::MEDIUM;
	tasks.Push(task);
}

void CascadesOptimizer::Reschedule(GroupId group) {
	memo.GetGroup(group).explored = false;
	CascadesTask task;
	task.kind = CascadesTaskKind::EXPLORE_GROUP;
	task.group = group;
	task.promise = CascadesRulePromise::MEDIUM;
	tasks.Push(task);
}

void CascadesOptimizer::InjectSelfTestFault(idx_t which) {
	if (memo.GroupCount() == 0) {
		return;
	}
	auto root = memo.GroupCount() - 1;
	switch (which) {
	case 1: {
		// 1. a child that is not a group: an id past the end of the memo.
		auto op = make_uniq<LogicalFilter>();
		op->estimated_cardinality = 1;
		AddExpression(root, memo.MakeExpr(std::move(op), {memo.GroupCount() + 10}));
		break;
	}
	case 2: {
		// 2. an expression that exposes different columns than the rest of its group.
		vector<unique_ptr<Expression>> select_list;
		select_list.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(1)));
		auto op = make_uniq<LogicalProjection>(TableIndex(999), std::move(select_list));
		op->estimated_cardinality = 1;
		AddExpression(root, memo.MakeExpr(std::move(op), {root}));
		break;
	}
	case 3: {
		// 3. a physical expression whose children do not match the operator's.
		auto op = make_uniq<LogicalFilter>();
		op->estimated_cardinality = 1;
		auto expr = memo.MakeExpr(std::move(op), {root});
		expr->physical = true;
		AddExpression(root, std::move(expr));
		break;
	}
	default: {
		// 4. a costed expression cheaper than the winner, in the winner's own group.
		auto cheaper = memo.MakeExpr(make_uniq<LogicalDummyScan>(TableIndex(0)), {});
		cheaper->rows = 1;
		cheaper->cost = -1;
		AddExpression(root, std::move(cheaper));
		break;
	}
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
