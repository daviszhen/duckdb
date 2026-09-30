//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/task.hpp
//
// The search as tasks rather than recursion - ORCA's CJob family, Columbia's task
// model. A task says "optimise this group for these properties" or "apply this
// rule there"; the queue decides what runs next, so a promising rule can be run
// before an unpromising one and a bound can cut a branch off mid-flight.
//
// The four tasks below are the whole search:
//
//   ExploreGroup   run this group's exploration/substitution rules to a fixed point
//   OptimizeExpr   give one expression a chance to become the group's winner
//   OptimizeInputs its children, one at a time, then come back and cost it
//   ApplyRule      one rule on one expression
//
// References: Graefe 1995 (task-driven search, branch and bound), ORCA's
// CJobQueue/CJobGroupOptimization, and Columbia's OptimizeGroup/Expr/Inputs/ApplyRule.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/cascade/cascades/rule.hpp"

namespace duckdb {

enum class CascadesTaskKind : uint8_t { OPTIMIZE_GROUP, EXPLORE_GROUP, OPTIMIZE_EXPR, OPTIMIZE_INPUTS, APPLY_RULE };

struct CascadesTask {
	CascadesTaskKind kind = CascadesTaskKind::OPTIMIZE_EXPR;
	GroupId group = 0;
	GroupExpr *expr = nullptr;
	//! OPTIMIZE_INPUTS: which child is being scheduled.
	idx_t input = 0;
	//! APPLY_RULE: which rule.
	CascadesRule *rule = nullptr;
	//! Promise of the rule that produced this task; the queue pops the highest first.
	CascadesRulePromise promise = CascadesRulePromise::MEDIUM;
	//! The properties this group has to satisfy for whoever scheduled it - ORCA's optimization
	//! context, carried by the job rather than passed as an argument, because the search descends by
	//! scheduling tasks. A group optimised under two different contexts can have two winners.
	RequiredProperties required;
};

//! A stack, ordered by rule promise. ORCA's CJobQueue is the full version (FIFO/LIFO per
//! priority class, with a limit); this keeps the ordering that matters for now.
class CascadesTaskQueue {
public:
	void Push(CascadesTask task);
	bool Pop(CascadesTask &task);
	bool Empty() const {
		return tasks.empty();
	}
	idx_t Size() const {
		return tasks.size();
	}

private:
	vector<CascadesTask> tasks;
};

} // namespace duckdb
