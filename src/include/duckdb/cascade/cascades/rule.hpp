//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascades/rule.hpp
//
// One expression of the paper's rewrites, as a memo rule.
//
// ORCA's CXform is the model (see CXform.h), and it is *three* kinds, not two:
//
//   Substitution    replace an expression with a simpler equivalent
//                   (ORCA: Project2ComputeScalar, Select2Filter)
//   Exploration     produce a peer alternative for the same group
//                   (ORCA: JoinCommutativity, PushGbBelowJoin, SplitGbAgg)
//   Implementation  logical -> physical
//                   (ORCA: GbAgg2HashAgg, LeftOuterJoin2HashJoin)
//
// Two mechanisms from ORCA come with it, because without them a rule set of any
// size is unusable:
//
//   promise      how promising the rule is on a given expression. `None` means
//                "the precondition does not hold - do not apply it", which is how
//                ORCA avoids matching rules it would then throw away. The rest
//                orders the task queue.
//   apply once   some rules may only be applied once per expression: on a deep
//                pattern tree the number of generated expressions explodes
//                otherwise (ORCA's IsApplyOnce).
//
// The rules themselves live in cascades/rules/, one file each, named after the
// paper rule they implement.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/cascade/cascade_bindings.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

//! A memo group id. The same alias memo.hpp declares: repeated here so that the rule headers stand
//! on their own - a unit test (or anything else that only needs the rule interface) can include one
//! without pulling in the memo, which used to be the only way to get this name.
using GroupId = idx_t;

class GroupExpr;
class Memo;
class CascadesOptimizer;

enum class CascadesRuleKind : uint8_t { SUBSTITUTION, EXPLORATION, IMPLEMENTATION };

//! ORCA's CXform::EXformPromise.
enum class CascadesRulePromise : uint8_t { NONE, LOW, MEDIUM, HIGH };

class CascadesRule {
public:
	CascadesRule(CascadesRuleKind kind, const char *name) : kind(kind), name(name) {
	}
	virtual ~CascadesRule() = default;

	//! Does this rule's pattern match the expression at all? Cheapest check first.
	virtual bool Matches(GroupExpr &expr) = 0;
	//! ORCA's Exfp(): NONE when the precondition does not hold on this expression. It gets the
	//! optimizer because the precondition is rarely visible in the expression alone - whether a
	//! GroupBy sits below the filter is a property of the memo - and ORCA's Exfp() is given the
	//! metadata for the same reason. A NONE promise means the task is never queued.
	virtual CascadesRulePromise Promise(CascadesOptimizer &optimizer, GroupExpr &expr) = 0;
	//! ORCA's IsApplyOnce().
	virtual bool ApplyOnce() const {
		return false;
	}
	//! Produce the replacements. Returns whether it added at least one expression: a rule that
	//! matched, was applied, and declined (or found nothing to do) must not be counted as if it
	//! had rewritten the plan - that distinction is how "the rule ran" is told from "the rule
	//! worked", and the statistics keep them apart.
	virtual bool Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) = 0;

	CascadesRuleKind Kind() const {
		return kind;
	}
	const char *Name() const {
		return name;
	}

private:
	CascadesRuleKind kind;
	const char *name;
};

} // namespace duckdb
