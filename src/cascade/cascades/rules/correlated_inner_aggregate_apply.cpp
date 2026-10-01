#include "duckdb/cascade/cascades/rules/correlated_inner_aggregate_apply.hpp"

#include "duckdb/cascade/cascade_bindings.hpp"
#include "duckdb/cascade/cascade_config.hpp"
#include "duckdb/cascade/cascade_correlation.hpp"
#include "duckdb/cascade/cascades/memo.hpp"
#include "duckdb/cascade/cascades/search.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

CorrelatedInnerAggregateApply::CorrelatedInnerAggregateApply()
    : CascadesRule(CascadesRuleKind::SUBSTITUTION, "correlated_inner_aggregate_apply") {
}

//! The Apply itself, not its consumer: matching the consumer was measured to be unreachable (the
//! rule was never given a group whose child holds the Apply), while matching the Apply reaches the
//! shapes. CorrelatedApplyToJoin matches the Apply for the same reason.
bool CorrelatedInnerAggregateApply::Matches(GroupExpr &expr) {
	return expr.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && expr.children.size() == 2;
}

namespace {

//! The one-row case: the body is a projection over a dummy scan, so it contributes exactly one row
//! per outer row and every expression it computes is a function of the outer row. Those expressions
//! can therefore be computed where the Apply's columns are read, and the Apply disappears.
struct OneRowBody {
	GroupExpr *body = nullptr;
	LogicalProjection *projection = nullptr;
	//! The body's own bindings for the columns the binder materialised -> the outer bindings.
	BindingExport export_map;
};

void CollectBindings(const Expression &expr, vector<ColumnBinding> &bindings) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		bindings.push_back(expr.Cast<BoundColumnRefExpression>().Binding());
		return;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) { CollectBindings(child, bindings); });
}

bool Contains(const vector<ColumnBinding> &bindings, const ColumnBinding &binding) {
	for (auto &candidate : bindings) {
		if (candidate == binding) {
			return true;
		}
	}
	return false;
}

bool DescribeOneRow(CascadesOptimizer &optimizer, GroupExpr &expr, OneRowBody &shape, string &reason) {
	if (!expr.op) {
		reason = "the Apply has no operator";
		return false;
	}
	auto &apply = expr.op->Cast<LogicalDependentJoin>();
	if (apply.join_type != JoinType::INNER) {
		reason = "the Apply is not INNER: join_type=" + EnumUtil::ToString(apply.join_type);
		return false;
	}
	if (apply.condition) {
		reason = "the Apply carries an ON condition";
		return false;
	}
	if (apply.correlated_columns.empty()) {
		reason = "the Apply has no correlated column";
		return false;
	}
	auto &memo = optimizer.GetMemo();
	auto &right_group = memo.GetGroup(expr.children[1]);
	string body_types;
	for (auto &candidate : right_group.exprs) {
		body_types += (body_types.empty() ? "" : ",") + EnumUtil::ToString(candidate->type);
		if (candidate->type != LogicalOperatorType::LOGICAL_PROJECTION || !candidate->op ||
		    candidate->children.size() != 1) {
			continue;
		}
		auto &projection = candidate->op->Cast<LogicalProjection>();
		if (projection.expressions.empty()) {
			continue;
		}
		// Its source has to be a dummy scan: that is what makes the body exactly one row and keeps
		// the expressions free of any table the sub-query scans itself.
		bool one_row = false;
		for (auto &inner : memo.GetGroup(candidate->children[0]).exprs) {
			if (inner->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
				one_row = true;
				break;
			}
		}
		if (!one_row) {
			continue;
		}
		// The expressions may only name the outer columns, and must not be volatile: inlining one
		// changes how often and where it is evaluated.
		vector<ColumnBinding> allowed;
		for (auto &column : apply.correlated_columns) {
			allowed.push_back(column.binding);
		}
		bool usable = true;
		for (auto &expression : projection.expressions) {
			if (expression->IsVolatile()) {
				reason = "a body expression is volatile, so inlining it would change its evaluation";
				usable = false;
				break;
			}
			vector<ColumnBinding> references;
			CollectBindings(*expression, references);
			for (auto &binding : references) {
				if (!Contains(allowed, binding)) {
					reason = "a body expression names a column that is not an outer column";
					usable = false;
					break;
				}
			}
			if (!usable) {
				break;
			}
		}
		if (!usable) {
			return false;
		}
		shape.body = candidate.get();
		shape.projection = &projection;
		return true;
	}
	reason = "the Apply's right side is not a one-row projection (it holds: " + body_types + ")";
	return false;
}

} // namespace

CascadesRulePromise CorrelatedInnerAggregateApply::Promise(CascadesOptimizer &optimizer, GroupExpr &expr) {
	OneRowBody shape;
	string reason;
	if (!DescribeOneRow(optimizer, expr, shape, reason)) {
		// ORCA's Exfp(): not applicable, so no task is queued. One reason per clause, so a declined
		// shape can be read off the output instead of guessed at.
		if (CascadeConfig::PrintPlans()) {
			Printer::Print("--- cascade(cascades) rule " + string(Name()) + ": " + reason);
		}
		return CascadesRulePromise::NONE;
	}
	return CascadesRulePromise::MEDIUM;
}

bool CorrelatedInnerAggregateApply::Apply(CascadesOptimizer &optimizer, GroupId group, GroupExpr &expr) {
	OneRowBody shape;
	string reason;
	if (!DescribeOneRow(optimizer, expr, shape, reason)) {
		return false;
	}
	auto &memo = optimizer.GetMemo();
	auto &left_group = memo.GetGroup(expr.children[0]);
	if (left_group.exprs.empty() || !left_group.exprs[0]->op) {
		return false;
	}
	auto left_bindings = left_group.exprs[0]->bindings;
	left_group.exprs[0]->op->ResolveOperatorTypes();
	auto &left_types = left_group.exprs[0]->op->types;
	if (left_types.size() != left_bindings.size()) {
		return false;
	}
	// What the replacement exposes: everything the Apply's left side produces, then the body's own
	// columns. The Apply exposed left + right, so the column set is the same; a projection renumbers
	// its own columns, which is why this goes through ReplaceExpression rather than AddExpression -
	// the parents read what the Apply exposed and are rewritten together with it.
	vector<unique_ptr<Expression>> expressions;
	for (idx_t i = 0; i < left_bindings.size(); i++) {
		expressions.push_back(make_uniq<BoundColumnRefExpression>(left_types[i], left_bindings[i]));
	}
	// The body's expressions already name the outer columns (checked in DescribeOneRow), so they can
	// be used as they are - above the join those bindings resolve.
	for (auto &expression : shape.projection->expressions) {
		expressions.push_back(expression->Copy());
	}
	auto table_index = optimizer.GetBinder().GenerateTableIndex();
	auto replacement = make_uniq<LogicalProjection>(table_index, std::move(expressions));
	replacement->ResolveOperatorTypes();
	optimizer.ReplaceExpression(group, &expr, memo.MakeExpr(std::move(replacement), {expr.children[0]}));
	optimizer.Reschedule(group);
	return true;
}

} // namespace duckdb
