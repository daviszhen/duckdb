// The correlation helpers - see cascade_correlation.hpp. Not a rule of its own: identities
// (3) and (4) of Figure 4 as a framework. A correlated predicate is lifted out of the
// sub-query's body one row-preserving operator at a time, the columns it reads are exposed
// through the projection on top, and it becomes a join condition with the right NULL
// semantics (only an equality may be made NULL-safe; a `<` can never be a hash key).
//
// Paper: Galindo-Legaria & Joshi (SIGMOD 2001), section 2.3.
//===----------------------------------------------------------------------===//

#include "duckdb/cascade/cascade_correlation.hpp"

#include "duckdb/cascade/cascade_bindings.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

//! True if any sub-expression references a column of the correlation domain.
bool ReferencesCorrelation(const Expression &expr, const CorrelatedColumns &correlated) {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    for (idx_t i = 0; i < correlated.size(); i++) {
			    if (correlated[i].binding == colref.Binding()) {
				    found = true;
				    return;
			    }
		    }
	    });
	return found;
}

//! True if any sub-expression reads a column of the sub-query's own body, i.e. a
//! column that is not one of the parameters handed in from the outer query.
bool ReferencesInner(const Expression &expr, const CorrelatedColumns &correlated) {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    for (idx_t i = 0; i < correlated.size(); i++) {
			    if (correlated[i].binding == colref.Binding()) {
				    return;
			    }
		    }
		    found = true;
	    });
	return found;
}

//! Move every column reference one scope closer to the query the sub-query is being
//! spliced into: a reference to the enclosing query is depth 1 while it is inside the
//! sub-query and depth 0 once the sub-query is gone. A depth 0 reference is already
//! local and stays put, and a reference to a further outer query - made by a
//! sub-query nested inside this one - shifts down with it.
void DecrementCorrelationDepth(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		if (colref.Depth() > 0) {
			colref.DepthMutable()--;
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [](Expression &child) { DecrementCorrelationDepth(child); });
}

void DecrementCorrelationDepth(LogicalOperator &op) {
	for (auto &expr : op.expressions) {
		DecrementCorrelationDepth(*expr);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		auto &condition = op.Cast<LogicalDependentJoin>().condition;
		if (condition) {
			DecrementCorrelationDepth(*condition);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			if (!condition.IsComparison()) {
				continue;
			}
			DecrementCorrelationDepth(*condition.LeftReference());
			DecrementCorrelationDepth(*condition.RightReference());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &condition = op.Cast<LogicalAnyJoin>().condition;
		if (condition) {
			DecrementCorrelationDepth(*condition);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// The grouping expressions live beside op.expressions, not in it.
		for (auto &group : op.Cast<LogicalAggregate>().groups) {
			DecrementCorrelationDepth(*group);
		}
		break;
	}
	default:
		break;
	}
	for (auto &child : op.children) {
		DecrementCorrelationDepth(*child);
	}
}

bool ListReferencesCorrelation(const vector<unique_ptr<Expression>> &exprs,
                                      const CorrelatedColumns &correlated) {
	for (auto &expr : exprs) {
		if (ReferencesCorrelation(*expr, correlated)) {
			return true;
		}
	}
	return false;
}

//! True if the sub-tree still mentions a correlated column once the filter
//! predicates above the correlation point have been extracted.
bool SubtreeReferencesCorrelation(const LogicalOperator &op, const CorrelatedColumns &correlated) {
	if (ListReferencesCorrelation(op.expressions, correlated)) {
		return true;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			if (!condition.IsComparison()) {
				// a single-expression condition exposes no accessor: assume the worst
				return true;
			}
			if (ReferencesCorrelation(condition.GetLHS(), correlated) ||
			    ReferencesCorrelation(condition.GetRHS(), correlated)) {
				return true;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &condition = op.Cast<LogicalAnyJoin>().condition;
		if (condition && ReferencesCorrelation(*condition, correlated)) {
			return true;
		}
		break;
	}
	default:
		break;
	}
	for (auto &child : op.children) {
		if (SubtreeReferencesCorrelation(*child, correlated)) {
			return true;
		}
	}
	return false;
}

//! Move correlated predicates out of the filters above the correlation point.
//! Only LogicalFilter and LogicalProjection are looked through: both preserve
//! rows, so a predicate lifted past them keeps its meaning. Any other operator
//! ends the walk, leaving whatever it holds to be reported by the caller.
unique_ptr<LogicalOperator> ExtractCorrelatedPredicates(unique_ptr<LogicalOperator> op,
                                                               const CorrelatedColumns &correlated,
                                                               vector<unique_ptr<Expression>> &extracted) {
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op->Cast<LogicalFilter>();
		filter.SplitPredicates();
		vector<unique_ptr<Expression>> local;
		for (auto &expr : op->expressions) {
			if (ReferencesCorrelation(*expr, correlated)) {
				extracted.push_back(std::move(expr));
			} else {
				local.push_back(std::move(expr));
			}
		}
		op->children[0] = ExtractCorrelatedPredicates(std::move(op->children[0]), correlated, extracted);
		if (local.empty()) {
			// nothing local left: the filter itself disappears
			return std::move(op->children[0]);
		}
		op->expressions = std::move(local);
		return op;
	}
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		op->children[0] = ExtractCorrelatedPredicates(std::move(op->children[0]), correlated, extracted);
		return op;
	}
	return op;
}

//! Columns referenced by the lifted predicates that live on the right side.
vector<NeededColumn> CollectRightColumns(const vector<unique_ptr<Expression>> &predicates,
                                                const CorrelatedColumns &correlated) {
	vector<NeededColumn> result;
	for (auto &predicate : predicates) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *predicate, [&](const BoundColumnRefExpression &colref) {
			    for (idx_t i = 0; i < correlated.size(); i++) {
				    if (correlated[i].binding == colref.Binding()) {
					    return;
				    }
			    }
			    for (auto &entry : result) {
				    if (entry.binding == colref.Binding()) {
					    return;
				    }
			    }
			    LogicalType type = colref.GetReturnType();
			    result.push_back(NeededColumn {colref.Binding(), std::move(type)});
		    });
	}
	return result;
}

//! The right sub-tree must output every column a lifted predicate references,
//! because the join condition is evaluated above it. Append whatever the top
//! projection does not already expose, and report the new bindings.
void ExposeRightColumns(LogicalOperator &right, const vector<NeededColumn> &needed,
                               vector<std::pair<ColumnBinding, ColumnBinding>> &mapping) {
	if (right.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		throw NotImplementedException(
		    "cascade: Apply elimination needs a projection on top of the subquery to expose the "
		    "correlated columns, but found a different operator");
	}
	auto &projection = right.Cast<LogicalProjection>();
	// A column can only be appended to this projection if the projection's own child
	// already produces it. When it does not - the column sits below a second
	// projection - these rules cannot expose it, and saying so beats emitting a plan
	// whose bindings do not resolve.
	if (!right.children.empty()) {
		auto child_bindings = right.children[0]->GetColumnBindings();
		for (auto &col : needed) {
			bool visible = false;
			for (auto &binding : child_bindings) {
				if (binding == col.binding) {
					visible = true;
					break;
				}
			}
			if (!visible) {
				throw NotImplementedException(
				    "cascade: a lifted correlated predicate names a column that the sub-query's own "
				    "projection does not expose");
			}
		}
	}
	// projections that already pass a needed column through keep their binding
	for (idx_t i = 0; i < projection.expressions.size(); i++) {
		auto &expr = projection.expressions[i];
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		for (auto &col : needed) {
			if (col.binding == colref.Binding()) {
				mapping.emplace_back(col.binding, ColumnBinding(projection.table_index, ProjectionIndex(i)));
			}
		}
	}
	for (auto &col : needed) {
		bool exposed = false;
		for (auto &entry : mapping) {
			if (entry.first == col.binding) {
				exposed = true;
				break;
			}
		}
		if (exposed) {
			continue;
		}
		auto position = projection.expressions.size();
		projection.expressions.push_back(make_uniq<BoundColumnRefExpression>(col.type, col.binding));
		mapping.emplace_back(col.binding, ColumnBinding(projection.table_index, ProjectionIndex(position)));
	}
}
//! Identities (3) and (4) of Galindo-Legaria & Joshi as a framework rather than a
//! single step. A correlated predicate travels up through the row-preserving operators
//! of the sub-query's body until it reaches the Apply, where it becomes a join
//! condition:
//!
//!   (3)  R A (sigma_p E) = sigma_p (R A E)          - a filter contributes its own
//!        predicates and disappears when nothing is left of it;
//!   (4)  R A (pi_v E)    = pi_{v + cols(R)}(R A E)  - a projection has to expose
//!        every column those predicates read, so that they keep being evaluable
//!        above it.
//!
//! The walk is bottom-up on purpose. A predicate lifted from *below* a projection is
//! re-expressed through it, which is what flattens a derived table that has a
//! correlated filter inside it - not just a correlated filter at the very top, where
//! one exposure is enough.
unique_ptr<LogicalOperator> LiftCorrelatedPredicates(unique_ptr<LogicalOperator> op,
                                                            const CorrelatedColumns &correlated,
                                                            vector<unique_ptr<Expression>> &pending) {
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		op->children[0] = LiftCorrelatedPredicates(std::move(op->children[0]), correlated, pending);
		if (!pending.empty()) {
			auto needed = CollectRightColumns(pending, correlated);
			vector<std::pair<ColumnBinding, ColumnBinding>> mapping;
			ExposeRightColumns(*op, needed, mapping);
			for (auto &predicate : pending) {
				RewriteExpressionBindings(predicate, mapping);
			}
		}
		return op;
	}
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		op->children[0] = LiftCorrelatedPredicates(std::move(op->children[0]), correlated, pending);
		auto &filter = op->Cast<LogicalFilter>();
		filter.SplitPredicates();
		vector<unique_ptr<Expression>> local;
		for (auto &expr : filter.expressions) {
			if (ReferencesCorrelation(*expr, correlated)) {
				pending.push_back(std::move(expr));
			} else {
				local.push_back(std::move(expr));
			}
		}
		if (local.empty()) {
			// nothing is left to evaluate here: the filter itself disappears
			return std::move(filter.children[0]);
		}
		filter.expressions = std::move(local);
		return op;
	}
	// Anything else - an aggregate, a join, a set operation - does not preserve the
	// bindings a predicate would have to be re-expressed through, so the walk stops.
	// The correlation that is left is reported by the caller.
	return op;
}
//! Turn an extracted correlated predicate into a join condition. Comparisons
//! become proper join conditions; anything else is kept as a single-expression
//! condition, which DuckDB resolves over the combined scope.
//!
//! null_safe upgrades an equality to IS NOT DISTINCT FROM, which is what makes a
//! marker two-valued: with plain equality a NULL on either side leaves the
//! comparison unknown, so a MARK join could hand back NULL.
void AddJoinCondition(LogicalComparisonJoin &join, unique_ptr<Expression> predicate,
                             const CorrelatedColumns &correlated, bool null_safe) {
	if (!BoundComparisonExpression::IsComparison(*predicate) ||
	    predicate->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		join.conditions.emplace_back(std::move(predicate));
		return;
	}
	auto &comparison = predicate->Cast<BoundFunctionExpression>();
	auto lhs = std::move(BoundComparisonExpression::LeftMutable(comparison));
	auto rhs = std::move(BoundComparisonExpression::RightMutable(comparison));
	// The binding resolver resolves a condition's left expression against the
	// left child and its right expression against the right child, so the side
	// that comes from the outer sub-tree has to be written on the left.
	if (!ReferencesCorrelation(*lhs, correlated) && ReferencesCorrelation(*rhs, correlated)) {
		std::swap(lhs, rhs);
		BoundComparisonExpression::FlipType(comparison);
	}
	if (null_safe && comparison.GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
		BoundComparisonExpression::SetType(comparison, ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	}
	join.conditions.emplace_back(std::move(lhs), std::move(rhs), comparison.GetExpressionType());
}

//! Whether the expression names any column of the given side.
bool ApplyReadsBindings(const Expression &expr, const vector<ColumnBinding> &side) {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    expr, [&](const BoundColumnRefExpression &colref) {
		    for (auto &binding : side) {
			    if (binding == colref.Binding()) {
				    found = true;
				    return;
			    }
		    }
	    });
	return found;
}

//! A join condition has to be one comparison. An ON predicate that is a conjunction of
//! comparisons is split; anything else is reported rather than guessed at.
bool CollectComparisons(unique_ptr<Expression> predicate, vector<unique_ptr<Expression>> &result) {
	if (predicate->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    predicate->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = predicate->Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.GetChildrenMutable()) {
			if (!CollectComparisons(std::move(child), result)) {
				return false;
			}
		}
		return true;
	}
	if (!BoundComparisonExpression::IsComparison(*predicate)) {
		return false;
	}
	result.push_back(std::move(predicate));
	return true;
}

} // namespace duckdb
