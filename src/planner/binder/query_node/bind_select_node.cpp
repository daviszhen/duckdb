#include <iostream>
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/column_alias_binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/expression_binder/group_binder.hpp"
#include "duckdb/planner/expression_binder/having_binder.hpp"
#include "duckdb/planner/expression_binder/qualify_binder.hpp"
#include "duckdb/planner/expression_binder/order_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"

namespace duckdb {

unique_ptr<Expression> Binder::BindOrderExpression(OrderBinder &order_binder, unique_ptr<ParsedExpression> expr) {
	std::cerr << "+++unique_ptr<Expression> Binder::BindOrderExpression(OrderBinder &order_binder, unique_ptr<ParsedExpression> expr) A| " << expr->ToString() << std::endl;
	// we treat the Distinct list as a order by
	auto bound_expr = order_binder.Bind(std::move(expr));
	if (!bound_expr) {
		// DISTINCT ON non-integer constant
		// remove the expression from the DISTINCT ON list
		return nullptr;
	}
    std::cerr << "unique_ptr<Expression> Binder::BindOrderExpression(OrderBinder &order_binder, unique_ptr<ParsedExpression> expr) B| " << bound_expr->ToString() << std::endl;
	D_ASSERT(bound_expr->type == ExpressionType::BOUND_COLUMN_REF);
	return bound_expr;
}

unique_ptr<Expression> Binder::BindDelimiter(ClientContext &context, OrderBinder &order_binder,
                                             unique_ptr<ParsedExpression> delimiter, const LogicalType &type,
                                             Value &delimiter_value) {
	auto new_binder = Binder::CreateBinder(context, this, true);
	if (delimiter->HasSubquery()) {
		if (!order_binder.HasExtraList()) {
			throw BinderException("Subquery in LIMIT/OFFSET not supported in set operation");
		}
		return order_binder.CreateExtraReference(std::move(delimiter));
	}
	ExpressionBinder expr_binder(*new_binder, context);
	expr_binder.target_type = type;
	auto expr = expr_binder.Bind(delimiter);
	if (expr->IsFoldable()) {
		//! this is a constant
		delimiter_value = ExpressionExecutor::EvaluateScalar(context, *expr).CastAs(context, type);
		return nullptr;
	}
	if (!new_binder->correlated_columns.empty()) {
		throw BinderException("Correlated columns not supported in LIMIT/OFFSET");
	}
	// move any correlated columns to this binder
	MoveCorrelatedExpressions(*new_binder);
	return expr;
}

duckdb::unique_ptr<BoundResultModifier> Binder::BindLimit(OrderBinder &order_binder, LimitModifier &limit_mod) {
	auto result = make_uniq<BoundLimitModifier>();
	if (limit_mod.limit) {
		Value val;
		result->limit = BindDelimiter(context, order_binder, std::move(limit_mod.limit), LogicalType::BIGINT, val);
		if (!result->limit) {
			result->limit_val = val.IsNull() ? NumericLimits<int64_t>::Maximum() : val.GetValue<int64_t>();
			if (result->limit_val < 0) {
				throw BinderException("LIMIT cannot be negative");
			}
		}
	}
	if (limit_mod.offset) {
		Value val;
		result->offset = BindDelimiter(context, order_binder, std::move(limit_mod.offset), LogicalType::BIGINT, val);
		if (!result->offset) {
			result->offset_val = val.IsNull() ? 0 : val.GetValue<int64_t>();
			if (result->offset_val < 0) {
				throw BinderException("OFFSET cannot be negative");
			}
		}
	}
	return std::move(result);
}

unique_ptr<BoundResultModifier> Binder::BindLimitPercent(OrderBinder &order_binder, LimitPercentModifier &limit_mod) {
	auto result = make_uniq<BoundLimitPercentModifier>();
	if (limit_mod.limit) {
		Value val;
		result->limit = BindDelimiter(context, order_binder, std::move(limit_mod.limit), LogicalType::DOUBLE, val);
		if (!result->limit) {
			result->limit_percent = val.IsNull() ? 100 : val.GetValue<double>();
			if (result->limit_percent < 0.0) {
				throw Exception("Limit percentage can't be negative value");
			}
		}
	}
	if (limit_mod.offset) {
		Value val;
		result->offset = BindDelimiter(context, order_binder, std::move(limit_mod.offset), LogicalType::BIGINT, val);
		if (!result->offset) {
			result->offset_val = val.IsNull() ? 0 : val.GetValue<int64_t>();
		}
	}
	return std::move(result);
}

void Binder::BindModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result) {
	std::cerr << "+++void Binder::BindModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result) | " << std::endl;
	for (auto &mod : statement.modifiers) {
		unique_ptr<BoundResultModifier> bound_modifier;
		switch (mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
            std::cerr << "void Binder::BindModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result) |DISTINCT_MODIFIER| " << std::endl;
			auto &distinct = mod->Cast<DistinctModifier>();
			auto bound_distinct = make_uniq<BoundDistinctModifier>();
			bound_distinct->distinct_type =
			    distinct.distinct_on_targets.empty() ? DistinctType::DISTINCT : DistinctType::DISTINCT_ON;
			if (distinct.distinct_on_targets.empty()) {
				for (idx_t i = 0; i < result.names.size(); i++) {
					distinct.distinct_on_targets.push_back(make_uniq<ConstantExpression>(Value::INTEGER(1 + i)));
				}
			}
			for (auto &distinct_on_target : distinct.distinct_on_targets) {
				auto expr = BindOrderExpression(order_binder, std::move(distinct_on_target));
				if (!expr) {
					continue;
				}
				bound_distinct->target_distincts.push_back(std::move(expr));
			}
			bound_modifier = std::move(bound_distinct);
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {
            std::cerr << "void Binder::BindModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result) |ORDER_MODIFIER| " << std::endl;
			auto &order = mod->Cast<OrderModifier>();
			auto bound_order = make_uniq<BoundOrderModifier>();
			auto &config = DBConfig::GetConfig(context);
			D_ASSERT(!order.orders.empty());
			auto &order_binders = order_binder.GetBinders();
			if (order.orders.size() == 1 && order.orders[0].expression->type == ExpressionType::STAR) {
				auto &star = order.orders[0].expression->Cast<StarExpression>();
				if (star.exclude_list.empty() && star.replace_list.empty() && !star.expr) {
					// ORDER BY ALL
					// replace the order list with the all elements in the SELECT list
					auto order_type = order.orders[0].type;
					auto null_order = order.orders[0].null_order;

					vector<OrderByNode> new_orders;
					for (idx_t i = 0; i < order_binder.MaxCount(); i++) {
						new_orders.emplace_back(order_type, null_order,
						                        make_uniq<ConstantExpression>(Value::INTEGER(i + 1)));
					}
					order.orders = std::move(new_orders);
				}
			}
			for (auto &order_node : order.orders) {
                std::cerr << "void Binder::BindModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result) |ORDER_MODIFIER| A | " << order_node.ToString() << std::endl;
				vector<unique_ptr<ParsedExpression>> order_list;
				order_binders[0]->ExpandStarExpression(std::move(order_node.expression), order_list);

				auto type = config.ResolveOrder(order_node.type);
				auto null_order = config.ResolveNullOrder(type, order_node.null_order);
				for (auto &order_expr : order_list) {
                    std::cerr << "void Binder::BindModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result) |ORDER_MODIFIER| B1 | " << order_expr->ToString() << std::endl;
					auto bound_expr = BindOrderExpression(order_binder, std::move(order_expr));
					if (!bound_expr) {
						continue;
					}
                    std::cerr << "void Binder::BindModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result) |ORDER_MODIFIER| B2 | " << bound_expr->ToString() << std::endl;
					bound_order->orders.emplace_back(type, null_order, std::move(bound_expr));
				}
			}
			if (!bound_order->orders.empty()) {
				bound_modifier = std::move(bound_order);
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER:
			bound_modifier = BindLimit(order_binder, mod->Cast<LimitModifier>());
			break;
		case ResultModifierType::LIMIT_PERCENT_MODIFIER:
			bound_modifier = BindLimitPercent(order_binder, mod->Cast<LimitPercentModifier>());
			break;
		default:
			throw Exception("Unsupported result modifier");
		}
		if (bound_modifier) {
			result.modifiers.push_back(std::move(bound_modifier));
		}
	}
}

static void AssignReturnType(unique_ptr<Expression> &expr, const vector<LogicalType> &sql_types) {
	if (!expr) {
		return;
	}
	if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
		return;
	}
	auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
	bound_colref.return_type = sql_types[bound_colref.binding.column_index];
}

void Binder::BindModifierTypes(BoundQueryNode &result, const vector<LogicalType> &sql_types, idx_t projection_index) {
	std::cerr << "+++void Binder::BindModifierTypes(BoundQueryNode &result, const vector<LogicalType> &sql_types, idx_t projection_index) " <<
	    projection_index << std::endl;
	for (auto &bound_mod : result.modifiers) {
		switch (bound_mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct = bound_mod->Cast<BoundDistinctModifier>();
			D_ASSERT(!distinct.target_distincts.empty());
			// set types of distinct targets
			for (auto &expr : distinct.target_distincts) {
				D_ASSERT(expr->type == ExpressionType::BOUND_COLUMN_REF);
				auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
				if (bound_colref.binding.column_index == DConstants::INVALID_INDEX) {
					throw BinderException("Ambiguous name in DISTINCT ON!");
				}
				D_ASSERT(bound_colref.binding.column_index < sql_types.size());
				bound_colref.return_type = sql_types[bound_colref.binding.column_index];
                std::cerr << "void Binder::BindModifierTypes(BoundQueryNode &result, const vector<LogicalType> &sql_types, idx_t projection_index)|DISTINCT_MODIFIER| " <<
				    bound_colref.ToString() << std::endl;
			}
			for (auto &target_distinct : distinct.target_distincts) {
				auto &bound_colref = target_distinct->Cast<BoundColumnRefExpression>();
				const auto &sql_type = sql_types[bound_colref.binding.column_index];
				if (sql_type.id() == LogicalTypeId::VARCHAR) {
					target_distinct = ExpressionBinder::PushCollation(context, std::move(target_distinct),
					                                                  StringType::GetCollation(sql_type), true);
				}
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
            std::cerr << "void Binder::BindModifierTypes(BoundQueryNode &result, const vector<LogicalType> &sql_types, idx_t projection_index)|LIMIT_MODIFIER| " << std::endl;
            auto &limit = bound_mod->Cast<BoundLimitModifier>();
			AssignReturnType(limit.limit, sql_types);
			AssignReturnType(limit.offset, sql_types);
			break;
		}
		case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
            std::cerr << "void Binder::BindModifierTypes(BoundQueryNode &result, const vector<LogicalType> &sql_types, idx_t projection_index)|LIMIT_PERCENT_MODIFIER| " << std::endl;
			auto &limit = bound_mod->Cast<BoundLimitPercentModifier>();
			AssignReturnType(limit.limit, sql_types);
			AssignReturnType(limit.offset, sql_types);
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order = bound_mod->Cast<BoundOrderModifier>();
			for (auto &order_node : order.orders) {
				auto &expr = order_node.expression;
				D_ASSERT(expr->type == ExpressionType::BOUND_COLUMN_REF);
				auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
				if (bound_colref.binding.column_index == DConstants::INVALID_INDEX) {
					throw BinderException("Ambiguous name in ORDER BY!");
				}
				D_ASSERT(bound_colref.binding.column_index < sql_types.size());
				const auto &sql_type = sql_types[bound_colref.binding.column_index];
				bound_colref.return_type = sql_types[bound_colref.binding.column_index];
                std::cerr << "void Binder::BindModifierTypes(BoundQueryNode &result, const vector<LogicalType> &sql_types, idx_t projection_index)|ORDER_MODIFIER| " <<
                          bound_colref.ToString() << std::endl;
				if (sql_type.id() == LogicalTypeId::VARCHAR) {
					order_node.expression = ExpressionBinder::PushCollation(context, std::move(order_node.expression),
					                                                        StringType::GetCollation(sql_type));
				}
			}
			break;
		}
		default:
			break;
		}
	}
}

unique_ptr<BoundQueryNode> Binder::BindNode(SelectNode &statement) {
	std::cerr << "+++unique_ptr<BoundQueryNode> Binder::BindNode(SelectNode &statement) " << std::endl;
	D_ASSERT(statement.from_table);
	// first bind the FROM table statement
	auto from = std::move(statement.from_table);
	std::cerr << "Binder::BindNode.from " << from->ToString() << std::endl;
	auto from_table = Bind(*from);
	std::cerr << "Binder::BindNode.select_node " << std::endl;
	return BindSelectNode(statement, std::move(from_table));
}

void Binder::BindWhereStarExpression(unique_ptr<ParsedExpression> &expr) {
	std::cerr << "+++void Binder::BindWhereStarExpression(unique_ptr<ParsedExpression> &expr) A | " << expr->ToString() << std::endl;
	// expand any expressions in the upper AND recursively
	if (expr->type == ExpressionType::CONJUNCTION_AND) {
		auto &conj = expr->Cast<ConjunctionExpression>();
		for (auto &child : conj.children) {
			BindWhereStarExpression(child);
		}
		return;
	}
	if (expr->type == ExpressionType::STAR) {
		auto &star = expr->Cast<StarExpression>();
		if (!star.columns) {
			throw ParserException("STAR expression is not allowed in the WHERE clause. Use COLUMNS(*) instead.");
		}
	}
	// expand the stars for this expression
	vector<unique_ptr<ParsedExpression>> new_conditions;
    std::cerr << "void Binder::BindWhereStarExpression(unique_ptr<ParsedExpression> &expr) B | " << expr->ToString() << std::endl;
	ExpandStarExpression(std::move(expr), new_conditions);

	// set up an AND conjunction between the expanded conditions
	expr = std::move(new_conditions[0]);
	for (idx_t i = 1; i < new_conditions.size(); i++) {
		auto and_conj = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(expr),
		                                                 std::move(new_conditions[i]));
		expr = std::move(and_conj);
	}
    std::cerr << "void Binder::BindWhereStarExpression(unique_ptr<ParsedExpression> &expr) C | " << expr->ToString() << std::endl;
}

unique_ptr<BoundQueryNode> Binder::BindSelectNode(SelectNode &statement, unique_ptr<BoundTableRef> from_table) {
	D_ASSERT(from_table);
	D_ASSERT(!statement.from_table);
	std::cerr << "++unique_ptr<BoundQueryNode> Binder::BindSelectNode(SelectNode &statement, unique_ptr<BoundTableRef> from_table) " << statement.ToString() << std::endl;
	auto result = make_uniq<BoundSelectNode>();
	result->projection_index = GenerateTableIndex();
	result->group_index = GenerateTableIndex();
	result->aggregate_index = GenerateTableIndex();
	result->groupings_index = GenerateTableIndex();
	result->window_index = GenerateTableIndex();
	result->prune_index = GenerateTableIndex();

    std::cerr << "projection_index " << result->projection_index
              << " group_index " << result->group_index
              << " aggregate_index " << result->aggregate_index
              << " groupings_index " << result->groupings_index
              << " window_index " << result->window_index
              << " prune_index " << result->prune_index << std::endl;

	std::cerr << "Binder::BindSelectNode.set|result.from_table" << std::endl;
    result->from_table = std::move(from_table);
	// bind the sample clause
	if (statement.sample) {
		result->sample_options = std::move(statement.sample);
	}

	// visit the select list and expand any "*" statements
	vector<unique_ptr<ParsedExpression>> new_select_list;
	ExpandStarExpressions(statement.select_list, new_select_list);

	if (new_select_list.empty()) {
		throw BinderException("SELECT list is empty after resolving * expressions!");
	}

    std::cerr << "Binder::BindSelectNode.set(replace)|statement.select_list" << std::endl;
	statement.select_list = std::move(new_select_list);
    for (idx_t i = 0; i < statement.select_list.size(); i++){
        std::cerr << "Binder::BindSelectNode.select_list " << i << " , " << statement.select_list[i]->ToString() << std::endl;
    }

	// create a mapping of (alias -> index) and a mapping of (Expression -> index) for the SELECT list
	case_insensitive_map_t<idx_t> alias_map;
	parsed_expression_map_t<idx_t> projection_map;
	for (idx_t i = 0; i < statement.select_list.size(); i++) {
		auto &expr = statement.select_list[i];
        std::cerr << "Binder::BindSelectNode.set|names " << i << " "<< expr->ToString() << " : " << expr->GetName()  << std::endl;
		result->names.push_back(expr->GetName());
		ExpressionBinder::QualifyColumnNames(*this, expr);
		if (!expr->alias.empty()) {
            std::cerr << "Binder::BindSelectNode.set|alias_map " << expr->alias << " -> " << i << std::endl;
			alias_map[expr->alias] = i;
            std::cerr << "Binder::BindSelectNode.set|names.alias " << i << " " << result->names[i] << " replaced by " << expr->alias << std::endl;
			result->names[i] = expr->alias;
		}
		projection_map[*expr] = i;
        std::cerr << "Binder::BindSelectNode.set|original_expressions " << i << " " << expr->ToString() << std::endl;
		result->original_expressions.push_back(expr->Copy());
	}
    std::cerr << "Binder::BindSelectNode.set|column_count " << statement.select_list.size() << std::endl;
	result->column_count = statement.select_list.size();

	// first visit the WHERE clause
	// the WHERE clause happens before the GROUP BY, PROJECTION or HAVING clauses
	if (statement.where_clause) {
		// bind any star expressions in the WHERE clause
        std::cerr << "Binder::BindSelectNode.BindWhereStarExpression | " << statement.where_clause->ToString() << std::endl;
		BindWhereStarExpression(statement.where_clause);

		ColumnAliasBinder alias_binder(*result, alias_map);
		WhereBinder where_binder(*this, context, &alias_binder);//看上去where可以引用投影列别名
		unique_ptr<ParsedExpression> condition = std::move(statement.where_clause);
        std::cerr << "Binder::BindSelectNode|where_binder.Bind A | "<< condition->ToString() << std::endl;
		result->where_clause = where_binder.Bind(condition);
        std::cerr << "Binder::BindSelectNode|where_binder.Bind B | "<< result->where_clause->ToString() << std::endl;
	}

    std::cerr << "Binder::BindSelectNode|order_binder " << std::endl;
	// now bind all the result modifiers; including DISTINCT and ORDER BY targets
	OrderBinder order_binder({this}, result->projection_index, statement, alias_map, projection_map);
	BindModifiers(order_binder, statement, *result);

	vector<unique_ptr<ParsedExpression>> unbound_groups;
	BoundGroupInformation info;
	auto &group_expressions = statement.groups.group_expressions;
    std::cerr << "Binder::BindSelectNode|group_expressions " << std::endl;
	if (!group_expressions.empty()) {
		// the statement has a GROUP BY clause, bind it
		unbound_groups.resize(group_expressions.size());
		GroupBinder group_binder(*this, context, statement, result->group_index, alias_map, info.alias_map);
		for (idx_t i = 0; i < group_expressions.size(); i++) {
            std::cerr << "Binder::BindSelectNode|group_expressions|A| " << i << " " << group_expressions[i]->ToString() << std::endl;
			// we keep a copy of the unbound expression;
			// we keep the unbound copy around to check for group references in the SELECT and HAVING clause
			// the reason we want the unbound copy is because we want to figure out whether an expression
			// is a group reference BEFORE binding in the SELECT/HAVING binder
			group_binder.unbound_expression = group_expressions[i]->Copy();
			group_binder.bind_index = i;

			// bind the groups
			LogicalType group_type;
            std::cerr << "Binder::BindSelectNode|group_expressions|B| " << std::endl;
			auto bound_expr = group_binder.Bind(group_expressions[i], &group_type);
            std::cerr << "Binder::BindSelectNode|group_expressions|C| " << bound_expr->ToString() << std::endl;
			D_ASSERT(bound_expr->return_type.id() != LogicalTypeId::INVALID);

			// push a potential collation, if necessary
			bound_expr = ExpressionBinder::PushCollation(context, std::move(bound_expr),
			                                             StringType::GetCollation(group_type), true);
            std::cerr << "Binder::BindSelectNode|group_expressions|D| " << bound_expr->ToString() << std::endl;
			result->groups.group_expressions.push_back(std::move(bound_expr));

			// in the unbound expression we DO bind the table names of any ColumnRefs
			// we do this to make sure that "table.a" and "a" are treated the same
			// if we wouldn't do this then (SELECT test.a FROM test GROUP BY a) would not work because "test.a" <> "a"
			// hence we convert "a" -> "test.a" in the unbound expression
			unbound_groups[i] = std::move(group_binder.unbound_expression);
            std::cerr << "Binder::BindSelectNode|group_expressions|E| " << std::endl;
			ExpressionBinder::QualifyColumnNames(*this, unbound_groups[i]);
            std::cerr << "Binder::BindSelectNode|group_expressions|F| " << unbound_groups[i]->ToString() << " , " << i << std::endl;
			info.map[*unbound_groups[i]] = i;
		}
	}
	std::cerr << "Binder::BindSelectNode|show_group_info|A| " << std::endl;
	for (auto & itx : info.map) {
        std::cerr << "Binder::BindSelectNode|show_group_info|A| " << itx.first.get().ToString() << " , " << itx.second << std::endl;
	}
    std::cerr << "Binder::BindSelectNode|show_group_info|B| " << std::endl;
    for (const auto& itx : info.alias_map) {
        std::cerr << "Binder::BindSelectNode|show_group_info|B| " << itx.first << " , " << itx.second << std::endl;
    }
    std::cerr << "Binder::BindSelectNode.set|groups.grouping_sets" << std::endl;
	result->groups.grouping_sets = std::move(statement.groups.grouping_sets);

	// bind the HAVING clause, if any
	if (statement.having) {
		HavingBinder having_binder(*this, context, *result, info, alias_map, statement.aggregate_handling);
		ExpressionBinder::QualifyColumnNames(*this, statement.having);
        std::cerr << "Binder::BindSelectNode.set|having" << std::endl;
		result->having = having_binder.Bind(statement.having);
	}

	// bind the QUALIFY clause, if any
	if (statement.qualify) {
		if (statement.aggregate_handling == AggregateHandling::FORCE_AGGREGATES) {
			throw BinderException("Combining QUALIFY with GROUP BY ALL is not supported yet");
		}
		QualifyBinder qualify_binder(*this, context, *result, info, alias_map);
		ExpressionBinder::QualifyColumnNames(*this, statement.qualify);
		result->qualify = qualify_binder.Bind(statement.qualify);
		if (qualify_binder.HasBoundColumns() && qualify_binder.BoundAggregates()) {
			throw BinderException("Cannot mix aggregates with non-aggregated columns!");
		}
	}

	// after that, we bind to the SELECT list
	SelectBinder select_binder(*this, context, *result, info, alias_map);
	vector<LogicalType> internal_sql_types;
	vector<idx_t> group_by_all_indexes;
	vector<string> new_names;
	std::cerr << "Binder::BindSelectNode|select_list| " << std::endl;
	for (idx_t i = 0; i < statement.select_list.size(); i++) {
		bool is_window = statement.select_list[i]->IsWindow();
		idx_t unnest_count = result->unnests.size();
		LogicalType result_type;
		std::cerr << "Binder::BindSelectNode|select_list|A| " << i << " " <<  statement.select_list[i]->ToString() << std::endl;
		auto expr = select_binder.Bind(statement.select_list[i], &result_type, true);
		bool is_original_column = i < result->column_count;
		bool can_group_by_all =
		    statement.aggregate_handling == AggregateHandling::FORCE_AGGREGATES && is_original_column;
		std::cerr << "Binder::BindSelectNode|select_list|B| " << expr->ToString() << " is_original_column " << is_original_column << " can_group_by_all " << can_group_by_all << std::endl;
		if (select_binder.HasExpandedExpressions()) {
			if (!is_original_column) {
				throw InternalException("Only original columns can have expanded expressions");
			}
			if (statement.aggregate_handling == AggregateHandling::FORCE_AGGREGATES) {
				throw BinderException("UNNEST of struct cannot be combined with GROUP BY ALL");
			}
			auto &struct_expressions = select_binder.ExpandedExpressions();
			D_ASSERT(!struct_expressions.empty());
			for (auto &struct_expr : struct_expressions) {
				new_names.push_back(struct_expr->GetName());
                std::cerr << "Binder::BindSelectNode|select_list|C| " << struct_expr->GetName() << " " << struct_expr->ToString() << std::endl;
				result->types.push_back(struct_expr->return_type);
				result->select_list.push_back(std::move(struct_expr));
			}
			struct_expressions.clear();
			continue;
		}
		if (can_group_by_all && select_binder.HasBoundColumns()) {
			if (select_binder.BoundAggregates()) {
				throw BinderException("Cannot mix aggregates with non-aggregated columns!");
			}
			if (is_window) {
				throw BinderException("Cannot group on a window clause");
			}
			if (result->unnests.size() > unnest_count) {
				throw BinderException("Cannot group on an UNNEST or UNLIST clause");
			}
            std::cerr << "Binder::BindSelectNode|select_list|D|group_by_all| " << i << std::endl;
			// we are forcing aggregates, and the node has columns bound
			// this entry becomes a group
			group_by_all_indexes.push_back(i);
		}
        std::cerr << "Binder::BindSelectNode|select_list|E| " << i  << " " << expr->ToString() << std::endl;
		result->select_list.push_back(std::move(expr));
		if (is_original_column) {
            std::cerr << "Binder::BindSelectNode|select_list|F| " << i << "  " << result->names[i] << " " << result_type.ToString() << std::endl;
			new_names.push_back(std::move(result->names[i]));
			result->types.push_back(result_type);
		}
        std::cerr << "Binder::BindSelectNode|select_list|G|internal_sql_types| " << result_type.ToString() << std::endl;
		internal_sql_types.push_back(result_type);
		if (can_group_by_all) {
			select_binder.ResetBindings();
		}
	}
    std::cerr << "Binder::BindSelectNode|select_list|group_by_all_indexes " << std::endl;
	// push the GROUP BY ALL expressions into the group set
	for (auto &group_by_all_index : group_by_all_indexes) {
        std::cerr << "Binder::BindSelectNode|select_list|group_by_all_indexes|A| " << std::endl;
		auto &expr = result->select_list[group_by_all_index];
		auto group_ref = make_uniq<BoundColumnRefExpression>(
		    expr->return_type, ColumnBinding(result->group_index, result->groups.group_expressions.size()));
        std::cerr << "Binder::BindSelectNode|select_list|group_by_all_indexes|B| " << group_ref->ToString() << std::endl;
		result->groups.group_expressions.push_back(std::move(expr));
		expr = std::move(group_ref);
	}
    std::cerr << "Binder::BindSelectNode.set|column_count " << std::endl;
	result->column_count = new_names.size();
    std::cerr << "Binder::BindSelectNode.set|names " << std::endl;
	result->names = std::move(new_names);
	for (auto name : result->names) {
        std::cerr << "Binder::BindSelectNode.set|names| "<< name << std::endl;
	}
	result->need_prune = result->select_list.size() > result->column_count;
    std::cerr << "Binder::BindSelectNode.set|need_prune| " << result->need_prune << std::endl;

	// in the normal select binder, we bind columns as if there is no aggregation
	// i.e. in the query [SELECT i, SUM(i) FROM integers;] the "i" will be bound as a normal column
	// since we have an aggregation, we need to either (1) throw an error, or (2) wrap the column in a FIRST() aggregate
	// we choose the former one [CONTROVERSIAL: this is the PostgreSQL behavior]
	if (!result->groups.group_expressions.empty() || !result->aggregates.empty() || statement.having ||
	    !result->groups.grouping_sets.empty()) {
		if (statement.aggregate_handling == AggregateHandling::NO_AGGREGATES_ALLOWED) {
			throw BinderException("Aggregates cannot be present in a Project relation!");
		} else if (select_binder.HasBoundColumns()) {
			auto &bound_columns = select_binder.GetBoundColumns();
			string error;
			error = "column \"%s\" must appear in the GROUP BY clause or must be part of an aggregate function.";
			if (statement.aggregate_handling == AggregateHandling::FORCE_AGGREGATES) {
				error += "\nGROUP BY ALL will only group entries in the SELECT list. Add it to the SELECT list or "
				         "GROUP BY this entry explicitly.";
			} else {
				error += "\nEither add it to the GROUP BY list, or use \"ANY_VALUE(%s)\" if the exact value of \"%s\" "
				         "is not important.";
			}
			throw BinderException(FormatError(bound_columns[0].query_location, error, bound_columns[0].name,
			                                  bound_columns[0].name, bound_columns[0].name));
		}
	}

	// QUALIFY clause requires at least one window function to be specified in at least one of the SELECT column list or
	// the filter predicate of the QUALIFY clause
	if (statement.qualify && result->windows.empty()) {
		throw BinderException("at least one window function must appear in the SELECT column or QUALIFY clause");
	}

	// now that the SELECT list is bound, we set the types of DISTINCT/ORDER BY expressions
	BindModifierTypes(*result, internal_sql_types, result->projection_index);
	return std::move(result);
}

} // namespace duckdb
