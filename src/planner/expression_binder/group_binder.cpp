#include "duckdb/planner/expression_binder/group_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

GroupBinder::GroupBinder(Binder &binder, ClientContext &context, SelectNode &node, idx_t group_index,
                         case_insensitive_map_t<idx_t> &alias_map, case_insensitive_map_t<idx_t> &group_alias_map)
    : ExpressionBinder(binder, context), node(node), alias_map(alias_map), group_alias_map(group_alias_map),
      group_index(group_index) {
}

BindResult GroupBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	std::cerr << "+++BindResult GroupBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) A| "<< expr_ptr->ToString() <<" depth " << depth << " root_expression "<< root_expression << std::endl;
	auto &expr = *expr_ptr;
	if (root_expression && depth == 0) {
		switch (expr.expression_class) {
		case ExpressionClass::COLUMN_REF:
            std::cerr << "BindResult GroupBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) B| " << std::endl;
			return BindColumnRef(expr.Cast<ColumnRefExpression>());
		case ExpressionClass::CONSTANT:
            std::cerr << "BindResult GroupBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) C| " << std::endl;
			return BindConstant(expr.Cast<ConstantExpression>());
		case ExpressionClass::PARAMETER:
			throw ParameterNotAllowedException("Parameter not supported in GROUP BY clause");
		default:
			break;
		}
	}
	switch (expr.expression_class) {
	case ExpressionClass::DEFAULT:
		return BindResult("GROUP BY clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("GROUP BY clause cannot contain window functions!");
	default:
        std::cerr << "BindResult GroupBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) D| " << std::endl;
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string GroupBinder::UnsupportedAggregateMessage() {
	return "GROUP BY clause cannot contain aggregates!";
}

BindResult GroupBinder::BindSelectRef(idx_t entry) {
	std::cerr << "+++BindResult GroupBinder::BindSelectRef(idx_t entry) A| " << entry << std::endl;
	if (used_aliases.find(entry) != used_aliases.end()) {
		// the alias has already been bound to before!
		// this happens if we group on the same alias twice
		// e.g. GROUP BY k, k or GROUP BY 1, 1
		// in this case, we can just replace the grouping with a constant since the second grouping has no effect
		// (the constant grouping will be optimized out later)
		std::cerr << "BindResult GroupBinder::BindSelectRef(idx_t entry) B| " << std::endl;
		return BindResult(make_uniq<BoundConstantExpression>(Value::INTEGER(42)));
	}
	if (entry >= node.select_list.size()) {
		throw BinderException("GROUP BY term out of range - should be between 1 and %d", (int)node.select_list.size());
	}
	// we replace the root expression, also replace the unbound expression
	unbound_expression = node.select_list[entry]->Copy();
	// move the expression that this refers to here and bind it
	auto select_entry = std::move(node.select_list[entry]);
	std::cerr << "BindResult GroupBinder::BindSelectRef(idx_t entry) C| " << entry << " , "<< select_entry->ToString() << std::endl;
	auto binding = Bind(select_entry, nullptr, false);
	std::cerr << "BindResult GroupBinder::BindSelectRef(idx_t entry) D| " << to_string(entry) << " , "<< bind_index << " : " << binding->ToString() << std::endl;
	// now replace the original expression in the select list with a reference to this group
	group_alias_map[to_string(entry)] = bind_index;
	std::cerr << "BindResult GroupBinder::BindSelectRef(idx_t entry) E|set.node.select_list| " << entry << " , " << to_string(entry) << std::endl;
	node.select_list[entry] = make_uniq<ColumnRefExpression>(to_string(entry));
	// insert into the set of used aliases
	used_aliases.insert(entry);
	return BindResult(std::move(binding));
}

BindResult GroupBinder::BindConstant(ConstantExpression &constant) {
	std::cerr << "+++BindResult GroupBinder::BindConstant(ConstantExpression &constant) A| " << constant.ToString() << std::endl;
	// constant as root expression
	if (!constant.value.type().IsIntegral()) {
		std::cerr << "BindResult GroupBinder::BindConstant(ConstantExpression &constant) B|!IsIntegral " << std::endl;
		// non-integral expression, we just leave the constant here.
		return ExpressionBinder::BindExpression(constant, 0);
	}
	// INTEGER constant: we use the integer as an index into the select list (e.g. GROUP BY 1)
	std::cerr << "BindResult GroupBinder::BindConstant(ConstantExpression &constant) C| " << std::endl;
	auto index = (idx_t)constant.value.GetValue<int64_t>();
	return BindSelectRef(index - 1);
}

BindResult GroupBinder::BindColumnRef(ColumnRefExpression &colref) {
    std::cerr << "+++BindResult GroupBinder::BindColumnRef(ColumnRefExpression &colref) A| " << colref.ToString() << std::endl;
	// columns in GROUP BY clauses:
	// FIRST refer to the original tables, and
	// THEN if no match is found refer to aliases in the SELECT list
	// THEN if no match is found, refer to outer queries

	// first try to bind to the base columns (original tables)
	auto result = ExpressionBinder::BindExpression(colref, 0);
	if (result.HasError()) {
		if (colref.IsQualified()) {
			// explicit table name: not an alias reference
			return result;
		}
		// failed to bind the column and the node is the root expression with depth = 0
		// check if refers to an alias in the select clause
		auto alias_name = colref.column_names[0];
		auto entry = alias_map.find(alias_name);
		if (entry == alias_map.end()) {
			// no matching alias found
			return result;
		}
		result = BindResult(BindSelectRef(entry->second));
		if (!result.HasError()) {
			group_alias_map[alias_name] = bind_index;
		}
	}
    std::cerr << "BindResult GroupBinder::BindColumnRef(ColumnRefExpression &colref) B| " << result.expression->ToString() << std::endl;
	return result;
}

} // namespace duckdb
