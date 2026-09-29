//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascade_keys.hpp
//
// "Does this side of a join expose a key?" - the question behind every rule that moves an
// operator across a join.
//
// Galindo-Legaria & Joshi state it as a premise rather than a rule: identities (7) to (9)
// "require that R contain a key R.key", pulling a GroupBy above a join needs "the relation
// being joined [to have] a key", and pushing one below needs "the key of the relation S
// [to be] part of the grouping columns". The catalog answers it from a UNIQUE constraint, and
// DUCKDB_CASCADE_KEYS answers it for schemas whose generator declares none (the TPC tools
// emit no primary keys at all, while the benchmarks' own DDL does).
//
// The key has to be *exposed* by the side, not merely to exist: the rules group or compare
// the side's output, so a key column the optimizer projected away cannot separate its rows.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class LogicalOperator;

//! The bindings of one key of `side`, in the side's own output, or nothing when the side is
//! not a base-table scan (under filters) whose table declares a usable unique constraint - or
//! one named by DUCKDB_CASCADE_KEYS - with every one of its columns exposed.
vector<ColumnBinding> CascadeSideKey(LogicalOperator &side);

} // namespace duckdb
