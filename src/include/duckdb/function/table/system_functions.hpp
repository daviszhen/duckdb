//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/system_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct PragmaCollations {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaTableInfo {
	static void GetColumnInfo(TableCatalogEntry &table, const ColumnDefinition &column, DataChunk &output);

	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaStorageInfo {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaMetadataInfo {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaVersion {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaPlatform {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaDatabaseSize {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBSchemasFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBConnectionCountFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

//! Which registry a reference name is looked up in. A branch and a snapshot are the same kind
//! of thing - a named point on the commit timeline that holds the retention bound down - so the
//! scan is identical for both and only the lookup and the error message differ.
enum class Git4DataRefKind : uint8_t { SNAPSHOT, BRANCH };

//! The name a table has *now*, given the name a mapping was written with and the oid it recorded. A
//! table can be renamed under a mapping, and the oid is what survives that; empty when neither the name
//! nor the oid resolves to a table.
DUCKDB_API string Git4DataResolveTableName(ClientContext &context, const string &name, idx_t oid);

struct Git4DataSnapshotScanFun {
	static void RegisterFunction(BuiltinFunctions &set);
	//! The table function itself, so a binder can put it on a LogicalGet directly
	static TableFunction GetFunction();
	//! Bind data for reading a table as of a pinned reference. Throws when the name is unknown
	//! in the registry the kind selects. Used by the binder for a table reference carrying
	//! AT (SNAPSHOT => 'name') or AT (BRANCH => 'name').
	static unique_ptr<FunctionData> CreateBindData(ClientContext &context, const string &table_name,
	                                               const string &reference_name, Git4DataRefKind kind);
	//! Refuse a read whose key does not resolve back to the table the statement named
	static void CheckReferenceSource(ClientContext &context, TableCatalogEntry &table, const string &key);
	//! Refuse a reference read of a table that the name it was reached by does not resolve to. A binder
	//! has the table the caller named; the read itself only carries a name, so this is where a read of
	//! one table by another table's name can still be caught.
};

//! The rows that differ between two references, as (row_id, change)
struct Git4DataDiffFun {
	static void RegisterFunction(BuiltinFunctions &set);
	//! The table function itself, so it can be registered on its own
	static TableFunction GetFunction();
	//! Bind data for a diff. Throws when either name is neither a snapshot nor a branch.
	static unique_ptr<FunctionData> CreateBindData(ClientContext &context, const string &table_name,
	                                               const string &left_name, const string &right_name);
};

struct DuckDBApproxDatabaseCountFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBColumnsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBConstraintsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBCoordinateSystemsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBSecretsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBWhichSecretFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBDatabasesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBDependenciesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBDialectsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBGrammarExtensionsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBExtensionsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBExtensionRepositoriesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegisterExternalResourceTypeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CreateExternalResourceFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DestroyExternalResourceFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegisterExternalResourceFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DeregisterExternalResourceFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBExternalResourceTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBExternalResourcesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

//! An external resource callback's handle/result column is part of the contract: it must be a MAP. Validate
//! and normalize to MAP(VARCHAR, VARCHAR), so a callback that returns something else gets a clear error
//! instead of crashing when the value is later read or emitted. NULL passes through unchanged.
Value RequireResourceMap(const Value &value, const string &function_name, const string &column);

struct DuckDBPreparedStatementsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBFunctionsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBKeywordsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBLogFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBLogContextFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBIndexesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBMemoryFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBEvictionQueuesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBExternalFileCacheFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBMetricsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBOptimizersFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBSecretTypeParametersFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBSecretTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBSequencesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBTriggersFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBSettingsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBTablesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBTableSample {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBTemporaryFilesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBVariablesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct DuckDBViewsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EnableLoggingFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EnableProfilingFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct TestType {
	TestType(LogicalType type_p, string name_p)
	    : type(std::move(type_p)), name(std::move(name_p)), min_value(Value::MinimumValue(type)),
	      max_value(Value::MaximumValue(type)) {
	}
	TestType(LogicalType type_p, string name_p, Value min, Value max)
	    : type(std::move(type_p)), name(std::move(name_p)), min_value(std::move(min)), max_value(std::move(max)) {
	}

	LogicalType type;
	string name;
	Value min_value;
	Value max_value;
};

struct TestAllTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static vector<TestType> GetTestTypes(bool large_enum = false, bool large_bignum = false);
};

struct TestVectorTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaUserAgent {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
