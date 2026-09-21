//===----------------------------------------------------------------------===//
// Git4Data -- the pragmas the feature adds to DuckDB
//===----------------------------------------------------------------------===//
// The feature's pragmas and the helpers they share used to live in pragma_functions.cpp, which is where DuckDB
// keeps its own; increment 3er moved them here so that file is upstream's again (it was 90% this feature's code)
// and so a reader looking for what `git4data_merge` does does not have to find it among the profiling pragmas.
// Upstream's registration calls RegisterGit4DataPragmas, so that one name is all this header has to declare.
//===----------------------------------------------------------------------===//

#ifndef GIT4DATA_PRAGMAS_HPP
#define GIT4DATA_PRAGMAS_HPP

#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

//! Register the feature's own pragmas. Called from PragmaFunctions::RegisterFunction, which registers the
//! engine's - they share one BuiltinFunctions set, and the feature's names all begin with `git4data_`.
void RegisterGit4DataPragmas(BuiltinFunctions &set);


// The pragmas that read the scope a statement is pointed at, and the helpers they share with the file above.
// Every one of these is defined in one of the two pragma files and used by both, so the linkage is external.
DuckTransactionManager &GetGit4DataTransactionManager(ClientContext &context);
string Git4DataSQLLiteral(const string &value);
Git4DataBranch Git4DataRequiredBranch(ClientContext &context, DuckTransactionManager &transaction_manager,
                                      const string &name);
const char *ResolveGit4DataReference(DuckTransactionManager &transaction_manager, const string &name);
string FormatGit4DataAtClause(const string &table_name, const string &reference, const char *unit);
vector<string> Git4DataPrimaryKeyColumnNames(TableCatalogEntry &table);
vector<string> Git4DataAddColumnNames(ClientContext &context, const string &table_name);
string BuildGit4DataRowComparison(ClientContext &context, const Git4DataBranch &branch, const string &left,
                                  const string &right, const vector<string> &key_columns, const char *alias);
string PragmaGit4DataPick(ClientContext &context, const FunctionParameters &parameters);
string PragmaGit4DataApply(ClientContext &context, const FunctionParameters &parameters);
string PragmaGit4DataMerge(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb

#endif
