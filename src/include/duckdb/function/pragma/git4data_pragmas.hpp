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
void Git4DataAddColumnNames(ClientContext &context, const string &table_name,
                            case_insensitive_set_t &names);
string Git4DataEscapedList(const vector<string> &names);
string BuildGit4DataRowComparison(TableCatalogEntry &table, const string &left_alias,
                                  const string &right_alias);
vector<string> Git4DataStoredSourceNames(ClientContext &context, const Git4DataBranch &branch,
                                           const string &key, idx_t source_oid, const string &record_name);
string PragmaGit4DataPick(ClientContext &context, const FunctionParameters &parameters);
string PragmaGit4DataApply(ClientContext &context, const FunctionParameters &parameters);
string PragmaGit4DataMerge(ClientContext &context, const FunctionParameters &parameters);

// The mapping pragmas, defined in git4data_pragma_mappings.cpp and registered here.
string PragmaGit4DataMaterialize(ClientContext &context, const FunctionParameters &parameters);
string PragmaGit4DataUnbind(ClientContext &context, const FunctionParameters &parameters);
string PragmaGit4DataBindBranchTable(ClientContext &context, const FunctionParameters &parameters);
void PragmaGit4DataBindInternal(ClientContext &context, const FunctionParameters &parameters);
string PragmaGit4DataBindBranchDelta(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb

#endif
