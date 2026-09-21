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

namespace duckdb {

//! Register the feature's own pragmas. Called from PragmaFunctions::RegisterFunction, which registers the
//! engine's - they share one BuiltinFunctions set, and the feature's names all begin with `git4data_`.
void RegisterGit4DataPragmas(BuiltinFunctions &set);

} // namespace duckdb

#endif
