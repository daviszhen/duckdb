// The runtime switches. The header lists each one together with the paper rule it turns on;
// all of them are off unless set, so the default path is DuckDB's own optimizer untouched.
// They exist so that the cascade optimizer and DuckDB's optimizer can be run over the same
// binary.
//===----------------------------------------------------------------------===//

#include "duckdb/cascade/cascade_config.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"

#include <cstdlib>

namespace duckdb {

static bool EnvFlagSet(const char *name) {
	auto value = std::getenv(name);
	if (!value) {
		return false;
	}
	return value[0] != '\0' && value[0] != '0';
}

bool CascadeConfig::UseCascadeOptimizer() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE");
	return enabled;
}

bool CascadeConfig::UseMemoOptimizer() {
	// The memo is the default optimizer now. The host optimizer is still reachable, but only by asking for
	// it: DUCKDB_HOST_OPTIMIZER=1 selects the fallback explicitly, and nothing selects it by default.
	static const bool enabled = !EnvFlagSet("DUCKDB_HOST_OPTIMIZER");
	return enabled;
}

bool CascadeConfig::MemoRules() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_MEMO_RULES");
	return enabled;
}

idx_t CascadeConfig::MemoSelfTest() {
	auto value = std::getenv("DUCKDB_CASCADE_MEMO_SELFTEST");
	if (!value || value[0] == '\0') {
		return 0;
	}
	auto parsed = std::strtoull(value, nullptr, 10);
	return static_cast<idx_t>(parsed);
}

bool CascadeConfig::KeepApply() {
	// Off unless asked for, which makes it the matrix's configuration rather than the default
	// path's. Measured both ways over the 88 sub-query files: with it off (the default) 84/88 pass
	// with no internal errors, because the host then decorrelates the sub-queries while planning -
	// the same thing it does for the default path, which gets 87/88. With it on the count is 33/88:
	// the applies survive into this pipeline, where the rules can only express part of them. Both
	// numbers are real; they answer different questions. The matrix asserts on this pipeline's own
	// handling, so it runs with the flag on, and the sweep is the default-path number, so it runs
	// with the flag off. Every rule added moves one file from the first group to the second.
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_KEEP_APPLY");
	return enabled;
}

bool CascadeConfig::BuildSegmentApply() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_SEGMENT");
	return enabled;
}

bool CascadeConfig::RunDuckOptimizers() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_OPTIMIZE");
	return enabled;
}

bool CascadeConfig::ReorderGroupBy() {
	static const bool enabled = []() {
		auto value = std::getenv("DUCKDB_CASCADE_REORDER");
		if (!value) {
			return true;
		}
		return value[0] != '\0' && value[0] != '0';
	}();
	return enabled;
}

bool CascadeConfig::ReorderSemijoins() {
	static const bool enabled = []() {
		auto value = std::getenv("DUCKDB_CASCADE_REORDER_SEMIJOIN");
		if (!value) {
			return true;
		}
		return value[0] != '\0' && value[0] != '0';
	}();
	return enabled;
}

bool CascadeConfig::PushLocalAggregates() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_LOCAL_AGG");
	return enabled;
}

bool CascadeConfig::PullUpAggregates() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_AGG_PULLUP");
	return enabled;
}

bool CascadeConfig::PushDownAggregates() {
	static const bool enabled = EnvFlagSet("DUCKDB_CASCADE_AGG_PUSHDOWN");
	return enabled;
}

bool CascadeConfig::IsDeclaredKey(const string &table_name, const string &column_name) {
	static const unordered_set<string> keys = []() {
		unordered_set<string> parsed;
		auto value = std::getenv("DUCKDB_CASCADE_KEYS");
		if (!value) {
			return parsed;
		}
		string current;
		for (const char *letter = value;; letter++) {
			if (*letter == ',' || *letter == '\0') {
				if (!current.empty()) {
					parsed.insert(StringUtil::Lower(current));
				}
				current.clear();
				if (*letter == '\0') {
					break;
				}
				continue;
			}
			current += *letter;
		}
		return parsed;
	}();
	if (keys.empty()) {
		return false;
	}
	return keys.find(StringUtil::Lower(table_name) + "." + StringUtil::Lower(column_name)) != keys.end();
}

bool CascadeConfig::FlattenFallback() {
	static const bool enabled = []() {
		auto value = std::getenv("DUCKDB_CASCADE_FLATTEN_FALLBACK");
		if (!value) {
			return true;
		}
		return value[0] != '\0' && value[0] != '0';
	}();
	return enabled;
}

bool CascadeConfig::PrintPlans() {
	static const bool enabled = []() {
		auto value = std::getenv("DUCKDB_CASCADE_PRINT");
		if (!value) {
			// there is no other way to see the rewrite, so print by default
			return true;
		}
		return value[0] != '\0' && value[0] != '0';
	}();
	return enabled;
}

} // namespace duckdb
