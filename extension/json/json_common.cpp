#include "json_common.hpp"

namespace duckdb {

JSONFunctionData::JSONFunctionData(bool constant, string path_p, idx_t len)
    : constant(constant), path(move(path_p)), len(len) {
}

unique_ptr<FunctionData> JSONFunctionData::Copy() {
	return make_unique<JSONFunctionData>(constant, path, len);
}

unique_ptr<FunctionData> JSONFunctionData::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	bool constant = false;
	string path = "";
	idx_t len = 0;
	if (arguments[1]->return_type.id() != LogicalTypeId::SQLNULL && arguments[1]->IsFoldable()) {
		constant = true;
		// Try to cast to string, so that we can allow integers as arguments (array index)
		auto value = ExpressionExecutor::EvaluateScalar(*arguments[1]);
		if (!value.TryCastAs(LogicalType::VARCHAR)) {
			throw Exception("Cannot JSON path argument to VARCHAR");
		}
		// Get the string
		auto query = value.GetValueUnsafe<string_t>();
		len = query.GetSize();
		auto ptr = query.GetDataUnsafe();
		// Empty strings and invalid $ paths yield an error
		if (len == 0 || (*ptr == '$' && !JSONCommon::ValidPathDollar(ptr, len))) {
			throw Exception("JSON path error");
		}
		// Copy over string to the bind data
		if (*ptr == '/' || *ptr == '$') {
			path = string(ptr, len);
		} else {
			path = "/" + string(ptr, len);
			len++;
		}
	}
	return make_unique<JSONFunctionData>(constant, path, len);
}

//! Some defines copied from yyjson.cpp
#define IDX_T_SAFE_DIG 19
#define IDX_T_MAX      ((idx_t)(~(idx_t)0))

static inline idx_t ReadString(const char *ptr, const char *const end, const bool escaped) {
	const char *const before = ptr;
	if (escaped) {
		while (ptr != end) {
			if (*ptr == '"') {
				break;
			}
			ptr++;
		}
		return ptr == end ? 0 : ptr - before;
	} else {
		while (ptr != end) {
			if (*ptr == '.' || *ptr == '[') {
				break;
			}
			ptr++;
		}
		return ptr - before;
	}
}

static inline idx_t ReadIndex(const char *ptr, const char *const end, idx_t &idx) {
	const char *const before = ptr;
	idx = 0;
	for (idx_t i = 0; i < IDX_T_SAFE_DIG; i++) {
		if (ptr == end) {
			// No closing ']'
			return 0;
		}
		if (*ptr == ']') {
			break;
		}
		uint8_t add = (uint8_t)(*ptr - '0');
		if (add <= 9) {
			idx = add + idx * 10;
		} else {
			// Not a digit
			return 0;
		}
		ptr++;
	}
	// Invalid if overflow
	return idx >= (idx_t)IDX_T_MAX ? 0 : ptr - before;
}

bool JSONCommon::ValidPathDollar(const char *ptr, const idx_t &len) {
	const char *const end = ptr + len;
	// Skip past '$'
	ptr++;
	while (ptr != end) {
		const auto &c = *ptr++;
		if (c == '.') {
			// Object
			bool escaped = false;
			if (*ptr == '"') {
				// Skip past opening '"'
				ptr++;
				escaped = true;
			}
			auto key_len = ReadString(ptr, end, escaped);
			if (key_len == 0) {
				return false;
			}
			ptr += key_len;
			if (escaped) {
				// Skip past closing '"'
				ptr++;
			}
		} else if (c == '[') {
			// Array
			if (*ptr == '#') {
				// Index from back of array
				ptr++;
				if (*ptr == ']') {
					ptr++;
					continue;
				}
				if (*ptr != '-') {
					return false;
				}
				// Skip past '-'
				ptr++;
			}
			idx_t idx;
			auto idx_len = ReadIndex(ptr, end, idx);
			if (idx_len == 0) {
				return false;
			}
			ptr += idx_len;
			// Skip past closing ']'
			ptr++;
		} else {
			return false;
		}
	}
	return true;
}

yyjson_val *JSONCommon::GetPointerDollar(yyjson_val *val, const char *ptr, const idx_t &len) {
	if (len == 1) {
		// Just '$'
		return val;
	}
	const char *const end = ptr + len;
	// Skip past '$'
	ptr++;
	while (val != nullptr && ptr != end) {
		const auto &c = *ptr++;
		if (c == '.') {
			// Object
			if (!yyjson_is_obj(val)) {
				return nullptr;
			}
			bool escaped = false;
			if (*ptr == '"') {
				// Skip past opening '"'
				ptr++;
				escaped = true;
			}
			auto key_len = ReadString(ptr, end, escaped);
			val = yyjson_obj_getn(val, ptr, key_len);
			ptr += key_len;
			if (escaped) {
				// Skip past closing '"'
				ptr++;
			}
		} else if (c == '[') {
			// Array
			if (!yyjson_is_arr(val)) {
				return nullptr;
			}
			bool from_back = false;
			if (*ptr == '#') {
				// Index from back of array
				ptr++;
				if (*ptr == ']') {
					return nullptr;
				}
				from_back = true;
				// Skip past '-'
				ptr++;
			}
			// Read index
			idx_t idx;
			auto idx_len = ReadIndex(ptr, end, idx);
			if (from_back) {
				auto arr_size = yyjson_arr_size(val);
				idx = idx > arr_size ? arr_size : arr_size - idx;
			}
			val = yyjson_arr_get(val, idx);
			ptr += idx_len;
			// Skip past closing ']'
			ptr++;
		} else {
			throw InternalException("Unexpected char when parsing JSON path");
		}
	}
	return val;
}

} // namespace duckdb