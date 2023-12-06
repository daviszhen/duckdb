#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"
#include "duckdb/execution/operator/scan/csv/base_csv_reader.hpp"
#include "duckdb/execution/operator/scan/csv/parse_chunk.hpp"

namespace duckdb {

bool CSVSniffer::TryCastVector(Vector &parse_chunk_col, idx_t size, const LogicalType &sql_type) {
	auto &sniffing_state_machine = best_candidate->GetStateMachineSniff();
	// try vector-cast from string to sql_type
	Vector dummy_result(sql_type);
	if (!sniffing_state_machine.dialect_options.date_format[LogicalTypeId::DATE].GetValue().Empty() &&
	    sql_type == LogicalTypeId::DATE) {
		// use the date format to cast the chunk
		string error_message;
		idx_t line_error;
		return BaseCSVReader::TryCastDateVector(sniffing_state_machine.dialect_options.date_format, parse_chunk_col,
		                                        dummy_result, size, error_message, line_error);
	}
	if (!sniffing_state_machine.dialect_options.date_format[LogicalTypeId::TIMESTAMP].GetValue().Empty() &&
	    sql_type == LogicalTypeId::TIMESTAMP) {
		// use the timestamp format to cast the chunk
		string error_message;
		return BaseCSVReader::TryCastTimestampVector(sniffing_state_machine.dialect_options.date_format,
		                                             parse_chunk_col, dummy_result, size, error_message);
	}
	// target type is not varchar: perform a cast
	string error_message;
	return VectorOperations::DefaultTryCast(parse_chunk_col, dummy_result, size, &error_message, true);
}

void CSVSniffer::RefineTypes() {
	auto &sniffing_state_machine = best_candidate->GetStateMachineSniff();
	// if data types were provided, exit here if number of columns does not match
	detected_types.assign(sniffing_state_machine.dialect_options.num_cols, LogicalType::VARCHAR);
	if (sniffing_state_machine.options.all_varchar) {
		// return all types varchar
		return;
	}
	DataChunk parse_chunk;
	parse_chunk.Initialize(BufferAllocator::Get(buffer_manager->context), detected_types, STANDARD_VECTOR_SIZE);
	for (idx_t i = 1; i < sniffing_state_machine.options.sample_size_chunks; i++) {
		bool finished_file = best_candidate->Finished();
		if (finished_file) {
			// we finished the file: stop
			// set sql types
			detected_types.clear();
			for (idx_t column_idx = 0; column_idx < best_sql_types_candidates_per_column_idx.size(); column_idx++) {
				LogicalType d_type = best_sql_types_candidates_per_column_idx[column_idx].back();
				if (best_sql_types_candidates_per_column_idx[column_idx].size() ==
				    sniffing_state_machine.options.auto_type_candidates.size()) {
					d_type = LogicalType::VARCHAR;
				}
				detected_types.push_back(d_type);
			}
			return;
		}
		best_candidate->Process<ParseChunk>(*best_candidate, parse_chunk);
		for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
			vector<LogicalType> &col_type_candidates = best_sql_types_candidates_per_column_idx[col];
			bool is_bool_type = col_type_candidates.back() == LogicalType::BOOLEAN;
			while (col_type_candidates.size() > 1) {
				const auto &sql_type = col_type_candidates.back();
				//	narrow down the date formats
				if (best_format_candidates.count(sql_type.id())) {
					auto &best_type_format_candidates = best_format_candidates[sql_type.id()];
					auto save_format_candidates = best_type_format_candidates;
					while (!best_type_format_candidates.empty()) {
						if (TryCastVector(parse_chunk.data[col], parse_chunk.size(), sql_type)) {
							break;
						}
						//	doesn't work - move to the next one
						best_type_format_candidates.pop_back();
						if (!best_type_format_candidates.empty()) {
							SetDateFormat(best_candidate->GetStateMachineSniff(), best_type_format_candidates.back(),
							              sql_type.id());
						}
					}
					//	if none match, then this is not a column of type sql_type,
					if (best_type_format_candidates.empty()) {
						//	so restore the candidates that did work.
						best_type_format_candidates.swap(save_format_candidates);
						if (!best_type_format_candidates.empty()) {
							SetDateFormat(best_candidate->GetStateMachineSniff(), best_type_format_candidates.back(),
							              sql_type.id());
						}
					}
				}
				if (TryCastVector(parse_chunk.data[col], parse_chunk.size(), sql_type)) {
					break;
				} else {
					if (col_type_candidates.back() == LogicalType::BOOLEAN && is_bool_type) {
						// If we thought this was a boolean value (i.e., T,F, True, False) and it is not, we
						// immediately pop to varchar.
						while (col_type_candidates.back() != LogicalType::VARCHAR) {
							col_type_candidates.pop_back();
						}
						break;
					}
					col_type_candidates.pop_back();
				}
			}
		}
		// reset parse chunk for the next iteration
		parse_chunk.Reset();
	}
	detected_types.clear();
	// set sql types
	for (idx_t column_idx = 0; column_idx < best_sql_types_candidates_per_column_idx.size(); column_idx++) {
		LogicalType d_type = best_sql_types_candidates_per_column_idx[column_idx].back();
		if (best_sql_types_candidates_per_column_idx[column_idx].size() ==
		    best_candidate->GetStateMachineSniff().options.auto_type_candidates.size()) {
			d_type = LogicalType::VARCHAR;
		}
		detected_types.push_back(d_type);
	}
}
} // namespace duckdb
