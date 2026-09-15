//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/scan_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_data.hpp"

namespace duckdb {

enum class InsertedScanType {
	// get all rows valid for the transaction
	STANDARD,
	// scan all rows including transaction local rows
	ALL_ROWS
};

enum class DeletedScanType {
	//! omit any rows that are deleted
	STANDARD,
	//! include all rows, including all deleted rows
	INCLUDE_ALL_DELETED,
	//! omit only deleted rows that have been committed and have a commit ts lower than the transaction start
	OMIT_COMMITTED_DELETES,
	//! omit the rows no reader can reach: rows that are dead for the bound the scan reads at, and that
	//! no reader's bound falls inside. A deleted row is still reachable when some reader sees it, which
	//! is what retained_floor and retained_ceiling below say. Used by vacuum, which has to keep exactly
	//! what a reference or an active transaction can still see and may reclaim everything else.
	OMIT_UNREACHABLE_DELETES
};

enum class UpdateScanType {
	//! allow updates
	STANDARD,
	// disallow updates - throw on updates
	DISALLOW_UPDATES,
	//! read the base column data only, ignoring any updates layered on top of it
	SKIP_UPDATES
};

struct ScanOptions {
	ScanOptions(TransactionData transaction); // NOLINT: allow implicit conversion from transaction

	TransactionData transaction;
	InsertedScanType insert_type = InsertedScanType::STANDARD;
	DeletedScanType delete_type = DeletedScanType::STANDARD;
	UpdateScanType update_type = UpdateScanType::STANDARD;
	//! Only read for DeletedScanType::OMIT_UNREACHABLE_DELETES. The lowest bound any reader reads at: a
	//! row deleted below it was deleted before every reader, so no reader sees it. The deletion
	//! comparisons elsewhere use transaction.visibility_bound, so a caller sets both: the bound it
	//! reads the table at, and this floor underneath it.
	VisibilityBound retained_floor = VisibilityBound::IncludingUncommitted();
	//! The highest bound any reader reads at: a row inserted at or after it came after every reader, so
	//! no reader sees it either. With no readers above the floor the two are the same, which leaves
	//! only the deletes below the floor omitted - the plain committed view.
	VisibilityBound retained_ceiling = VisibilityBound::IncludingUncommitted();
};

enum class TableScanType {
	//! Scan all rows, including any deleted rows. Committed updates are merged in.
	TABLE_SCAN_ALL_ROWS = 1,
	//! Scan all rows, excluding any permanently deleted rows.
	//! Permanently deleted rows are rows which no transaction will ever need again.
	TABLE_SCAN_OMIT_PERMANENTLY_DELETED = 2,
	//! Scan the latest committed rows
	TABLE_SCAN_COMMITTED_ROWS = 3,
	//! Scan the base column data with updates skipped, so the pre-image of an update
	//! can be observed. Used to measure retention.
	TABLE_SCAN_BASE_ROWS = 4
};

} // namespace duckdb
