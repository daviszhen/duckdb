//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/checkpoint_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/checkpoint_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

struct CheckpointOptions {
	CheckpointOptions()
	    : wal_action(CheckpointWALAction::DONT_DELETE_WAL), action(CheckpointAction::CHECKPOINT_IF_REQUIRED),
	      type(CheckpointType::FULL_CHECKPOINT), visibility_bound(VisibilityBound::IncludingUncommitted()) {
	}

	CheckpointWALAction wal_action;
	CheckpointAction action;
	CheckpointType type;
	//! Identifies this checkpoint. Compared for equality only; empty when no checkpoint is running
	optional_idx checkpoint_id;
	//! What this checkpoint sees: timestamps below this bound are written
	VisibilityBound visibility_bound;
	//! Deletes at or after this bound still have to stay distinguishable, so their ids are
	//! written to disk. Held down by a pinned snapshot; AllCommitted() when nothing is pinned.
	VisibilityBound retention_bound = VisibilityBound::AllCommitted();
	//! The bound below which a delete or insert id has to be written to disk. Only a pinned
	//! snapshot outlives the file, and only it can need an id the reloaded timeline cannot
	//! otherwise reconstruct, so active transactions are deliberately not part of this bound.
	//! Keeping it AllCommitted() while nothing is pinned leaves the compact id-free forms in
	//! use, which is what an unpinned database writes.
	VisibilityBound id_retention_bound = VisibilityBound::AllCommitted();
	//! Whether vacuum may run even though this is a concurrent checkpoint, because a reference is the
	//! only reason it is not a full one and no transaction is writing. The vacuum carries the version
	//! information each reference needs into the row groups it writes, which is what makes it correct
	//! without the exclusive vacuum lock a full checkpoint would hold.
	bool can_vacuum_under_reference = false;
	//! The WAL lock - in case we are holding it during the entire checkpoint.
	//! This is only required if we are doing a checkpoint instead of writing to the WAL
	optional_ptr<unique_lock<mutex>> wal_lock;
};

} // namespace duckdb
