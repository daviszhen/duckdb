//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_version_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/checkpoint/row_group_writer.hpp"

namespace duckdb {

struct DeleteInfo;
class MetadataManager;
class BufferManager;
struct MetaBlockPointer;

class RowVersionManager {
public:
	explicit RowVersionManager(BufferManager &buffer_manager) noexcept;

	//! Returns the number of non-deleted rows in this segment
	idx_t GetRowCount(ScanOptions options, idx_t count);
	//! Count visible rows starting at a vector boundary, including a partial final vector
	idx_t GetRowCount(ScanOptions options, idx_t start_vector, idx_t count);

	idx_t GetSelVector(ScanOptions options, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count);
	//! Bulk visibility check. Returns the number of visible rows.
	idx_t GetVisibleRows(TransactionData transaction, const idx_t *offsets, idx_t count, SelectionVector &visible_sel);

	void AppendVersionInfo(TransactionData transaction, idx_t count, idx_t row_group_start, idx_t row_group_end);
	void CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count);
	void RevertAppend(idx_t new_count);
	void CleanupAppend(VisibilityBound lowest_visibility_bound, idx_t row_group_start, idx_t count);

	idx_t DeleteRows(idx_t vector_idx, transaction_t transaction_id, row_t rows[], idx_t count);
	void CommitDelete(idx_t vector_idx, transaction_t commit_id, const DeleteInfo &info);

	//! The insert and delete ids of a run of rows of one vector, for a caller carrying the version
	//! information of those rows into another row group. sel names the rows; null means the run is the
	//! whole vector from its first row.
	void GetVersionIdsForRows(idx_t vector_idx, const SelectionVector *sel, idx_t count, transaction_t *insert_ids,
	                          transaction_t *delete_ids);
	//! Record the version information of rows moving into this row group from another one, so that a
	//! reader pinned below them keeps seeing exactly what it saw. `start` is the row the run begins at
	//! within this row group.
	void SetVersionInfoForAppendedRows(const transaction_t *insert_ids, const transaction_t *delete_ids, idx_t start,
	                                   idx_t count);

	//! Attempts to compress the per-row insert/delete ids of each vector into constants. Ids that precede
	//! lowest_visibility_bound look the same to every active and future transaction, so they can collapse.
	//! Cheap when nothing can have changed: the pass only runs when version ids were modified since the
	//! last pass, or when a previous pass left ids that can still compress once older transactions finish
	void CompressVersionIds(VisibilityBound lowest_visibility_bound);

	vector<MetaBlockPointer> Checkpoint(RowGroupWriter &writer);
	static shared_ptr<RowVersionManager> Deserialize(MetaBlockPointer delete_pointer, MetadataManager &manager);

	//! Whether a checkpoint would have to rewrite the version information rather than reuse what is
	//! already on disk. Deletes count, and so do inserts: an insert at or after the id retention
	//! bound has to keep its id, and only the id-bearing on-disk form can carry one. Callers that
	//! have no bound to hand pass the default, which reports the delete case alone.
	bool HasUnserializedChanges(VisibilityBound id_retention_bound = VisibilityBound::AllCommitted());
	bool HasDeletes();
	bool HasUncommittedChanges();
	vector<MetaBlockPointer> GetStoragePointers();

private:
	mutex version_lock;
	FixedSizeAllocator allocator;
	vector<unique_ptr<ChunkVectorInfo>> vector_info;
	optional_idx uncheckpointed_delete_commit;
	vector<MetaBlockPointer> storage_pointers;
	//! Whether a compression pass may achieve anything: set when version ids are modified, cleared when a
	//! pass finds no ids that could still compress. For deserialized version info this is derived from the
	//! deserialized content (with the current storage format checkpointed ids are always settled).
	bool needs_compression_check = false;

private:
	FixedSizeAllocator &GetAllocator() {
		return allocator;
	}
	optional_ptr<ChunkVectorInfo> GetChunkInfo(idx_t vector_idx);
	ChunkVectorInfo &GetVectorInfo(idx_t vector_idx);
	void FillVectorInfo(idx_t vector_idx);
};

} // namespace duckdb
