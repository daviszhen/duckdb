//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/chunk_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/index_pointer.hpp"
#include "duckdb/common/enums/scan_options.hpp"
#include "duckdb/transaction/transaction_data.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {
class RowGroup;
struct SelectionVector;
class Transaction;
struct TransactionData;
struct DeleteInfo;
class Serializer;
class Deserializer;
class FixedSizeAllocator;

//! The type of a serialized chunk info entry
//! In-memory all chunk infos are represented by ChunkVectorInfo - CONSTANT_INFO (a fully deleted vector) and
//! EMPTY_INFO (no deletes) only exist as tags in the serialized format
enum class ChunkInfoType : uint8_t {
	CONSTANT_INFO,
	VECTOR_INFO,
	EMPTY_INFO,
	//! The full insert and delete state, ids included. Written when a pinned snapshot still
	//! has to tell "this row existed when I look" and "this row was already deleted when I
	//! look" apart from "this row came later" and "this row is gone for everyone". The
	//! compact forms above assume every id is visible to everyone, which a pin breaks.
	CHUNK_INFO_WITH_IDS
};

//! ChunkVectorInfo holds the version information (the insert and delete ids) of the rows of a single vector within
//! a row group. The insert and delete ids are stored as constants when all rows share the same id, and as per-row
//! id arrays otherwise.

//! The result of a CompressVersionIds pass over a vector
enum class VersionCompressionResult : uint8_t {
	//! No per-row id arrays remain - there is nothing left to compress
	FULLY_COMPRESSED,
	//! Some ids are not yet visible to all transactions - compression can succeed once the
	//! lowest active start advances past them, without any further modifications
	PENDING,
	//! Per-row arrays remain that cannot compress without further modifications
	//! (some rows are not deleted)
	SETTLED
};

//! The storage state of the delete side of a ChunkVectorInfo. Exhaustive and mutually exclusive:
//! CONSTANT - all rows share constant_delete_id (NOT_DELETED_ID for none-deleted, or a single delete id)
//! MASKED   - partially deleted, all deleted rows share one committed id (mask_delete_id): which rows are
//!            deleted is stored in deleted_mask, deleted_data freed
//! ARRAY    - per-row delete ids materialized in deleted_data
enum class DeleteIdState : uint8_t { CONSTANT, MASKED, ARRAY };

class ChunkVectorInfo {
public:
	explicit ChunkVectorInfo(FixedSizeAllocator &allocator, idx_t start, transaction_t insert_id = 0);
	ChunkVectorInfo(FixedSizeAllocator &allocator, idx_t start, transaction_t insert_id, transaction_t delete_id);
	~ChunkVectorInfo();

	//! The row index of the first row
	idx_t start;

public:
	//! Gets up to max_count entries from the chunk info. If the ret is 0>ret>max_count, the selection vector is filled
	//! with the tuples
	idx_t GetSelVector(ScanOptions options, optional_ptr<SelectionVector> sel_vector, idx_t max_count) const;
	idx_t GetRowCount(ScanOptions options, idx_t max_count);
	//! Returns whether or not a single row in the ChunkVectorInfo should be used or not for the given transaction
	bool Fetch(TransactionData transaction, row_t row);
	void CommitAppend(transaction_t commit_id, idx_t start, idx_t end);
	bool Cleanup(VisibilityBound lowest_visibility_bound) const;
	string ToString(idx_t max_count) const;

	void Append(idx_t start, idx_t end, transaction_t commit_id);

	//! Performs a delete in the ChunkVectorInfo - returns how many tuples were actually deleted
	//! The number of rows that were actually deleted might be lower than the input count
	//! In case we delete rows that were already deleted
	//! Note that "rows" is written to to reflect the row ids that were actually deleted
	//! i.e. after calling this function, rows will hold [0..actual_delete_count] row ids of the actually deleted tuples
	idx_t Delete(transaction_t transaction_id, row_t rows[], idx_t count);
	void CommitDelete(transaction_t commit_id, const DeleteInfo &info);

	//! Attempts to compress the per-row insert/delete ids into constants. Ids that precede
	//! lowest_visibility_bound look the same to every active and future transaction, so they can collapse
	VersionCompressionResult CompressVersionIds(VisibilityBound lowest_visibility_bound);
	//! Whether a compression pass could achieve anything for this vector (see recheck_compression)
	bool RecheckCompression() const {
		return recheck_compression;
	}
	//! Verifies (in DEBUG) that a disarmed recheck_compression matches the actual ids: nothing may be
	//! compressible now or in the future without a modification that re-arms the check
	void VerifyCachedCompressionState() const;

	bool HasDeletes(VisibilityBound visibility_bound = VisibilityBound::IncludingUncommitted()) const;
	bool HasUncommittedChanges() const;
	bool AnyDeleted() const;
	bool HasConstantInsertionId() const;
	transaction_t ConstantInsertId() const;
	bool HasConstantDeleteId() const;
	transaction_t ConstantDeleteId() const;

	//! Serialize the delete side. Deletes at or after retention_bound keep their ids, so a
	//! reader pinned there can still tell them from deletes visible to everyone.
	void Write(WriteStream &writer, VisibilityBound visibility_bound, VisibilityBound retention_bound) const;
	//! The delete id of the given row, NOT_DELETED_ID when the row is alive
	transaction_t GetDeleteId(idx_t row) const;
	//! The insert id of the given row
	transaction_t GetInsertId(idx_t row) const;
	//! Record the insert ids of a run of rows moving in from another chunk, so that a reader pinned
	//! below them still does not see them. Runs arrive in increasing row order, so a run beginning at
	//! the vector's first row may set the vector's constant and a later run overwrites its own part.
	void SetInsertIds(idx_t start, idx_t count, const transaction_t *ids);
	//! The same for the delete side: a row deleted at or after the retention bound has to keep the id
	//! that says so, or a reader pinned there loses a row it still sees.
	void SetDeleteIds(idx_t start, idx_t count, const transaction_t *ids);
	//! Whether any insert id is at or after the given bound, so a reader pinned there still
	//! needs the insert side written out. Without this the checkpoint would skip a vector that
	//! holds only freshly inserted rows and they would read as visible to everyone on reload.
	bool HasPendingInserts(VisibilityBound retention_bound) const;
	static unique_ptr<ChunkVectorInfo> Read(FixedSizeAllocator &allocator, ReadStream &reader);

private:
	//! The rows a retention pass has to keep, for DeletedScanType::OMIT_UNREACHABLE_DELETES
	idx_t GetReachableSelVector(const ScanOptions &options, optional_ptr<SelectionVector> sel_vector,
	                            idx_t max_count) const;

	template <class INSERT_OP, class DELETE_OP>
	idx_t TemplatedGetSelVector(const SnapshotView &view, optional_ptr<SelectionVector> sel_vector,
	                            idx_t max_count) const;

	IndexPointer GetInsertedPointer() const;
	IndexPointer GetDeletedPointer() const;
	IndexPointer GetInitializedInsertedPointer();
	IndexPointer GetInitializedDeletedPointer();
	//! Frees the per-row delete ids (if any)
	void FreeDeleteData();
	//! Frees the per-row insert ids (if any)
	void FreeInsertData();
	//! ARRAY -> MASKED: record alive rows as invalid bits, free the per-row delete array. mask_id is the
	//! shared committed id of the deleted rows (0 when they are already visible to all transactions)
	void CompressDeleteToMask(transaction_t mask_id);
	//! MASKED -> ARRAY: re-materialize the per-row delete array from the bitmask
	void DecompressDeleteMask();

private:
	FixedSizeAllocator &allocator;
	//! The transaction ids of the transactions that inserted the tuples (if any)
	IndexPointer inserted_data;
	//! The constant insert id (if there is only one)
	transaction_t constant_insert_id;

	//! The transaction ids of the transactions that deleted the tuples (if any)
	IndexPointer deleted_data;
	//! The constant delete id (if there is only one, e.g. because the entire vector was deleted in one transaction)
	transaction_t constant_delete_id;
	//! Bitmask used in the MASKED state: valid bit == the row is deleted, invalid bit == the row is alive.
	//! Matches the on-disk VECTOR_INFO orientation. Only meaningful when delete_state == DeleteIdState::MASKED.
	ValidityMask deleted_mask;
	//! The single committed id shared by every deleted row in the MASKED state. 0 means the deletes are
	//! visible to all transactions (the value used when read from disk); a non-zero committed id means the
	//! mask was folded from one committed transaction whose delete is not yet visible to every snapshot.
	//! Only meaningful when delete_state == DeleteIdState::MASKED.
	transaction_t mask_delete_id = 0;
	//! The current delete-side storage state - the single source of truth for the delete side
	DeleteIdState delete_state = DeleteIdState::CONSTANT;
	//! Whether a compression pass could achieve anything for this vector: armed by any id modification,
	//! disarmed when a pass compresses the vector fully or finds it settled (live rows block the collapse
	//! until a further delete re-arms it). CompressVersionIds returns the cached SETTLED without
	//! re-scanning the ids while this is false.
	bool recheck_compression = true;
};

} // namespace duckdb
