//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/data_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/table_index_list.hpp"

namespace duckdb {
class AttachedDatabase;
class DatabaseInstance;
struct CheckpointOptions;
class TableIOManager;
class RowGroupCollection;

struct DataTableInfo {
	friend class DataTable;

public:
	DataTableInfo(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, vector<Identifier> schema_path,
	              Identifier table);

	//! Bind unknown indexes throwing an exception if binding fails.
	//! Only binds the specified index type, or all if no type is specified.
	void BindIndexes(ClientContext &context, const optional<string> &index_type = {});

	//! Whether or not the table is temporary
	bool IsTemporary() const;

	AttachedDatabase &GetDB() const {
		return db;
	}
	//! The identity that survives a reopen (see `persisted_table_id` below)
	uint64_t GetPersistedTableId() const {
		return persisted_table_id;
	}
	bool PersistedTableIdIsFromDisk() const {
		return persisted_table_id_on_disk;
	}
	void SetPersistedTableIdFromDisk(bool from_disk) {
		persisted_table_id_on_disk = from_disk;
	}
	void SetPersistedTableId(uint64_t id) {
		persisted_table_id = id;
	}

	TableIOManager &GetIOManager() {
		return *table_io_manager;
	}

	TableIndexList &GetIndexes() {
		return indexes;
	}
	//! Find and move out an IndexStorageInfo by name from the stored collection.
	IndexStorageInfo ExtractIndexStorageInfo(const Identifier &name);
	unique_ptr<StorageLockKey> GetSharedLock() {
		return checkpoint_lock.GetSharedLock();
	}
	bool AppendRequiresNewRowGroup(RowGroupCollection &collection, optional_idx checkpoint_id);
	optional_idx CheckpointRowGroupCount(const CheckpointOptions &options) const;
	void VerifyIndexBuffers() const;

	Identifier GetSchemaName();
	//! The full (possibly nested) schema path of the table
	const vector<Identifier> &GetSchemaPath() const;
	Identifier GetTableName();
	void SetTableName(Identifier name);

private:
	//! The database instance of the table
	AttachedDatabase &db;
	//! The table IO manager
	shared_ptr<TableIOManager> table_io_manager;
	//! Lock for modifying the name
	mutex name_lock;
	//! The (possibly nested) schema path of the table, outermost schema first
	vector<Identifier> schema_path;
	//! The name of the table
	Identifier table;
	//! An identity for this table that survives a reopen, minted at the first checkpoint that writes it.
	//!
	//! The catalog's own `oid` is a per-process counter and is *not* written to the file - a table carries its
	//! name, columns, constraints and storage, and nothing that names the entry (measured, 3bs and 3ds) - so
	//! after a reopen nothing tells a table from another table that took its name, and a reference bound to the
	//! first would silently read the second. This is that identity: minted once, written with the table's
	//! metadata, read back at open, and compared by the feature (decision of 2026-09-21). A file written before
	//! this property reads as zero, which means "unknown" - the behaviour before it existed.
	uint64_t persisted_table_id = 0;
	//! Whether that identity came *from the file* - set when the checkpoint reader reads it, and when a
	//! checkpoint writes it. It is what makes the comparison honest: a table created by a statement that the log
	//! replayed has an identity nothing persisted (the identity is assigned by an internal statement, and
	//! internal statements are not logged), so the feature compares only where the file carries one.
	bool persisted_table_id_on_disk = false;
	//! The physical list of indexes of this table
	TableIndexList indexes;
	//! Index storage information of the indexes created by this table
	vector<IndexStorageInfo> index_storage_infos;
	//! Lock held while checkpointing
	StorageLock checkpoint_lock;
	//! The last seen checkpoint while doing a concurrent operation, if any
	optional_idx last_seen_checkpoint;
	//! The amount of row groups the checkpoint is processing
	optional_idx checkpoint_row_group_count;
};

} // namespace duckdb
