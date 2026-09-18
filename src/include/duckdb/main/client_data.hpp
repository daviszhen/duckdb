//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

class AttachedDatabase;
class BufferedFileWriter;
class ClientContext;
class CatalogSearchPath;
class FileOpener;
class FileSystem;
class QueryProfiler;
class PreparedStatementData;
class RandomEngine;
class BufferManager;

struct ClientData {
public:
	explicit ClientData(ClientContext &context);
	~ClientData();

public:
	//! The registry records this connection's current transaction has added, as "<kind>\t<key>" strings. A
	//! rollback takes their rows with it, so they are what the in-memory registries have to be rid of - and
	//! records another connection added while uncommitted are none of this one's business.
	vector<string> git4data_added_records;

	//! Set when a rollback happened on this connection, and cleared by the first git4data operation after it:
	//! a reference is a row in the registry tables, so this connection's rollback may have taken one away and
	//! its in-memory registries are rebuilt from the tables before it uses them again. Per connection on
	//! purpose - another connection's rollback says nothing about the references this one has not committed
	//! yet, and a rebuild is a clear-and-reload, so a shared flag would wipe them (measured, with two
	//! connections and a real rollback on one of them).
	bool git4data_registry_reload_needed = false;

	//! The query profiler of this client.
	shared_ptr<QueryProfiler> profiler;

	//! The set of temporary objects belonging to this client.
	shared_ptr<AttachedDatabase> temporary_objects;
	//! The set of bound prepared statements belonging to this client.
	identifier_map_t<shared_ptr<PreparedStatementData>> prepared_statements;

	//! The random generator used by random().
	//! Its seed value can be set by setseed().
	unique_ptr<RandomEngine> random_engine;

	//! The catalog search path.
	unique_ptr<CatalogSearchPath> catalog_search_path;

	//! The file opener of the client context.
	unique_ptr<FileOpener> file_opener;
	//! The clients' file system wrapper.
	unique_ptr<FileSystem> client_file_system;
	//! The clients' buffer manager wrapper.
	unique_ptr<BufferManager> client_buffer_manager;

	//! The writer used to log queries, if logging is enabled.
	//! DEPRECATED: Now, queries are written to the generic log.
	unique_ptr<BufferedFileWriter> log_query_writer;

public:
	DUCKDB_API static ClientData &Get(ClientContext &context);
	DUCKDB_API static const ClientData &Get(const ClientContext &context);
};

} // namespace duckdb
