#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"

using namespace duckdb;

//! A reference is bound to a *table*, and the catalog's `oid` - which is what tells that table from another one
//! that takes its name - is a per-process counter that is not written to the file (measured, 3bs and 3ds). So
//! after a reopen nothing on a table says which table it is, and a mapping read back from the file has nothing
//! to compare against: dropping a mapped table while the database is closed and creating another of the same
//! name and shape leaves the reference reading the new table, silently.
//!
//! This is the engine half of the fix (decision 2 of the three format decisions, 2026-09-21): a table **a
//! reference is bound to** carries an identity, minted at the first checkpoint that writes it and read back at
//! open. It is written *only* for such a table, because a file carrying the field at all is a file an older
//! build refuses (its table-metadata reader rejects an unknown field - measured, the compatibility table in the
//! note), and writing it for every table would take that from the feature's pin-free files too.
//!
//! What the identity is *for* - refusing a read through a table that is not the one a mapping was made against -
//! is the feature half, and it is not in this test.

static uint64_t TableIdentity(Connection &con, const string &table_name) {
	// the catalog is read through a transaction, and a test has no statement of its own running: a read-only
	// one of its own, rolled back, is the least that can be said
	con.BeginTransaction();
	auto &entry = Catalog::GetEntry<TableCatalogEntry>(*con.context, QualifiedName(Identifier(table_name)));
	auto id = entry.GetStorage().GetDataTableInfo()->GetPersistedTableId();
	con.Rollback();
	return id;
}

TEST_CASE("git4data: a mapped table's identity survives a reopen, a recreated one differs", "[git4data][api]") {
	auto path = TestCreatePath("git4data_table_identity.db");
	uint64_t first = 0;
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (k INTEGER PRIMARY KEY, v INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT range, range FROM range(10)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t_b (k INTEGER PRIMARY KEY, v INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA git4data_create_branch('b', '')"));

		// an unmapped table carries none: nothing has a mapping that could compare it
		REQUIRE(TableIdentity(con, "t") == 0);

		// the branch's own table is what a read resolves through the mapping, so that one is mapped
		REQUIRE_NO_FAIL(con.Query("PRAGMA git4data_bind_branch_table('t', 'b', 't_b')"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		first = TableIdentity(con, "t_b");
		REQUIRE(first != 0);
	}
	{
		DuckDB db(path);
		Connection con(db);
		// the identity the file carries is the one the table is given back
		REQUIRE(TableIdentity(con, "t_b") == first);

		// a table dropped and recreated under the same name and shape is a different table, and the file now
		// holds its identity rather than the old one's
		REQUIRE_NO_FAIL(con.Query("DROP TABLE t_b"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t_b (k INTEGER PRIMARY KEY, v INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		REQUIRE(TableIdentity(con, "t_b") != first);
	}
}
