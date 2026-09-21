#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"

using namespace duckdb;

//! What a read does when the table it resolves carries no identity the file can vouch for, and what that leaves
//! behind for the *next* reopen.
//!
//! A mapping records an identity, and a read compares the table it resolves against it. When the table carries an
//! identity nothing has persisted - a table the log recreated after a crash, or one created since the last
//! checkpoint - there is nothing to compare, and the documented behaviour is that the read is **not** refused.
//! What the read must not do is *write* the mapping's value onto that table: the checkpoint would then write it
//! as the table's own identity (property 106), and from the next reopen on a table that took the mapping's name
//! would answer the mapping's reads correctly - the laundering increment 3el measured and retracted in the
//! checkpoint writer, reached through a second door in the reader.
//!
//! The staging is what makes it observable: the swap happens with a reopen that **does not** checkpoint, so the
//! replacement has no identity from the file; the read then runs once; and a second reopen, which does
//! checkpoint, is what would carry the laundered value into the file.
TEST_CASE("git4data: a read adopts a mapping's identity without writing it", "[git4data][api]") {
	auto path = TestCreatePath("git4data_identity_adoption.db");
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (k INTEGER PRIMARY KEY, v INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 10), (2, 20)"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA git4data_create_branch('b', '')"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA git4data_materialize('t', 'b', 'tb')"));
	}
	{
		// the caller replaces the table the branch was given, and the database is closed without checkpointing -
		// which is the window in which a table carries no identity the file can vouch for. That setting is
		// global, so it goes to the database rather than to a statement.
		DBConfig config;
		config.options.checkpoint_on_shutdown = false;
		DuckDB db(path, &config);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("DROP TABLE tb"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tb (k INTEGER PRIMARY KEY, v INTEGER)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO tb VALUES (1, 999), (2, 999)"));
		// the read is not refused: there is no fact about the table to compare the mapping against
		auto adopted = con.Query("SELECT sum(v) FROM t AT (BRANCH => 'b')");
		CHECK(!adopted->HasError());
	}
	{
		// this close checkpoints, so whatever identity the replacement carries now is written with it
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	{
		DuckDB db(path);
		Connection con(db);
		auto after = con.Query("SELECT sum(v) FROM t AT (BRANCH => 'b')");
		INFO("after the checkpointed reopen the read answered: "
		     << (after->HasError() ? "an error" : after->ToString()));
		// the replacement is not the table the branch was given, whatever identity it may have picked up
		CHECK(after->HasError());
	}
}
