#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;

//! A mapping is a row, and the tables it names are the *caller's* - which is why dropping a branch takes them
//! with it: a copy the feature made has no other reader once the branch is gone.
//!
//! That has an exception the SQL surface cannot stage any more: a table of the feature's own registry. No
//! statement can write such a mapping now (`git4data_bind_branch_table` and `git4data_bind_branch_delta` refuse
//! to aim at one of those tables, 3dn), but a **file written before that refusal existed** can hold one, and so
//! can a file whose records came from the checkpoint block - which is the case the block fallback is kept for.
//! The drop reads the mapping and runs `DROP TABLE IF EXISTS` through the same flag that lets the feature write
//! the registry, so without a check it takes the registry table with the branch: measured, `git4data.snapshots`
//! was gone after `git4data_drop_branch`, and with it every reference the database held.
//!
//! **The state is staged the way an older file holds it**: the mapping's row is written by a statement marked
//! `internal` - which is what a `git4data_*` pragma's own generated statement carries, and the reason it can
//! write the registry at all - and then the database is closed and reopened, so what the test works with is the
//! mapping as the *file* has it rather than something left in memory. The source table is given the shape of
//! `git4data.snapshots`, because a read maps a branch's line onto the source's columns by position, so this is
//! also the shape in which a file holding such a mapping is readable at all.
TEST_CASE("git4data: a mapping aimed at a registry table cannot drop it with the branch", "[git4data][api]") {
	auto path = TestCreatePath("git4data_registry_aim.db");
	{
		DuckDB db(path);
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("CREATE TABLE s (name VARCHAR PRIMARY KEY, pin UBIGINT, created TIMESTAMP, "
		                          "line VARCHAR)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO s VALUES ('mine', 1, TIMESTAMP '2020-01-01 00:00:00', '')"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA git4data_create_snapshot('keep_me')"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA git4data_create_branch('b', '')"));

		// the row an older build's binding would have written, run the way that binding's own statement ran
		auto statements =
		    con.ExtractStatements("INSERT INTO git4data.storage VALUES ('b', 's', 'git4data.snapshots', true)");
		REQUIRE(statements.size() == 1);
		statements[0]->internal = true;
		auto written = con.Query(std::move(statements[0]));
		REQUIRE(!written->HasError());
	}

	// reopened: the mapping the file holds is the mapping the feature reads back
	{
		DuckDB db(path);
		Connection con(db);

		auto mapping = con.Query("SELECT storage FROM git4data.storage WHERE branch = 'b'");
		REQUIRE(CHECK_COLUMN(mapping, 0, {Value("git4data.snapshots")}));

		// the branch reads the registry table as its own line - one row, the snapshot this test made. A read is
		// not what the guard is about, and this is what a database holding such a mapping shows
		auto read = con.Query("SELECT count(*) FROM s AT (BRANCH => 'b')");
		REQUIRE(CHECK_COLUMN(read, 0, {Value::BIGINT(1)}));

		// dropping the branch refuses rather than taking the registry table with it
		// checked rather than required: a refusal that stops the test would hide what happens without it, and
		// what happens without it is the whole point - the drop goes through and takes the registry table
		auto refused = con.Query("PRAGMA git4data_drop_branch('b')");
		CHECK(refused->HasError());
		CHECK(StringUtil::Contains(refused->GetError(), "is one of the tables the reference registry is kept in"));

		// and nothing was lost: the registry table is there, the reference is there, and the branch is still the
		// branch it was - which the row below says, since that is what a reopen reads back
		REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM git4data.snapshots"));
		auto still_there = con.Query("SELECT (SELECT count(*) FROM git4data.snapshots WHERE name = 'keep_me'), "
		                             "(SELECT count(*) FROM git4data.branches WHERE name = 'b')");
		REQUIRE(CHECK_COLUMN(still_there, 0, {Value::BIGINT(1)}));
		REQUIRE(CHECK_COLUMN(still_there, 1, {Value::BIGINT(1)}));

		// the way out: a mapping aimed at the registry is a state to be cleared, and unbind clears it without
		// dropping anything - after which the branch drops as any other does, and the registry survives it
		REQUIRE_NO_FAIL(con.Query("PRAGMA git4data_unbind('s', 'b')"));
		REQUIRE_NO_FAIL(con.Query("PRAGMA git4data_drop_branch('b')"));
		auto after = con.Query("SELECT (SELECT count(*) FROM git4data.snapshots WHERE name = 'keep_me'), "
		                       "(SELECT count(*) FROM git4data.branches WHERE name = 'b')");
		REQUIRE(CHECK_COLUMN(after, 0, {Value::BIGINT(1)}));
		REQUIRE(CHECK_COLUMN(after, 1, {Value::BIGINT(0)}));
	}
	DeleteDatabase(path);
}
