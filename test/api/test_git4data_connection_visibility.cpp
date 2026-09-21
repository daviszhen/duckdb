#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"

using namespace duckdb;

//! What a connection sees of a reference another transaction has added, and what a rollback leaves behind.
//!
//! **The feature does not claim reference-level isolation inside a transaction, deliberately.** It did, for three
//! increments: 3cr made a reference a transaction had added and not committed invisible to every other
//! connection, and 3cv withdrew it - measured, the connection asking is not observable at the point where the
//! question has to be answered, because the pragma that *makes* a reference runs before the statement it belongs
//! to and in a different client context, and three candidate identities (the connection id, the `ClientData` and
//! the connection's `MetaTransaction`) each differ between the pragma and the very next statement of the same
//! connection. Without an identity the check hides the reference from everyone, including the connection that
//! has just made it. `Git4DataReferenceVisible` is the seam that is left for the day the identity is
//! observable, and it answers `true` today.
//!
//! So this test asserts the behaviour as it is, and says why - which is the point of it existing. The mutation
//! sweep (increment 3ew, `git4data-mutation-sweep.py`) reported this seam as "nothing pins it", and the first
//! version of this test asserted the isolation the feature does not provide: it failed, and reading the seam's
//! own comment is what explained the failure. What *is* isolated, and what this test does assert, is what a
//! rollback leaves: a reference a finished transaction added is gone by the next rebuild, so the window 3cs and
//! 3ct closed is closed here as well.
TEST_CASE("git4data: what another connection sees of an uncommitted reference", "[git4data][api]") {
	auto path = TestCreatePath("git4data_connection_visibility.db");
	{
		DuckDB db(path);
		Connection writer(db);
		Connection reader(db);

		REQUIRE_NO_FAIL(writer.Query("CREATE TABLE t (k INTEGER PRIMARY KEY, v INTEGER)"));
		REQUIRE_NO_FAIL(writer.Query("INSERT INTO t VALUES (1, 10)"));
		REQUIRE_NO_FAIL(writer.Query("PRAGMA git4data_create_snapshot('s0')"));

		// a committed reference is both connections' to read
		auto both = reader.Query("SELECT count(*) FROM t AT (SNAPSHOT => 's0')");
		REQUIRE(!both->HasError());
		CHECK(both->GetValue(0, 0).GetValue<int64_t>() == 1);

		REQUIRE_NO_FAIL(writer.Query("BEGIN"));
		REQUIRE_NO_FAIL(writer.Query("INSERT INTO t VALUES (2, 20)"));
		REQUIRE_NO_FAIL(writer.Query("PRAGMA git4data_create_snapshot('uncommitted')"));

		// the inserted *row* is not the reader's yet: that is ordinary transaction isolation, and it holds
		auto rows = reader.Query("SELECT count(*) FROM t");
		REQUIRE(!rows->HasError());
		CHECK(rows->GetValue(0, 0).GetValue<int64_t>() == 1);

		// The *reference* is reachable by both connections - deliberately, per the withdrawal above - and what
		// it reads is a **committed** state: a snapshot's pin is the commit it captured, and the row the writer
		// has inserted since is not part of it. Measured, and worth stating because the first version of this
		// test read the two together and reported a dirty read the feature does not have: the name is shared
		// before the transaction commits, the *state* is not.
		auto own = writer.Query("SELECT count(*) FROM t AT (SNAPSHOT => 'uncommitted')");
		REQUIRE(!own->HasError());
		CHECK(own->GetValue(0, 0).GetValue<int64_t>() == 1);
		auto other = reader.Query("SELECT count(*) FROM t AT (SNAPSHOT => 'uncommitted')");
		REQUIRE(!other->HasError());
		CHECK(other->GetValue(0, 0).GetValue<int64_t>() == 1);

		// a rollback takes the reference with it, and the next pragma's rebuild is what removes it from the
		// registry the in-memory half holds as well
		REQUIRE_NO_FAIL(writer.Query("ROLLBACK"));
		REQUIRE_NO_FAIL(reader.Query("PRAGMA git4data_create_snapshot('trigger')"));
		auto listed = reader.Query("SELECT string_agg(name, ',' ORDER BY name) FROM git4data.snapshots");
		REQUIRE(!listed->HasError());
		CHECK(listed->GetValue(0, 0).ToString() == "s0,trigger");
		auto gone = reader.Query("SELECT count(*) FROM t AT (SNAPSHOT => 'uncommitted')");
		REQUIRE(gone->HasError());
		CHECK(gone->GetError().find("does not exist") != string::npos);
	}
}
