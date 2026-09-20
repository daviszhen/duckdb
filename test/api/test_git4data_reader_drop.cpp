#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"

using namespace duckdb;

//! A read at a reference is correct only while the versions it pinned are still there, and what keeps them there
//! is the *reference*: dropping it releases the retention bound, and a checkpoint may then discard rows a read
//! that is still under way is going to ask for next.
//!
//! No sqllogictest can see that: a statement there runs to completion, so the drop can only ever happen before or
//! after the read. This test streams the read, fetches one chunk so it is under way, and then drops the reference
//! and forces a checkpoint from another connection in the middle of it - which is what a caller sees when two
//! connections work on one database and one of them drops a reference the other is reading through.
TEST_CASE("git4data: a read that is still fetching keeps its pinned state when the reference is dropped",
          "[git4data][api]") {
	auto path = TestCreatePath("git4data_reader_drop.db");
	{
		DuckDB db(path);
		Connection reader(db);
		Connection dropper(db);

		REQUIRE_NO_FAIL(reader.Query("CREATE TABLE t (k INTEGER PRIMARY KEY, v INTEGER)"));
		REQUIRE_NO_FAIL(reader.Query("INSERT INTO t SELECT range, range FROM range(200000)"));
		REQUIRE_NO_FAIL(reader.Query("CHECKPOINT"));
		REQUIRE_NO_FAIL(reader.Query("PRAGMA git4data_create_snapshot('pinned')"));

		// the table moves on: every value goes up, and half the rows are deleted
		REQUIRE_NO_FAIL(reader.Query("UPDATE t SET v = v + 1000000"));
		REQUIRE_NO_FAIL(reader.Query("DELETE FROM t WHERE k % 2 = 0"));
		REQUIRE_NO_FAIL(reader.Query("CHECKPOINT"));

		// what the table holds now, so the two states are told apart by their size alone
		auto current = reader.Query("SELECT count(*) FROM t");
		REQUIRE(CHECK_COLUMN(current, 0, {Value::BIGINT(100000)}));

		// the read at the snapshot, streamed: fetching one chunk leaves it under way, with the rest of the
		// table still to be scanned
		auto stream = reader.SendQuery("SELECT k, v FROM t AT (SNAPSHOT => 'pinned')");
		REQUIRE(stream->GetResultType() == QueryResultType::STREAM_RESULT);
		REQUIRE(!stream->HasError());
		auto first = stream->Fetch();
		REQUIRE(first);
		REQUIRE(first->size() > 0);
		idx_t rows = first->size();

		// another connection drops the reference this read is using, and forces the reclamation the drop
		// releases
		REQUIRE_NO_FAIL(dropper.Query("PRAGMA git4data_drop_snapshot('pinned')"));
		REQUIRE_NO_FAIL(dropper.Query("CHECKPOINT"));

		// the reference is gone for anything that starts now, while the read that started under it is allowed
		// to finish with what it pinned
		auto refused = dropper.Query("SELECT count(*) FROM t AT (SNAPSHOT => 'pinned')");
		REQUIRE(refused->HasError());

		// and the read finishes: it must still deliver the 200,000 rows it pinned, not the 100,000 the table
		// holds now
		while (true) {
			auto chunk = stream->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			rows += chunk->size();
		}
		REQUIRE(!stream->HasError());
		REQUIRE(rows == 200000);
	}
	DeleteDatabase(path);
}
