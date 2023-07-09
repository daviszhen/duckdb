#include <iostream>
#include "duckdb.hpp"

using namespace duckdb;

void printLine(){
    std::cerr << "=======================" << std::endl;
}

int main() {
	DuckDB db(nullptr);

	Connection con(db);

    printLine();
	auto result = con.Query("INSTALL tpch");
	result->Print();
	result = con.Query("LOAD tpch");
	result->Print();
	result =con.Query("CALL dbgen(sf=0.1)");
    result->Print();
    printLine();
	result = con.Query("show tables");
	result->Print();
    printLine();
    result = con.Query("DESCRIBE lineitem");
    result->Print();
    printLine();
	result = con.Query("SELECT\n"
                        "    l_orderkey,\n"
                        "    sum(l_extendedprice * (1 - l_discount)) AS revenue,\n"
                        "    o_orderdate,\n"
                        "    o_shippriority\n"
                        "FROM\n"
                        "    customer\n"
                        "    JOIN orders ON (c_custkey=o_custkey)\n"
                        "    JOIN lineitem ON (l_orderkey=o_orderkey)\n"
                        "WHERE\n"
                        "    c_mktsegment = 'BUILDING'\n"
                        "    AND o_orderdate < CAST('1995-03-15' AS date)\n"
                        "    AND l_shipdate > CAST('1995-03-15' AS date)\n"
                        "GROUP BY\n"
                        "    l_orderkey,\n"
                        "    o_orderdate,\n"
                        "    o_shippriority\n"
                        "ORDER BY\n"
                        "    revenue DESC,\n"
                        "    o_orderdate\n"
                        "LIMIT 10;");
	result->Print();
}
