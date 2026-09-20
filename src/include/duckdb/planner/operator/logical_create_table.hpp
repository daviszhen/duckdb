//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_create_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class SchemaCatalogEntry;
struct CreateInfo;

class LogicalCreateTable : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_TABLE;

public:
	LogicalCreateTable(SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info);

	//! Schema to insert to
	SchemaCatalogEntry &schema;
	//! Create Table information
	unique_ptr<BoundCreateTableInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	//! A deserialized CREATE TABLE is *re-bound* - its private constructor below builds a fresh binder and
	//! binds the create info through it - and a fresh binder has no idea which statement it is planning. So the
	//! round trip is not transparent: a statement the engine wrote comes back as a caller's, measured with
	//! `debug_verify_serializer`, which is the mode that does this round trip. The engine-wrote-this flag is
	//! what lets an extension keep state in tables of its own while refusing a caller the same writes (the
	//! git4data reference registry), so under that mode the feature's own `CREATE TABLE git4data.snapshots` was
	//! refused by the guard that exists to refuse a caller: 84 of 84 feature files failing. Serialization is
	//! therefore not claimed for this operator, which is what the flag below is for.
	bool SupportSerialization() const override {
		return false;
	}

	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;

private:
	LogicalCreateTable(ClientContext &context, unique_ptr<CreateInfo> info);
};
} // namespace duckdb
