//===----------------------------------------------------------------------===//
// Git4Data -- the guards, and the names and identities they reason about
//===----------------------------------------------------------------------===//
// What a caller may not do (write the registry through SQL, keep it in a schema of its own, reach a reference by
// a name another kind already holds), and how a table is named, resolved and told from another that takes its
// name. None of it needs the registry's own tables or rows, which is why the cut is clean: increment 3eu moved
// these 261 lines out of git4data_registry.cpp, changing no line of them. Everything here is declared in
// git4data_registry.hpp, so no header had to change either.
//===----------------------------------------------------------------------===//

#include "duckdb/storage/git4data_registry.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_data.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// The guard
//===----------------------------------------------------------------------===//
optional_ptr<TableCatalogEntry> Git4DataResolveTableOrNull(ClientContext &context, const string &name) {
	// a name taken from the catalog is one identifier, and a dot in it is part of the name - measured:
	// reading a table named "a.b" at a branch looked it up as a table "b" in a schema "a" and found nothing
	auto exact =
	    Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName(Identifier(name)), OnEntryNotFound::RETURN_NULL);
	if (exact) {
		return exact;
	}
	// a name a caller wrote may be qualified, which is the other reading of the same string: with no table
	// of that literal name, the dot separates a schema from a table
	auto qualified = QualifiedName::Parse(name);
	return Catalog::GetEntry<TableCatalogEntry>(context, qualified, OnEntryNotFound::RETURN_NULL);
}

TableCatalogEntry &Git4DataResolveTable(ClientContext &context, const string &name) {
	auto result = Git4DataResolveTableOrNull(context, name);
	if (!result) {
		throw CatalogException("Table with name %s does not exist", name);
	}
	return *result;
}

string Git4DataTableName(const TableCatalogEntry &table) {
	auto &schema = table.ParentSchema().name.GetIdentifierName();
	auto name = table.name.GetIdentifierName();
	if (StringUtil::CIEquals(schema, DEFAULT_SCHEMA)) {
		return name;
	}
	// the bare name would resolve to a table of that name in the default schema, or to none at all. Each
	// component is quoted as its own identifier, so a name holding a dot is still part of a qualified name.
	return KeywordHelper::WriteOptionallyQuoted(schema) + "." + KeywordHelper::WriteOptionallyQuoted(name);
}

string Git4DataTableSQLName(const string &name) {
	// each component quoted as its own identifier, so a name holding a dot still names one table
	return QualifiedName::Parse(name).ToString(QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA);
}

string Git4DataSourceKey(const TableCatalogEntry &table) {
	auto &schema = table.ParentSchema().name.GetIdentifierName();
	auto name = table.name.GetIdentifierName();
	// qualified even when the table is in the default schema, unlike the name a mapping's *tables* are held
	// under: a reference is keyed by the table it is about, two schemas can hold a table of the same name, and
	// a bare key would resolve through the search path rather than to the table the statement named - which is
	// the ambiguity a reference read used to refuse.
	return KeywordHelper::WriteOptionallyQuoted(schema) + "." + KeywordHelper::WriteOptionallyQuoted(name);
}

string Git4DataSourceKey(ClientContext &context, const string &written) {
	// a name may be written any way a table can be named - bare, qualified, quoted - so the key comes from the
	// table it reaches rather than from the text. A name that resolves to nothing is returned as written, and
	// the caller's own error is the one that surfaces.
	auto entry = Git4DataResolveTableOrNull(context, written);
	if (!entry) {
		return written;
	}
	return Git4DataSourceKey(*entry);
}

bool Git4DataSourceKeyMatches(ClientContext &context, const string &stored, const string &key) {
	if (StringUtil::CIEquals(stored, key)) {
		return true;
	}
	// A record written before keys carried a schema holds the name the caller wrote, which the search path
	// resolved when it was written, so the stored name is matched by the table it resolves to as well - and
	// both sides are resolved rather than compared as text, since `t` and `main.t` are one table while `t`
	// under a search path that answers `other.t` is not.
	auto stored_entry = Git4DataResolveTableOrNull(context, stored);
	auto key_entry = Git4DataResolveTableOrNull(context, key);
	if (stored_entry && key_entry) {
		return stored_entry->oid == key_entry->oid;
	}
	// neither name resolves - the table is gone, which is what an apply after a drop looks like - so the two
	// are compared as names: a stored name without a schema is the default schema's, exactly as it was written
	auto stored_name = QualifiedName::Parse(stored);
	auto key_name = QualifiedName::Parse(key);
	auto stored_schema =
	    stored_name.Schema().empty() ? string(DEFAULT_SCHEMA) : stored_name.Schema().GetIdentifierName();
	auto key_schema = key_name.Schema().empty() ? string(DEFAULT_SCHEMA) : key_name.Schema().GetIdentifierName();
	return StringUtil::CIEquals(stored_schema, key_schema) &&
	       StringUtil::CIEquals(stored_name.Name().GetIdentifierName(), key_name.Name().GetIdentifierName());
}

void Git4DataCheckSourceBoundTable(ClientContext &context, const Git4DataBranch &branch, const string &stored_source,
                                   idx_t stored_oid, idx_t requested_oid) {
	// A rename matches by oid and a mapping read back from a file carries no oid at all, so both are left alone:
	// what this refuses is a name that reaches a table the mapping was not made against.
	if (stored_oid == 0 || requested_oid == 0 || stored_oid == requested_oid) {
		return;
	}
	throw InvalidInputException(
	    "Branch \"%s\" has a mapping for \"%s\", and the table carrying that name now is not the one the "
	    "mapping was made against - the table it was made against is no longer reachable under that name. Bind "
	    "the mapping "
	    "again (git4data_bind_branch_delta, git4data_bind_branch_table) or take it back with git4data_unbind.",
	    // `git4data_materialize` was in that list and cannot work here: it copies what the branch *reads*, and
	    // what it reads is the mapping this refusal is about, so it answers with this same sentence (measured,
	    // increment 3cc) - a message that names a way out which refuses with the message is a loop, and the first
	    // version of this one had it.
	    branch.name, stored_source);
}

optional_ptr<const Git4DataStorageTable> Git4DataFindStorageTable(ClientContext &context, const Git4DataBranch &branch,
                                                                  const string &key, idx_t source_oid) {
	for (auto &entry : branch.storage_tables) {
		if (Git4DataSourceKeyMatches(context, entry.source, key) ||
		    (source_oid != 0 && entry.source_oid == source_oid)) {
			return &entry;
		}
	}
	return nullptr;
}

optional_ptr<const Git4DataBranchDelta> Git4DataFindDeltaTable(ClientContext &context, const Git4DataBranch &branch,
                                                               const string &key, idx_t source_oid) {
	for (auto &entry : branch.deltas) {
		if (Git4DataSourceKeyMatches(context, entry.source, key) ||
		    (source_oid != 0 && entry.source_oid == source_oid)) {
			return &entry;
		}
	}
	return nullptr;
}

uint64_t Git4DataNewTableIdentity() {
	auto uuid = UUIDv4::GenerateRandomUUID();
	return uuid.lower == 0 ? 1 : uint64_t(uuid.lower);
}

uint64_t Git4DataMintTableIdentity(TableCatalogEntry &table) {
	// an identity belongs to the *table*, not to the mapping that recorded it: two branches can be given the
	// same table, and minting a fresh value for the second would leave the first reading through a table whose
	// identity no longer matches its row - measured, every read of the first branch was then refused as a swap
	auto current = Git4DataTableIdentity(table);
	if (current != 0) {
		return current;
	}
	auto identity = Git4DataNewTableIdentity();
	table.GetStorage().GetDataTableInfo()->SetPersistedTableId(identity);
	return identity;
}

uint64_t Git4DataTableIdentity(TableCatalogEntry &table) {
	auto &info = table.GetStorage().GetDataTableInfo();
	return info ? info->GetPersistedTableId() : 0;
}

bool Git4DataTableIdentityIsFromDisk(TableCatalogEntry &table) {
	auto &info = table.GetStorage().GetDataTableInfo();
	return info && info->PersistedTableIdIsFromDisk();
}

bool Git4DataMappingNameMatches(const Identifier &schema, const Identifier &table, const string &name) {
	// the row may hold the table's own name, which is how a mapping in the default schema is written
	if (StringUtil::CIEquals(name, table.GetIdentifierName())) {
		return true;
	}
	// or SQL as a caller wrote it, which is parsed into components before anything is compared
	auto parsed = QualifiedName::Parse(name);
	if (!StringUtil::CIEquals(parsed.Name().GetIdentifierName(), table.GetIdentifierName())) {
		return false;
	}
	auto recorded = parsed.Schema().GetIdentifierName();
	if (recorded.empty() || StringUtil::CIEquals(recorded, DEFAULT_SCHEMA)) {
		// an unqualified name is a name in the default schema, which is also the only schema a mapping leaves out
		return StringUtil::CIEquals(schema.GetIdentifierName(), DEFAULT_SCHEMA);
	}
	return StringUtil::CIEquals(recorded, schema.GetIdentifierName());
}

bool Git4DataTableIsMapped(AttachedDatabase &db, const Identifier &schema, const Identifier &table) {
	auto matches = [&](const string &name) {
		return Git4DataMappingNameMatches(schema, table, name);
	};
	auto &transaction_manager = DuckTransactionManager::Get(db);
	for (auto &branch : transaction_manager.branches.List()) {
		for (auto &storage : branch.storage_tables) {
			if (matches(storage.storage)) {
				return true;
			}
		}
		for (auto &delta : branch.deltas) {
			if (matches(delta.added) || matches(delta.removed)) {
				return true;
			}
		}
	}
	return false;
}

bool Git4DataIsRegistrySchema(const string &schema) {
	return StringUtil::CIEquals(schema, GIT4DATA_REGISTRY_SCHEMA);
}

bool Git4DataIsRegistryTable(const TableCatalogEntry &table) {
	return Git4DataIsRegistrySchema(table.ParentSchema().name.GetIdentifierName());
}

void Git4DataCheckRegistryWrite(const TableCatalogEntry &table) {
	if (!Git4DataIsRegistryTable(table)) {
		return;
	}
	throw InvalidInputException(
	    "\"%s\" is one of the tables the reference registry is kept in, and a reference's state cannot be "
	    "edited through SQL: a snapshot's row carries the pin that holds the history it reads, a branch's rows "
	    "say which tables it reads, and the feature's own statements write them. Use the git4data_* pragmas.",
	    table.name.GetIdentifierName());
}

void Git4DataCheckRegistrySchema(const string &schema) {
	if (!StringUtil::CIEquals(schema, GIT4DATA_REGISTRY_SCHEMA)) {
		return;
	}
	throw InvalidInputException(
	    "\"%s\" is the schema the reference registry is kept in, and dropping or altering it would take every "
	    "snapshot and branch with it. Use the git4data_* pragmas to drop a reference.",
	    schema);
}

void Git4DataCheckRegistryCreate(const string &schema, const string &table) {
	if (!StringUtil::CIEquals(schema, GIT4DATA_REGISTRY_SCHEMA)) {
		return;
	}
	throw InvalidInputException(
	    "\"%s\" is in the schema the reference registry is kept in, and that schema is the feature's own: its "
	    "tables are how a reference's state is kept, so a statement cannot create one there. Use the git4data_* "
	    "pragmas.",
	    table);
}

void Git4DataCheckRegistrySchemaCreate(const string &schema) {
	if (!StringUtil::CIEquals(schema, GIT4DATA_REGISTRY_SCHEMA)) {
		return;
	}
	throw InvalidInputException(
	    "\"%s\" is the name of the schema the reference registry is kept in, and the name is reserved: a "
	    "schema of that name is hidden from every listing, and no statement can create or edit anything in it. "
	    "Use a different name.",
	    schema);
}

void Git4DataCheckReferenceAtName(const string &name, bool branch, const string &what) {
	throw BinderException("AT (%s => ...) reads a table as the reference saw it, and \"%s\" is %s: a reference "
	                      "is kept for the table a statement names. Name that table, or read without the clause.",
	                      branch ? "BRANCH" : "SNAPSHOT", name, what);
}

void Git4DataCheckRegistryTarget(const string &schema, const string &table) {
	if (!StringUtil::CIEquals(schema, GIT4DATA_REGISTRY_SCHEMA)) {
		return;
	}
	throw InvalidInputException(
	    "\"%s\" is one of the tables the reference registry is kept in, and a reference's state cannot be "
	    "edited through SQL: a snapshot's row carries the pin that holds the history it reads, and a "
	    "branch's rows say which tables it reads. Use the git4data_* pragmas.",
	    table);
}

} // namespace duckdb
