/**
 * @file catalog_coverage_tests.cpp
 * @brief Targeted unit tests to increase coverage of the Catalog module
 */

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/value.hpp"
#include "distributed/raft_types.hpp"

using namespace cloudsql;

namespace {

/**
 * @brief Tests Catalog behavior with missing entities and invalid lookups.
 */
TEST(CatalogCoverageTests, MissingEntities) {
    auto catalog = Catalog::create();

    // Invalid table lookup
    EXPECT_FALSE(catalog->get_table(9999).has_value());
    EXPECT_FALSE(catalog->table_exists(9999));
    EXPECT_FALSE(catalog->table_exists_by_name("non_existent"));
    EXPECT_FALSE(catalog->get_table_by_name("non_existent").has_value());

    // Invalid index lookup
    EXPECT_FALSE(catalog->get_index(8888).has_value());
    EXPECT_TRUE(catalog->get_table_indexes(9999).empty());

    // Dropping non-existent entities
    EXPECT_FALSE(catalog->drop_table(9999));
    EXPECT_FALSE(catalog->drop_index(8888));

    // Update stats for non-existent table
    EXPECT_FALSE(catalog->update_table_stats(9999, 100));
}

/**
 * @brief Tests Catalog behavior with duplicate entities and creation edge cases.
 */
TEST(CatalogCoverageTests, DuplicateEntities) {
    auto catalog = Catalog::create();
    std::vector<ColumnInfo> cols = {{"id", common::ValueType::TYPE_INT64, 0}};

    oid_t tid = catalog->create_table("test_table", cols);
    ASSERT_NE(tid, 0);

    // Duplicate table creation should throw
    EXPECT_THROW(catalog->create_table("test_table", cols), std::runtime_error);

    // Create an index
    oid_t iid = catalog->create_index("idx_id", tid, {0}, IndexType::BTree, true);
    ASSERT_NE(iid, 0);

    // Duplicate index creation should throw
    EXPECT_THROW(catalog->create_index("idx_id", tid, {0}, IndexType::BTree, true),
                 std::runtime_error);

    // Creating index on missing table
    EXPECT_EQ(catalog->create_index("idx_missing", 9999, {0}, IndexType::BTree, false), 0);
}

/**
 * @brief Helper to serialize CreateTable command for Raft simulation
 */
std::vector<uint8_t> serialize_create_table(const std::string& name,
                                            const std::vector<ColumnInfo>& columns) {
    std::vector<uint8_t> data;
    data.push_back(1);  // Type 1

    uint32_t name_len = name.size();
    size_t off = data.size();
    data.resize(off + 4 + name_len);
    std::memcpy(data.data() + off, &name_len, 4);
    std::memcpy(data.data() + off + 4, name.data(), name_len);

    uint32_t col_count = columns.size();
    off = data.size();
    data.resize(off + 4);
    std::memcpy(data.data() + off, &col_count, 4);

    for (const auto& col : columns) {
        uint32_t cname_len = col.name.size();
        off = data.size();
        data.resize(off + 4 + cname_len + 1 + 2);
        std::memcpy(data.data() + off, &cname_len, 4);
        std::memcpy(data.data() + off + 4, col.name.data(), cname_len);
        data[off + 4 + cname_len] = static_cast<uint8_t>(col.type);
        std::memcpy(data.data() + off + 4 + cname_len + 1, &col.position, 2);
    }

    uint32_t shard_count = 1;
    off = data.size();
    data.resize(off + 4);
    std::memcpy(data.data() + off, &shard_count, 4);

    std::string addr = "127.0.0.1";
    uint32_t addr_len = addr.size();
    uint32_t sid = 0;
    uint16_t port = 6441;

    off = data.size();
    data.resize(off + 4 + addr_len + 4 + 2);
    std::memcpy(data.data() + off, &addr_len, 4);
    std::memcpy(data.data() + off + 4, addr.data(), addr_len);
    std::memcpy(data.data() + off + 4 + addr_len, &sid, 4);
    std::memcpy(data.data() + off + 4 + addr_len + 4, &port, 2);

    return data;
}

/**
 * @brief Tests the Raft state machine application (apply) in the Catalog.
 */
TEST(CatalogCoverageTests, RaftApply) {
    auto catalog = Catalog::create();

    // 1. Replay CreateTable
    std::vector<ColumnInfo> cols = {{"id", common::ValueType::TYPE_INT64, 0}};
    std::vector<uint8_t> create_data = serialize_create_table("raft_table", cols);

    raft::LogEntry entry;
    entry.term = 1;
    entry.index = 1;
    entry.data = create_data;

    catalog->apply(entry);
    EXPECT_TRUE(catalog->table_exists_by_name("raft_table"));

    auto table_opt = catalog->get_table_by_name("raft_table");
    ASSERT_TRUE(table_opt.has_value());
    oid_t tid = (*table_opt)->table_id;

    // 2. Replay DropTable
    std::vector<uint8_t> drop_data;
    drop_data.push_back(2);  // Type 2
    drop_data.resize(5);
    std::memcpy(drop_data.data() + 1, &tid, 4);

    entry.index = 2;
    entry.data = drop_data;

    catalog->apply(entry);
    EXPECT_FALSE(catalog->table_exists(tid));
    EXPECT_FALSE(catalog->table_exists_by_name("raft_table"));

    // 3. Replay with empty data (should do nothing)
    entry.index = 3;
    entry.data.clear();
    catalog->apply(entry);
}

// ============= New Tests =============

/**
 * @brief Tests basic table creation and retrieval by ID
 */
TEST(CatalogCoverageTests, CreateAndGetTable) {
    auto catalog = Catalog::create();
    std::vector<ColumnInfo> cols = {{"id", common::ValueType::TYPE_INT64, 0},
                                    {"name", common::ValueType::TYPE_TEXT, 1}};

    oid_t tid = catalog->create_table("users", cols);
    ASSERT_NE(tid, 0);

    // Retrieve by ID
    auto table_opt = catalog->get_table(tid);
    ASSERT_TRUE(table_opt.has_value());
    EXPECT_EQ((*table_opt)->name, "users");
    EXPECT_EQ((*table_opt)->num_columns(), 2);
}

/**
 * @brief Tests table creation and retrieval by name
 */
TEST(CatalogCoverageTests, CreateAndGetTableByName) {
    auto catalog = Catalog::create();
    std::vector<ColumnInfo> cols = {{"id", common::ValueType::TYPE_INT64, 0}};

    oid_t tid = catalog->create_table("products", cols);
    ASSERT_NE(tid, 0);

    // Retrieve by name
    auto table_opt = catalog->get_table_by_name("products");
    ASSERT_TRUE(table_opt.has_value());
    EXPECT_EQ((*table_opt)->table_id, tid);
}

/**
 * @brief Tests get_all_tables returns created tables
 */
TEST(CatalogCoverageTests, GetAllTables) {
    auto catalog = Catalog::create();
    std::vector<ColumnInfo> cols = {{"id", common::ValueType::TYPE_INT64, 0}};

    catalog->create_table("table1", cols);
    catalog->create_table("table2", cols);
    catalog->create_table("table3", cols);

    auto tables = catalog->get_all_tables();
    EXPECT_EQ(tables.size(), 3);
}

/**
 * @brief Tests get_table_indexes returns created indexes
 */
TEST(CatalogCoverageTests, GetTableIndexes) {
    auto catalog = Catalog::create();
    std::vector<ColumnInfo> cols = {{"id", common::ValueType::TYPE_INT64, 0}};

    oid_t tid = catalog->create_table("indexed_table", cols);
    ASSERT_NE(tid, 0);

    oid_t idx1_id = catalog->create_index("idx1", tid, {0}, IndexType::BTree, false);
    ASSERT_NE(idx1_id, 0);
    oid_t idx2_id = catalog->create_index("idx2", tid, {0}, IndexType::Hash, true);
    ASSERT_NE(idx2_id, 0);

    auto indexes = catalog->get_table_indexes(tid);
    EXPECT_EQ(indexes.size(), 2);
}

/**
 * @brief Tests catalog save and load functionality
 *
 * Note: save() and load() are stubs that don't fully persist table data.
 * save() writes a header comment but no table data.
 * load() reads but doesn't parse table entries.
 */
TEST(CatalogCoverageTests, SaveAndLoad) {
    auto catalog = Catalog::create();
    std::vector<ColumnInfo> cols = {{"id", common::ValueType::TYPE_INT64, 0}};
    catalog->create_table("persisted_table", cols);

    // save() and load() are stubs that don't fully persist table data.
    // save() writes a header comment but no table data.
    // load() reads but doesn't parse table entries.
    // This test verifies the save/load cycle works without crashing.
    std::string temp_path = "./test_data/catalog_save_load_test.bin";
    std::filesystem::remove(temp_path);

    // Save catalog - stub implementation writes header only
    EXPECT_NO_THROW(catalog->save(temp_path));

    // Create new catalog and load - stub implementation returns true
    auto loaded_catalog = Catalog::create();
    EXPECT_NO_THROW(loaded_catalog->load(temp_path));

    // Cleanup
    std::filesystem::remove(temp_path);
}

/**
 * @brief Tests version increments after catalog operations
 */
TEST(CatalogCoverageTests, VersionIncrement) {
    auto catalog = Catalog::create();
    uint64_t initial_version = catalog->get_version();

    std::vector<ColumnInfo> cols = {{"id", common::ValueType::TYPE_INT64, 0}};
    catalog->create_table("versioned_table", cols);

    EXPECT_GT(catalog->get_version(), initial_version);
}

/**
 * @brief Tests that print() doesn't crash
 */
TEST(CatalogCoverageTests, PrintDoesNotCrash) {
    auto catalog = Catalog::create();
    std::vector<ColumnInfo> cols = {{"id", common::ValueType::TYPE_INT64, 0}};
    catalog->create_table("printed_table", cols);

    // Should not throw or crash
    EXPECT_NO_THROW(catalog->print());
}

}  // namespace
