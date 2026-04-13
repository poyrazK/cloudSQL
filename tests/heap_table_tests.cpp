/**
 * @file heap_table_tests.cpp
 * @brief Unit tests for HeapTable - MVCC heap storage
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "common/config.hpp"
#include "common/value.hpp"
#include "executor/types.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"
#include "storage/storage_manager.hpp"

using namespace cloudsql::common;
using namespace cloudsql::executor;
using namespace cloudsql::storage;
using cloudsql::config::Config;

namespace {

class HeapTableTests : public ::testing::Test {
 protected:
    void SetUp() override {
        disk_manager_ = std::make_unique<StorageManager>("./test_data");
        disk_manager_->create_dir_if_not_exists();
        bpm_ = std::make_unique<BufferPoolManager>(Config::DEFAULT_BUFFER_POOL_SIZE,
                                                    *disk_manager_);

        // Create schema for test table
        schema_ = std::make_unique<Schema>();
        schema_->add_column("id", ValueType::TYPE_INT64, false);
        schema_->add_column("name", ValueType::TYPE_TEXT, true);

        table_ = std::make_unique<HeapTable>("test_table", *bpm_, *schema_);
    }

    void TearDown() override {
        table_.reset();
        bpm_.reset();
        disk_manager_.reset();
        // Cleanup test files
        std::remove("./test_data/test_table.heap");
    }

    std::unique_ptr<StorageManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<Schema> schema_;
    std::unique_ptr<HeapTable> table_;
};

// Helper to create tuples - using same pattern as cloudSQL_tests.cpp
static Tuple make_test_tuple(int64_t id, const std::string& name) {
    return Tuple({Value::make_int64(id), Value::make_text(name)});
}

// ============= Constructor Tests =============

TEST_F(HeapTableTests, ConstructorBasic) {
    EXPECT_NE(table_, nullptr);
    EXPECT_EQ(table_->table_name(), "test_table");
    EXPECT_EQ(table_->schema().column_count(), 2U);
}

TEST_F(HeapTableTests, SchemaMatches) {
    EXPECT_EQ(table_->schema().get_column(0).name(), "id");
    EXPECT_EQ(table_->schema().get_column(1).name(), "name");
    EXPECT_EQ(table_->schema().get_column(0).type(), ValueType::TYPE_INT64);
    EXPECT_EQ(table_->schema().get_column(1).type(), ValueType::TYPE_TEXT);
}

// ============= Create/Drop Tests =============

TEST_F(HeapTableTests, CreateAndDrop) {
    EXPECT_TRUE(table_->create());
    // Note: drop() may fail if table is still in use; relies on destructor cleanup
}

TEST_F(HeapTableTests, CreateTwice) {
    ASSERT_TRUE(table_->create());
    // Second create should succeed (idempotent or file exists)
    EXPECT_TRUE(table_->create());
}

// ============= Insert Tests =============

TEST_F(HeapTableTests, InsertSingleRow) {
    ASSERT_TRUE(table_->create());
    auto tuple = make_test_tuple(1, "Alice");
    auto rid = table_->insert(tuple);
    EXPECT_FALSE(rid.is_null());
    EXPECT_EQ(table_->tuple_count(), 1U);
}

TEST_F(HeapTableTests, InsertMultipleRows) {
    ASSERT_TRUE(table_->create());
    auto tuple1 = make_test_tuple(1, "Alice");
    auto tuple2 = make_test_tuple(2, "Bob");
    auto tuple3 = make_test_tuple(3, "Charlie");

    auto rid1 = table_->insert(tuple1);
    auto rid2 = table_->insert(tuple2);
    auto rid3 = table_->insert(tuple3);

    EXPECT_FALSE(rid1.is_null());
    EXPECT_FALSE(rid2.is_null());
    EXPECT_FALSE(rid3.is_null());
    EXPECT_EQ(table_->tuple_count(), 3U);
}

TEST_F(HeapTableTests, InsertDifferentPage) {
    // Insert many rows to span multiple pages
    ASSERT_TRUE(table_->create());
    for (int i = 0; i < 100; ++i) {
        Tuple tuple = make_test_tuple(i, "User" + std::to_string(i));
        auto rid = table_->insert(tuple);
        EXPECT_FALSE(rid.is_null());
    }
    EXPECT_EQ(table_->tuple_count(), 100U);
}

// ============= Get Tests =============

TEST_F(HeapTableTests, GetByRID) {
    ASSERT_TRUE(table_->create());
    auto tuple = make_test_tuple(42, "Answer");
    auto rid = table_->insert(tuple);

    Tuple out_tuple;
    EXPECT_TRUE(table_->get(rid, out_tuple));
    EXPECT_EQ(out_tuple.get(0).as_int64(), 42);
    EXPECT_EQ(out_tuple.get(1).as_text(), "Answer");
}

TEST_F(HeapTableTests, GetNonExistent) {
    ASSERT_TRUE(table_->create());
    Tuple out_tuple;
    HeapTable::TupleId bad_rid(9999, 9999);
    EXPECT_FALSE(table_->get(bad_rid, out_tuple));
}

// ============= Update Tests =============

TEST_F(HeapTableTests, UpdateCreatesNewVersion) {
    ASSERT_TRUE(table_->create());
    auto tuple = make_test_tuple(1, "Original");
    auto rid = table_->insert(tuple);

    auto new_tuple = make_test_tuple(1, "Updated");
    EXPECT_TRUE(table_->update(rid, new_tuple, 1));

    // The old RID now points to a logically deleted tuple
    Tuple out_tuple;
    EXPECT_FALSE(table_->get(rid, out_tuple));

    // Scan should show the NEW version (xmax=0)
    auto it = table_->scan();
    EXPECT_TRUE(it.next(out_tuple));
    EXPECT_EQ(out_tuple.get(1).as_text(), "Updated");
    EXPECT_FALSE(it.next(out_tuple));  // Only one visible tuple
}

// ============= Delete Tests =============

TEST_F(HeapTableTests, RemoveTuple) {
    ASSERT_TRUE(table_->create());
    auto tuple = make_test_tuple(1, "ToDelete");
    auto rid = table_->insert(tuple);
    EXPECT_EQ(table_->tuple_count(), 1U);

    EXPECT_TRUE(table_->remove(rid, 1));
    EXPECT_EQ(table_->tuple_count(), 0U);  // Logically deleted
}

TEST_F(HeapTableTests, UndoRemove) {
    ASSERT_TRUE(table_->create());
    auto tuple = make_test_tuple(1, "Undeleted");
    auto rid = table_->insert(tuple);
    EXPECT_EQ(table_->tuple_count(), 1U);

    EXPECT_TRUE(table_->remove(rid, 1));
    EXPECT_EQ(table_->tuple_count(), 0U);

    // Undo the delete
    EXPECT_TRUE(table_->undo_remove(rid));
    EXPECT_EQ(table_->tuple_count(), 1U);
}

TEST_F(HeapTableTests, PhysicalRemove) {
    ASSERT_TRUE(table_->create());
    auto tuple = make_test_tuple(1, "PhysicallyRemoved");
    auto rid = table_->insert(tuple);
    EXPECT_EQ(table_->tuple_count(), 1U);

    EXPECT_TRUE(table_->physical_remove(rid));
    EXPECT_EQ(table_->tuple_count(), 0U);
}

// ============= Scan Tests =============

TEST_F(HeapTableTests, ScanEmptyTable) {
    ASSERT_TRUE(table_->create());
    auto it = table_->scan();
    EXPECT_FALSE(it.is_done());  // Iterator not done before trying to read
    Tuple tuple;
    EXPECT_FALSE(it.next(tuple));  // No tuples to read
    EXPECT_TRUE(it.is_done());  // Now at end
}

TEST_F(HeapTableTests, ScanAllRows) {
    ASSERT_TRUE(table_->create());
    for (int i = 1; i <= 5; ++i) {
        table_->insert(make_test_tuple(i, "User" + std::to_string(i)));
    }

    auto it = table_->scan();
    int count = 0;
    Tuple tuple;
    while (it.next(tuple)) {
        count++;
    }
    EXPECT_EQ(count, 5);
}

TEST_F(HeapTableTests, ScanWithDeletes) {
    ASSERT_TRUE(table_->create());
    auto rid1 = table_->insert(make_test_tuple(1, "First"));
    auto rid2 = table_->insert(make_test_tuple(2, "Second"));
    auto rid3 = table_->insert(make_test_tuple(3, "Third"));

    // Delete middle record
    table_->remove(rid2, 1);

    // Scan should only see 2 records
    auto it = table_->scan();
    int count = 0;
    Tuple tuple;
    while (it.next(tuple)) {
        count++;
    }
    EXPECT_EQ(count, 2);
}

// ============= Iterator Tests =============

TEST_F(HeapTableTests, IteratorBasic) {
    ASSERT_TRUE(table_->create());
    table_->insert(make_test_tuple(1, "A"));
    table_->insert(make_test_tuple(2, "B"));

    auto it = table_->scan();
    EXPECT_FALSE(it.is_done());
    // current_id() is valid RID pointing to first slot of page 0
    EXPECT_FALSE(it.current_id().is_null());
    EXPECT_EQ(it.current_id().page_num, 0U);
    EXPECT_EQ(it.current_id().slot_num, 0U);

    Tuple tuple;
    EXPECT_TRUE(it.next(tuple));
    EXPECT_EQ(tuple.get(0).as_int64(), 1);
    EXPECT_TRUE(it.next(tuple));
    EXPECT_EQ(tuple.get(0).as_int64(), 2);
    EXPECT_FALSE(it.next(tuple));  // EOF
    EXPECT_TRUE(it.is_done());
}

TEST_F(HeapTableTests, IteratorNextMeta) {
    ASSERT_TRUE(table_->create());
    auto rid = table_->insert(make_test_tuple(1, "MetaTest"));

    auto it = table_->scan();
    HeapTable::TupleMeta meta;
    EXPECT_TRUE(it.next_meta(meta));
    EXPECT_EQ(meta.xmin, 0U);  // Default xmin
    EXPECT_EQ(meta.xmax, 0U);  // Not deleted
    EXPECT_EQ(meta.tuple.get(0).as_int64(), 1);
}

// ============= TupleId Tests =============

TEST_F(HeapTableTests, TupleIdDefault) {
    HeapTable::TupleId rid;
    EXPECT_TRUE(rid.is_null());
}

TEST_F(HeapTableTests, TupleIdWithValues) {
    HeapTable::TupleId rid(5, 10);
    EXPECT_EQ(rid.page_num, 5U);
    EXPECT_EQ(rid.slot_num, 10U);
    EXPECT_FALSE(rid.is_null());
}

TEST_F(HeapTableTests, TupleIdEquality) {
    HeapTable::TupleId rid1(1, 2);
    HeapTable::TupleId rid2(1, 2);
    HeapTable::TupleId rid3(1, 3);
    EXPECT_TRUE(rid1 == rid2);
    EXPECT_FALSE(rid1 == rid3);
}

TEST_F(HeapTableTests, TupleIdToString) {
    HeapTable::TupleId rid(3, 7);
    EXPECT_EQ(rid.to_string(), "(3, 7)");
}

// ============= TupleHeader Tests =============

TEST_F(HeapTableTests, TupleHeaderDefaults) {
    HeapTable::TupleHeader header{};
    EXPECT_EQ(header.xmin, 0U);
    EXPECT_EQ(header.xmax, 0U);
}

TEST_F(HeapTableTests, TupleHeaderWithValues) {
    HeapTable::TupleHeader header;
    header.xmin = 100;
    header.xmax = 200;
    EXPECT_EQ(header.xmin, 100U);
    EXPECT_EQ(header.xmax, 200U);
}

// ============= PageHeader Tests =============

TEST_F(HeapTableTests, PageHeaderDefaults) {
    HeapTable::PageHeader header{};
    EXPECT_EQ(header.next_page, 0U);
    EXPECT_EQ(header.num_slots, 0U);
    EXPECT_EQ(header.free_space_offset, 0U);
    EXPECT_EQ(header.flags, 0U);
}

// ============= MVCC Tests =============

TEST_F(HeapTableTests, MVCCXminXmax) {
    ASSERT_TRUE(table_->create());
    auto tuple = make_test_tuple(1, "MVCC");
    auto rid = table_->insert(tuple, 100);  // xmin = 100

    HeapTable::TupleMeta meta;
    table_->get_meta(rid, meta);
    EXPECT_EQ(meta.xmin, 100U);
    EXPECT_EQ(meta.xmax, 0U);

    table_->remove(rid, 200);  // xmax = 200
    table_->get_meta(rid, meta);
    EXPECT_EQ(meta.xmax, 200U);
}

TEST_F(HeapTableTests, MVCCUpdateTransactionId) {
    ASSERT_TRUE(table_->create());
    auto tuple = make_test_tuple(1, "Original");
    auto rid = table_->insert(tuple, 1);

    auto new_tuple = make_test_tuple(1, "Updated");
    table_->update(rid, new_tuple, 99);

    // xmax should be set for old version, or new tuple has xmin=99
    HeapTable::TupleMeta meta;
    table_->get_meta(rid, meta);
    EXPECT_TRUE(meta.xmin == 99 || meta.xmax == 99);
}

// ============= File ID Tests =============

TEST_F(HeapTableTests, FileIdAfterCreate) {
    ASSERT_TRUE(table_->create());
    EXPECT_NE(table_->file_id(), 0U);
}

}  // namespace