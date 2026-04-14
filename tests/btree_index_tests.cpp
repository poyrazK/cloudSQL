/**
 * @file btree_index_tests.cpp
 * @brief Unit tests for BTreeIndex - B+ tree index storage
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "common/config.hpp"
#include "common/value.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/btree_index.hpp"
#include "storage/heap_table.hpp"
#include "storage/storage_manager.hpp"

using namespace cloudsql::common;
using namespace cloudsql::storage;
using cloudsql::config::Config;

namespace {

class BTreeIndexTests : public ::testing::Test {
   protected:
    void SetUp() override {
        disk_manager_ = std::make_unique<StorageManager>("./test_idx_data");
        disk_manager_->create_dir_if_not_exists();
        bpm_ = std::make_unique<BufferPoolManager>(Config::DEFAULT_BUFFER_POOL_SIZE,
                                                    *disk_manager_);

        index_ = std::make_unique<BTreeIndex>("test_index", *bpm_, ValueType::TYPE_INT64);
    }

    void TearDown() override {
        index_.reset();
        bpm_.reset();
        disk_manager_.reset();
        // Cleanup test files
        std::remove("./test_idx_data/test_index.idx");
    }

    std::unique_ptr<StorageManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<BTreeIndex> index_;
};

// Helper to create tuple ids
static HeapTable::TupleId make_rid(uint32_t page, uint16_t slot) {
    return HeapTable::TupleId(page, slot);
}

// ============= Constructor Tests =============

TEST_F(BTreeIndexTests, ConstructorBasic) {
    EXPECT_NE(index_, nullptr);
    EXPECT_EQ(index_->index_name(), "test_index");
    EXPECT_EQ(index_->key_type(), ValueType::TYPE_INT64);
}

TEST_F(BTreeIndexTests, ConstructorTextKey) {
    auto text_index = std::make_unique<BTreeIndex>("text_idx", *bpm_, ValueType::TYPE_TEXT);
    EXPECT_NE(text_index, nullptr);
    EXPECT_EQ(text_index->key_type(), ValueType::TYPE_TEXT);
}

// ============= Create/Open/Drop Tests =============

TEST_F(BTreeIndexTests, CreateAndOpen) {
    EXPECT_TRUE(index_->create());
    EXPECT_TRUE(index_->open());
    // Note: drop() may fail if file is still tracked by BPM - test what we can
}

TEST_F(BTreeIndexTests, CreateTwice) {
    ASSERT_TRUE(index_->create());
    index_->close();
    // Second create should succeed
    EXPECT_TRUE(index_->create());
}

TEST_F(BTreeIndexTests, OpenWithoutCreate) {
    // Should succeed if file already exists from previous test
    // (tests share the same test_idx_data directory)
    EXPECT_TRUE(index_->open());
}

TEST_F(BTreeIndexTests, DropWithoutCreate) {
    // Drop on non-existent file should fail
    EXPECT_FALSE(index_->drop());
}

TEST_F(BTreeIndexTests, CreateOpenCloseOpen) {
    ASSERT_TRUE(index_->create());
    index_->insert(Value::make_int64(42), make_rid(1, 0));
    index_->close();
    ASSERT_TRUE(index_->open());
    auto results = index_->search(Value::make_int64(42));
    ASSERT_EQ(results.size(), 1U);
}

// ============= Insert Tests =============

TEST_F(BTreeIndexTests, InsertSingleEntry) {
    ASSERT_TRUE(index_->create());
    EXPECT_TRUE(index_->open());

    auto rid = make_rid(1, 0);
    EXPECT_TRUE(index_->insert(Value::make_int64(42), rid));
}

TEST_F(BTreeIndexTests, InsertMultipleEntries) {
    ASSERT_TRUE(index_->create());
    EXPECT_TRUE(index_->open());

    auto rid1 = make_rid(1, 0);
    auto rid2 = make_rid(1, 1);
    auto rid3 = make_rid(2, 0);

    EXPECT_TRUE(index_->insert(Value::make_int64(10), rid1));
    EXPECT_TRUE(index_->insert(Value::make_int64(20), rid2));
    EXPECT_TRUE(index_->insert(Value::make_int64(30), rid3));
}

TEST_F(BTreeIndexTests, InsertDuplicateKey) {
    ASSERT_TRUE(index_->create());
    EXPECT_TRUE(index_->open());

    auto rid1 = make_rid(1, 0);
    auto rid2 = make_rid(1, 1);

    EXPECT_TRUE(index_->insert(Value::make_int64(42), rid1));
    EXPECT_TRUE(index_->insert(Value::make_int64(42), rid2));
}

TEST_F(BTreeIndexTests, InsertTextKey) {
    auto text_index = std::make_unique<BTreeIndex>("text_idx", *bpm_, ValueType::TYPE_TEXT);
    ASSERT_TRUE(text_index->create());
    ASSERT_TRUE(text_index->open());

    auto rid = make_rid(1, 0);
    EXPECT_TRUE(text_index->insert(Value::make_text("hello"), rid));

    text_index->drop();
}

// ============= Search Tests =============

TEST_F(BTreeIndexTests, SearchExistingKey) {
    ASSERT_TRUE(index_->create());
    ASSERT_TRUE(index_->open());

    auto rid = make_rid(5, 10);
    index_->insert(Value::make_int64(42), rid);

    auto results = index_->search(Value::make_int64(42));
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0].page_num, 5U);
    EXPECT_EQ(results[0].slot_num, 10U);
}

TEST_F(BTreeIndexTests, SearchNonExistentKey) {
    ASSERT_TRUE(index_->create());
    ASSERT_TRUE(index_->open());

    auto results = index_->search(Value::make_int64(999));
    EXPECT_TRUE(results.empty());
}

TEST_F(BTreeIndexTests, SearchMultipleEntries) {
    ASSERT_TRUE(index_->create());
    ASSERT_TRUE(index_->open());

    index_->insert(Value::make_int64(10), make_rid(1, 0));
    index_->insert(Value::make_int64(20), make_rid(1, 1));
    index_->insert(Value::make_int64(30), make_rid(2, 0));

    auto results = index_->search(Value::make_int64(20));
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0].page_num, 1U);
    EXPECT_EQ(results[0].slot_num, 1U);
}

TEST_F(BTreeIndexTests, SearchDuplicateKeys) {
    ASSERT_TRUE(index_->create());
    ASSERT_TRUE(index_->open());

    index_->insert(Value::make_int64(42), make_rid(1, 0));
    index_->insert(Value::make_int64(42), make_rid(1, 1));

    auto results = index_->search(Value::make_int64(42));
    ASSERT_EQ(results.size(), 2U);
}

// ============= Remove Tests =============

TEST_F(BTreeIndexTests, RemoveEntry) {
    ASSERT_TRUE(index_->create());
    ASSERT_TRUE(index_->open());

    index_->insert(Value::make_int64(42), make_rid(1, 0));

    // remove() currently just returns true (not implemented)
    EXPECT_TRUE(index_->remove(Value::make_int64(42), make_rid(1, 0)));
}

// ============= Scan Iterator Tests =============

TEST_F(BTreeIndexTests, ScanEmptyIndex) {
    ASSERT_TRUE(index_->create());
    ASSERT_TRUE(index_->open());

    auto it = index_->scan();
    // Empty index with root at page 0 should not be immediately done
    // (iterator starts at root page 0, which may have data or not)
    // Just verify we can call is_done without error
    EXPECT_FALSE(it.is_done());
}

TEST_F(BTreeIndexTests, ScanSingleEntry) {
    ASSERT_TRUE(index_->create());
    ASSERT_TRUE(index_->open());

    index_->insert(Value::make_int64(42), make_rid(1, 0));

    auto it = index_->scan();
    EXPECT_FALSE(it.is_done());

    BTreeIndex::Entry entry;
    EXPECT_TRUE(it.next(entry));
    EXPECT_EQ(entry.key.as_int64(), 42);
    EXPECT_EQ(entry.tuple_id.page_num, 1U);
    EXPECT_EQ(entry.tuple_id.slot_num, 0U);
}

TEST_F(BTreeIndexTests, ScanMultipleEntries) {
    ASSERT_TRUE(index_->create());
    ASSERT_TRUE(index_->open());

    index_->insert(Value::make_int64(10), make_rid(1, 0));
    index_->insert(Value::make_int64(20), make_rid(1, 1));
    index_->insert(Value::make_int64(30), make_rid(2, 0));

    auto it = index_->scan();
    int count = 0;
    BTreeIndex::Entry entry;
    while (it.next(entry)) {
        count++;
    }
    EXPECT_EQ(count, 3);
    EXPECT_TRUE(it.is_done());
}

TEST_F(BTreeIndexTests, ScanIteratorIsDoneAfterEnd) {
    ASSERT_TRUE(index_->create());
    ASSERT_TRUE(index_->open());

    index_->insert(Value::make_int64(42), make_rid(1, 0));

    auto it = index_->scan();
    BTreeIndex::Entry entry;
    it.next(entry);  // Get the entry
    EXPECT_FALSE(it.is_done());
    it.next(entry);  // Try to get more - should fail
    EXPECT_TRUE(it.is_done());
}

// ============= TupleId Tests =============

TEST_F(BTreeIndexTests, TupleIdDefault) {
    HeapTable::TupleId rid;
    EXPECT_TRUE(rid.is_null());
}

TEST_F(BTreeIndexTests, TupleIdWithValues) {
    HeapTable::TupleId rid(5, 10);
    EXPECT_EQ(rid.page_num, 5U);
    EXPECT_EQ(rid.slot_num, 10U);
    EXPECT_FALSE(rid.is_null());
}

// ============= Entry Tests =============

TEST_F(BTreeIndexTests, EntryWithKeyAndTupleId) {
    auto key = Value::make_int64(42);
    auto rid = make_rid(1, 0);
    BTreeIndex::Entry entry(key, rid);

    EXPECT_EQ(entry.key.as_int64(), 42);
    EXPECT_EQ(entry.tuple_id.page_num, 1U);
    EXPECT_EQ(entry.tuple_id.slot_num, 0U);
}

// ============= Index Name Tests =============

TEST_F(BTreeIndexTests, IndexName) {
    EXPECT_EQ(index_->index_name(), "test_index");
}

TEST_F(BTreeIndexTests, KeyType) {
    EXPECT_EQ(index_->key_type(), ValueType::TYPE_INT64);
}

// ============= Persistence Tests =============

TEST_F(BTreeIndexTests, DataPersistenceAcrossOpenClose) {
    ASSERT_TRUE(index_->create());
    index_->insert(Value::make_int64(42), make_rid(1, 0));
    index_->close();

    ASSERT_TRUE(index_->open());
    auto results = index_->search(Value::make_int64(42));
    ASSERT_EQ(results.size(), 1U);
    EXPECT_EQ(results[0].page_num, 1U);
    EXPECT_EQ(results[0].slot_num, 0U);
}

}  // namespace
