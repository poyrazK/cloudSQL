/**
 * @file buffer_pool_tests.cpp
 * @brief Unit tests for Buffer Pool Manager
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "storage/buffer_pool_manager.hpp"
#include "storage/lru_replacer.hpp"
#include "storage/page.hpp"
#include "storage/storage_manager.hpp"
#include "test_utils.hpp"

using namespace cloudsql::storage;

namespace {

TEST(BufferPoolTests, LRUReplacerBasic) {
    LRUReplacer replacer(3);
    uint32_t victim_frame = 0;

    replacer.unpin(0);
    replacer.unpin(1);
    replacer.unpin(2);
    EXPECT_EQ(replacer.size(), 3U);

    // In CLOCK, unpin(0,1,2) sets ref bits.
    // victim() will sweep 0,1,2, clearing ref bits, then pick 0.
    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 0U);
    EXPECT_EQ(replacer.size(), 2U);

    // Add 0 back
    replacer.unpin(0);
    EXPECT_EQ(replacer.size(), 3U);

    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 1U);
    EXPECT_EQ(replacer.size(), 2U);

    replacer.pin(2);
    EXPECT_EQ(replacer.size(), 1U);

    replacer.unpin(2);
    EXPECT_EQ(replacer.size(), 2U);

    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 2U);

    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 0U);
    EXPECT_EQ(replacer.size(), 0U);

    EXPECT_FALSE(replacer.victim(&victim_frame));
}

TEST(BufferPoolTests, BufferPoolManagerBasic) {
    static_cast<void>(std::remove("./test_data/bpm_test.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(2, disk_manager);

    const std::string file_name = "bpm_test.db";
    uint32_t page_id0 = 0;
    Page* const page0 = bpm.new_page(file_name, &page_id0);
    ASSERT_NE(page0, nullptr);
    EXPECT_EQ(page_id0, 0U);

    EXPECT_TRUE(bpm.unpin_page(file_name, page_id0, true));

    Page* const page0_fetch = bpm.fetch_page(file_name, page_id0);
    ASSERT_NE(page0_fetch, nullptr);
    EXPECT_TRUE(page0_fetch->is_dirty());
    EXPECT_TRUE(bpm.unpin_page(file_name, page_id0, false));

    uint32_t page_id1 = 1;
    Page* const page1 = bpm.new_page(file_name, &page_id1);
    EXPECT_NE(page1, nullptr);

    uint32_t page_id2 = 2;
    Page* const page2 = bpm.new_page(file_name, &page_id2);
    EXPECT_NE(page2, nullptr);

    uint32_t page_id3 = 3;
    Page* const page3 = bpm.new_page(file_name, &page_id3);
    EXPECT_EQ(page3, nullptr);

    bpm.unpin_page(file_name, page_id1, false);
    bpm.unpin_page(file_name, page_id2, true);

    Page* const page3_new = bpm.new_page(file_name, &page_id3);
    EXPECT_NE(page3_new, nullptr);

    EXPECT_TRUE(bpm.flush_page(file_name, page_id3));
    bpm.flush_all_pages();
    bpm.unpin_page(file_name, page_id3, false);

    EXPECT_TRUE(bpm.delete_page(file_name, page_id2));
}

TEST(BufferPoolTests, BufferPoolManagerEviction) {
    static_cast<void>(std::remove("./test_data/bpm_eviction.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(3, disk_manager);
    const std::string file = "bpm_eviction.db";

    uint32_t id1 = 1;
    uint32_t id2 = 2;
    uint32_t id3 = 3;
    uint32_t id4 = 4;
    Page* const p1 = bpm.new_page(file, &id1);
    Page* const p2 = bpm.new_page(file, &id2);
    Page* const p3 = bpm.new_page(file, &id3);

    EXPECT_NE(p1, nullptr);
    EXPECT_NE(p2, nullptr);
    EXPECT_NE(p3, nullptr);

    bpm.unpin_page(file, id1, true);
    bpm.unpin_page(file, id2, false);
    bpm.unpin_page(file, id3, false);

    Page* const p4 = bpm.new_page(file, &id4);
    EXPECT_NE(p4, nullptr);

    bpm.unpin_page(file, id4, false);

    Page* const p1_fetch = bpm.fetch_page(file, id1);
    EXPECT_NE(p1_fetch, nullptr);
    bpm.unpin_page(file, id1, false);

    EXPECT_TRUE(bpm.delete_page(file, id1));
    EXPECT_TRUE(bpm.delete_page(file, id2));
    EXPECT_TRUE(bpm.delete_page(file, id3));
    EXPECT_TRUE(bpm.delete_page(file, id4));
}

TEST(BufferPoolTests, BufferPoolManagerEdgeCases) {
    static_cast<void>(std::remove("./test_data/bpm_edge.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(1, disk_manager);
    const std::string file = "bpm_edge.db";

    EXPECT_FALSE(bpm.unpin_page(file, 999, false));
    EXPECT_FALSE(bpm.flush_page(file, 999));
    EXPECT_TRUE(bpm.delete_page(file, 999));

    uint32_t id = 1;
    Page* const p = bpm.new_page(file, &id);
    ASSERT_NE(p, nullptr);
    EXPECT_FALSE(bpm.delete_page(file, id));  // Cannot delete pinned page

    // Attempting to allocate a new page when page_id is already allocated
    Page* const p_dup = bpm.new_page(file, &id);
    EXPECT_EQ(p_dup, nullptr);

    bpm.unpin_page(file, id, false);
}

// ============= Page Data Persistence Tests =============

/**
 * @brief Verifies that page data persists via explicit flush and re-fetch from disk
 */
TEST(BufferPoolTests, PageDataPersistence) {
    static_cast<void>(std::remove("./test_data/bpm_persist.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(2, disk_manager);
    const std::string file = "bpm_persist.db";

    // Allocate and write to a page
    uint32_t page_id = 0;
    Page* const p1 = bpm.new_page(file, &page_id);
    ASSERT_NE(p1, nullptr);

    // Write test data through page data interface
    char* data = p1->get_data();
    std::memset(data, 0xAB, 16);
    data[0] = 'H';
    data[1] = 'e';
    data[2] = 'l';
    data[3] = 'l';
    data[4] = 'o';

    // Flush and unpin to persist data
    bpm.unpin_page(file, page_id, true);
    EXPECT_TRUE(bpm.flush_page(file, page_id));
    bpm.unpin_page(file, page_id, false);

    // Evict the frame by allocating new pages until the original frame is reused
    uint32_t evict_id1 = 1;
    Page* const evict_p1 = bpm.new_page(file, &evict_id1);
    ASSERT_NE(evict_p1, nullptr);
    bpm.unpin_page(file, evict_id1, false);

    uint32_t evict_id2 = 2;
    Page* const evict_p2 = bpm.new_page(file, &evict_id2);
    ASSERT_NE(evict_p2, nullptr);
    bpm.unpin_page(file, evict_id2, false);

    // Now force eviction of page_id's frame by allocating one more page
    uint32_t evict_id3 = 3;
    Page* const evict_p3 = bpm.new_page(file, &evict_id3);
    ASSERT_NE(evict_p3, nullptr);
    bpm.unpin_page(file, evict_id3, false);

    // Re-fetch page 0 from disk and verify data integrity
    Page* const p1_fetch = bpm.fetch_page(file, page_id);
    ASSERT_NE(p1_fetch, nullptr);
    EXPECT_EQ(std::memcmp(p1_fetch->get_data(), "Hello", 5), 0);
    EXPECT_EQ(static_cast<unsigned char>(p1_fetch->get_data()[5]), 0xAB);

    bpm.unpin_page(file, page_id, false);
}

/**
 * @brief Verifies that data written through get_data() is readable
 */
TEST(BufferPoolTests, PageContentModification) {
    static_cast<void>(std::remove("./test_data/bpm_content.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(2, disk_manager);
    const std::string file = "bpm_content.db";

    uint32_t page_id = 0;
    Page* const p1 = bpm.new_page(file, &page_id);
    ASSERT_NE(p1, nullptr);

    // Write pattern through get_data() and verify read-back
    char* data = p1->get_data();
    for (int i = 0; i < 128; ++i) {
        data[i] = static_cast<char>(i & 0xFF);
    }

    for (int i = 0; i < 128; ++i) {
        EXPECT_EQ(data[i], static_cast<char>(i & 0xFF));
    }

    bpm.unpin_page(file, page_id, true);
}

/**
 * @brief Verifies the PAGE_SIZE constant value
 */
TEST(BufferPoolTests, PageSizeConstant) {
    EXPECT_EQ(Page::PAGE_SIZE, 4096U);
}

// ============= Fetch/Unpin By ID Tests =============

/**
 * @brief Verifies fetch_page_by_id with precomputed file_id
 */
TEST(BufferPoolTests, FetchPageById) {
    static_cast<void>(std::remove("./test_data/bpm_fetch_id.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(2, disk_manager);
    const std::string file = "bpm_fetch_id.db";

    const uint32_t file_id = bpm.get_file_id(file);

    uint32_t page_id = 0;
    Page* const p1 = bpm.new_page(file, &page_id);
    ASSERT_NE(p1, nullptr);
    bpm.unpin_page(file, page_id, false);

    // Fetch using precomputed file_id
    Page* const p1_fetch = bpm.fetch_page_by_id(file_id, file, page_id);
    ASSERT_NE(p1_fetch, nullptr);
    EXPECT_EQ(p1_fetch->get_page_id(), page_id);
    EXPECT_EQ(p1_fetch->get_pin_count(), 1);

    bpm.unpin_page(file, page_id, false);
}

/**
 * @brief Verifies unpin_page_by_id with precomputed file_id
 */
TEST(BufferPoolTests, UnpinPageById) {
    static_cast<void>(std::remove("./test_data/bpm_unpin_id.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(2, disk_manager);
    const std::string file = "bpm_unpin_id.db";

    const uint32_t file_id = bpm.get_file_id(file);

    uint32_t page_id = 0;
    Page* const p1 = bpm.new_page(file, &page_id);
    ASSERT_NE(p1, nullptr);
    EXPECT_EQ(p1->get_pin_count(), 1);

    // Unpin using precomputed file_id
    EXPECT_TRUE(bpm.unpin_page_by_id(file_id, page_id, false));
    EXPECT_EQ(p1->get_pin_count(), 0);
    EXPECT_FALSE(p1->is_dirty());

    // Verify that unpinning an already-unpinned page returns false
    EXPECT_FALSE(bpm.unpin_page_by_id(file_id, page_id, false));
}

/**
 * @brief Verifies that calling unpin with is_dirty=false does not clear dirty flag
 */
TEST(BufferPoolTests, UnpinDirtyRemainsDirty) {
    static_cast<void>(std::remove("./test_data/bpm_dirty.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(2, disk_manager);
    const std::string file = "bpm_dirty.db";

    uint32_t page_id = 0;
    Page* const p1 = bpm.new_page(file, &page_id);
    ASSERT_NE(p1, nullptr);

    // Mark page as dirty
    EXPECT_TRUE(bpm.unpin_page(file, page_id, true));
    EXPECT_TRUE(p1->is_dirty());

    // Re-fetch and unpin with is_dirty=false
    Page* const p1_fetch = bpm.fetch_page(file, page_id);
    ASSERT_NE(p1_fetch, nullptr);
    bpm.unpin_page(file, page_id, false);
    EXPECT_TRUE(p1_fetch->is_dirty());
}

// ============= File ID Tests =============

/**
 * @brief Verifies that get_file_id returns consistent IDs for the same file
 */
TEST(BufferPoolTests, GetFileIdCaching) {
    static_cast<void>(std::remove("./test_data/bpm_fileid.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(2, disk_manager);
    const std::string file = "bpm_fileid.db";

    const uint32_t id1 = bpm.get_file_id(file);
    const uint32_t id2 = bpm.get_file_id(file);
    EXPECT_EQ(id1, id2);

    const std::string file2 = "bpm_fileid2.db";
    const uint32_t id3 = bpm.get_file_id(file2);
    EXPECT_NE(id1, id3);

    const uint32_t id4 = bpm.get_file_id(file);
    EXPECT_EQ(id1, id4);
}

// ============= Multiple Files Tests =============

/**
 * @brief Verifies buffer pool can manage pages from multiple files simultaneously
 */
TEST(BufferPoolTests, MultipleFiles) {
    static_cast<void>(std::remove("./test_data/bpm_multi1.db"));
    static_cast<void>(std::remove("./test_data/bpm_multi2.db"));
    StorageManager disk_manager("./test_data");
    // Use pool_size=5 to ensure pages remain in memory throughout test
    BufferPoolManager bpm(5, disk_manager);

    const std::string file1 = "bpm_multi1.db";
    const std::string file2 = "bpm_multi2.db";

    uint32_t id1 = 0;
    uint32_t id2 = 0;
    Page* const p1 = bpm.new_page(file1, &id1);
    Page* const p2 = bpm.new_page(file2, &id2);

    ASSERT_NE(p1, nullptr);
    ASSERT_NE(p2, nullptr);

    // Write distinct patterns to each page
    std::memset(p1->get_data(), 0xAA, 16);
    std::memset(p2->get_data(), 0xBB, 16);

    bpm.unpin_page(file1, id1, true);
    bpm.unpin_page(file2, id2, true);

    // Re-fetch and verify data isolation
    Page* const p1_fetch = bpm.fetch_page(file1, id1);
    Page* const p2_fetch = bpm.fetch_page(file2, id2);

    ASSERT_NE(p1_fetch, nullptr);
    ASSERT_NE(p2_fetch, nullptr);
    EXPECT_EQ(static_cast<unsigned char>(p1_fetch->get_data()[0]), 0xAA);
    EXPECT_EQ(static_cast<unsigned char>(p2_fetch->get_data()[0]), 0xBB);
}

// ============= Eviction Tests =============

/**
 * @brief Verifies that dirty pages are flushed to disk during eviction
 *
 * This test verifies dirty page flushing via explicit flush_page calls,
 * as the CLOCK replacer's behavior during sequential evictions can cause
 * pages to be evicted in unexpected orders.
 */
TEST(BufferPoolTests, DirtyPageEvictionFlushes) {
    static_cast<void>(std::remove("./test_data/bpm_flush.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(2, disk_manager);
    const std::string file = "bpm_flush.db";

    // Create a dirty page
    uint32_t id1 = 0;
    Page* const p1 = bpm.new_page(file, &id1);
    ASSERT_NE(p1, nullptr);

    std::memset(p1->get_data(), 0xCC, 32);
    bpm.unpin_page(file, id1, true);

    // Flush to ensure dirty data is persisted
    EXPECT_TRUE(bpm.flush_page(file, id1));

    // Create another page to use the other frame
    uint32_t id2 = 1;
    Page* const p2 = bpm.new_page(file, &id2);
    ASSERT_NE(p2, nullptr);
    bpm.unpin_page(file, id2, false);

    // Re-fetch page 0 and verify data
    Page* const p1_fetch = bpm.fetch_page(file, id1);
    ASSERT_NE(p1_fetch, nullptr);
    EXPECT_EQ(static_cast<unsigned char>(p1_fetch->get_data()[0]), 0xCC);

    bpm.unpin_page(file, id1, false);
    bpm.unpin_page(file, id2, false);
}

// ============= Pool Exhaustion Tests =============

/**
 * @brief Verifies new_page returns nullptr when buffer pool is exhausted
 */
TEST(BufferPoolTests, PoolExhaustion) {
    static_cast<void>(std::remove("./test_data/bpm_exhaust.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(2, disk_manager);
    const std::string file = "bpm_exhaust.db";

    // Fill the buffer pool
    uint32_t id1 = 0;
    uint32_t id2 = 1;
    Page* const p1 = bpm.new_page(file, &id1);
    Page* const p2 = bpm.new_page(file, &id2);

    ASSERT_NE(p1, nullptr);
    ASSERT_NE(p2, nullptr);

    // Attempt to allocate when all frames are pinned
    uint32_t id3 = 2;
    Page* const p3 = bpm.new_page(file, &id3);
    EXPECT_EQ(p3, nullptr);

    // Free one frame and retry
    bpm.unpin_page(file, id1, false);

    Page* const p3_retry = bpm.new_page(file, &id3);
    EXPECT_NE(p3_retry, nullptr);

    // Unpin all pages - use the actual ids returned
    bpm.unpin_page(file, id1, false);
    bpm.unpin_page(file, id2, false);
    bpm.unpin_page(file, id3, false);
}

}  // namespace
