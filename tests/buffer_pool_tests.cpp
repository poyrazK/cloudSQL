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

    replacer.unpin(1);
    replacer.unpin(2);
    replacer.unpin(3);
    EXPECT_EQ(replacer.size(), 3U);

    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 1U);
    EXPECT_EQ(replacer.size(), 2U);

    replacer.unpin(4);
    EXPECT_EQ(replacer.size(), 3U);

    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 2U);
    EXPECT_EQ(replacer.size(), 2U);

    replacer.pin(3);
    EXPECT_EQ(replacer.size(), 1U);

    replacer.unpin(3);
    EXPECT_EQ(replacer.size(), 2U);

    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 4U);

    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 3U);
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
    EXPECT_FALSE(bpm.delete_page(file, id));  // Pinned

    // new page again with same ID
    Page* const p_dup = bpm.new_page(file, &id);
    EXPECT_EQ(p_dup, nullptr);

    bpm.unpin_page(file, id, false);
}

}  // namespace
