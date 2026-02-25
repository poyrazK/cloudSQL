/**
 * @file buffer_pool_tests.cpp
 * @brief Unit tests for Buffer Pool Manager
 */

#include <cstdio>
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

using cloudsql::tests::tests_failed;
using cloudsql::tests::tests_passed;

TEST(LRUReplacer_Basic) {
    LRUReplacer replacer(3);
    uint32_t victim_frame = 0;

    replacer.unpin(1);
    replacer.unpin(2);
    replacer.unpin(3);
    EXPECT_EQ(replacer.size(), 3u);

    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 1u);
    EXPECT_EQ(replacer.size(), 2u);

    replacer.unpin(4);
    EXPECT_EQ(replacer.size(), 3u);

    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 2u);
    EXPECT_EQ(replacer.size(), 2u);

    replacer.pin(3);
    EXPECT_EQ(replacer.size(), 1u);

    replacer.unpin(3);
    EXPECT_EQ(replacer.size(), 2u);

    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 4u);

    EXPECT_TRUE(replacer.victim(&victim_frame));
    EXPECT_EQ(victim_frame, 3u);
    EXPECT_EQ(replacer.size(), 0u);

    EXPECT_FALSE(replacer.victim(&victim_frame));
}

TEST(BufferPoolManager_Basic) {
    static_cast<void>(std::remove("./test_data/bpm_test.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(2, disk_manager);

    const std::string file_name = "bpm_test.db";
    uint32_t page_id0 = 0;
    Page* page0 = bpm.new_page(file_name, &page_id0);
    EXPECT_TRUE(page0 != nullptr);
    EXPECT_EQ(page_id0, 0u);

    EXPECT_TRUE(bpm.unpin_page(file_name, page_id0, true));

    page0 = bpm.fetch_page(file_name, page_id0);
    EXPECT_TRUE(page0 != nullptr);
    EXPECT_TRUE(page0->is_dirty());
    EXPECT_TRUE(bpm.unpin_page(file_name, page_id0, false));

    uint32_t page_id1 = 1;
    Page* page1 = bpm.new_page(file_name, &page_id1);
    EXPECT_TRUE(page1 != nullptr);

    uint32_t page_id2 = 2;
    Page* page2 = bpm.new_page(file_name, &page_id2);
    EXPECT_TRUE(page2 != nullptr);

    uint32_t page_id3 = 3;
    Page* page3 = bpm.new_page(file_name, &page_id3);
    EXPECT_FALSE(page3 != nullptr);

    bpm.unpin_page(file_name, page_id1, false);
    bpm.unpin_page(file_name, page_id2, true);

    page3 = bpm.new_page(file_name, &page_id3);
    EXPECT_TRUE(page3 != nullptr);

    bpm.flush_page(file_name, page_id3);
    bpm.flush_all_pages();
    bpm.unpin_page(file_name, page_id3, false);

    bpm.delete_page(file_name, page_id2);
}

TEST(BufferPoolManager_Eviction) {
    static_cast<void>(std::remove("./test_data/bpm_eviction.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(3, disk_manager);
    const std::string file = "bpm_eviction.db";

    uint32_t id1 = 1, id2 = 2, id3 = 3, id4 = 4;
    Page* p1 = bpm.new_page(file, &id1);
    Page* p2 = bpm.new_page(file, &id2);
    Page* p3 = bpm.new_page(file, &id3);

    EXPECT_TRUE(p1 != nullptr);
    EXPECT_TRUE(p2 != nullptr);
    EXPECT_TRUE(p3 != nullptr);

    bpm.unpin_page(file, id1, true);
    bpm.unpin_page(file, id2, false);
    bpm.unpin_page(file, id3, false);

    Page* p4 = bpm.new_page(file, &id4);
    EXPECT_TRUE(p4 != nullptr);

    bpm.unpin_page(file, id4, false);

    p1 = bpm.fetch_page(file, id1);
    EXPECT_TRUE(p1 != nullptr);
    bpm.unpin_page(file, id1, false);

    bpm.delete_page(file, id1);
    bpm.delete_page(file, id2);
    bpm.delete_page(file, id3);
    bpm.delete_page(file, id4);
}

TEST(BufferPoolManager_EdgeCases) {
    static_cast<void>(std::remove("./test_data/bpm_edge.db"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager bpm(1, disk_manager);
    const std::string file = "bpm_edge.db";

    EXPECT_FALSE(bpm.unpin_page(file, 999, false));
    EXPECT_FALSE(bpm.flush_page(file, 999));
    EXPECT_TRUE(bpm.delete_page(file, 999));

    uint32_t id = 1;
    Page* p = bpm.new_page(file, &id);
    EXPECT_TRUE(p != nullptr);
    EXPECT_FALSE(bpm.delete_page(file, id));  // Pinned

    // new page again with same ID
    Page* p_dup = bpm.new_page(file, &id);
    EXPECT_TRUE(p_dup == nullptr);

    bpm.unpin_page(file, id, false);
}

}  // namespace

int main() {
    std::cout << "Buffer Pool Unit Tests\n";
    std::cout << "======================\n";

    RUN_TEST(LRUReplacer_Basic);
    RUN_TEST(BufferPoolManager_Basic);
    RUN_TEST(BufferPoolManager_Eviction);
    RUN_TEST(BufferPoolManager_EdgeCases);

    std::cout << "\nResults: \n" << tests_passed << " passed, \n" << tests_failed << " failed\n";
    return (tests_failed > 0);
}
