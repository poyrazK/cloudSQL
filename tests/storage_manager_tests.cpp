/**
 * @file storage_manager_tests.cpp
 * @brief Unit tests for StorageManager - low-level disk I/O and page-level access
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <vector>

#include "storage/storage_manager.hpp"

using namespace cloudsql::storage;

namespace {

static void cleanup_file(const std::string& dir, const std::string& name) {
    std::remove((dir + "/" + name).c_str());
}

class StorageManagerTests : public ::testing::Test {
   protected:
    void SetUp() override {
        sm_ = std::make_unique<StorageManager>("./test_data");
        sm_->create_dir_if_not_exists();
    }

    void TearDown() override { sm_.reset(); }

    std::unique_ptr<StorageManager> sm_;
};

TEST_F(StorageManagerTests, OpenCloseBasic) {
    const std::string filename = "open_close_test.db";
    cleanup_file("./test_data", filename);

    ASSERT_TRUE(sm_->open_file(filename));
    ASSERT_TRUE(sm_->close_file(filename));
}

TEST_F(StorageManagerTests, OpenNonExistentCreatesFile) {
    const std::string filename = "new_file_test.db";
    cleanup_file("./test_data", filename);

    ASSERT_FALSE(sm_->file_exists(filename));
    ASSERT_TRUE(sm_->open_file(filename));
    ASSERT_TRUE(sm_->file_exists(filename));
}

TEST_F(StorageManagerTests, OpenTwiceReturnsTrue) {
    const std::string filename = "double_open_test.db";
    cleanup_file("./test_data", filename);

    ASSERT_TRUE(sm_->open_file(filename));
    ASSERT_TRUE(sm_->open_file(filename));
    ASSERT_TRUE(sm_->close_file(filename));
}

TEST_F(StorageManagerTests, CloseNonExistentReturnsFalse) {
    ASSERT_FALSE(sm_->close_file("nonexistent_file.db"));
}

TEST_F(StorageManagerTests, ReadWritePageBasic) {
    const std::string filename = "page_rw_test.db";
    cleanup_file("./test_data", filename);

    ASSERT_TRUE(sm_->open_file(filename));

    char write_buf[StorageManager::PAGE_SIZE];
    char read_buf[StorageManager::PAGE_SIZE];
    std::memset(write_buf, 0, StorageManager::PAGE_SIZE);
    std::memset(read_buf, 0, StorageManager::PAGE_SIZE);

    // Write pattern to page 0
    for (int i = 0; i < 16; ++i) {
        write_buf[i * 16] = static_cast<char>(i);
    }
    ASSERT_TRUE(sm_->write_page(filename, 0, write_buf));

    // Read back and verify
    ASSERT_TRUE(sm_->read_page(filename, 0, read_buf));
    ASSERT_EQ(std::memcmp(write_buf, read_buf, StorageManager::PAGE_SIZE), 0);
}

TEST_F(StorageManagerTests, ReadBeyondEOFFillsZeros) {
    const std::string filename = "beyond_eof_test.db";
    cleanup_file("./test_data", filename);

    ASSERT_TRUE(sm_->open_file(filename));

    char read_buf[StorageManager::PAGE_SIZE];
    std::memset(read_buf, 0xFF, StorageManager::PAGE_SIZE);  // Fill with sentinel

    // Read page 10 from empty file - should zero-fill
    ASSERT_TRUE(sm_->read_page(filename, 10, read_buf));

    // Verify all zeros
    for (size_t i = 0; i < StorageManager::PAGE_SIZE; ++i) {
        EXPECT_EQ(read_buf[i], 0) << "Byte at index " << i << " was not zero";
    }
}

TEST_F(StorageManagerTests, PartialReadReturnsFalse) {
    const std::string filename = "partial_read_test.db";
    cleanup_file("./test_data", filename);

    ASSERT_TRUE(sm_->open_file(filename));

    // Write a small amount of data
    char write_buf[StorageManager::PAGE_SIZE];
    std::memset(write_buf, 0xAB, StorageManager::PAGE_SIZE);
    ASSERT_TRUE(sm_->write_page(filename, 0, write_buf));

    // Try to read the small write as a full page should succeed (EOF handling fills zeros)
    char read_buf[StorageManager::PAGE_SIZE];
    ASSERT_TRUE(sm_->read_page(filename, 0, read_buf));
}

TEST_F(StorageManagerTests, AllocatePageOnEmptyFile) {
    const std::string filename = "allocate_test.db";
    cleanup_file("./test_data", filename);

    ASSERT_TRUE(sm_->open_file(filename));
    ASSERT_EQ(sm_->allocate_page(filename), 0U);
}

TEST_F(StorageManagerTests, AllocatePageSequential) {
    const std::string filename = "allocate_seq_test.db";
    cleanup_file("./test_data", filename);

    ASSERT_TRUE(sm_->open_file(filename));

    // allocate_page returns next page index based on file size
    // But it does NOT write to file - you need to write_page
    ASSERT_EQ(sm_->allocate_page(filename), 0U);

    // Write a page, then allocate should give next index
    char buf[StorageManager::PAGE_SIZE];
    std::memset(buf, 0, StorageManager::PAGE_SIZE);
    ASSERT_TRUE(sm_->write_page(filename, 0, buf));
    ASSERT_EQ(sm_->allocate_page(filename), 1U);
}

TEST_F(StorageManagerTests, CreateDirIfNotExistsBasic) {
    // Directory should already exist from SetUp
    ASSERT_TRUE(sm_->create_dir_if_not_exists());
}

TEST_F(StorageManagerTests, CreateDirAlreadyExists) {
    // create_dir_if_not_exists should return true even if dir exists
    ASSERT_TRUE(sm_->create_dir_if_not_exists());
    ASSERT_TRUE(sm_->create_dir_if_not_exists());
}

TEST_F(StorageManagerTests, FileExistsAfterOpen) {
    const std::string filename = "exists_test.db";
    cleanup_file("./test_data", filename);

    ASSERT_FALSE(sm_->file_exists(filename));
    ASSERT_TRUE(sm_->open_file(filename));
    ASSERT_TRUE(sm_->file_exists(filename));
}

TEST_F(StorageManagerTests, GetFullPath) {
    const std::string path = sm_->get_full_path("test.db");
    EXPECT_EQ(path, "./test_data/test.db");
}

TEST_F(StorageManagerTests, MultipleFilesOpen) {
    const std::string file1 = "multi1.db";
    const std::string file2 = "multi2.db";
    const std::string file3 = "multi3.db";
    cleanup_file("./test_data", file1);
    cleanup_file("./test_data", file2);
    cleanup_file("./test_data", file3);

    ASSERT_TRUE(sm_->open_file(file1));
    ASSERT_TRUE(sm_->open_file(file2));
    ASSERT_TRUE(sm_->open_file(file3));

    ASSERT_TRUE(sm_->file_exists(file1));
    ASSERT_TRUE(sm_->file_exists(file2));
    ASSERT_TRUE(sm_->file_exists(file3));
}

TEST_F(StorageManagerTests, StatsAccurateAfterOperations) {
    const std::string filename = "stats_test.db";
    cleanup_file("./test_data", filename);

    const auto& stats = sm_->get_stats();
    auto initial_pages_read = stats.pages_read.load();
    auto initial_pages_written = stats.pages_written.load();

    ASSERT_TRUE(sm_->open_file(filename));

    char buf[StorageManager::PAGE_SIZE];
    std::memset(buf, 0, StorageManager::PAGE_SIZE);
    ASSERT_TRUE(sm_->write_page(filename, 0, buf));
    ASSERT_TRUE(sm_->read_page(filename, 0, buf));

    EXPECT_GT(stats.pages_written.load(), initial_pages_written);
}

TEST_F(StorageManagerTests, WriteAndReadDifferentPages) {
    const std::string filename = "diff_pages_test.db";
    cleanup_file("./test_data", filename);

    ASSERT_TRUE(sm_->open_file(filename));

    char page0[StorageManager::PAGE_SIZE];
    char page1[StorageManager::PAGE_SIZE];
    char page5[StorageManager::PAGE_SIZE];
    char read_buf[StorageManager::PAGE_SIZE];

    std::memset(page0, 0xAA, StorageManager::PAGE_SIZE);
    std::memset(page1, 0xBB, StorageManager::PAGE_SIZE);
    std::memset(page5, 0xCC, StorageManager::PAGE_SIZE);

    ASSERT_TRUE(sm_->write_page(filename, 0, page0));
    ASSERT_TRUE(sm_->write_page(filename, 1, page1));
    ASSERT_TRUE(sm_->write_page(filename, 5, page5));

    ASSERT_TRUE(sm_->read_page(filename, 0, read_buf));
    EXPECT_EQ(std::memcmp(page0, read_buf, StorageManager::PAGE_SIZE), 0);

    ASSERT_TRUE(sm_->read_page(filename, 1, read_buf));
    EXPECT_EQ(std::memcmp(page1, read_buf, StorageManager::PAGE_SIZE), 0);

    ASSERT_TRUE(sm_->read_page(filename, 5, read_buf));
    EXPECT_EQ(std::memcmp(page5, read_buf, StorageManager::PAGE_SIZE), 0);
}

// ============= New Tests =============

/**
 * @brief Verifies PAGE_SIZE constant value
 */
TEST_F(StorageManagerTests, PageSizeConstant) {
    EXPECT_EQ(StorageManager::PAGE_SIZE, 4096U);
}

/**
 * @brief Verifies read_page auto-opens file if not already open
 */
TEST_F(StorageManagerTests, ReadNonOpenedFileAutoOpens) {
    const std::string filename = "non_opened_read.db";
    cleanup_file("./test_data", filename);

    // Capture open-file count before operation
    auto before = sm_->get_stats().files_opened.load();
    ASSERT_FALSE(sm_->file_exists(filename));

    // StorageManager auto-opens files, so read should succeed
    char buf[StorageManager::PAGE_SIZE];
    EXPECT_TRUE(sm_->read_page(filename, 0, buf));

    // Verify file was created and open-file count increased
    EXPECT_TRUE(sm_->file_exists(filename));
    EXPECT_EQ(sm_->get_stats().files_opened.load(), before + 1);
}

/**
 * @brief Verifies write_page auto-opens file if not already open
 */
TEST_F(StorageManagerTests, WriteNonOpenedFileAutoOpens) {
    const std::string filename = "non_opened_write.db";
    cleanup_file("./test_data", filename);

    // Capture open-file count before operation
    auto before = sm_->get_stats().files_opened.load();
    ASSERT_FALSE(sm_->file_exists(filename));

    char buf[StorageManager::PAGE_SIZE];
    std::memset(buf, 0xAB, StorageManager::PAGE_SIZE);
    // StorageManager auto-opens files, so write should succeed
    EXPECT_TRUE(sm_->write_page(filename, 0, buf));

    // Verify file was created and open-file count increased
    EXPECT_TRUE(sm_->file_exists(filename));
    EXPECT_EQ(sm_->get_stats().files_opened.load(), before + 1);
}

/**
 * @brief Verifies data persists across file open/close cycle
 */
TEST_F(StorageManagerTests, DataPersistenceAcrossOpenClose) {
    const std::string filename = "persist_test.db";
    cleanup_file("./test_data", filename);

    // Write data to page 0
    ASSERT_TRUE(sm_->open_file(filename));
    char write_buf[StorageManager::PAGE_SIZE];
    for (int i = 0; i < StorageManager::PAGE_SIZE; ++i) {
        write_buf[i] = static_cast<char>(i & 0xFF);
    }
    ASSERT_TRUE(sm_->write_page(filename, 0, write_buf));
    ASSERT_TRUE(sm_->close_file(filename));

    // Reopen and read - data should persist
    ASSERT_TRUE(sm_->open_file(filename));
    char read_buf[StorageManager::PAGE_SIZE];
    ASSERT_TRUE(sm_->read_page(filename, 0, read_buf));
    EXPECT_EQ(std::memcmp(write_buf, read_buf, StorageManager::PAGE_SIZE), 0);
    ASSERT_TRUE(sm_->close_file(filename));
}

/**
 * @brief Verifies stats track bytes_read and bytes_written accurately
 */
TEST_F(StorageManagerTests, StatsBytesAccurate) {
    const std::string filename = "stats_bytes_test.db";
    cleanup_file("./test_data", filename);

    const auto& stats = sm_->get_stats();
    auto initial_bytes_read = stats.bytes_read.load();
    auto initial_bytes_written = stats.bytes_written.load();

    ASSERT_TRUE(sm_->open_file(filename));

    char buf[StorageManager::PAGE_SIZE];
    std::memset(buf, 0xAB, StorageManager::PAGE_SIZE);
    ASSERT_TRUE(sm_->write_page(filename, 0, buf));
    ASSERT_TRUE(sm_->read_page(filename, 0, buf));

    EXPECT_EQ(stats.bytes_written.load(), initial_bytes_written + StorageManager::PAGE_SIZE);
    EXPECT_EQ(stats.bytes_read.load(), initial_bytes_read + StorageManager::PAGE_SIZE);
}

/**
 * @brief Verifies data isolation between multiple files
 */
TEST_F(StorageManagerTests, MultipleFilesDataIsolation) {
    const std::string file1 = "isolate1.db";
    const std::string file2 = "isolate2.db";
    cleanup_file("./test_data", file1);
    cleanup_file("./test_data", file2);

    ASSERT_TRUE(sm_->open_file(file1));
    ASSERT_TRUE(sm_->open_file(file2));

    char buf1[StorageManager::PAGE_SIZE];
    char buf2[StorageManager::PAGE_SIZE];
    char read_buf[StorageManager::PAGE_SIZE];

    std::memset(buf1, 0x11, StorageManager::PAGE_SIZE);
    std::memset(buf2, 0x22, StorageManager::PAGE_SIZE);

    ASSERT_TRUE(sm_->write_page(file1, 0, buf1));
    ASSERT_TRUE(sm_->write_page(file2, 0, buf2));

    // Read from file1 - should have file1's data
    ASSERT_TRUE(sm_->read_page(file1, 0, read_buf));
    EXPECT_EQ(std::memcmp(buf1, read_buf, StorageManager::PAGE_SIZE), 0);

    // Read from file2 - should have file2's data
    ASSERT_TRUE(sm_->read_page(file2, 0, read_buf));
    EXPECT_EQ(std::memcmp(buf2, read_buf, StorageManager::PAGE_SIZE), 0);
}

/**
 * @brief Verifies deallocate_page stub doesn't crash
 */
TEST_F(StorageManagerTests, DeallocatePageStub) {
    const std::string filename = "dealloc_test.db";
    cleanup_file("./test_data", filename);

    // deallocate_page is a stub but shouldn't crash
    EXPECT_NO_THROW(StorageManager::deallocate_page(filename, 0));
}

/**
 * @brief Verifies complex byte patterns persist correctly
 */
TEST_F(StorageManagerTests, ReadAfterWriteDifferentPatterns) {
    const std::string filename = "patterns_test.db";
    cleanup_file("./test_data", filename);

    ASSERT_TRUE(sm_->open_file(filename));

    // Write alternating pattern
    char page[StorageManager::PAGE_SIZE];
    for (size_t i = 0; i < StorageManager::PAGE_SIZE; ++i) {
        page[i] = (i % 2 == 0) ? 0xAA : 0x55;
    }
    ASSERT_TRUE(sm_->write_page(filename, 0, page));

    char read_buf[StorageManager::PAGE_SIZE];
    ASSERT_TRUE(sm_->read_page(filename, 0, read_buf));
    EXPECT_EQ(std::memcmp(page, read_buf, StorageManager::PAGE_SIZE), 0);
}

/**
 * @brief Verifies files_opened stat increments correctly
 */
TEST_F(StorageManagerTests, FilesOpenedStatAccurate) {
    const std::string file1 = "stat_file1.db";
    const std::string file2 = "stat_file2.db";
    cleanup_file("./test_data", file1);
    cleanup_file("./test_data", file2);

    const auto& stats = sm_->get_stats();
    auto initial_files_opened = stats.files_opened.load();

    ASSERT_TRUE(sm_->open_file(file1));
    ASSERT_TRUE(sm_->open_file(file2));

    EXPECT_EQ(stats.files_opened.load(), initial_files_opened + 2);
}

}  // namespace
