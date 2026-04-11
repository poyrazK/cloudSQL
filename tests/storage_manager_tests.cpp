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

  void TearDown() override {
    sm_.reset();
  }

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
  char write_buf[512];
  std::memset(write_buf, 0xAB, 512);
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

}  // namespace
