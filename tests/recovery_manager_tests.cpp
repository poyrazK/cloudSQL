/**
 * @file recovery_manager_tests.cpp
 * @brief Unit tests for Recovery Manager
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <string>

#include "catalog/catalog.hpp"
#include "recovery/log_manager.hpp"
#include "recovery/recovery_manager.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/storage_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::recovery;

namespace {

constexpr size_t TEST_BPM_SIZE = 10;

TEST(RecoveryManagerTests, Basic) {
    const std::string log_file = "recovery_test.log";
    static_cast<void>(std::remove(log_file.c_str()));

    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(TEST_BPM_SIZE, disk_manager);
    LogManager lm(log_file);

    RecoveryManager rm(bpm, *catalog, lm);
    EXPECT_TRUE(rm.recover());

    static_cast<void>(std::remove(log_file.c_str()));
}

TEST(RecoveryManagerTests, RecoverMultipleTimes) {
    const std::string log_file = "recovery_repeat_test.log";
    static_cast<void>(std::remove(log_file.c_str()));

    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(TEST_BPM_SIZE, disk_manager);
    LogManager lm(log_file);

    RecoveryManager rm(bpm, *catalog, lm);

    // recover() can be called multiple times safely
    EXPECT_TRUE(rm.recover());
    EXPECT_TRUE(rm.recover());
    EXPECT_TRUE(rm.recover());

    static_cast<void>(std::remove(log_file.c_str()));
}

TEST(RecoveryManagerTests, RecoverWithDifferentCatalogs) {
    const std::string log_file = "recovery_catalog_test.log";
    static_cast<void>(std::remove(log_file.c_str()));

    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(TEST_BPM_SIZE, disk_manager);
    LogManager lm(log_file);

    // Create first catalog, run recovery
    auto catalog1 = Catalog::create();
    RecoveryManager rm1(bpm, *catalog1, lm);
    EXPECT_TRUE(rm1.recover());

    // Create different catalog instance, run recovery again
    auto catalog2 = Catalog::create();
    RecoveryManager rm2(bpm, *catalog2, lm);
    EXPECT_TRUE(rm2.recover());

    static_cast<void>(std::remove(log_file.c_str()));
}

TEST(RecoveryManagerTests, RecoverWithDifferentBufferPools) {
    const std::string log_file = "recovery_bpm_test.log";
    static_cast<void>(std::remove(log_file.c_str()));

    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    LogManager lm(log_file);

    // Use first buffer pool
    storage::BufferPoolManager bpm1(TEST_BPM_SIZE, disk_manager);
    RecoveryManager rm1(bpm1, *catalog, lm);
    EXPECT_TRUE(rm1.recover());

    // Use second buffer pool
    storage::BufferPoolManager bpm2(TEST_BPM_SIZE, disk_manager);
    RecoveryManager rm2(bpm2, *catalog, lm);
    EXPECT_TRUE(rm2.recover());

    static_cast<void>(std::remove(log_file.c_str()));
}

}  // namespace
