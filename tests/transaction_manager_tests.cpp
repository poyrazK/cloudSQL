/**
 * @file transaction_manager_tests.cpp
 * @brief Unit tests for Transaction Manager
 */

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/transaction.hpp"

using namespace cloudsql;
using namespace cloudsql::transaction;

namespace {

TEST(TransactionManagerTests, Basic) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm);

    Transaction* const txn1 = tm.begin();
    ASSERT_NE(txn1, nullptr);
    EXPECT_EQ(txn1->get_state(), TransactionState::RUNNING);

    tm.commit(txn1);
    EXPECT_EQ(txn1->get_state(), TransactionState::COMMITTED);

    Transaction* const txn2 = tm.begin();
    tm.abort(txn2);
    EXPECT_EQ(txn2->get_state(), TransactionState::ABORTED);
}

TEST(TransactionManagerTests, Isolation) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm);

    Transaction* const txn1 = tm.begin();
    Transaction* const txn2 = tm.begin();

    EXPECT_GT(txn2->get_id(), txn1->get_id());

    tm.commit(txn1);
    tm.commit(txn2);
}

}  // namespace
