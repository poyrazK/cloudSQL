/**
 * @file transaction_manager_tests.cpp
 * @brief Unit tests for Transaction Manager
 */

#include <gtest/gtest.h>

#include <string>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::transaction;

namespace {

TEST(TransactionManagerTests, Basic) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

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
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn1 = tm.begin();
    Transaction* const txn2 = tm.begin();

    EXPECT_GT(txn2->get_id(), txn1->get_id());

    tm.commit(txn1);
    tm.commit(txn2);
}

TEST(TransactionManagerTests, BeginWithIsolationLevels) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn1 = tm.begin(IsolationLevel::READ_COMMITTED);
    ASSERT_NE(txn1, nullptr);
    EXPECT_EQ(txn1->get_state(), TransactionState::RUNNING);

    Transaction* const txn2 = tm.begin(IsolationLevel::REPEATABLE_READ);
    ASSERT_NE(txn2, nullptr);
    EXPECT_EQ(txn2->get_state(), TransactionState::RUNNING);

    Transaction* const txn3 = tm.begin(IsolationLevel::SERIALIZABLE);
    ASSERT_NE(txn3, nullptr);
    EXPECT_EQ(txn3->get_state(), TransactionState::RUNNING);

    tm.commit(txn1);
    tm.commit(txn2);
    tm.commit(txn3);
}

TEST(TransactionManagerTests, PrepareTransaction) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn = tm.begin();
    ASSERT_NE(txn, nullptr);
    EXPECT_EQ(txn->get_state(), TransactionState::RUNNING);

    tm.prepare(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::PREPARED);

    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

TEST(TransactionManagerTests, GetTransaction) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn1 = tm.begin();
    ASSERT_NE(txn1, nullptr);

    // Get existing transaction
    EXPECT_EQ(tm.get_transaction(txn1->get_id()), txn1);

    // Get non-existent transaction
    EXPECT_EQ(tm.get_transaction(9999), nullptr);

    tm.commit(txn1);

    // After commit, transaction is no longer active
    EXPECT_EQ(tm.get_transaction(txn1->get_id()), nullptr);
}

TEST(TransactionManagerTests, CommitIdempotent) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);

    // Commit again should be safe (idempotent)
    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

TEST(TransactionManagerTests, AbortIdempotent) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);

    // Abort again should be safe (idempotent)
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);
}

TEST(TransactionManagerTests, MultipleTransactions) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* txns[5];
    for (int i = 0; i < 5; ++i) {
        txns[i] = tm.begin();
        ASSERT_NE(txns[i], nullptr);
        EXPECT_EQ(txns[i]->get_state(), TransactionState::RUNNING);
    }

    // Verify IDs are ordered
    for (int i = 1; i < 5; ++i) {
        EXPECT_GT(txns[i]->get_id(), txns[i - 1]->get_id());
    }

    // Commit even-indexed transactions, abort odd-indexed
    for (int i = 0; i < 5; ++i) {
        if (i % 2 == 0) {
            tm.commit(txns[i]);
            EXPECT_EQ(txns[i]->get_state(), TransactionState::COMMITTED);
        } else {
            tm.abort(txns[i]);
            EXPECT_EQ(txns[i]->get_state(), TransactionState::ABORTED);
        }
    }
}

}  // namespace
