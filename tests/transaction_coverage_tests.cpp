/**
 * @file transaction_coverage_tests.cpp
 * @brief Targeted unit tests to increase coverage of Transaction and Lock Manager
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::transaction;
using namespace cloudsql::storage;

namespace {

/**
 * @class TransactionCoverageTests
 * @brief Fixture for transaction-related coverage tests to ensure proper resource management.
 */
class TransactionCoverageTests : public ::testing::Test {
   protected:
    void SetUp() override {
        catalog = Catalog::create();
        disk_manager = std::make_unique<StorageManager>("./test_data");
        bpm = std::make_unique<BufferPoolManager>(config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                                  *disk_manager);
        lm = std::make_unique<LockManager>();
        tm = std::make_unique<TransactionManager>(*lm, *catalog, *bpm, nullptr);

        std::vector<ColumnInfo> cols = {{"id", common::ValueType::TYPE_INT64, 0},
                                        {"val", common::ValueType::TYPE_TEXT, 1}};
        catalog->create_table("rollback_stress", cols);

        executor::Schema schema;
        schema.add_column("id", common::ValueType::TYPE_INT64);
        schema.add_column("val", common::ValueType::TYPE_TEXT);

        table = std::make_unique<HeapTable>("rollback_stress", *bpm, schema);
        table->create();

        txn = nullptr;
    }

    void TearDown() override {
        if (txn != nullptr) {
            tm->abort(txn);
        }
        table.reset();
        tm.reset();
        lm.reset();
        bpm.reset();
        disk_manager.reset();
        catalog.reset();

        static_cast<void>(std::remove("./test_data/rollback_stress.heap"));
    }

    std::unique_ptr<Catalog> catalog;
    std::unique_ptr<StorageManager> disk_manager;
    std::unique_ptr<BufferPoolManager> bpm;
    std::unique_ptr<LockManager> lm;
    std::unique_ptr<TransactionManager> tm;
    std::unique_ptr<HeapTable> table;
    Transaction* txn;
};

/**
 * @brief Stress tests the LockManager with concurrent shared and exclusive requests.
 */
TEST(TransactionCoverageTestsStandalone, LockManagerConcurrency) {
    LockManager lm;
    const int num_readers = 5;
    std::vector<std::thread> readers;
    std::atomic<int> shared_granted{0};
    std::atomic<bool> stop{false};

    Transaction writer_txn(100);

    // Writers holds exclusive lock initially
    ASSERT_TRUE(lm.acquire_exclusive(&writer_txn, "RESOURCE"));

    for (int i = 0; i < num_readers; ++i) {
        readers.emplace_back([&, i]() {
            Transaction reader_txn(i);
            if (lm.acquire_shared(&reader_txn, "RESOURCE")) {
                shared_granted++;
                while (!stop) {
                    std::this_thread::yield();
                }
                lm.unlock(&reader_txn, "RESOURCE");
            }
        });
    }

    // Readers should be blocked by the writer
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(shared_granted.load(), 0);

    // Release writer lock, readers should proceed
    lm.unlock(&writer_txn, "RESOURCE");

    // Wait for all readers to get the lock
    for (int i = 0; i < 50 && shared_granted.load() < num_readers; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_EQ(shared_granted.load(), num_readers);

    stop = true;
    for (auto& t : readers) {
        t.join();
    }
}

/**
 * @brief Tests deep rollback functionality via the Undo Log.
 */
TEST_F(TransactionCoverageTests, DeepRollback) {
    txn = tm->begin();

    // 1. Insert some data
    auto rid1 = table->insert(
        executor::Tuple({common::Value::make_int64(1), common::Value::make_text("A")}),
        txn->get_id());
    txn->add_undo_log(UndoLog::Type::INSERT, "rollback_stress", rid1);

    auto rid2 = table->insert(
        executor::Tuple({common::Value::make_int64(2), common::Value::make_text("B")}),
        txn->get_id());
    txn->add_undo_log(UndoLog::Type::INSERT, "rollback_stress", rid2);

    // 2. Update data
    table->remove(rid1, txn->get_id());  // Mark old version deleted
    auto rid1_new = table->insert(
        executor::Tuple({common::Value::make_int64(1), common::Value::make_text("A_NEW")}),
        txn->get_id());
    txn->add_undo_log(UndoLog::Type::UPDATE, "rollback_stress", rid1_new, rid1);

    // 3. Delete data
    table->remove(rid2, txn->get_id());
    txn->add_undo_log(UndoLog::Type::DELETE, "rollback_stress", rid2);

    EXPECT_EQ(table->tuple_count(), 1U);  // rid1_new is active, rid1 and rid2 are logically deleted

    // 4. Abort
    tm->abort(txn);
    txn = nullptr;  // Marked as aborted and handled by TearDown if still set

    // 5. Verify restoration
    EXPECT_EQ(table->tuple_count(),
              0U);  // Inserted rows should be physically removed or logically invisible

    // The table should be empty because we aborted the inserts
    auto iter = table->scan();
    executor::Tuple t;
    EXPECT_FALSE(iter.next(t));
}

}  // namespace
