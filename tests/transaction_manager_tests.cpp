/**
 * @file transaction_manager_tests.cpp
 * @brief Unit tests for Transaction Manager
 */

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/value.hpp"
#include "executor/types.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"
#include "test_utils.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::transaction;
using namespace cloudsql::common;
using namespace cloudsql::executor;

namespace {

using cloudsql::tests::tests_failed;
using cloudsql::tests::tests_passed;

TEST(TransactionManager_Basic) {
    LockManager lm;
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(128, disk_manager);
    TransactionManager tm(lm, *catalog, sm);

    auto* const txn = tm.begin();
    EXPECT_TRUE(txn != nullptr);
    EXPECT_EQ(txn->get_state(), TransactionState::RUNNING);

    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

TEST(TransactionManager_Snapshot) {
    LockManager lm;
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(128, disk_manager);
    TransactionManager tm(lm, *catalog, sm);

    auto* const txn1 = tm.begin();
    auto* const txn2 = tm.begin();

    const auto& snap2 = txn2->get_snapshot();
    EXPECT_TRUE(snap2.active_txns.count(txn1->get_id()) > 0);

    tm.commit(txn1);
    tm.commit(txn2);
}

TEST(TransactionManager_RollbackInsert) {
    LockManager lm;
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(128, disk_manager);
    TransactionManager tm(lm, *catalog, sm);

    static_cast<void>(std::remove("./test_data/rb_insert.heap"));
    static_cast<void>(
        catalog->create_table("rb_insert", {{"id", common::ValueType::TYPE_INT64, 0}}));
    HeapTable table("rb_insert", sm,
                    Schema({ColumnMeta("id", common::ValueType::TYPE_INT64, true)}));
    static_cast<void>(table.create());

    auto* const txn = tm.begin();
    const auto tid = table.insert(Tuple({common::Value::make_int64(1)}), txn->get_id());
    txn->add_undo_log(UndoLog::Type::INSERT, "rb_insert", tid);

    EXPECT_EQ(table.tuple_count(), static_cast<uint64_t>(1));

    tm.abort(txn);

    EXPECT_EQ(table.tuple_count(), static_cast<uint64_t>(0));
}

}  // namespace

int main() {
    std::cout << "Transaction Manager Unit Tests" << "\n";
    std::cout << "==============================" << "\n";

    RUN_TEST(TransactionManager_Basic);
    RUN_TEST(TransactionManager_Snapshot);
    RUN_TEST(TransactionManager_RollbackInsert);

        std::cout << "\nResults: " << tests_passed << " passed, ";
    std::cout << tests_failed << " failed\n";
    return (tests_failed > 0);
}
