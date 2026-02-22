/**
 * @file transaction_manager_tests.cpp
 * @brief Unit tests for Transaction Manager
 */

#include <cstdio>
#include <iostream>
#include <vector>
#include <cstdint>

#include "catalog/catalog.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "transaction/transaction.hpp"
#include "storage/heap_table.hpp"
#include "executor/types.hpp"
#include "common/value.hpp"
#include "test_utils.hpp"

using namespace cloudsql;
using namespace cloudsql::transaction;
using namespace cloudsql::storage;
using namespace cloudsql::executor;

namespace {

using cloudsql::tests::tests_passed;
using cloudsql::tests::tests_failed;

TEST(TransactionManager_Basic) {
    LockManager lm;
    auto catalog = Catalog::create();
    StorageManager sm("./test_data");
    TransactionManager tm(lm, *catalog, sm);

    auto* const txn = tm.begin();
    EXPECT_TRUE(txn != nullptr);
    EXPECT_EQ(txn->get_state(), TransactionState::RUNNING);

    const txn_id_t id = txn->get_id();
    EXPECT_PTR_EQ(tm.get_transaction(id), txn);

    tm.commit(txn);
    /* Note: txn pointer is now invalid as it was owned by tm and deleted */
    EXPECT_TRUE(tm.get_transaction(id) == nullptr);
}

TEST(TransactionManager_AbortCleanup) {
    LockManager lm;
    auto catalog = Catalog::create();
    StorageManager sm("./test_data");
    TransactionManager tm(lm, *catalog, sm);

    auto* const txn = tm.begin();
    const txn_id_t id = txn->get_id();
    txn->add_exclusive_lock("RID1");

    /* Lock should be released on abort */
    tm.abort(txn);
    EXPECT_TRUE(tm.get_transaction(id) == nullptr);

    /* Verify lock released by trying to acquire it again */
    auto* const txn2 = tm.begin();
    EXPECT_TRUE(lm.acquire_exclusive(txn2, "RID1"));
    tm.commit(txn2);
}

TEST(TransactionManager_RollbackInsert) {
    LockManager lm;
    auto catalog = Catalog::create();
    StorageManager sm("./test_data");
    TransactionManager tm(lm, *catalog, sm);

    static_cast<void>(std::remove("./test_data/rb_insert.heap"));
    static_cast<void>(catalog->create_table("rb_insert", {{"id", common::ValueType::TYPE_INT64, 0}}));
    HeapTable table("rb_insert", sm, Schema({ColumnMeta("id", common::ValueType::TYPE_INT64, true)}));
    static_cast<void>(table.create());

    auto* const txn = tm.begin();
    const auto tid = table.insert(Tuple({common::Value::make_int64(1)}),
                            txn->get_id());
    txn->add_undo_log(UndoLog::Type::INSERT, "rb_insert", tid);

    EXPECT_EQ(table.tuple_count(), static_cast<uint64_t>(1));
    tm.abort(txn);

    /* After abort, record should be logically deleted (xmax set) */
    EXPECT_EQ(table.tuple_count(), static_cast<uint64_t>(0));
}

} // namespace

int main() {
    std::cout << "Transaction Manager Unit Tests" << "\n";
    std::cout << "========================" << "\n" << "\n";

    RUN_TEST(TransactionManager_Basic);
    RUN_TEST(TransactionManager_AbortCleanup);
    RUN_TEST(TransactionManager_RollbackInsert);

    std::cout << "\n" << "========================" << "\n";
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed"
              << "\n";

    return (tests_failed > 0);
}
