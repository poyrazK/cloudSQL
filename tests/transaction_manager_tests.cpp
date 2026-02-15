/**
 * @file transaction_manager_tests.cpp
 * @brief Unit tests for Transaction Manager
 */

#include <iostream>
#include <cassert>
#include <stdexcept>
#include <cstdio>
#include <vector>
#include "transaction/transaction_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "catalog/catalog.hpp"
#include "storage/storage_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::transaction;
using namespace cloudsql::storage;
using namespace cloudsql::executor;

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) void test_##name()
#define RUN_TEST(name) do { \
    std::cout << "  " << #name << "... "; \
    try { \
        test_##name(); \
        std::cout << "PASSED" << std::endl; \
        tests_passed++; \
    } catch (const std::exception& e) { \
        std::cout << "FAILED: " << e.what() << std::endl; \
        tests_failed++; \
    } \
} while(0)

#define EXPECT_EQ(a, b) do { \
    if ((a) != (b)) { \
        throw std::runtime_error("Expected match but got different values"); \
    } \
} while(0)

#define EXPECT_TRUE(a) do { \
    if (!(a)) { \
        throw std::runtime_error("Expected true but got false"); \
    } \
} while(0)

TEST(TransactionManager_Basic) {
    LockManager lm;
    auto catalog = Catalog::create();
    StorageManager sm("./test_data");
    TransactionManager tm(lm, *catalog, sm);

    auto txn = tm.begin();
    EXPECT_TRUE(txn != nullptr);
    EXPECT_EQ(txn->get_state(), TransactionState::RUNNING);
    
    txn_id_t id = txn->get_id();
    EXPECT_EQ(tm.get_transaction(id), txn);

    tm.commit(txn);
    /* Note: txn pointer is now invalid as it was owned by tm and deleted */
    EXPECT_TRUE(tm.get_transaction(id) == nullptr);
}

TEST(TransactionManager_AbortCleanup) {
    LockManager lm;
    auto catalog = Catalog::create();
    StorageManager sm("./test_data");
    TransactionManager tm(lm, *catalog, sm);

    auto txn = tm.begin();
    txn_id_t id = txn->get_id();
    txn->add_exclusive_lock("RID1");
    
    /* Lock should be released on abort */
    tm.abort(txn);
    EXPECT_TRUE(tm.get_transaction(id) == nullptr);
    
    /* Verify lock released by trying to acquire it again */
    auto txn2 = tm.begin();
    EXPECT_TRUE(lm.acquire_exclusive(txn2, "RID1"));
    tm.commit(txn2);
}

TEST(TransactionManager_RollbackInsert) {
    LockManager lm;
    auto catalog = Catalog::create();
    StorageManager sm("./test_data");
    TransactionManager tm(lm, *catalog, sm);

    std::remove("./test_data/rb_insert.heap");
    catalog->create_table("rb_insert", {{"id", common::TYPE_INT64, 0}});
    HeapTable table("rb_insert", sm, Schema({{"id", common::TYPE_INT64}}));
    table.create();

    auto txn = tm.begin();
    auto tid = table.insert(Tuple(std::vector<common::Value>{common::Value::make_int64(1)}), txn->get_id());
    txn->add_undo_log(UndoLog::Type::INSERT, "rb_insert", tid);

    EXPECT_EQ(table.tuple_count(), static_cast<uint64_t>(1));
    tm.abort(txn);
    
    /* After abort, record should be logically deleted (xmax set) */
    EXPECT_EQ(table.tuple_count(), static_cast<uint64_t>(0));
}

int main() {
    std::cout << "Transaction Manager Unit Tests" << std::endl;
    std::cout << "========================" << std::endl << std::endl;
    
    RUN_TEST(TransactionManager_Basic);
    RUN_TEST(TransactionManager_AbortCleanup);
    RUN_TEST(TransactionManager_RollbackInsert);
    
    std::cout << std::endl << "========================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed" << std::endl;
    
    return (tests_failed > 0);
}
