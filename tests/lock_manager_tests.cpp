/**
 * @file lock_manager_tests.cpp
 * @brief Unit tests for Lock Manager
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "storage/heap_table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

using namespace cloudsql::transaction;
using namespace cloudsql::storage;

namespace {

constexpr auto TEST_SLEEP_MS = std::chrono::milliseconds(100);

TEST(LockManagerTests, Shared) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);
    HeapTable::TupleId rid1(1, 1);

    EXPECT_TRUE(lm.acquire_shared(&txn1, rid1));
    EXPECT_TRUE(lm.acquire_shared(&txn2, rid1));

    static_cast<void>(lm.unlock(&txn1, rid1));
    static_cast<void>(lm.unlock(&txn2, rid1));
}

TEST(LockManagerTests, Exclusive) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);
    HeapTable::TupleId rid1(1, 1);

    EXPECT_TRUE(lm.acquire_exclusive(&txn1, rid1));
    EXPECT_FALSE(lm.acquire_shared(&txn2, rid1));

    static_cast<void>(lm.unlock(&txn1, rid1));
    EXPECT_TRUE(lm.acquire_shared(&txn2, rid1));
    static_cast<void>(lm.unlock(&txn2, rid1));
}

TEST(LockManagerTests, Upgrade) {
    LockManager lm;
    Transaction txn1(1);
    HeapTable::TupleId rid1(1, 1);

    EXPECT_TRUE(lm.acquire_shared(&txn1, rid1));
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, rid1));

    static_cast<void>(lm.unlock(&txn1, rid1));
}

TEST(LockManagerTests, Wait) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);
    Transaction txn3(3);
    HeapTable::TupleId rid1(1, 1);

    std::atomic<int> shared_granted{0};

    // 1. Get Exclusive
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, rid1));

    // 2. Try to get Shared from two other txns (should block)
    std::thread t2([&]() {
        if (lm.acquire_shared(&txn2, rid1)) {
            shared_granted++;
        }
    });
    std::thread t3([&]() {
        if (lm.acquire_shared(&txn3, rid1)) {
            shared_granted++;
        }
    });

    // Small sleep to ensure threads are waiting
    std::this_thread::sleep_for(TEST_SLEEP_MS);
    EXPECT_EQ(shared_granted.load(), 0);

    // 3. Release Exclusive (should grant both shared)
    static_cast<void>(lm.unlock(&txn1, rid1));

    t2.join();
    t3.join();

    EXPECT_EQ(shared_granted.load(), 2);

    static_cast<void>(lm.unlock(&txn2, rid1));
    static_cast<void>(lm.unlock(&txn3, rid1));
}

TEST(LockManagerTests, Deadlock) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);
    HeapTable::TupleId ridA(1, 1);
    HeapTable::TupleId ridB(1, 2);

    // txn1 holds A, txn2 holds B
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, ridA));
    EXPECT_TRUE(lm.acquire_exclusive(&txn2, ridB));

    // txn1 waits for B
    std::thread t1([&]() { static_cast<void>(lm.acquire_exclusive(&txn1, ridB)); });

    // Small sleep to ensure t1 is waiting
    std::this_thread::sleep_for(TEST_SLEEP_MS);

    // txn2 waits for A -> Deadlock!
    static_cast<void>(lm.unlock(&txn1, ridA));
    static_cast<void>(lm.acquire_exclusive(&txn2, ridA));

    static_cast<void>(lm.unlock(&txn2, ridB));
    t1.join();

    static_cast<void>(lm.unlock(&txn1, ridB));
    static_cast<void>(lm.unlock(&txn2, ridA));
}

}  // namespace
