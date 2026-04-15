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

// ============= New Tests =============

/**
 * @brief Verifies unlocking a non-existent lock returns false
 */
TEST(LockManagerTests, UnlockNonExistent) {
    LockManager lm;
    Transaction txn(1);
    HeapTable::TupleId rid(1, 1);

    // Unlock on non-existent lock should return false
    EXPECT_FALSE(lm.unlock(&txn, rid));
}

/**
 * @brief Verifies exclusive lock blocks shared lock acquisition
 */
TEST(LockManagerTests, ExclusiveBlocksShared) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);
    HeapTable::TupleId rid(1, 1);

    // Acquire exclusive
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, rid));

    // Shared should block (using try with immediate timeout behavior)
    std::atomic<bool> shared_acquired{false};
    std::thread t([&]() {
        if (lm.acquire_shared(&txn2, rid)) {
            shared_acquired = true;
        }
    });

    // Give thread time to try acquiring
    std::this_thread::sleep_for(TEST_SLEEP_MS);
    EXPECT_FALSE(shared_acquired.load());

    // Release exclusive
    static_cast<void>(lm.unlock(&txn1, rid));

    t.join();
    EXPECT_TRUE(shared_acquired.load());
    static_cast<void>(lm.unlock(&txn2, rid));
}

/**
 * @brief Verifies shared lock blocks exclusive lock acquisition
 */
TEST(LockManagerTests, SharedBlocksExclusive) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);
    HeapTable::TupleId rid(1, 1);

    // Acquire shared
    EXPECT_TRUE(lm.acquire_shared(&txn1, rid));

    // Exclusive should block
    std::atomic<bool> exclusive_acquired{false};
    std::thread t([&]() {
        if (lm.acquire_exclusive(&txn2, rid)) {
            exclusive_acquired = true;
        }
    });

    // Give thread time to try acquiring
    std::this_thread::sleep_for(TEST_SLEEP_MS);
    EXPECT_FALSE(exclusive_acquired.load());

    // Release shared
    static_cast<void>(lm.unlock(&txn1, rid));

    t.join();
    EXPECT_TRUE(exclusive_acquired.load());
    static_cast<void>(lm.unlock(&txn2, rid));
}

/**
 * @brief Verifies unlock only releases the specific transaction's lock
 */
TEST(LockManagerTests, UnlockReleasesOnlyTxn) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);
    HeapTable::TupleId rid(1, 1);

    // Two transactions acquire shared
    EXPECT_TRUE(lm.acquire_shared(&txn1, rid));
    EXPECT_TRUE(lm.acquire_shared(&txn2, rid));

    // Unlock txn1 - should not affect txn2
    static_cast<void>(lm.unlock(&txn1, rid));

    // txn2 should still hold the lock - can still read
    EXPECT_TRUE(lm.acquire_shared(&txn2, rid));

    static_cast<void>(lm.unlock(&txn2, rid));
}

/**
 * @brief Verifies lock re-acquisition by same transaction is idempotent
 */
TEST(LockManagerTests, LockSameRIDTwice) {
    LockManager lm;
    Transaction txn(1);
    HeapTable::TupleId rid(1, 1);

    // Lock manager allows re-acquisition by same transaction
    EXPECT_TRUE(lm.acquire_shared(&txn, rid));
    EXPECT_TRUE(lm.acquire_shared(&txn, rid));

    // Should only need one unlock
    static_cast<void>(lm.unlock(&txn, rid));
}

/**
 * @brief Verifies three transactions can hold shared locks simultaneously
 */
TEST(LockManagerTests, MultipleSharedLocks) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);
    Transaction txn3(3);
    HeapTable::TupleId rid(1, 1);

    EXPECT_TRUE(lm.acquire_shared(&txn1, rid));
    EXPECT_TRUE(lm.acquire_shared(&txn2, rid));
    EXPECT_TRUE(lm.acquire_shared(&txn3, rid));

    static_cast<void>(lm.unlock(&txn1, rid));
    static_cast<void>(lm.unlock(&txn2, rid));
    static_cast<void>(lm.unlock(&txn3, rid));
}

/**
 * @brief Verifies transactions can hold locks on different RIDs independently
 */
TEST(LockManagerTests, LockDifferentRIDs) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);
    HeapTable::TupleId ridA(1, 1);
    HeapTable::TupleId ridB(1, 2);

    // txn1 gets exclusive on ridA, txn2 gets exclusive on ridB - both should succeed
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, ridA));
    EXPECT_TRUE(lm.acquire_exclusive(&txn2, ridB));

    static_cast<void>(lm.unlock(&txn1, ridA));
    static_cast<void>(lm.unlock(&txn2, ridB));
}

/**
 * @brief Verifies exclusive lock followed by shared on different RID succeeds
 */
TEST(LockManagerTests, ExclusiveThenShared) {
    LockManager lm;
    Transaction txn(1);
    HeapTable::TupleId ridA(1, 1);
    HeapTable::TupleId ridB(1, 2);

    EXPECT_TRUE(lm.acquire_exclusive(&txn, ridA));
    EXPECT_TRUE(lm.acquire_shared(&txn, ridB));

    static_cast<void>(lm.unlock(&txn, ridA));
    static_cast<void>(lm.unlock(&txn, ridB));
}

}  // namespace
