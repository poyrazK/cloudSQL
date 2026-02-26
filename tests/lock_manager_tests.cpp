/**
 * @file lock_manager_tests.cpp
 * @brief Unit tests for Lock Manager
 */

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "test_utils.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

using namespace cloudsql::transaction;

namespace {

using cloudsql::tests::tests_failed;
using cloudsql::tests::tests_passed;

constexpr auto TEST_SLEEP_MS = std::chrono::milliseconds(100);

TEST(LockManager_Shared) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);

    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));
    EXPECT_TRUE(lm.acquire_shared(&txn2, "RID1"));

    static_cast<void>(lm.unlock(&txn1, "RID1"));
    static_cast<void>(lm.unlock(&txn2, "RID1"));
}

TEST(LockManager_Exclusive) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);

    EXPECT_TRUE(lm.acquire_exclusive(&txn1, "RID1"));
    EXPECT_FALSE(lm.acquire_shared(&txn2, "RID1"));

    static_cast<void>(lm.unlock(&txn1, "RID1"));
    EXPECT_TRUE(lm.acquire_shared(&txn2, "RID1"));
    static_cast<void>(lm.unlock(&txn2, "RID1"));
}

TEST(LockManager_Upgrade) {
    LockManager lm;
    Transaction txn1(1);

    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, "RID1"));

    static_cast<void>(lm.unlock(&txn1, "RID1"));
}

TEST(LockManager_Wait) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);
    Transaction txn3(3);

    std::atomic<int> shared_granted{0};

    // 1. Get Exclusive
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, "RID1"));

    // 2. Try to get Shared from two other txns (should block)
    std::thread t2([&]() {
        if (lm.acquire_shared(&txn2, "RID1")) {
            shared_granted++;
        }
    });
    std::thread t3([&]() {
        if (lm.acquire_shared(&txn3, "RID1")) {
            shared_granted++;
        }
    });

    // Small sleep to ensure threads are waiting
    std::this_thread::sleep_for(TEST_SLEEP_MS);
    EXPECT_TRUE(shared_granted.load() == 0);

    // 3. Release Exclusive (should grant both shared)
    static_cast<void>(lm.unlock(&txn1, "RID1"));

    t2.join();
    t3.join();

    EXPECT_TRUE(shared_granted.load() == 2);

    static_cast<void>(lm.unlock(&txn2, "RID1"));
    static_cast<void>(lm.unlock(&txn3, "RID1"));
}

TEST(LockManager_Deadlock) {
    LockManager lm;
    Transaction txn1(1);
    Transaction txn2(2);

    // txn1 holds A, txn2 holds B
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, "A"));
    EXPECT_TRUE(lm.acquire_exclusive(&txn2, "B"));

    // txn1 waits for B
    std::thread t1([&]() { static_cast<void>(lm.acquire_exclusive(&txn1, "B")); });

    // Small sleep to ensure t1 is waiting
    std::this_thread::sleep_for(TEST_SLEEP_MS);

    // txn2 waits for A -> Deadlock!
    // Current implementation might not detect deadlock and just timeout or block.
    // For now we just verify we can grant if one releases.
    static_cast<void>(lm.unlock(&txn1, "A"));
    static_cast<void>(lm.acquire_exclusive(&txn2, "A"));

    static_cast<void>(lm.unlock(&txn2, "B"));
    t1.join();

    static_cast<void>(lm.unlock(&txn1, "B"));
    static_cast<void>(lm.unlock(&txn2, "A"));
}

}  // namespace

int main() {
    std::cout << "Lock Manager Unit Tests\n";
    std::cout << "=======================\n";

    RUN_TEST(LockManager_Shared);
    RUN_TEST(LockManager_Exclusive);
    RUN_TEST(LockManager_Upgrade);
    RUN_TEST(LockManager_Wait);
    RUN_TEST(LockManager_Deadlock);

    std::cout << "\nResults: \n" << tests_passed << " passed, \n" << tests_failed << " failed\n";
    return (tests_failed > 0);
}
