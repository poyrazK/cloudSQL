/**
 * @file lock_manager_tests.cpp
 * @brief Unit tests for Lock Manager
 */

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>

#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "test_utils.hpp"

using namespace cloudsql::transaction;

namespace {

using cloudsql::tests::tests_passed;
using cloudsql::tests::tests_failed;

constexpr uint64_t TXN_101 = 101;
constexpr uint64_t TXN_102 = 102;
constexpr uint64_t TXN_103 = 103;
constexpr int SLEEP_MS = 100;

TEST(LockManager_SharedBasic) {
    LockManager lm;
    Transaction txn1(TXN_101);
    Transaction txn2(TXN_102);

    /* Both should be able to get shared lock */
    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));
    EXPECT_TRUE(lm.acquire_shared(&txn2, "RID1"));

    /* Already held lock should return true */
    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));

    static_cast<void>(lm.unlock(&txn1, "RID1"));
    static_cast<void>(lm.unlock(&txn2, "RID1"));
}

TEST(LockManager_ExclusiveBasic) {
    LockManager lm;
    Transaction txn1(TXN_101);
    Transaction txn2(TXN_102);

    EXPECT_TRUE(lm.acquire_exclusive(&txn1, "RID1"));

    /* txn2 should block, but for unit test we can't block main thread easily without spawning */
    /* Let's test the already held case */
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, "RID1"));

    static_cast<void>(lm.unlock(&txn1, "RID1"));

    /* Now txn2 can get it */
    EXPECT_TRUE(lm.acquire_exclusive(&txn2, "RID1"));
    static_cast<void>(lm.unlock(&txn2, "RID1"));
}

TEST(LockManager_SharedExclusiveContention) {
    LockManager lm;
    Transaction txn1(TXN_101);
    Transaction txn2(TXN_102);
    std::atomic<bool> txn2_granted{false};

    static_cast<void>(lm.acquire_shared(&txn1, "RID1"));

    std::thread t2([&]() {
        if (lm.acquire_exclusive(&txn2, "RID1")) {
            txn2_granted = true;
        }
    });

    /* Give it some time to try and block */
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_MS));
    EXPECT_FALSE(txn2_granted.load());

    static_cast<void>(lm.unlock(&txn1, "RID1"));

    /* Wait for t2 to finish */
    t2.join();
    EXPECT_TRUE(txn2_granted.load());
    static_cast<void>(lm.unlock(&txn2, "RID1"));
}

TEST(LockManager_UpgradeBasic) {
    LockManager lm;
    Transaction txn1(TXN_101);

    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, "RID1")); /* Upgrade */

    static_cast<void>(lm.unlock(&txn1, "RID1"));
}

TEST(LockManager_MultipleSharedContention) {
    LockManager lm;
    Transaction txn1(TXN_101);
    Transaction txn2(TXN_102);
    Transaction txn3(TXN_103);

    std::atomic<int> shared_granted{0};

    static_cast<void>(lm.acquire_exclusive(&txn1, "RID1"));

    std::thread t2([&]() {
        if (lm.acquire_shared(&txn2, "RID1")) { shared_granted++; }
    });
    std::thread t3([&]() {
        if (lm.acquire_shared(&txn3, "RID1")) { shared_granted++; }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_MS));
    EXPECT_TRUE(shared_granted.load() == 0); // NOLINT(readability-simplify-boolean-expr)

    static_cast<void>(lm.unlock(&txn1, "RID1"));

    t2.join();
    t3.join();
    EXPECT_TRUE(shared_granted.load() == 2); // NOLINT(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

    static_cast<void>(lm.unlock(&txn2, "RID1"));
    static_cast<void>(lm.unlock(&txn3, "RID1"));
}

TEST(LockManager_UnlockInvalid) {
    LockManager lm;
    Transaction txn1(TXN_101);

    /* Unlock non-existent RID */
    EXPECT_FALSE(lm.unlock(&txn1, "NON_EXISTENT"));

    /* Unlock RID not held by this txn */
    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));
    Transaction txn2(TXN_102);
    EXPECT_FALSE(lm.unlock(&txn2, "RID1"));

    static_cast<void>(lm.unlock(&txn1, "RID1"));
}

TEST(LockManager_AbortedWait) {
    LockManager lm;
    Transaction txn1(TXN_101);
    Transaction txn2(TXN_102);
    std::atomic<bool> success{false};

    static_cast<void>(lm.acquire_exclusive(&txn1, "RID1"));

    std::thread t2([&]() { success = lm.acquire_shared(&txn2, "RID1"); });

    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_MS));

    /* Abort txn2 while it's waiting */
    txn2.set_state(TransactionState::ABORTED);
    static_cast<void>(lm.unlock(&txn1, "RID1")); /* This will trigger notify_all and wake txn2 */

    t2.join();
    EXPECT_FALSE(success.load());
}

TEST(LockManager_RedundantShared) {
    LockManager lm;
    Transaction txn1(TXN_101);
    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));
    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));  // Coverage for line 19-20
    static_cast<void>(lm.unlock(&txn1, "RID1"));
}

TEST(LockManager_ExclusiveAbortedWait) {
    LockManager lm;
    Transaction txn1(TXN_101);
    Transaction txn2(TXN_102);
    std::atomic<bool> success{true};

    static_cast<void>(lm.acquire_exclusive(&txn1, "RID1"));

    std::thread t2([&]() { success = lm.acquire_exclusive(&txn2, "RID1"); });

    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_MS));
    txn2.set_state(TransactionState::ABORTED);
    static_cast<void>(lm.unlock(&txn1, "RID1"));

    t2.join();
    EXPECT_FALSE(success.load());  // Coverage for line 100-101
}

} // namespace

int main() {
    std::cout << "Lock Manager Unit Tests" << "\n";
    std::cout << "========================" << "\n" << "\n";

    RUN_TEST(LockManager_SharedBasic);
    RUN_TEST(LockManager_ExclusiveBasic);
    RUN_TEST(LockManager_SharedExclusiveContention);
    RUN_TEST(LockManager_UpgradeBasic);
    RUN_TEST(LockManager_MultipleSharedContention);
    RUN_TEST(LockManager_UnlockInvalid);
    RUN_TEST(LockManager_AbortedWait);
    RUN_TEST(LockManager_RedundantShared);
    RUN_TEST(LockManager_ExclusiveAbortedWait);

    std::cout << "\n" << "========================" << "\n";
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed"
              << "\n";

    return (tests_failed > 0);
}
