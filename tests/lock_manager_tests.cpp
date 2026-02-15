/**
 * @file lock_manager_tests.cpp
 * @brief Unit tests for Lock Manager
 */

#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>
#include <cassert>
#include <stdexcept>
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

using namespace cloudsql::transaction;

// Simple test framework (re-using patterns from cloudSQL_tests.cpp)
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

#define EXPECT_TRUE(a) do { \
    if (!(a)) { \
        throw std::runtime_error("Expected true but got false"); \
    } \
} while(0)

#define EXPECT_FALSE(a) do { \
    if (a) { \
        throw std::runtime_error("Expected false but got true"); \
    } \
} while(0)

TEST(LockManager_SharedBasic) {
    LockManager lm;
    Transaction txn1(101);
    Transaction txn2(102);

    /* Both should be able to get shared lock */
    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));
    EXPECT_TRUE(lm.acquire_shared(&txn2, "RID1"));

    /* Already held lock should return true */
    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));

    lm.unlock(&txn1, "RID1");
    lm.unlock(&txn2, "RID1");
}

TEST(LockManager_ExclusiveBasic) {
    LockManager lm;
    Transaction txn1(101);
    Transaction txn2(102);

    EXPECT_TRUE(lm.acquire_exclusive(&txn1, "RID1"));
    
    /* txn2 should block, but for unit test we can't block main thread easily without spawning */
    /* Let's test the already held case */
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, "RID1"));

    lm.unlock(&txn1, "RID1");
    
    /* Now txn2 can get it */
    EXPECT_TRUE(lm.acquire_exclusive(&txn2, "RID1"));
    lm.unlock(&txn2, "RID1");
}

TEST(LockManager_SharedExclusiveContention) {
    LockManager lm;
    Transaction txn1(101);
    Transaction txn2(102);
    std::atomic<bool> txn2_granted{false};

    lm.acquire_shared(&txn1, "RID1");

    std::thread t2([&]() {
        if (lm.acquire_exclusive(&txn2, "RID1")) {
            txn2_granted = true;
        }
    });

    /* Give it some time to try and block */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(txn2_granted.load());

    lm.unlock(&txn1, "RID1");

    /* Wait for t2 to finish */
    t2.join();
    EXPECT_TRUE(txn2_granted.load());
    lm.unlock(&txn2, "RID1");
}

TEST(LockManager_UpgradeBasic) {
    LockManager lm;
    Transaction txn1(101);

    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));
    EXPECT_TRUE(lm.acquire_exclusive(&txn1, "RID1")); /* Upgrade */

    lm.unlock(&txn1, "RID1");
}

TEST(LockManager_MultipleSharedContention) {
    LockManager lm;
    Transaction txn1(101);
    Transaction txn2(102);
    Transaction txn3(103);
    
    std::atomic<int> shared_granted{0};

    lm.acquire_exclusive(&txn1, "RID1");

    std::thread t2([&]() {
        if (lm.acquire_shared(&txn2, "RID1")) shared_granted++;
    });
    std::thread t3([&]() {
        if (lm.acquire_shared(&txn3, "RID1")) shared_granted++;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(shared_granted.load() == 0);

    lm.unlock(&txn1, "RID1");

    t2.join();
    t3.join();
    EXPECT_TRUE(shared_granted.load() == 2);
    
    lm.unlock(&txn2, "RID1");
    lm.unlock(&txn3, "RID1");
}

TEST(LockManager_UnlockInvalid) {
    LockManager lm;
    Transaction txn1(101);
    
    /* Unlock non-existent RID */
    EXPECT_FALSE(lm.unlock(&txn1, "NON_EXISTENT"));
    
    /* Unlock RID not held by this txn */
    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));
    Transaction txn2(102);
    EXPECT_FALSE(lm.unlock(&txn2, "RID1"));
    
    lm.unlock(&txn1, "RID1");
}

TEST(LockManager_AbortedWait) {
    LockManager lm;
    Transaction txn1(101);
    Transaction txn2(102);
    std::atomic<bool> success{false};

    lm.acquire_exclusive(&txn1, "RID1");

    std::thread t2([&]() {
        success = lm.acquire_shared(&txn2, "RID1");
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    /* Abort txn2 while it's waiting */
    txn2.set_state(TransactionState::ABORTED);
    lm.unlock(&txn1, "RID1"); /* This will trigger notify_all and wake txn2 */

    t2.join();
    EXPECT_FALSE(success.load());
}

TEST(LockManager_RedundantShared) {
    LockManager lm;
    Transaction txn1(101);
    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1"));
    EXPECT_TRUE(lm.acquire_shared(&txn1, "RID1")); // Coverage for line 19-20
    lm.unlock(&txn1, "RID1");
}

TEST(LockManager_ExclusiveAbortedWait) {
    LockManager lm;
    Transaction txn1(101);
    Transaction txn2(102);
    std::atomic<bool> success{true};

    lm.acquire_exclusive(&txn1, "RID1");

    std::thread t2([&]() {
        success = lm.acquire_exclusive(&txn2, "RID1");
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    txn2.set_state(TransactionState::ABORTED);
    lm.unlock(&txn1, "RID1");

    t2.join();
    EXPECT_FALSE(success.load()); // Coverage for line 100-101
}

int main() {
    std::cout << "Lock Manager Unit Tests" << std::endl;
    std::cout << "========================" << std::endl << std::endl;
    
    RUN_TEST(LockManager_SharedBasic);
    RUN_TEST(LockManager_ExclusiveBasic);
    RUN_TEST(LockManager_SharedExclusiveContention);
    RUN_TEST(LockManager_UpgradeBasic);
    RUN_TEST(LockManager_MultipleSharedContention);
    RUN_TEST(LockManager_UnlockInvalid);
    RUN_TEST(LockManager_AbortedWait);
    RUN_TEST(LockManager_RedundantShared);
    RUN_TEST(LockManager_ExclusiveAbortedWait);
    
    std::cout << std::endl << "========================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed" << std::endl;
    
    return (tests_failed > 0);
}
