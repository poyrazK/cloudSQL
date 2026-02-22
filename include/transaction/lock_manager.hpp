/**
 * @file lock_manager.hpp
 * @brief Lock Manager for 2PL concurrency control
 */

#ifndef CLOUDSQL_TRANSACTION_LOCK_MANAGER_HPP
#define CLOUDSQL_TRANSACTION_LOCK_MANAGER_HPP

#include <condition_variable>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "transaction/transaction.hpp"

namespace cloudsql::transaction {

enum class LockMode : uint8_t { SHARED, EXCLUSIVE };

class LockManager {
   private:
    struct LockRequest {
        txn_id_t txn_id = 0;
        LockMode mode = LockMode::SHARED;
        bool granted = false;
    };

    struct LockQueue {
        std::list<LockRequest> request_queue;
        std::condition_variable cv;
        bool upgrading = false;  // Prevents starvation during upgrade (not fully impl yet)
    };

    std::mutex latch_;
    std::unordered_map<std::string, LockQueue> lock_table_;  // RID -> LockQueue

   public:
    LockManager() = default;
    ~LockManager() = default;

    // Disable copy/move for lock manager
    LockManager(const LockManager&) = delete;
    LockManager& operator=(const LockManager&) = delete;
    LockManager(LockManager&&) = delete;
    LockManager& operator=(LockManager&&) = delete;

    /**
     * @brief Acquire a shared (read) lock on a tuple
     */
    bool acquire_shared(Transaction* txn, const std::string& rid);

    /**
     * @brief Acquire an exclusive (write) lock on a tuple
     */
    bool acquire_exclusive(Transaction* txn, const std::string& rid);

    /**
     * @brief Unlock a tuple
     */
    bool unlock(Transaction* txn, const std::string& rid);
};

}  // namespace cloudsql::transaction

#endif  // CLOUDSQL_TRANSACTION_LOCK_MANAGER_HPP
