/**
 * @file transaction_manager.hpp
 * @brief Transaction Manager for lifecycle management
 */

#ifndef CLOUDSQL_TRANSACTION_TRANSACTION_MANAGER_HPP
#define CLOUDSQL_TRANSACTION_TRANSACTION_MANAGER_HPP

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

namespace cloudsql::transaction {

/**
 * @brief Manages the lifecycle of transactions
 */
class TransactionManager {
   public:
    explicit TransactionManager(LockManager& lock_manager, Catalog& catalog,
                                storage::BufferPoolManager& bpm,
                                recovery::LogManager* log_manager = nullptr);

    ~TransactionManager() = default;

    // Disable copy/move
    TransactionManager(const TransactionManager&) = delete;
    TransactionManager& operator=(const TransactionManager&) = delete;
    TransactionManager(TransactionManager&&) = delete;
    TransactionManager& operator=(TransactionManager&&) = delete;

    /**
     * @brief Start a new transaction
     * @param level Isolation level
     * @return Pointer to the new transaction
     */
    Transaction* begin(IsolationLevel level = IsolationLevel::REPEATABLE_READ);

    /**
     * @brief Commit a transaction
     */
    void commit(Transaction* txn);

    /**
     * @brief Prepare a transaction (2PC Phase 1)
     */
    void prepare(Transaction* txn);

    /**
     * @brief Abort a transaction
     */
    void abort(Transaction* txn);

    /**
     * @brief Get transaction by ID
     */
    Transaction* get_transaction(txn_id_t txn_id);

   private:
    LockManager& lock_manager_;
    Catalog& catalog_;
    storage::BufferPoolManager& bpm_;
    recovery::LogManager* log_manager_;

    std::atomic<txn_id_t> next_txn_id_{1};
    std::mutex manager_latch_;

    // All active transactions
    std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> active_transactions_;

    // Transactions that have recently finished (for cleanup/safety)
    std::deque<std::unique_ptr<Transaction>> completed_transactions_;

    /**
     * @brief Undo changes made by a transaction
     */
    bool undo_transaction(Transaction* txn);
};

}  // namespace cloudsql::transaction

#endif  // CLOUDSQL_TRANSACTION_TRANSACTION_MANAGER_HPP
