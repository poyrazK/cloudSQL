/**
 * @file transaction_manager.hpp
 * @brief Transaction Manager for lifecycle management
 */

#ifndef CLOUDSQL_TRANSACTION_TRANSACTION_MANAGER_HPP
#define CLOUDSQL_TRANSACTION_TRANSACTION_MANAGER_HPP

#include <atomic>
#include <memory>
#include <unordered_map>

#include "catalog/catalog.hpp"
#include "recovery/log_manager.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

namespace cloudsql::transaction {

class TransactionManager {
   private:
    std::atomic<txn_id_t> next_txn_id_{1};
    std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> active_transactions_;
    LockManager& lock_manager_;
    Catalog& catalog_;
    storage::BufferPoolManager& bpm_;
    recovery::LogManager* log_manager_;
    std::mutex manager_latch_;

    void undo_transaction(Transaction* txn);

   public:
    explicit TransactionManager(LockManager& lock_manager, Catalog& catalog,
                                storage::BufferPoolManager& bpm,
                                recovery::LogManager* log_manager = nullptr)
        : lock_manager_(lock_manager), catalog_(catalog), bpm_(bpm), log_manager_(log_manager){}

    Transaction* begin(IsolationLevel level = IsolationLevel::REPEATABLE_READ);
    void commit(Transaction* txn);
    void abort(Transaction* txn);

    Transaction* get_transaction(txn_id_t txn_id);
};

}  // namespace cloudsql::transaction

#endif  // CLOUDSQL_TRANSACTION_TRANSACTION_MANAGER_HPP
