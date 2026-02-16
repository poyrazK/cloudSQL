/**
 * @file transaction_manager.hpp
 * @brief Transaction Manager for lifecycle management
 */

#ifndef CLOUDSQL_TRANSACTION_TRANSACTION_MANAGER_HPP
#define CLOUDSQL_TRANSACTION_TRANSACTION_MANAGER_HPP

#include <atomic>
#include <unordered_map>
#include <memory>
#include "transaction/transaction.hpp"
#include "transaction/lock_manager.hpp"
#include "catalog/catalog.hpp"
#include "storage/storage_manager.hpp"
#include "recovery/log_manager.hpp"

namespace cloudsql {
namespace transaction {

class TransactionManager {
private:
    std::atomic<txn_id_t> next_txn_id_{1};
    std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> active_transactions_;
    LockManager& lock_manager_;
    Catalog& catalog_;
    storage::StorageManager& storage_manager_;
    recovery::LogManager* log_manager_;
    std::mutex manager_latch_;

    void undo_transaction(Transaction* txn);

public:
    explicit TransactionManager(LockManager& lock_manager, 
                                Catalog& catalog,
                                storage::StorageManager& storage_manager,
                                recovery::LogManager* log_manager = nullptr) 
        : lock_manager_(lock_manager), catalog_(catalog), 
          storage_manager_(storage_manager), log_manager_(log_manager) {}

    Transaction* begin(IsolationLevel level = IsolationLevel::REPEATABLE_READ);
    void commit(Transaction* txn);
    void abort(Transaction* txn);
    
    Transaction* get_transaction(txn_id_t txn_id);
};

} // namespace transaction
} // namespace cloudsql

#endif // CLOUDSQL_TRANSACTION_TRANSACTION_MANAGER_HPP
