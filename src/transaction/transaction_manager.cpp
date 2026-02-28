/**
 * @file transaction_manager.cpp
 * @brief Transaction manager implementation
 */

#include "transaction/transaction_manager.hpp"

#include <memory>
#include <mutex>
#include <utility>

#include "recovery/log_manager.hpp"
#include "recovery/log_record.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

namespace cloudsql::transaction {

TransactionManager::TransactionManager(LockManager& lock_manager, recovery::LogManager* log_manager)
    : lock_manager_(lock_manager), log_manager_(log_manager) {}

Transaction* TransactionManager::begin(IsolationLevel level) {
    const std::scoped_lock<std::mutex> lock(manager_latch_);
    const txn_id_t txn_id = next_txn_id_++;
    auto txn = std::make_unique<Transaction>(txn_id, level);
    Transaction* const txn_ptr = txn.get();
    active_transactions_[txn_id] = std::move(txn);

    if (log_manager_ != nullptr) {
        recovery::LogRecord record(txn_id, txn_ptr->get_prev_lsn(), recovery::LogRecordType::BEGIN);
        const recovery::lsn_t lsn = log_manager_->append_log_record(record);
        txn_ptr->set_prev_lsn(lsn);
    }

    return txn_ptr;
}

void TransactionManager::commit(Transaction* txn) {
    if (log_manager_ != nullptr) {
        recovery::LogRecord record(txn->get_id(), txn->get_prev_lsn(),
                                   recovery::LogRecordType::COMMIT);
        const recovery::lsn_t lsn = log_manager_->append_log_record(record);
        txn->set_prev_lsn(lsn);
        log_manager_->flush(true);
    }

    const auto lock_set = txn->get_shared_lock_set();
    for (const auto& rid : lock_set) {
        lock_manager_.unlock(txn, rid);
    }
    const auto ex_lock_set = txn->get_exclusive_lock_set();
    for (const auto& rid : ex_lock_set) {
        lock_manager_.unlock(txn, rid);
    }

    txn->set_state(TransactionState::COMMITTED);

    {
        const std::scoped_lock<std::mutex> lock(manager_latch_);
        completed_transactions_.push_back(std::move(active_transactions_[txn->get_id()]));
        static_cast<void>(active_transactions_.erase(txn->get_id()));

        constexpr size_t MAX_COMPLETED = 100;
        if (completed_transactions_.size() > MAX_COMPLETED) {
            completed_transactions_.pop_front();
        }
    }
}

void TransactionManager::abort(Transaction* txn) {
    if (log_manager_ != nullptr) {
        recovery::LogRecord record(txn->get_id(), txn->get_prev_lsn(),
                                   recovery::LogRecordType::ABORT);
        const recovery::lsn_t lsn = log_manager_->append_log_record(record);
        txn->set_prev_lsn(lsn);
        log_manager_->flush(true);
    }

    const auto lock_set = txn->get_shared_lock_set();
    for (const auto& rid : lock_set) {
        lock_manager_.unlock(txn, rid);
    }
    const auto ex_lock_set = txn->get_exclusive_lock_set();
    for (const auto& rid : ex_lock_set) {
        lock_manager_.unlock(txn, rid);
    }

    txn->set_state(TransactionState::ABORTED);

    {
        const std::scoped_lock<std::mutex> lock(manager_latch_);
        completed_transactions_.push_back(std::move(active_transactions_[txn->get_id()]));
        static_cast<void>(active_transactions_.erase(txn->get_id()));

        constexpr size_t MAX_COMPLETED = 100;
        if (completed_transactions_.size() > MAX_COMPLETED) {
            completed_transactions_.pop_front();
        }
    }
}

Transaction* TransactionManager::get_transaction(txn_id_t txn_id) {
    const std::scoped_lock<std::mutex> lock(manager_latch_);
    if (active_transactions_.find(txn_id) != active_transactions_.end()) {
        return active_transactions_[txn_id].get();
    }
    return nullptr;
}

}  // namespace cloudsql::transaction
