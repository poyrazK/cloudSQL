/**
 * @file transaction_manager.cpp
 * @brief Transaction Manager implementation
 */

#include "transaction/transaction_manager.hpp"

#include <algorithm>
#include <memory>
#include <mutex>
#include <utility>

#include "executor/types.hpp"
#include "recovery/log_record.hpp"
#include "storage/heap_table.hpp"
#include "transaction/transaction.hpp"

namespace cloudsql::transaction {

Transaction* TransactionManager::begin(IsolationLevel level) {
    const std::scoped_lock<std::mutex> lock(manager_latch_);

    const txn_id_t txn_id = next_txn_id_++;
    auto txn = std::make_unique<Transaction>(txn_id, level);

    /* Log BEGIN */
    if (log_manager_ != nullptr) {
        recovery::LogRecord log(txn_id, -1, recovery::LogRecordType::BEGIN);
        const auto lsn = log_manager_->append_log_record(log);
        txn->set_prev_lsn(lsn);
    }

    /* Capture Snapshot */
    TransactionSnapshot snapshot;
    snapshot.xmax = next_txn_id_.load();
    snapshot.xmin = snapshot.xmax;

    for (const auto& [id, _] : active_transactions_) {
        snapshot.active_txns.insert(id);
        snapshot.xmin = std::min(id, snapshot.xmin);
    }

    txn->set_snapshot(std::move(snapshot));

    Transaction* const txn_ptr = txn.get();
    active_transactions_[txn_id] = std::move(txn);

    return txn_ptr;
}

void TransactionManager::commit(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }

    if (log_manager_ != nullptr) {
        recovery::LogRecord log(txn->get_id(), txn->get_prev_lsn(),
                                recovery::LogRecordType::COMMIT);
        static_cast<void>(log_manager_->append_log_record(log));
        log_manager_->flush(true);
    }

    txn->set_state(TransactionState::COMMITTED);

    /* Release all locks */
    for (const auto& rid : txn->get_shared_locks()) {
        lock_manager_.unlock(txn, rid);
    }
    for (const auto& rid : txn->get_exclusive_locks()) {
        lock_manager_.unlock(txn, rid);
    }

    const std::scoped_lock<std::mutex> lock(manager_latch_);
    static_cast<void>(active_transactions_.erase(txn->get_id()));
}

void TransactionManager::abort(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }

    /* Undo all changes */
    undo_transaction(txn);

    if (log_manager_ != nullptr) {
        recovery::LogRecord log(txn->get_id(), txn->get_prev_lsn(), recovery::LogRecordType::ABORT);
        static_cast<void>(log_manager_->append_log_record(log));
        log_manager_->flush(true);
    }

    txn->set_state(TransactionState::ABORTED);

    /* Release all locks */
    for (const auto& rid : txn->get_shared_locks()) {
        lock_manager_.unlock(txn, rid);
    }
    for (const auto& rid : txn->get_exclusive_locks()) {
        lock_manager_.unlock(txn, rid);
    }

    const std::scoped_lock<std::mutex> lock(manager_latch_);
    static_cast<void>(active_transactions_.erase(txn->get_id()));
}

void TransactionManager::undo_transaction(Transaction* txn) {
    const auto& logs = txn->get_undo_logs();
    /* Undo in reverse order */
    for (auto it = logs.rbegin(); it != logs.rend(); ++it) {
        const auto& log = *it;
        auto table_meta = catalog_.get_table_by_name(log.table_name);
        if (!table_meta) {
            continue;
        }

        /* Reconstruct schema for HeapTable */
        executor::Schema schema;
        for (const auto& col : (*table_meta)->columns) {
            schema.add_column(col.name, col.type);
        }

        storage::HeapTable table(log.table_name, storage_manager_, schema);

        switch (log.type) {
            case UndoLog::Type::INSERT:
                static_cast<void>(table.physical_remove(log.rid));
                break;
            case UndoLog::Type::DELETE:
                /* TODO: Implement DELETE undo */
                static_cast<void>(0);
                break;
            case UndoLog::Type::UPDATE:
                /* TODO: Implement UPDATE undo */
                static_cast<void>(1);
                break;
        }
    }
}

Transaction* TransactionManager::get_transaction(txn_id_t txn_id) {
    const std::scoped_lock<std::mutex> lock(manager_latch_);
    auto it = active_transactions_.find(txn_id);
    if (it != active_transactions_.end()) {
        return it->second.get();
    }
    return nullptr;
}

}  // namespace cloudsql::transaction
