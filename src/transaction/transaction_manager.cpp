/**
 * @file transaction_manager.cpp
 * @brief Transaction manager implementation
 */

#include "transaction/transaction_manager.hpp"

#include <algorithm>
#include <cstddef>
#include <iostream>
#include <memory>
#include <mutex>
#include <utility>

#include "catalog/catalog.hpp"
#include "executor/types.hpp"
#include "recovery/log_manager.hpp"
#include "recovery/log_record.hpp"
#include "storage/btree_index.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

namespace cloudsql::transaction {

TransactionManager::TransactionManager(LockManager& lock_manager, Catalog& catalog,
                                       storage::BufferPoolManager& bpm,
                                       recovery::LogManager* log_manager)
    : lock_manager_(lock_manager), catalog_(catalog), bpm_(bpm), log_manager_(log_manager) {}

Transaction* TransactionManager::begin(IsolationLevel level) {
    const std::scoped_lock<std::mutex> lock(manager_latch_);
    const txn_id_t txn_id = next_txn_id_++;
    auto txn = std::make_unique<Transaction>(txn_id, level);

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

    if (log_manager_ != nullptr) {
        recovery::LogRecord record(txn_id, txn_ptr->get_prev_lsn(), recovery::LogRecordType::BEGIN);
        const recovery::lsn_t lsn = log_manager_->append_log_record(record);
        txn_ptr->set_prev_lsn(lsn);
    }

    return txn_ptr;
}

void TransactionManager::prepare(Transaction* txn) {
    if (txn->get_state() != TransactionState::RUNNING) {
        return;
    }

    if (log_manager_ != nullptr) {
        recovery::LogRecord record(txn->get_id(), txn->get_prev_lsn(),
                                   recovery::LogRecordType::PREPARE);
        const recovery::lsn_t lsn = log_manager_->append_log_record(record);
        txn->set_prev_lsn(lsn);
        log_manager_->flush(true);
    }

    txn->set_state(TransactionState::PREPARED);
}

void TransactionManager::commit(Transaction* txn) {
    if (txn->get_state() == TransactionState::COMMITTED) {
        return;
    }

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
        auto it = active_transactions_.find(txn->get_id());
        if (it != active_transactions_.end()) {
            completed_transactions_.push_back(std::move(it->second));
            active_transactions_.erase(it);
        }

        constexpr std::size_t MAX_COMPLETED = 100;
        if (completed_transactions_.size() > MAX_COMPLETED) {
            completed_transactions_.pop_front();
        }
    }
}

void TransactionManager::abort(Transaction* txn) {
    if (txn->get_state() == TransactionState::ABORTED) {
        return;
    }

    /* Undo all changes if not already committed */
    if (txn->get_state() != TransactionState::COMMITTED) {
        undo_transaction(txn);
    }

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
        auto it = active_transactions_.find(txn->get_id());
        if (it != active_transactions_.end()) {
            completed_transactions_.push_back(std::move(it->second));
            active_transactions_.erase(it);
        }

        constexpr std::size_t MAX_COMPLETED = 100;
        if (completed_transactions_.size() > MAX_COMPLETED) {
            completed_transactions_.pop_front();
        }
    }
}

bool TransactionManager::undo_transaction(Transaction* txn) {
    const auto& logs = txn->get_undo_logs();
    bool success = true;
    /* Undo in reverse order */
    for (auto it = logs.rbegin(); it != logs.rend(); ++it) {
        const auto& log = *it;
        auto table_meta_opt = catalog_.get_table_by_name(log.table_name);
        if (!table_meta_opt) {
            continue;
        }
        const auto* table_meta = table_meta_opt.value();

        /* Reconstruct schema for HeapTable */
        executor::Schema schema;
        for (const auto& col : table_meta->columns) {
            schema.add_column(col.name, col.type);
        }

        storage::HeapTable table(log.table_name, bpm_, schema);

        switch (log.type) {
            case UndoLog::Type::INSERT: {
                /* For INSERT undo, remove from indexes and then physical remove from heap */
                executor::Tuple tuple;
                if (table.get(log.rid, tuple)) {
                    for (const auto& idx_info : table_meta->indexes) {
                        if (!idx_info.column_positions.empty()) {
                            uint16_t pos = idx_info.column_positions[0];
                            common::ValueType ktype = table_meta->columns[pos].type;
                            storage::BTreeIndex index(idx_info.name, bpm_, ktype);
                            static_cast<void>(index.remove(tuple.get(pos), log.rid));
                        }
                    }
                }
                if (!table.physical_remove(log.rid)) {
                    std::cerr << "Rollback ERROR: physical_remove failed for INSERT undo\n";
                    success = false;
                }
                break;
            }
            case UndoLog::Type::DELETE: {
                /* For DELETE undo, reset xmax and re-insert into indexes */
                if (!table.undo_remove(log.rid)) {
                    std::cerr << "Rollback ERROR: undo_remove failed for DELETE undo\n";
                    success = false;
                } else {
                    executor::Tuple tuple;
                    if (table.get(log.rid, tuple)) {
                        for (const auto& idx_info : table_meta->indexes) {
                            if (!idx_info.column_positions.empty()) {
                                uint16_t pos = idx_info.column_positions[0];
                                common::ValueType ktype = table_meta->columns[pos].type;
                                storage::BTreeIndex index(idx_info.name, bpm_, ktype);
                                static_cast<void>(index.insert(tuple.get(pos), log.rid));
                            }
                        }
                    }
                }
                break;
            }
            case UndoLog::Type::UPDATE: {
                /* For UPDATE undo, remove new version from indexes/heap and restore old version's xmax/indexes */
                executor::Tuple new_tuple;
                if (table.get(log.rid, new_tuple)) {
                    for (const auto& idx_info : table_meta->indexes) {
                        if (!idx_info.column_positions.empty()) {
                            uint16_t pos = idx_info.column_positions[0];
                            common::ValueType ktype = table_meta->columns[pos].type;
                            storage::BTreeIndex index(idx_info.name, bpm_, ktype);
                            static_cast<void>(index.remove(new_tuple.get(pos), log.rid));
                        }
                    }
                }
                if (!table.physical_remove(log.rid)) {
                    std::cerr << "Rollback ERROR: physical_remove failed for new version in UPDATE undo\n";
                    success = false;
                }

                if (log.old_rid.has_value()) {
                    if (!table.undo_remove(log.old_rid.value())) {
                        std::cerr << "Rollback ERROR: undo_remove failed for old version in UPDATE undo\n";
                        success = false;
                    } else {
                        executor::Tuple old_tuple;
                        if (table.get(log.old_rid.value(), old_tuple)) {
                            for (const auto& idx_info : table_meta->indexes) {
                                if (!idx_info.column_positions.empty()) {
                                    uint16_t pos = idx_info.column_positions[0];
                                    common::ValueType ktype = table_meta->columns[pos].type;
                                    storage::BTreeIndex index(idx_info.name, bpm_, ktype);
                                    static_cast<void>(index.insert(old_tuple.get(pos), log.old_rid.value()));
                                }
                            }
                        }
                    }
                }
                break;
            }
        }
    }
    return success;
}

Transaction* TransactionManager::get_transaction(txn_id_t txn_id) {
    const std::scoped_lock<std::mutex> lock(manager_latch_);
    if (active_transactions_.find(txn_id) != active_transactions_.end()) {
        return active_transactions_[txn_id].get();
    }
    return nullptr;
}

}  // namespace cloudsql::transaction
