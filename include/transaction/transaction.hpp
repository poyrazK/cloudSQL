/**
 * @file transaction.hpp
 * @brief Transaction context definitions
 */

#ifndef CLOUDSQL_TRANSACTION_TRANSACTION_HPP
#define CLOUDSQL_TRANSACTION_TRANSACTION_HPP

#include <atomic>
#include <vector>
#include <unordered_set>
#include <mutex>
#include "common/config.hpp"
#include "storage/heap_table.hpp"

namespace cloudsql {
namespace transaction {

using txn_id_t = uint64_t;

enum class TransactionState {
    RUNNING,
    COMMITTED,
    ABORTED
};

enum class IsolationLevel {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
};

/**
 * @brief Represents a snapshot of the system state for MVCC
 */
struct TransactionSnapshot {
    txn_id_t xmin; // Lower water mark (all txns < xmin are finished)
    txn_id_t xmax; // Upper water mark (all txns >= xmax are in the future)
    std::unordered_set<txn_id_t> active_txns; // Set of txns between xmin and xmax that are still running

    bool is_visible(txn_id_t id) const {
        if (id < xmin) return true;
        if (id >= xmax) return false;
        return active_txns.find(id) == active_txns.end();
    }
};

/**
 * @brief Represents a change that can be undone
 */
struct UndoLog {
    enum class Type { INSERT, DELETE, UPDATE };
    Type type;
    std::string table_name;
    storage::HeapTable::TupleId rid;
};

/**
 * @brief Represents a single transaction context
 */
class Transaction {
private:
    txn_id_t txn_id_;
    TransactionState state_;
    IsolationLevel isolation_level_;
    TransactionSnapshot snapshot_;
    
    // Locks held by this transaction (for auto-release on commit/abort)
    std::mutex lock_set_mutex_;
    std::unordered_set<std::string> shared_locks_;    // RID string
    std::unordered_set<std::string> exclusive_locks_;

    // Changes to undo on rollback
    std::vector<UndoLog> undo_logs_;

public:
    explicit Transaction(txn_id_t txn_id, IsolationLevel level = IsolationLevel::REPEATABLE_READ)
        : txn_id_(txn_id), state_(TransactionState::RUNNING), isolation_level_(level) {}

    txn_id_t get_id() const { return txn_id_; }
    TransactionState get_state() const { return state_; }
    void set_state(TransactionState state) { state_ = state; }
    IsolationLevel get_isolation_level() const { return isolation_level_; }

    const TransactionSnapshot& get_snapshot() const { return snapshot_; }
    void set_snapshot(TransactionSnapshot snapshot) { snapshot_ = std::move(snapshot); }

    void add_shared_lock(const std::string& rid) {
        std::lock_guard<std::mutex> lock(lock_set_mutex_);
        shared_locks_.insert(rid);
    }

    void add_exclusive_lock(const std::string& rid) {
        std::lock_guard<std::mutex> lock(lock_set_mutex_);
        exclusive_locks_.insert(rid);
    }

    const std::unordered_set<std::string>& get_shared_locks() { return shared_locks_; }
    const std::unordered_set<std::string>& get_exclusive_locks() { return exclusive_locks_; }

    void add_undo_log(UndoLog::Type type, const std::string& table_name, const storage::HeapTable::TupleId& rid) {
        undo_logs_.push_back({type, table_name, rid});
    }

    const std::vector<UndoLog>& get_undo_logs() const { return undo_logs_; }
};

} // namespace transaction
} // namespace cloudsql

#endif // CLOUDSQL_TRANSACTION_TRANSACTION_HPP
