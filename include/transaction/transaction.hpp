/**
 * @file transaction.hpp
 * @brief Transaction context definitions
 */

#ifndef CLOUDSQL_TRANSACTION_TRANSACTION_HPP
#define CLOUDSQL_TRANSACTION_TRANSACTION_HPP

#include <atomic>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "common/config.hpp"
#include "storage/heap_table.hpp"

namespace cloudsql::transaction {

using txn_id_t = uint64_t;

enum class TransactionState : uint8_t { RUNNING, COMMITTED, ABORTED };

enum class IsolationLevel : uint8_t { READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE };

/**
 * @brief Represents a snapshot of the system state for MVCC
 */
struct TransactionSnapshot {
    txn_id_t xmin = 0;  // Lower water mark (all txns < xmin are finished)
    txn_id_t xmax = 0;  // Upper water mark (all txns >= xmax are in the future)
    std::unordered_set<txn_id_t>
        active_txns;  // Set of txns between xmin and xmax that are still running

    [[nodiscard]] bool is_visible(txn_id_t id) const {
        if (id < xmin) {
            return true;
        }
        if (id >= xmax) {
            return false;
        }
        return active_txns.find(id) == active_txns.end();
    }
};

/**
 * @brief Represents a change that can be undone
 */
struct UndoLog {
    enum class Type : uint8_t { INSERT, DELETE, UPDATE };
    Type type = Type::INSERT;
    std::string table_name;
    storage::HeapTable::TupleId rid;
};

/**
 * @brief Represents a single transaction context
 */
class Transaction {
   private:
    txn_id_t txn_id_;
    std::atomic<TransactionState> state_;
    IsolationLevel isolation_level_;
    TransactionSnapshot snapshot_;
    int32_t prev_lsn_ = -1;  // Last LSN for this transaction

    // Locks held by this transaction (for auto-release on commit/abort)
    std::mutex lock_set_mutex_;
    std::unordered_set<std::string> shared_locks_;  // RID string
    std::unordered_set<std::string> exclusive_locks_;

    // Changes to undo on rollback
    std::vector<UndoLog> undo_logs_;

   public:
    explicit Transaction(txn_id_t txn_id, IsolationLevel level = IsolationLevel::REPEATABLE_READ)
        : txn_id_(txn_id), state_(TransactionState::RUNNING), isolation_level_(level) {}

    ~Transaction() = default;

    // Disable copy/move for transaction
    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;
    Transaction(Transaction&&) = delete;
    Transaction& operator=(Transaction&&) = delete;

    [[nodiscard]] txn_id_t get_id() const { return txn_id_; }
    [[nodiscard]] TransactionState get_state() const { return state_.load(); }
    void set_state(TransactionState state) { state_.store(state); }
    [[nodiscard]] IsolationLevel get_isolation_level() const { return isolation_level_; }

    [[nodiscard]] const TransactionSnapshot& get_snapshot() const { return snapshot_; }
    void set_snapshot(TransactionSnapshot snapshot) { snapshot_ = std::move(snapshot); }

    [[nodiscard]] int32_t get_prev_lsn() const { return prev_lsn_; }
    void set_prev_lsn(int32_t lsn) { prev_lsn_ = lsn; }

    void add_shared_lock(const std::string& rid) {
        const std::scoped_lock<std::mutex> lock(lock_set_mutex_);
        shared_locks_.insert(rid);
    }

    void add_exclusive_lock(const std::string& rid) {
        const std::scoped_lock<std::mutex> lock(lock_set_mutex_);
        exclusive_locks_.insert(rid);
    }

    [[nodiscard]] const std::unordered_set<std::string>& get_shared_locks() {
        const std::scoped_lock<std::mutex> lock(lock_set_mutex_);
        return shared_locks_;
    }
    [[nodiscard]] const std::unordered_set<std::string>& get_exclusive_locks() {
        const std::scoped_lock<std::mutex> lock(lock_set_mutex_);
        return exclusive_locks_;
    }

    void add_undo_log(UndoLog::Type type, const std::string& table_name,
                      const storage::HeapTable::TupleId& rid) {
        undo_logs_.push_back({type, table_name, rid});
    }

    [[nodiscard]] const std::vector<UndoLog>& get_undo_logs() const { return undo_logs_; }
};

}  // namespace cloudsql::transaction

#endif  // CLOUDSQL_TRANSACTION_TRANSACTION_HPP
