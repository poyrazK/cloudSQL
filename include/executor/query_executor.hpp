/**
 * @file query_executor.hpp
 * @brief High-level query executor
 */

#ifndef CLOUDSQL_EXECUTOR_QUERY_EXECUTOR_HPP
#define CLOUDSQL_EXECUTOR_QUERY_EXECUTOR_HPP

#include "catalog/catalog.hpp"
#include "common/cluster_manager.hpp"
#include "distributed/raft_types.hpp"
#include "executor/operator.hpp"
#include "executor/types.hpp"
#include "parser/statement.hpp"
#include "recovery/log_manager.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace cloudsql::executor {

/**
 * @brief State machine for a specific data shard
 */
class ShardStateMachine : public raft::RaftStateMachine {
   public:
    ShardStateMachine(std::string table_name, storage::BufferPoolManager& bpm, Catalog& catalog)
        : table_name_(std::move(table_name)), bpm_(bpm), catalog_(catalog) {}

    void apply(const raft::LogEntry& entry) override;

   private:
    std::string table_name_;
    storage::BufferPoolManager& bpm_;
    Catalog& catalog_;
};

/**
 * @brief Top-level executor that coordinates planning and operator execution
 */
class QueryExecutor {
   public:
    QueryExecutor(Catalog& catalog, storage::BufferPoolManager& bpm,
                  transaction::LockManager& lock_manager,
                  transaction::TransactionManager& transaction_manager,
                  recovery::LogManager* log_manager = nullptr,
                  cluster::ClusterManager* cluster_manager = nullptr);
    ~QueryExecutor();

    // Disable copy/move for executor
    QueryExecutor(const QueryExecutor&) = delete;
    QueryExecutor& operator=(const QueryExecutor&) = delete;
    QueryExecutor(QueryExecutor&&) = delete;
    QueryExecutor& operator=(QueryExecutor&&) = delete;

    /**
     * @brief Set the context ID for the current execution (for shuffle data lookups)
     */
    void set_context_id(const std::string& ctx_id) { context_id_ = ctx_id; }

    /**
     * @brief Execute a SQL statement and return results
     */
    QueryResult execute(const parser::Statement& stmt);

   private:
    Catalog& catalog_;
    storage::BufferPoolManager& bpm_;
    transaction::LockManager& lock_manager_;
    transaction::TransactionManager& transaction_manager_;
    recovery::LogManager* log_manager_;
    cluster::ClusterManager* cluster_manager_;
    std::string context_id_;
    transaction::Transaction* current_txn_ = nullptr;

    QueryResult execute_select(const parser::SelectStatement& stmt, transaction::Transaction* txn);
    QueryResult execute_create_table(const parser::CreateTableStatement& stmt);
    QueryResult execute_create_index(const parser::CreateIndexStatement& stmt);
    QueryResult execute_drop_table(const parser::DropTableStatement& stmt);
    QueryResult execute_drop_index(const parser::DropIndexStatement& stmt);
    QueryResult execute_insert(const parser::InsertStatement& stmt, transaction::Transaction* txn);
    QueryResult execute_update(const parser::UpdateStatement& stmt, transaction::Transaction* txn);
    QueryResult execute_delete(const parser::DeleteStatement& stmt, transaction::Transaction* txn);

    /* Transaction control */
    QueryResult execute_begin();
    QueryResult execute_commit();
    QueryResult execute_rollback();

    /* Helper to build operator tree from SELECT */
    std::unique_ptr<Operator> build_plan(const parser::SelectStatement& stmt,
                                         transaction::Transaction* txn);
};

}  // namespace cloudsql::executor

#endif  // CLOUDSQL_EXECUTOR_QUERY_EXECUTOR_HPP
