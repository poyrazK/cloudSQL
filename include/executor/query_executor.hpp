/**
 * @file query_executor.hpp
 * @brief High-level query executor
 */

#ifndef CLOUDSQL_EXECUTOR_QUERY_EXECUTOR_HPP
#define CLOUDSQL_EXECUTOR_QUERY_EXECUTOR_HPP

#include "parser/statement.hpp"
#include "executor/types.hpp"
#include "executor/operator.hpp"
#include "catalog/catalog.hpp"
#include "storage/storage_manager.hpp"

namespace cloudsql {
namespace executor {

/**
 * @brief Top-level executor that coordinates planning and operator execution
 */
class QueryExecutor {
public:
    QueryExecutor(Catalog& catalog, storage::StorageManager& storage_manager);
    ~QueryExecutor() = default;

    /**
     * @brief Execute a SQL statement and return results
     */
    QueryResult execute(const parser::Statement& stmt);

private:
    Catalog& catalog_;
    storage::StorageManager& storage_manager_;

    QueryResult execute_select(const parser::SelectStatement& stmt);
    QueryResult execute_create_table(const parser::CreateTableStatement& stmt);
    QueryResult execute_insert(const parser::InsertStatement& stmt);
    
    /* Helper to build operator tree from SELECT */
    std::unique_ptr<Operator> build_plan(const parser::SelectStatement& stmt);
};

} // namespace executor
} // namespace cloudsql

#endif // CLOUDSQL_EXECUTOR_QUERY_EXECUTOR_HPP
