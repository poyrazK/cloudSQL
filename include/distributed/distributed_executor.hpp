/**
 * @file distributed_executor.hpp
 * @brief High-level executor for distributed queries
 */
#ifndef SQL_ENGINE_DISTRIBUTED_EXECUTOR_HPP
#define SQL_ENGINE_DISTRIBUTED_EXECUTOR_HPP

#include <memory>
#include <string>

#include "catalog/catalog.hpp"
#include "common/cluster_manager.hpp"
#include "executor/query_executor.hpp"
#include "parser/statement.hpp"

namespace cloudsql::executor {

/**
 * @brief Handles distributed query routing and execution
 */
class DistributedExecutor {
   public:
    DistributedExecutor(Catalog& catalog, cluster::ClusterManager& cm);

    /**
     * @brief Execute a statement across the cluster
     */
    QueryResult execute(const parser::Statement& stmt, const std::string& raw_sql);

   private:
    /**
     * @brief Fetch data for a table from all nodes and broadcast it to all nodes
     */
    bool broadcast_table(const std::string& table_name);

    Catalog& catalog_;
    cluster::ClusterManager& cluster_manager_;
};

}  // namespace cloudsql::executor

#endif  // SQL_ENGINE_DISTRIBUTED_EXECUTOR_HPP
