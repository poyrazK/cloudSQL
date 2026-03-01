/**
 * @file distributed_executor.cpp
 * @brief High-level executor for distributed queries
 */

#include "distributed/distributed_executor.hpp"

#include <future>
#include <iostream>
#include <string>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/cluster_manager.hpp"
#include "distributed/shard_manager.hpp"
#include "network/rpc_client.hpp"
#include "network/rpc_message.hpp"
#include "parser/expression.hpp"
#include "parser/statement.hpp"

namespace cloudsql::executor {

DistributedExecutor::DistributedExecutor(Catalog& catalog, cluster::ClusterManager& cm)
    : catalog_(catalog), cluster_manager_(cm) {}

QueryResult DistributedExecutor::execute(const parser::Statement& stmt,
                                         const std::string& raw_sql) {
    (void)catalog_;  // Suppress unused warning

    // 1. Check if it's a DDL (Catalog) operation
    const auto type = stmt.type();
    if (type == parser::StmtType::CreateTable || type == parser::StmtType::DropTable ||
        type == parser::StmtType::CreateIndex || type == parser::StmtType::DropIndex) {
        // These are handled by Raft via the Catalog locally on the leader
        // and replicated to followers.
        return QueryResult();  // Default is success
    }

    auto data_nodes = cluster_manager_.get_data_nodes();
    if (data_nodes.empty()) {
        QueryResult res;
        res.set_error("No active data nodes in cluster");
        return res;
    }

    // 2. Distributed Transaction Management (2PC)
    // For simplicity, we assume a single active global transaction ID.
    constexpr uint64_t GLOBAL_TXN_ID = 1;

    if (type == parser::StmtType::TransactionCommit) {
        std::string errors;

        network::TxnOperationArgs args;
        args.txn_id = GLOBAL_TXN_ID;
        auto payload = args.serialize();

        // Phase 1: Prepare (Parallel)
        std::vector<std::future<std::pair<bool, std::string>>> prepare_futures;
        for (const auto& node : data_nodes) {
            prepare_futures.push_back(std::async(std::launch::async, [&node, payload]() {
                network::RpcClient client(node.address, node.cluster_port);
                if (client.connect()) {
                    std::vector<uint8_t> resp_payload;
                    if (client.call(network::RpcType::TxnPrepare, payload, resp_payload)) {
                        auto reply = network::QueryResultsReply::deserialize(resp_payload);
                        if (reply.success) return std::make_pair(true, std::string(""));
                        return std::make_pair(
                            false, "[" + node.id + "] Prepare failed: " + reply.error_msg);
                    }
                    return std::make_pair(false, "[" + node.id + "] RPC failed during prepare");
                }
                return std::make_pair(false, "[" + node.id + "] Connection failed during prepare");
            }));
        }

        bool all_prepared = true;
        for (auto& f : prepare_futures) {
            auto res = f.get();
            if (!res.first) {
                all_prepared = false;
                errors += res.second + "; ";
            }
        }

        // Phase 2: Commit or Abort (Parallel)
        const auto phase2_type =
            all_prepared ? network::RpcType::TxnCommit : network::RpcType::TxnAbort;

        std::vector<std::future<void>> phase2_futures;
        for (const auto& node : data_nodes) {
            phase2_futures.push_back(
                std::async(std::launch::async, [&node, payload, phase2_type]() {
                    network::RpcClient client(node.address, node.cluster_port);
                    if (client.connect()) {
                        std::vector<uint8_t> resp_payload;
                        static_cast<void>(client.call(phase2_type, payload, resp_payload));
                    }
                }));
        }
        for (auto& f : phase2_futures) f.get();

        if (all_prepared) {
            return QueryResult();
        }
        QueryResult res;
        res.set_error("Distributed transaction aborted: " + errors);
        return res;
    }

    if (type == parser::StmtType::TransactionRollback) {
        network::TxnOperationArgs args;
        args.txn_id = GLOBAL_TXN_ID;
        auto payload = args.serialize();

        std::vector<std::future<void>> rollback_futures;
        for (const auto& node : data_nodes) {
            rollback_futures.push_back(std::async(std::launch::async, [&node, payload]() {
                network::RpcClient client(node.address, node.cluster_port);
                if (client.connect()) {
                    std::vector<uint8_t> resp_payload;
                    static_cast<void>(
                        client.call(network::RpcType::TxnAbort, payload, resp_payload));
                }
            }));
        }
        for (auto& f : rollback_futures) f.get();
        return QueryResult();
    }

    // 3. Query Analysis for Routing
    std::vector<cluster::NodeInfo> target_nodes;

    if (type == parser::StmtType::Insert) {
        const auto* insert_stmt = dynamic_cast<const parser::InsertStatement*>(&stmt);
        if (insert_stmt && !insert_stmt->values().empty() && !insert_stmt->values()[0].empty()) {
            // Assume first column is sharding key
            const auto* first_val_expr = insert_stmt->values()[0][0].get();
            if (first_val_expr->type() == parser::ExprType::Constant) {
                const auto* const_expr = dynamic_cast<const parser::ConstantExpr*>(first_val_expr);
                if (const_expr) {
                    common::Value pk_val = const_expr->value();

                    uint32_t shard_idx = cluster::ShardManager::compute_shard(
                        pk_val, static_cast<uint32_t>(data_nodes.size()));
                    target_nodes.push_back(data_nodes[shard_idx]);
                }
            }
        }
    }

    // Fallback: Broadcast if we couldn't determine a specific shard
    if (target_nodes.empty()) {
        target_nodes = data_nodes;
    }

    network::ExecuteFragmentArgs args;
    args.sql = raw_sql;
    auto payload = args.serialize();

    bool all_success = true;
    std::string errors;

    for (const auto& node : target_nodes) {
        network::RpcClient client(node.address, node.cluster_port);
        if (client.connect()) {
            std::vector<uint8_t> resp_payload;
            if (client.call(network::RpcType::ExecuteFragment, payload, resp_payload)) {
                auto reply = network::QueryResultsReply::deserialize(resp_payload);
                if (!reply.success) {
                    all_success = false;
                    errors += "[" + node.id + "]: " + reply.error_msg + "; ";
                }
            } else {
                all_success = false;
                errors += "Failed to contact data node " + node.id + "; ";
            }
        } else {
            all_success = false;
            errors += "Failed to connect to data node " + node.id + "; ";
        }
    }

    if (all_success) return QueryResult();

    QueryResult res;
    res.set_error(errors);
    return res;
}

}  // namespace cloudsql::executor
