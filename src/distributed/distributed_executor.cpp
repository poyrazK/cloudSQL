/**
 * @file distributed_executor.cpp
 * @brief High-level executor for distributed queries
 */

#include "distributed/distributed_executor.hpp"

#include <algorithm>
#include <future>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/cluster_manager.hpp"
#include "common/value.hpp"
#include "distributed/shard_manager.hpp"
#include "network/rpc_client.hpp"
#include "network/rpc_message.hpp"
#include "parser/expression.hpp"
#include "parser/statement.hpp"

namespace cloudsql::executor {

namespace {

/**
 * @brief Simple helper to extract sharding key from WHERE clause
 * Currently handles only "id = constant" format for POC
 */
bool try_extract_sharding_key(const parser::Expression* where, common::Value& out_val) {
    if (where == nullptr || where->type() != parser::ExprType::Binary) {
        return false;
    }

    const auto* bin_expr = dynamic_cast<const parser::BinaryExpr*>(where);
    if (bin_expr == nullptr || bin_expr->op() != parser::TokenType::Eq) {
        return false;
    }

    // Check if left is Column and right is Constant
    if (bin_expr->left().type() == parser::ExprType::Column &&
        bin_expr->right().type() == parser::ExprType::Constant) {
        const auto* const_expr = dynamic_cast<const parser::ConstantExpr*>(&bin_expr->right());
        if (const_expr != nullptr) {
            out_val = const_expr->value();
            return true;
        }
    }

    // Check if right is Column and left is Constant
    if (bin_expr->right().type() == parser::ExprType::Column &&
        bin_expr->left().type() == parser::ExprType::Constant) {
        const auto* const_expr = dynamic_cast<const parser::ConstantExpr*>(&bin_expr->left());
        if (const_expr != nullptr) {
            out_val = const_expr->value();
            return true;
        }
    }

    return false;
}

/**
 * @brief Normalizes a column identifier by stripping table qualification
 */
std::string normalize_key(const parser::Expression& expr) {
    std::string s = expr.to_string();
    size_t dot = s.find_last_of('.');
    if (dot != std::string::npos) {
        return s.substr(dot + 1);
    }
    return s;
}

}  // namespace

DistributedExecutor::DistributedExecutor(Catalog& catalog, cluster::ClusterManager& cm)
    : catalog_(catalog), cluster_manager_(cm) {}

namespace {
static std::atomic<uint64_t> next_context_id{1};
}

QueryResult DistributedExecutor::execute(const parser::Statement& stmt,
                                         const std::string& raw_sql) {
    (void)catalog_;  // Suppress unused warning

    // 1. Check if it's a DDL (Catalog) operation
    const auto type = stmt.type();
    if (type == parser::StmtType::CreateTable || type == parser::StmtType::DropTable ||
        type == parser::StmtType::CreateIndex || type == parser::StmtType::DropIndex) {
        // Metadata operations (Group 0) must be routed to the Catalog Leader
        std::string leader_id = cluster_manager_.get_leader(0);
        auto nodes = cluster_manager_.get_coordinators();

        const cluster::NodeInfo* target = nullptr;
        if (!leader_id.empty()) {
            for (const auto& n : nodes) {
                if (n.id == leader_id) {
                    target = &n;
                    break;
                }
            }
        }

        // Fallback: route to first coordinator if leader unknown (leader will redirect or proxy)
        if (!target && !nodes.empty()) target = &nodes[0];

        if (target) {
            network::RpcClient client(target->address, target->cluster_port);
            if (client.connect()) {
                // In a full implementation, DDL would be sent as a Catalog-specific RPC
                // For POC, we treat it success locally after replication initiation
            }
        }
        return {};
    }

    auto data_nodes = cluster_manager_.get_data_nodes();
    if (data_nodes.empty()) {
        QueryResult res;
        res.set_error("No active data nodes in cluster");
        return res;
    }

    // Step 2: Advanced Joins: Broadcast or Shuffle Join Orchestration
    std::string context_id = "ctx_" + std::to_string(next_context_id.fetch_add(1));

    if (type == parser::StmtType::Select) {
        const auto* select_stmt = dynamic_cast<const parser::SelectStatement*>(&stmt);
        if (select_stmt != nullptr && !select_stmt->joins().empty()) {
            // POC: For multi-shard joins, use Shuffle Join if tables are "large"
            // (In this POC, we always Shuffle if it's a complex join)
            for (const auto& join : select_stmt->joins()) {
                const std::string left_table = select_stmt->from()->to_string();
                const std::string right_table = join.table->to_string();

                // Assume join key is in the condition
                std::string left_key;
                std::string right_key;
                if (join.condition && join.condition->type() == parser::ExprType::Binary) {
                    const auto* bin_expr =
                        dynamic_cast<const parser::BinaryExpr*>(join.condition.get());
                    if (bin_expr != nullptr && bin_expr->op() == parser::TokenType::Eq) {
                        left_key = normalize_key(bin_expr->left());
                        right_key = normalize_key(bin_expr->right());
                    }
                }

                if (left_key.empty() || right_key.empty()) {
                    QueryResult res;
                    res.set_error("Shuffle Join requires equality join condition");
                    return res;
                }

                std::cout << "[Executor] Orchestrating Shuffle Join (" << left_table << " <-> "
                          << right_table << ")\n";

                // Phase 1: Instruct nodes to shuffle Left Table
                network::ShuffleFragmentArgs left_args;
                left_args.context_id = context_id;
                left_args.table_name = left_table;
                left_args.join_key_col = left_key;
                auto left_payload = left_args.serialize();

                for (const auto& node : data_nodes) {
                    network::RpcClient client(node.address, node.cluster_port);
                    if (!client.connect()) {
                        QueryResult res;
                        res.set_error("Failed to connect to node " + node.id + " for shuffle");
                        return res;
                    }
                    std::vector<uint8_t> resp;
                    if (!client.call(network::RpcType::ShuffleFragment, left_payload, resp)) {
                        QueryResult res;
                        res.set_error("Shuffle RPC failed on node " + node.id);
                        return res;
                    }
                    auto reply = network::QueryResultsReply::deserialize(resp);
                    if (!reply.success) {
                        QueryResult res;
                        res.set_error("Shuffle failed on node " + node.id + ": " + reply.error_msg);
                        return res;
                    }
                }

                // Phase 2: Instruct nodes to shuffle Right Table
                network::ShuffleFragmentArgs right_args;
                right_args.context_id = context_id;
                right_args.table_name = right_table;
                right_args.join_key_col = right_key;
                auto right_payload = right_args.serialize();

                for (const auto& node : data_nodes) {
                    network::RpcClient client(node.address, node.cluster_port);
                    if (!client.connect()) {
                        QueryResult res;
                        res.set_error("Failed to connect to node " + node.id + " for shuffle");
                        return res;
                    }
                    std::vector<uint8_t> resp;
                    if (!client.call(network::RpcType::ShuffleFragment, right_payload, resp)) {
                        QueryResult res;
                        res.set_error("Shuffle RPC failed on node " + node.id);
                        return res;
                    }
                    auto reply = network::QueryResultsReply::deserialize(resp);
                    if (!reply.success) {
                        QueryResult res;
                        res.set_error("Shuffle failed on node " + node.id + ": " + reply.error_msg);
                        return res;
                    }
                }
            }
        }
    }

    // 2. Distributed Transaction Management (2PC)
    // For simplicity, we assume a single active global transaction ID.
    constexpr uint64_t GLOBAL_TXN_ID = 1;

    if (type == parser::StmtType::TransactionRollback) {
        network::TxnOperationArgs args;
        args.txn_id = GLOBAL_TXN_ID;
        auto payload = args.serialize();

        std::vector<std::future<void>> rollback_futures;
        for (const auto& node : data_nodes) {
            rollback_futures.push_back(std::async(std::launch::async, [node, payload]() {
                network::RpcClient client(node.address, node.cluster_port);
                if (client.connect()) {
                    std::vector<uint8_t> resp_payload;
                    static_cast<void>(
                        client.call(network::RpcType::TxnAbort, payload, resp_payload));
                }
            }));
        }
        for (auto& f : rollback_futures) {
            f.get();
        }
        return {};
    }

    if (type == parser::StmtType::TransactionCommit) {
        std::string errors;

        network::TxnOperationArgs args;
        args.txn_id = GLOBAL_TXN_ID;
        auto payload = args.serialize();

        // Phase 1: Prepare (Parallel)
        std::vector<std::future<std::pair<bool, std::string>>> prepare_futures;
        for (const auto& node : data_nodes) {
            prepare_futures.push_back(std::async(std::launch::async, [node, payload]() {
                network::RpcClient client(node.address, node.cluster_port);
                if (client.connect()) {
                    std::vector<uint8_t> resp_payload;
                    if (client.call(network::RpcType::TxnPrepare, payload, resp_payload)) {
                        auto reply = network::QueryResultsReply::deserialize(resp_payload);
                        if (reply.success) {
                            return std::make_pair(true, std::string(""));
                        }
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
            auto res_p = f.get();
            if (!res_p.first) {
                all_prepared = false;
                errors += res_p.second + "; ";
            }
        }

        // Phase 2: Commit or Abort (Parallel)
        const auto phase2_type =
            all_prepared ? network::RpcType::TxnCommit : network::RpcType::TxnAbort;

        std::vector<std::future<void>> phase2_futures;
        for (const auto& node : data_nodes) {
            phase2_futures.push_back(std::async(std::launch::async, [node, payload, phase2_type]() {
                network::RpcClient client(node.address, node.cluster_port);
                if (client.connect()) {
                    std::vector<uint8_t> resp_payload;
                    static_cast<void>(client.call(phase2_type, payload, resp_payload));
                }
            }));
        }
        for (auto& f : phase2_futures) {
            f.get();
        }

        if (all_prepared) {
            return {};
        }
        QueryResult res;
        res.set_error("Distributed transaction aborted: " + errors);
        return res;
    }

    // 3. Query Analysis for Routing
    std::vector<cluster::NodeInfo> target_nodes;

    if (type == parser::StmtType::Insert) {
        const auto* insert_stmt = dynamic_cast<const parser::InsertStatement*>(&stmt);
        if (insert_stmt != nullptr && !insert_stmt->values().empty() &&
            !insert_stmt->values()[0].empty()) {
            // Assume first column is sharding key
            const auto* first_val_expr = insert_stmt->values()[0][0].get();
            if (first_val_expr->type() == parser::ExprType::Constant) {
                const auto* const_expr = dynamic_cast<const parser::ConstantExpr*>(first_val_expr);
                if (const_expr != nullptr) {
                    const common::Value pk_val = const_expr->value();

                    const uint32_t shard_idx = cluster::ShardManager::compute_shard(
                        pk_val, static_cast<uint32_t>(data_nodes.size()));

                    // Leader-Aware Routing: Find shard leader
                    std::string leader_id = cluster_manager_.get_leader(shard_idx + 1);
                    bool found_leader = false;
                    if (!leader_id.empty()) {
                        for (const auto& node : data_nodes) {
                            if (node.id == leader_id) {
                                target_nodes.push_back(node);
                                found_leader = true;
                                break;
                            }
                        }
                    }

                    if (!found_leader) target_nodes.push_back(data_nodes[shard_idx]);
                }
            }
        }
    } else if (type == parser::StmtType::Select || type == parser::StmtType::Update ||
               type == parser::StmtType::Delete) {
        // Try shard pruning based on WHERE clause
        const parser::Expression* where_expr = nullptr;
        if (type == parser::StmtType::Select) {
            where_expr = dynamic_cast<const parser::SelectStatement*>(&stmt)->where();
        } else if (type == parser::StmtType::Update) {
            where_expr = dynamic_cast<const parser::UpdateStatement*>(&stmt)->where();
        } else if (type == parser::StmtType::Delete) {
            where_expr = dynamic_cast<const parser::DeleteStatement*>(&stmt)->where();
        }

        common::Value pk_val;
        if (try_extract_sharding_key(where_expr, pk_val)) {
            const uint32_t shard_idx = cluster::ShardManager::compute_shard(
                pk_val, static_cast<uint32_t>(data_nodes.size()));

            // Leader-Aware Routing: Route mutations/queries to the current shard leader
            std::string leader_id = cluster_manager_.get_leader(shard_idx + 1);
            bool found_leader = false;
            if (!leader_id.empty()) {
                for (const auto& node : data_nodes) {
                    if (node.id == leader_id) {
                        target_nodes.push_back(node);
                        found_leader = true;
                        break;
                    }
                }
            }
            if (!found_leader) target_nodes.push_back(data_nodes[shard_idx]);
        }
    }

    // Fallback: Broadcast if we couldn't determine a specific shard
    if (target_nodes.empty()) {
        target_nodes = data_nodes;
    }

    network::ExecuteFragmentArgs fragment_args;
    fragment_args.sql = raw_sql;
    fragment_args.context_id = context_id;
    auto fragment_payload = fragment_args.serialize();

    bool all_success = true;
    std::string errors;
    std::vector<executor::Tuple> aggregated_rows;

    std::vector<std::future<std::pair<bool, network::QueryResultsReply>>> query_futures;
    for (const auto& node : target_nodes) {
        query_futures.push_back(std::async(std::launch::async, [node, fragment_payload]() {
            network::RpcClient client(node.address, node.cluster_port);
            network::QueryResultsReply reply;
            if (client.connect()) {
                std::vector<uint8_t> resp_payload;
                if (client.call(network::RpcType::ExecuteFragment, fragment_payload,
                                resp_payload)) {
                    reply = network::QueryResultsReply::deserialize(resp_payload);
                    return std::make_pair(true, reply);
                }
            }
            reply.success = false;
            reply.error_msg = "Failed to contact node " + node.id;
            return std::make_pair(false, reply);
        }));
    }

    for (auto& f : query_futures) {
        auto res_fut = f.get();
        if (res_fut.first && res_fut.second.success) {
            for (auto& row : res_fut.second.rows) {
                aggregated_rows.push_back(std::move(row));
            }
        } else {
            all_success = false;
            errors += "[" + res_fut.second.error_msg + "]; ";
        }
    }

    if (all_success) {
        QueryResult res;

        // Step 2: Check for global aggregates (COUNT, SUM)
        bool has_aggregate = false;
        if (type == parser::StmtType::Select) {
            const auto* select_stmt = dynamic_cast<const parser::SelectStatement*>(&stmt);
            for (const auto& col : select_stmt->columns()) {
                if (col->type() == parser::ExprType::Function) {
                    const auto* func = dynamic_cast<const parser::FunctionExpr*>(col.get());
                    if (func->name() == "COUNT" || func->name() == "SUM") {
                        has_aggregate = true;
                        break;
                    }
                }
            }
        }

        if (has_aggregate && !aggregated_rows.empty()) {
            // Simplified Merge: Assume single aggregate for POC
            // Sum up values from the first column of all rows
            int64_t total = 0;
            std::cout << "[Executor] Merging " << aggregated_rows.size() << " aggregate results\n";
            for (const auto& row : aggregated_rows) {
                if (!row.empty()) {
                    total += row.get(0).as_int64();
                }
            }
            executor::Tuple merged_tuple;
            merged_tuple.values().push_back(common::Value::make_int64(total));
            res.add_row(std::move(merged_tuple));
        } else {
            for (auto& row : aggregated_rows) {
                res.add_row(std::move(row));
            }
        }
        return res;
    }

    QueryResult res;
    res.set_error(errors);
    return res;
}

bool DistributedExecutor::broadcast_table(const std::string& table_name) {
    auto data_nodes = cluster_manager_.get_data_nodes();
    if (data_nodes.empty()) {
        return false;
    }

    // Use a unique context for this broadcast
    std::string context_id =
        "broadcast_" + table_name + "_" + std::to_string(next_context_id.fetch_add(1));

    // 1. Fetch data from all shards
    network::ExecuteFragmentArgs fetch_args;
    fetch_args.sql = "SELECT * FROM " + table_name;
    fetch_args.context_id = context_id;
    fetch_args.is_fetch_all = true;
    auto fetch_payload = fetch_args.serialize();

    std::vector<executor::Tuple> all_rows;
    for (const auto& node : data_nodes) {
        network::RpcClient client(node.address, node.cluster_port);
        if (client.connect()) {
            std::vector<uint8_t> resp_payload;
            if (client.call(network::RpcType::ExecuteFragment, fetch_payload, resp_payload)) {
                auto reply = network::QueryResultsReply::deserialize(resp_payload);
                if (reply.success) {
                    all_rows.insert(all_rows.end(), std::make_move_iterator(reply.rows.begin()),
                                    std::make_move_iterator(reply.rows.end()));
                }
            }
        }
    }

    if (all_rows.empty()) {
        return true;  // Empty table is fine
    }

    // 2. Push data to all nodes
    network::PushDataArgs push_args;
    push_args.context_id = context_id;  // Data nodes will look for this context
    push_args.table_name = table_name;
    push_args.rows = std::move(all_rows);
    auto push_payload = push_args.serialize();

    for (const auto& node : data_nodes) {
        network::RpcClient client(node.address, node.cluster_port);
        if (client.connect()) {
            std::vector<uint8_t> resp_payload;
            static_cast<void>(client.call(network::RpcType::PushData, push_payload, resp_payload));
        }
    }

    return true;
}

}  // namespace cloudsql::executor
