/**
 * @file distributed_executor.cpp
 * @brief High-level executor for distributed queries
 */

#include "distributed/distributed_executor.hpp"

#include <algorithm>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/bloom_filter.hpp"
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

/**
 * @brief Strips LIMIT and OFFSET clauses from a SQL string for fragment execution
 */
std::string strip_limit_offset(const std::string& sql) {
    std::string s = sql;
    std::string upper_s = s;
    std::transform(upper_s.begin(), upper_s.end(), upper_s.begin(), ::toupper);

    size_t limit_pos = upper_s.find(" LIMIT ");
    size_t offset_pos = upper_s.find(" OFFSET ");

    size_t strip_pos = std::string::npos;
    if (limit_pos != std::string::npos && offset_pos != std::string::npos) {
        strip_pos = std::min(limit_pos, offset_pos);
    } else if (limit_pos != std::string::npos) {
        strip_pos = limit_pos;
    } else if (offset_pos != std::string::npos) {
        strip_pos = offset_pos;
    }

    if (strip_pos != std::string::npos) {
        std::string result = s.substr(0, strip_pos);
        // Ensure it ends with a semicolon if the original did
        if (s.back() == ';') result += ";";
        return result;
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
    auto data_nodes = cluster_manager_.get_data_nodes();
    // CRUCIAL: Sort data nodes to ensure consistent sharding indices across the cluster
    std::sort(data_nodes.begin(), data_nodes.end(),
              [](const auto& a, const auto& b) { return a.id < b.id; });

    // 1. Check if it's a DDL (Catalog) operation
    const auto type = stmt.type();
    if (type == parser::StmtType::CreateTable || type == parser::StmtType::DropTable ||
        type == parser::StmtType::CreateIndex || type == parser::StmtType::DropIndex) {
        QueryResult res;
        try {
            // Local update (triggers Raft replication attempt)
            if (type == parser::StmtType::CreateTable) {
                const auto& ct = dynamic_cast<const parser::CreateTableStatement&>(stmt);
                std::vector<ColumnInfo> catalog_cols;
                uint16_t pos = 0;
                for (const auto& col : ct.columns()) {
                    common::ValueType vtype = common::ValueType::TYPE_INT32;  // Simplified for POC
                    if (col.type_ == "TEXT") vtype = common::ValueType::TYPE_TEXT;
                    catalog_cols.emplace_back(col.name_, vtype, pos++);
                }
                catalog_.create_table(ct.table_name(), std::move(catalog_cols));
            } else if (type == parser::StmtType::DropTable) {
                const auto& dt = dynamic_cast<const parser::DropTableStatement&>(stmt);
                auto meta = catalog_.get_table_by_name(dt.table_name());
                if (meta) {
                    catalog_.drop_table((*meta)->table_id);
                }
            }

            // Explicit forward to data nodes to ensure they have metadata IMMEDIATELY (POC
            // workaround for Raft lag)
            network::ExecuteFragmentArgs args;
            args.sql = raw_sql;
            args.context_id = "ddl_sync";
            auto payload = args.serialize();

            for (const auto& node : data_nodes) {
                network::RpcClient client(node.address, node.cluster_port);
                if (client.connect()) {
                    std::vector<uint8_t> resp;
                    static_cast<void>(
                        client.call(network::RpcType::ExecuteFragment, payload, resp));
                }
            }

            res.set_rows_affected(1);
            // Small sleep after DDL to let things settle
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        } catch (const std::exception& e) {
            res.set_error(e.what());
        }
        return res;
    }

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
            for (const auto& join : select_stmt->joins()) {
                const std::string left_table = select_stmt->from()->to_string();
                const std::string right_table = join.table->to_string();

                // Check join type - shuffle join only supports INNER joins
                if (join.type != parser::SelectStatement::JoinType::Inner) {
                    QueryResult res;
                    std::string join_type_name;
                    switch (join.type) {
                        case parser::SelectStatement::JoinType::Left: join_type_name = "LEFT"; break;
                        case parser::SelectStatement::JoinType::Right: join_type_name = "RIGHT"; break;
                        case parser::SelectStatement::JoinType::Full: join_type_name = "FULL"; break;
                        default: join_type_name = "OUTER"; break;
                    }
                    res.set_error("Distributed Shuffle Join only supports INNER joins. " +
                                  join_type_name +
                                  " joins are not yet supported in distributed mode.");
                    return res;
                }

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

                // Phase 1: Instruct nodes to shuffle Left Table
                network::ShuffleFragmentArgs left_args;
                left_args.context_id = context_id;
                left_args.table_name = left_table;
                left_args.join_key_col = left_key;
                auto left_payload = left_args.serialize();

                // Bloom filter built from left table will be sent before Phase 2
                bool phase1_success = true;
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

                if (!phase1_success) {
                    QueryResult res;
                    res.set_error("Shuffle failed on node during Phase 1");
                    return res;
                }

                // After Phase 1, collect bloom filter bits from each data node and aggregate
                // via bitwise OR to create the combined bloom filter
                std::vector<uint8_t> aggregated_bits;
                size_t total_expected = 0;
                size_t max_hashes = 0;

                for (const auto& node : data_nodes) {
                    network::RpcClient client(node.address, node.cluster_port);
                    if (!client.connect()) {
                        continue;
                    }
                    network::BloomFilterBitsArgs bits_args;
                    bits_args.context_id = context_id;
                    std::vector<uint8_t> resp;
                    if (client.call(network::RpcType::BloomFilterBits, bits_args.serialize(),
                                    resp)) {
                        auto reply = network::BloomFilterBitsArgs::deserialize(resp);
                        if (reply.filter_data.size() > aggregated_bits.size()) {
                            aggregated_bits.resize(reply.filter_data.size(), 0);
                        }
                        // Bitwise OR aggregation
                        for (size_t i = 0; i < reply.filter_data.size(); i++) {
                            aggregated_bits[i] |= reply.filter_data[i];
                        }
                        total_expected += reply.expected_elements;
                        max_hashes = std::max(max_hashes, reply.num_hashes);
                    }
                }

                // Broadcast the aggregated bloom filter to all nodes for Phase 2 filtering
                network::BloomFilterArgs bf_args;
                bf_args.context_id = context_id;
                bf_args.build_table = left_table;
                bf_args.probe_table = right_table;
                bf_args.probe_key_col = right_key;  // Tell probe side which column to filter on
                bf_args.filter_data = aggregated_bits;
                bf_args.expected_elements = total_expected;
                bf_args.num_hashes = max_hashes > 0 ? max_hashes : 4;
                auto bf_payload = bf_args.serialize();

                for (const auto& node : data_nodes) {
                    network::RpcClient client(node.address, node.cluster_port);
                    if (!client.connect()) {
                        continue;  // Best effort for POC
                    }
                    std::vector<uint8_t> resp;
                    client.call(network::RpcType::BloomFilterPush, bf_payload, resp);
                }

                // Phase 2: Instruct nodes to shuffle Right Table (now with bloom filter available)
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
        if (insert_stmt != nullptr && !insert_stmt->values().empty()) {
            std::unordered_map<uint32_t, std::vector<std::vector<std::string>>> partitions;

            for (const auto& row_exprs : insert_stmt->values()) {
                if (row_exprs.empty()) continue;
                // Assume first column is sharding key
                if (row_exprs[0]->type() == parser::ExprType::Constant) {
                    const auto* const_expr =
                        dynamic_cast<const parser::ConstantExpr*>(row_exprs[0].get());
                    if (const_expr != nullptr) {
                        const common::Value pk_val = const_expr->value();
                        const uint32_t shard_idx = cluster::ShardManager::compute_shard(
                            pk_val, static_cast<uint32_t>(data_nodes.size()));

                        std::vector<std::string> row_vals;
                        for (const auto& expr : row_exprs) {
                            row_vals.push_back(expr->to_string());
                        }
                        partitions[shard_idx].push_back(std::move(row_vals));
                    }
                }
            }

            uint64_t total_affected = 0;
            std::string errors;
            for (auto& [shard_idx, rows] : partitions) {
                if (shard_idx >= data_nodes.size()) continue;
                const auto& node = data_nodes[shard_idx];
                network::RpcClient client(node.address, node.cluster_port);
                if (client.connect()) {
                    std::string shard_sql =
                        "INSERT INTO " + insert_stmt->table()->to_string() + " VALUES ";
                    for (size_t i = 0; i < rows.size(); ++i) {
                        shard_sql += "(";
                        for (size_t j = 0; j < rows[i].size(); ++j) {
                            shard_sql +=
                                rows[i][j] + std::string(j == rows[i].size() - 1 ? "" : ", ");
                        }
                        shard_sql += std::string(")") + (i == rows.size() - 1 ? "" : ", ");
                    }

                    network::ExecuteFragmentArgs args;
                    args.sql = shard_sql;
                    args.context_id = context_id;
                    std::vector<uint8_t> resp;
                    if (client.call(network::RpcType::ExecuteFragment, args.serialize(), resp)) {
                        auto reply = network::QueryResultsReply::deserialize(resp);
                        if (reply.success) {
                            total_affected += rows.size();
                        } else {
                            errors += "[" + node.id + "] INSERT failed: " + reply.error_msg + "; ";
                        }
                    } else {
                        errors += "[" + node.id + "] RPC failed; ";
                    }
                } else {
                    errors += "[" + node.id + "] Connect failed; ";
                }
            }

            QueryResult res;
            if (!errors.empty()) res.set_error(errors);
            res.set_rows_affected(total_affected);
            return res;
        }
    } else if (type == parser::StmtType::Select || type == parser::StmtType::Update ||
               type == parser::StmtType::Delete) {
        bool is_join = false;
        if (type == parser::StmtType::Select) {
            const auto* sel = dynamic_cast<const parser::SelectStatement*>(&stmt);
            if (sel && !sel->joins().empty()) is_join = true;
        }

        // Try shard pruning based on WHERE clause, but ONLY if NOT a join (joins are complex in
        // POC)
        const parser::Expression* where_expr = nullptr;
        if (!is_join) {
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
    }

    // Fallback: Broadcast if we couldn't determine a specific shard
    if (target_nodes.empty()) {
        target_nodes = data_nodes;
    }

    network::ExecuteFragmentArgs fragment_args;
    // Strip LIMIT/OFFSET from fragment SQL to ensure data nodes return all rows for global
    // processing
    fragment_args.sql = (type == parser::StmtType::Select) ? strip_limit_offset(raw_sql) : raw_sql;
    fragment_args.context_id = context_id;
    auto fragment_payload = fragment_args.serialize();

    bool all_success = true;
    std::string errors;
    std::vector<executor::Tuple> aggregated_rows;
    Schema result_schema;
    bool schema_captured = false;

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
            if (!schema_captured) {
                result_schema = res_fut.second.schema;
                schema_captured = true;
            }
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
        res.set_schema(std::move(result_schema));

        // Step 2: Check for global aggregates (COUNT, SUM, MIN, MAX)
        bool is_global_aggregate = false;
        std::vector<std::string> agg_types;

        if (type == parser::StmtType::Select) {
            const auto* select_stmt = dynamic_cast<const parser::SelectStatement*>(&stmt);
            if (select_stmt != nullptr && select_stmt->group_by().empty()) {
                for (const auto& col : select_stmt->columns()) {
                    if (col->type() == parser::ExprType::Function) {
                        const auto* func = dynamic_cast<const parser::FunctionExpr*>(col.get());
                        std::string name = func->name();
                        std::transform(name.begin(), name.end(), name.begin(),
                                       [](unsigned char c) { return std::toupper(c); });
                        if (name == "COUNT" || name == "SUM" || name == "MIN" || name == "MAX" ||
                            name == "AVG") {
                            is_global_aggregate = true;
                            agg_types.push_back(name);
                        } else {
                            agg_types.push_back("");
                        }
                    } else {
                        agg_types.push_back("");
                    }
                }
            }
        }

        if (is_global_aggregate && !aggregated_rows.empty()) {
            std::vector<common::Value> final_vals(agg_types.size(), common::Value::make_null());
            std::vector<bool> initialized(agg_types.size(), false);

            for (const auto& row : aggregated_rows) {
                if (row.size() < agg_types.size()) continue;
                for (size_t i = 0; i < agg_types.size(); ++i) {
                    if (agg_types[i].empty()) continue;

                    const auto& val = row.get(i);
                    if (val.is_null()) continue;

                    if (!initialized[i]) {
                        final_vals[i] = val;
                        initialized[i] = true;
                        continue;
                    }

                    if (agg_types[i] == "COUNT" || agg_types[i] == "SUM") {
                        int64_t current = final_vals[i].to_int64();
                        int64_t added = val.to_int64();
                        final_vals[i] = common::Value::make_int64(current + added);
                    } else if (agg_types[i] == "MIN") {
                        if (val < final_vals[i]) final_vals[i] = val;
                    } else if (agg_types[i] == "MAX") {
                        if (final_vals[i] < val) final_vals[i] = val;
                    }
                }
            }

            executor::Tuple merged_tuple;
            for (auto& v : final_vals) {
                merged_tuple.values().push_back(std::move(v));
            }
            res.add_row(std::move(merged_tuple));
        } else {
            // Global Sorting: If ORDER BY present, re-sort the combined results
            if (type == parser::StmtType::Select) {
                const auto* select_stmt = dynamic_cast<const parser::SelectStatement*>(&stmt);
                if (select_stmt != nullptr && !select_stmt->order_by().empty()) {
                    // Simplification: only handles first ORDER BY key for POC
                    const auto& sort_key = select_stmt->order_by()[0];
                    std::string col_name = sort_key->to_string();
                    size_t col_idx = res.schema().find_column(col_name);
                    if (col_idx == static_cast<size_t>(-1)) {
                        // try unqualified
                        size_t dot = col_name.find_last_of('.');
                        if (dot != std::string::npos)
                            col_idx = res.schema().find_column(col_name.substr(dot + 1));
                    }

                    if (col_idx == static_cast<size_t>(-1)) {
                        // Fallback for POC if ORDER BY key is not in projection
                        col_idx = 0;
                    }

                    if (col_idx != static_cast<size_t>(-1) &&
                        col_idx < res.schema().columns().size()) {
                        std::sort(aggregated_rows.begin(), aggregated_rows.end(),
                                  [col_idx](const auto& a, const auto& b) {
                                      return a.get(col_idx) < b.get(col_idx);
                                  });
                    }
                }
            }

            // Global Limit/Offset
            if (type == parser::StmtType::Select) {
                const auto* sel = dynamic_cast<const parser::SelectStatement*>(&stmt);
                if (sel && (sel->has_limit() || sel->has_offset())) {
                    int64_t limit = sel->limit();
                    int64_t offset = sel->offset();

                    if (offset > 0) {
                        if (static_cast<size_t>(offset) >= aggregated_rows.size()) {
                            aggregated_rows.clear();
                        } else {
                            aggregated_rows.erase(aggregated_rows.begin(),
                                                  aggregated_rows.begin() + offset);
                        }
                    }

                    if (limit >= 0 && static_cast<size_t>(limit) < aggregated_rows.size()) {
                        aggregated_rows.resize(limit);
                    }
                }
            }

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
