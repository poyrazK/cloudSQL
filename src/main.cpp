/**
 * @file main.cpp
 * @brief SQL Engine - Main Entry Point (C++ Edition)
 *
 * A lightweight, distributed SQL database engine for cloud platforms.
 *
 * @author SQL Engine Team
 * @version 0.2.0
 * @date 2024
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/cluster_manager.hpp"
#include "common/config.hpp"
#include "distributed/distributed_executor.hpp"
#include "distributed/raft_manager.hpp"
#include "distributed/shard_manager.hpp"
#include "executor/query_executor.hpp"
#include "network/rpc_client.hpp"
#include "network/rpc_message.hpp"
#include "network/rpc_server.hpp"
#include "network/server.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"
#include "recovery/log_manager.hpp"
#include "recovery/recovery_manager.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace {

/**
 * @brief Async-safe shutdown flag
 */
std::atomic<bool> shutdown_requested{false};  // NOLINT

constexpr uint32_t CONST_MAX_PORT = 65535;
constexpr auto SLEEP_MS = std::chrono::milliseconds(100);

/**
 * @brief Thread-safe getter for the global server instance
 */
std::unique_ptr<cloudsql::network::Server>& get_server_instance() {
    static std::unique_ptr<cloudsql::network::Server> instance = nullptr;
    return instance;
}

/**
 * Signal handler for graceful shutdown - async-safe
 */
void signal_handler(int sig) {
    (void)sig;
    shutdown_requested.store(true);
}

/**
 * Print usage information
 */
void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [OPTIONS]\n\n";
    std::cout << "Options:\n";
    std::cout << "  -p, --port PORT           PostgreSQL client port (default: 5432)\n";
    std::cout
        << "  -cp, --cluster-port PORT  Internal cluster communication port (default: 6432)\n";
    std::cout << "  -d, --data DIR            Data directory (default: ./data)\n";
    std::cout << "  -c, --config FILE         Configuration file (optional)\n";
    std::cout << "  -m, --mode MODE           Run mode: standalone, coordinator, or data\n";
    std::cout << "  -s, --seed NODES          Seed coordinator addresses (comma-separated)\n";
    std::cout << "  -h, --help                Show this help message\n";
    std::cout << "  -v, --version             Show version information\n";
}

/**
 * Print version information
 */
void print_version() {
    std::cout << "SQL Engine 0.2.0 (C++ Edition)\n";
    std::cout << "A lightweight PostgreSQL-compatible distributed database\n\n";
    std::cout << "Copyright (c) 2024 SQL Engine Team\n";
    std::cout << "License: MIT\n";
}

}  // namespace

/**
 * Main entry point
 */
int main(int argc, char* argv[]) {
    try {
        /* Ignore SIGPIPE to prevent crashes when writing to closed sockets */
        static_cast<void>(std::signal(SIGPIPE, SIG_IGN));

        cloudsql::config::Config config;

        /* Convert argv to vector of strings for safer parsing */
        const std::vector<std::string> cmd_args(argv, argv + argc);

        /* Parse command line arguments */
        for (size_t i = 1; i < cmd_args.size(); ++i) {
            const std::string& arg = cmd_args[i];
            if (arg == "-h" || arg == "--help") {
                if (!cmd_args.empty()) {
                    print_usage(cmd_args[0].c_str());
                }
                return 0;
            }
            if (arg == "-v" || arg == "--version") {
                print_version();
                return 0;
            }
            if ((arg == "-p" || arg == "--port") && i + 1 < cmd_args.size()) {
                try {
                    const std::string& port_str = cmd_args[++i];
                    const unsigned long port_val = std::stoul(port_str);
                    if (port_val > CONST_MAX_PORT) {
                        throw std::out_of_range("Port out of range");
                    }
                    config.port = static_cast<uint16_t>(port_val);
                } catch (const std::exception& e) {
                    std::cerr << "Invalid port: " << cmd_args[i] << " (" << e.what() << ")\n";
                    return 1;
                }
            } else if ((arg == "-cp" || arg == "--cluster-port") && i + 1 < cmd_args.size()) {
                try {
                    const std::string& port_str = cmd_args[++i];
                    const unsigned long port_val = std::stoul(port_str);
                    if (port_val > CONST_MAX_PORT) {
                        throw std::out_of_range("Cluster port out of range");
                    }
                    config.cluster_port = static_cast<uint16_t>(port_val);
                } catch (const std::exception& e) {
                    std::cerr << "Invalid cluster port: " << cmd_args[i] << " (" << e.what()
                              << ")\n";
                    return 1;
                }
            } else if ((arg == "-d" || arg == "--data") && i + 1 < cmd_args.size()) {
                config.data_dir = cmd_args[++i];
            } else if ((arg == "-c" || arg == "--config") && i + 1 < cmd_args.size()) {
                config.config_file = cmd_args[++i];
                static_cast<void>(config.load(config.config_file));
            } else if ((arg == "-m" || arg == "--mode") && i + 1 < cmd_args.size()) {
                const std::string& mode_str = cmd_args[++i];
                if (mode_str == "coordinator" || mode_str == "distributed") {
                    config.mode = cloudsql::config::RunMode::Coordinator;
                } else if (mode_str == "data") {
                    config.mode = cloudsql::config::RunMode::Data;
                } else {
                    config.mode = cloudsql::config::RunMode::Standalone;
                }
            } else if ((arg == "-s" || arg == "--seed") && i + 1 < cmd_args.size()) {
                config.seed_nodes = cmd_args[++i];
            } else {
                std::cerr << "Unknown option: " << arg << "\n";
                if (!cmd_args.empty()) {
                    print_usage(cmd_args[0].c_str());
                }
                return 1;
            }
        }

        std::cout << "=== SQL Engine ===" << std::endl;
        std::cout << "Version: 0.2.0" << std::endl;
        std::string mode_display = "Standalone";
        if (config.mode == cloudsql::config::RunMode::Coordinator) {
            mode_display = "Coordinator";
        } else if (config.mode == cloudsql::config::RunMode::Data) {
            mode_display = "Data";
        }
        std::cout << "Mode: " << mode_display << std::endl;
        std::cout << "Data directory: " << config.data_dir << std::endl;
        if (config.mode != cloudsql::config::RunMode::Data) {
            std::cout << "Client Port:    " << config.port << std::endl;
        }
        if (config.mode != cloudsql::config::RunMode::Standalone) {
            std::cout << "Cluster Port:   " << config.cluster_port << std::endl;
        }
        std::cout << std::endl;

        /* Set up signal handlers */
        static_cast<void>(std::signal(SIGINT, signal_handler));
        static_cast<void>(std::signal(SIGTERM, signal_handler));

        /* Initialize storage manager & buffer pool */
        auto disk_manager = std::make_unique<cloudsql::storage::StorageManager>(config.data_dir);
        auto bpm = std::make_unique<cloudsql::storage::BufferPoolManager>(
            cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, *disk_manager);
        /* Initialize catalog */
        const auto catalog = cloudsql::Catalog::create();
        if (!catalog) {
            std::cerr << "Failed to initialize catalog" << std::endl;
            return 1;
        }

        /* Initialize log manager & run recovery */
        auto log_manager =
            std::make_unique<cloudsql::recovery::LogManager>(config.data_dir + "/wal.log");

        std::cout << "Running Crash Recovery..." << std::endl;
        cloudsql::recovery::RecoveryManager rm(*bpm, *catalog, *log_manager);
        if (!rm.recover()) {
            std::cerr << "Crash recovery failed. Restarting anyway." << std::endl;
        }
        log_manager->run_flush_thread();

        /* Initialize transaction management */
        cloudsql::transaction::LockManager lock_manager;
        cloudsql::transaction::TransactionManager transaction_manager(lock_manager, *catalog, *bpm,
                                                                      log_manager.get());

        std::unique_ptr<cloudsql::network::RpcServer> rpc_server = nullptr;
        std::unique_ptr<cloudsql::cluster::ClusterManager> cluster_manager = nullptr;
        std::unique_ptr<cloudsql::raft::RaftManager> raft_manager = nullptr;

        /* Distributed Infrastructure */
        if (config.mode != cloudsql::config::RunMode::Standalone) {
            cluster_manager = std::make_unique<cloudsql::cluster::ClusterManager>(&config);
            catalog->set_cluster_manager(cluster_manager.get());
            rpc_server = std::make_unique<cloudsql::network::RpcServer>(config.cluster_port);

            const std::string node_id = "node_" + std::to_string(config.cluster_port);
            raft_manager = std::make_unique<cloudsql::raft::RaftManager>(node_id, *cluster_manager,
                                                                         *rpc_server);
            cluster_manager->set_raft_manager(raft_manager.get());

            /* Every node in distributed mode participates in the Catalog group (ID 0) */
            auto catalog_group = raft_manager->get_or_create_group(0);
            catalog_group->set_state_machine(catalog.get());
            catalog->set_raft_group(catalog_group.get());

            /* Register self in Group 0 */
            cluster_manager->add_node_to_group(0, node_id);

            /* Register Seed Nodes if in Coordinator Mode */
            if (config.mode == cloudsql::config::RunMode::Coordinator && !config.seed_nodes.empty()) {
                std::stringstream ss(config.seed_nodes);
                std::string node_addr;
                while (std::getline(ss, node_addr, ',')) {
                    size_t colon_pos = node_addr.find(':');
                    if (colon_pos != std::string::npos) {
                        std::string host = node_addr.substr(0, colon_pos);
                        uint16_t port =
                            static_cast<uint16_t>(std::stoi(node_addr.substr(colon_pos + 1)));
                        std::string sid = "node_" + std::to_string(port);
                        cluster_manager->register_node(sid, host, port,
                                                       cloudsql::config::RunMode::Data);
                        cluster_manager->add_node_to_group(0, sid);
                        std::cout << "[Cluster] Registered seed data node: " << sid << " (" << host
                                  << ":" << port << ")" << std::endl;
                    }
                }

                /* Broadcast full cluster map to all data nodes to ensure consistent sharding */
                auto all_data_nodes = cluster_manager->get_data_nodes();
                for (const auto& target : all_data_nodes) {
                    for (const auto& info : all_data_nodes) {
                        cloudsql::network::RegisterNodeArgs rargs;
                        rargs.id = info.id;
                        rargs.address = info.address;
                        rargs.port = info.cluster_port;
                        rargs.mode = 2; // Data
                        
                        cloudsql::network::RpcClient client(target.address, target.cluster_port);
                        if (client.connect()) {
                            std::vector<uint8_t> resp;
                            static_cast<void>(client.call(cloudsql::network::RpcType::RegisterNode, rargs.serialize(), resp));
                        }
                    }
                    // Also tell them about the coordinator
                    cloudsql::network::RegisterNodeArgs cargs;
                    cargs.id = node_id;
                    cargs.address = "127.0.0.1"; // Assume local for POC
                    cargs.port = config.cluster_port;
                    cargs.mode = 1; // Coordinator
                    
                    cloudsql::network::RpcClient client(target.address, target.cluster_port);
                    if (client.connect()) {
                        std::vector<uint8_t> resp;
                        static_cast<void>(client.call(cloudsql::network::RpcType::RegisterNode, cargs.serialize(), resp));
                    }
                }
            }

            if (config.mode == cloudsql::config::RunMode::Data) {
                // Data nodes also participate in shard consensus (e.g. Group 1)
                auto shard_group = raft_manager->get_or_create_group(1);
                cluster_manager->add_node_to_group(1, node_id);
                // Mock state machine for shard 1
                static cloudsql::executor::ShardStateMachine shard_sm("data", *bpm, *catalog);
                shard_group->set_state_machine(&shard_sm);

                // Register execution handler for Data nodes
                rpc_server->set_handler(
                    cloudsql::network::RpcType::RegisterNode,
                    [&](const cloudsql::network::RpcHeader& h, const std::vector<uint8_t>& p,
                        int fd) {
                        (void)h;
                        auto args = cloudsql::network::RegisterNodeArgs::deserialize(p);
                        if (cluster_manager != nullptr) {
                            cluster_manager->register_node(args.id, args.address, args.port,
                                                           static_cast<cloudsql::config::RunMode>(args.mode));
                        }
                        cloudsql::network::QueryResultsReply reply;
                        reply.success = true;
                        auto resp_p = reply.serialize();
                        cloudsql::network::RpcHeader resp_h;
                        resp_h.type = cloudsql::network::RpcType::QueryResults;
                        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
                        char h_buf[cloudsql::network::RpcHeader::HEADER_SIZE];
                        resp_h.encode(h_buf);
                        static_cast<void>(send(fd, h_buf, cloudsql::network::RpcHeader::HEADER_SIZE, 0));
                        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
                    });

                rpc_server->set_handler(
                    cloudsql::network::RpcType::ExecuteFragment,
                    [&](const cloudsql::network::RpcHeader& h, const std::vector<uint8_t>& p,
                        int fd) {
                        (void)h;
                        auto args = cloudsql::network::ExecuteFragmentArgs::deserialize(p);
                        cloudsql::network::QueryResultsReply reply;
                        try {
                            auto lexer = std::make_unique<cloudsql::parser::Lexer>(args.sql);
                            cloudsql::parser::Parser parser(std::move(lexer));
                            auto stmt = parser.parse_statement();
                            if (stmt) {
                                cloudsql::executor::QueryExecutor exec(
                                    *catalog, *bpm, lock_manager, transaction_manager,
                                    log_manager.get(), cluster_manager.get());
                                exec.set_context_id(args.context_id);
                                exec.set_local_only(true); // Crucial for fragment execution
                                auto res = exec.execute(*stmt);
                                reply.success = res.success();
                                if (res.success()) {
                                    reply.rows = res.rows();
                                    reply.schema = res.schema(); // Populate schema for merge logic
                                } else {
                                    reply.error_msg = res.error();
                                }
                            } else {
                                reply.success = false;
                                reply.error_msg = "Parse error";
                            }
                        } catch (const std::exception& e) {
                            reply.success = false;
                            reply.error_msg = e.what();
                        }

                        auto resp_p = reply.serialize();
                        cloudsql::network::RpcHeader resp_h;
                        resp_h.type = cloudsql::network::RpcType::QueryResults;
                        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
                        char h_buf[cloudsql::network::RpcHeader::HEADER_SIZE];
                        resp_h.encode(h_buf);
                        static_cast<void>(send(fd, h_buf, cloudsql::network::RpcHeader::HEADER_SIZE, 0));
                        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
                    });

                // Register 2PC Handlers
                rpc_server->set_handler(
                    cloudsql::network::RpcType::TxnPrepare,
                    [&](const cloudsql::network::RpcHeader& h, const std::vector<uint8_t>& p,
                        int fd) {
                        (void)h;
                        auto args = cloudsql::network::TxnOperationArgs::deserialize(p);
                        cloudsql::network::QueryResultsReply reply;
                        try {
                            log_manager->flush(true);
                            reply.success = true;
                        } catch (const std::exception& e) {
                            reply.success = false;
                            reply.error_msg = e.what();
                        }

                        auto resp_p = reply.serialize();
                        cloudsql::network::RpcHeader resp_h;
                        resp_h.type = cloudsql::network::RpcType::QueryResults;
                        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
                        char h_buf[cloudsql::network::RpcHeader::HEADER_SIZE];
                        resp_h.encode(h_buf);
                        static_cast<void>(send(fd, h_buf, cloudsql::network::RpcHeader::HEADER_SIZE, 0));
                        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
                    });

                rpc_server->set_handler(
                    cloudsql::network::RpcType::TxnCommit,
                    [&](const cloudsql::network::RpcHeader& h, const std::vector<uint8_t>& p,
                        int fd) {
                        (void)h;
                        auto args = cloudsql::network::TxnOperationArgs::deserialize(p);
                        cloudsql::network::QueryResultsReply reply;
                        try {
                            auto txn = transaction_manager.get_transaction(args.txn_id);
                            if (txn) {
                                transaction_manager.commit(txn);
                            }
                            reply.success = true;
                        } catch (const std::exception& e) {
                            reply.success = false;
                            reply.error_msg = e.what();
                        }

                        auto resp_p = reply.serialize();
                        cloudsql::network::RpcHeader resp_h;
                        resp_h.type = cloudsql::network::RpcType::QueryResults;
                        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
                        char h_buf[cloudsql::network::RpcHeader::HEADER_SIZE];
                        resp_h.encode(h_buf);
                        static_cast<void>(send(fd, h_buf, cloudsql::network::RpcHeader::HEADER_SIZE, 0));
                        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
                    });

                rpc_server->set_handler(
                    cloudsql::network::RpcType::TxnAbort,
                    [&](const cloudsql::network::RpcHeader& h, const std::vector<uint8_t>& p,
                        int fd) {
                        (void)h;
                        auto args = cloudsql::network::TxnOperationArgs::deserialize(p);
                        cloudsql::network::QueryResultsReply reply;
                        try {
                            auto txn = transaction_manager.get_transaction(args.txn_id);
                            if (txn) {
                                transaction_manager.abort(txn);
                            }
                            reply.success = true;
                        } catch (const std::exception& e) {
                            reply.success = false;
                            reply.error_msg = e.what();
                        }

                        auto resp_p = reply.serialize();
                        cloudsql::network::RpcHeader resp_h;
                        resp_h.type = cloudsql::network::RpcType::QueryResults;
                        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
                        char h_buf[cloudsql::network::RpcHeader::HEADER_SIZE];
                        resp_h.encode(h_buf);
                        static_cast<void>(send(fd, h_buf, cloudsql::network::RpcHeader::HEADER_SIZE, 0));
                        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
                    });

                rpc_server->set_handler(
                    cloudsql::network::RpcType::PushData,
                    [&](const cloudsql::network::RpcHeader& h, const std::vector<uint8_t>& p,
                        int fd) {
                        (void)h;
                        auto args = cloudsql::network::PushDataArgs::deserialize(p);
                        if (cluster_manager != nullptr) {
                            cluster_manager->buffer_shuffle_data(args.context_id, args.table_name,
                                                                 std::move(args.rows));
                        }

                        cloudsql::network::QueryResultsReply reply;
                        reply.success = true;
                        auto resp_p = reply.serialize();
                        cloudsql::network::RpcHeader resp_h;
                        resp_h.type = cloudsql::network::RpcType::QueryResults;
                        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
                        char h_buf[cloudsql::network::RpcHeader::HEADER_SIZE];
                        resp_h.encode(h_buf);
                        static_cast<void>(
                            send(fd, h_buf, cloudsql::network::RpcHeader::HEADER_SIZE, 0));
                        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
                    });

                rpc_server->set_handler(
                    cloudsql::network::RpcType::ShuffleFragment,
                    [&](const cloudsql::network::RpcHeader& h, const std::vector<uint8_t>& p,
                        int fd) {
                        (void)h;
                        auto args = cloudsql::network::ShuffleFragmentArgs::deserialize(p);
                        cloudsql::network::QueryResultsReply reply;
                        try {
                            auto table_meta_opt = catalog->get_table_by_name(args.table_name);
                            if (!table_meta_opt.has_value()) {
                                throw std::runtime_error("Table not found: " + args.table_name);
                            }
                            const auto* table_meta = table_meta_opt.value();
                            cloudsql::executor::Schema schema;
                            for (const auto& col : table_meta->columns) {
                                schema.add_column(col.name, col.type);
                            }
                            cloudsql::storage::HeapTable table(args.table_name, *bpm, schema);

                            const size_t key_idx = schema.find_column(args.join_key_col);
                            if (key_idx == static_cast<size_t>(-1)) {
                                throw std::runtime_error("Join key column not found: " +
                                                         args.join_key_col);
                            }

                            auto data_nodes = cluster_manager->get_data_nodes();
                            if (data_nodes.empty()) {
                                throw std::runtime_error("No data nodes available for shuffle");
                            }

                            std::sort(data_nodes.begin(), data_nodes.end(),
                                      [](const auto& a, const auto& b) { return a.id < b.id; });

                            std::unordered_map<std::string, std::vector<cloudsql::executor::Tuple>>
                                partitions;

                            for (const auto& node : data_nodes) {
                                partitions[node.id] = {};
                            }

                            auto iter = table.scan();
                            cloudsql::storage::HeapTable::TupleMeta t_meta;
                            while (iter.next_meta(t_meta)) {
                                if (t_meta.xmax == 0) {  // Visible
                                    const auto& key_val = t_meta.tuple.get(key_idx);
                                    uint32_t node_idx =
                                        cloudsql::cluster::ShardManager::compute_shard(
                                            key_val, static_cast<uint32_t>(data_nodes.size()));
                                    partitions[data_nodes[node_idx].id].push_back(
                                        std::move(t_meta.tuple));
                                }
                            }

                            bool overall_success = true;
                            std::string delivery_errors;

                            for (auto& [node_id, rows] : partitions) {
                                const cloudsql::cluster::NodeInfo* target_node = nullptr;
                                for (const auto& n : data_nodes) {
                                    if (n.id == node_id) {
                                        target_node = &n;
                                        break;
                                    }
                                }

                                if (target_node != nullptr) {
                                    cloudsql::network::RpcClient client(target_node->address,
                                                                        target_node->cluster_port);
                                    if (!client.connect()) {
                                        overall_success = false;
                                        delivery_errors += "Connect failed to " + node_id + "; ";
                                        continue;
                                    }

                                    cloudsql::network::PushDataArgs push_args;
                                    push_args.context_id = args.context_id;
                                    push_args.table_name = args.table_name;
                                    push_args.rows = std::move(rows);
                                    std::vector<uint8_t> resp;
                                    if (!client.call(cloudsql::network::RpcType::PushData,
                                                     push_args.serialize(), resp)) {
                                        overall_success = false;
                                        delivery_errors += "RPC failed to " + node_id + "; ";
                                    } else {
                                        auto push_reply =
                                            cloudsql::network::QueryResultsReply::deserialize(resp);
                                        if (!push_reply.success) {
                                            overall_success = false;
                                            delivery_errors += "Push failed on " + node_id + ": " +
                                                               push_reply.error_msg + "; ";
                                        }
                                    }
                                }
                            }
                            if (overall_success) {
                                reply.success = true;
                            } else {
                                reply.success = false;
                                reply.error_msg = "Shuffle delivery failed: " + delivery_errors;
                            }
                        } catch (const std::exception& e) {
                            reply.success = false;
                            reply.error_msg = e.what();
                        }

                        auto resp_p = reply.serialize();
                        cloudsql::network::RpcHeader resp_h;
                        resp_h.type = cloudsql::network::RpcType::QueryResults;
                        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
                        char h_buf[cloudsql::network::RpcHeader::HEADER_SIZE];
                        resp_h.encode(h_buf);
                        static_cast<void>(
                            send(fd, h_buf, cloudsql::network::RpcHeader::HEADER_SIZE, 0));
                        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
                    });
            }

            std::cout << "Starting internal RPC server on port " << config.cluster_port << "..." << std::endl;
            if (!rpc_server->start()) {
                std::cerr << "Failed to start RPC server" << std::endl;
                log_manager->stop_flush_thread();
                return 1;
            }
            raft_manager->start();
        }

        if (config.mode == cloudsql::config::RunMode::Data) {
            std::cout << "Data node online. Waiting for Coordinator instructions..." << std::endl;
        } else {
            /* Standalone or Coordinator mode: start PostgreSQL server */
            auto& server = get_server_instance();
            server = cloudsql::network::Server::create(config.port, *catalog, *bpm, config,
                                                       cluster_manager.get());
            if (!server) {
                std::cerr << "Failed to create PostgreSQL server" << std::endl;
                if (rpc_server) {
                    rpc_server->stop();
                }
                log_manager->stop_flush_thread();
                return 1;
            }

            std::cout << "Starting PostgreSQL server on port " << config.port << "..." << std::endl;
            if (!server->start()) {
                std::cerr << "Failed to start PostgreSQL server" << std::endl;
                if (rpc_server) {
                    rpc_server->stop();
                }
                log_manager->stop_flush_thread();
                return 1;
            }

            if (config.mode == cloudsql::config::RunMode::Coordinator) {
                std::cout << "Coordinator node joining cluster..." << std::endl;
            }
        }

        std::cout << "Node ready. Press Ctrl+C to stop." << std::endl;

        /* Monitor shutdown flag */
        while (!shutdown_requested.load()) {
            /* Check if STDIN is piped SQL */
            if (!isatty(STDIN_FILENO)) {
                std::string line;
                if (std::getline(std::cin, line)) {
                    if (line.empty() || line[0] == '#') continue;
                    try {
                        auto lexer = std::make_unique<cloudsql::parser::Lexer>(line);
                        cloudsql::parser::Parser parser(std::move(lexer));
                        auto stmt = parser.parse_statement();
                        if (stmt) {
                            if (config.mode == cloudsql::config::RunMode::Coordinator) {
                                cloudsql::executor::DistributedExecutor dist_exec(*catalog, *cluster_manager);
                                dist_exec.execute(*stmt, line);
                            } else {
                                cloudsql::executor::QueryExecutor exec(*catalog, *bpm, lock_manager, 
                                                                     transaction_manager, log_manager.get(), 
                                                                     cluster_manager.get());
                                exec.execute(*stmt);
                            }
                        }
                    } catch (...) {}
                } else {
                    // EOF reached
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    shutdown_requested.store(true);
                }
            }
            std::this_thread::sleep_for(SLEEP_MS);
        }

        /* Cleanup */
        std::cout << std::endl << "Shutting down..." << std::endl;
        auto& server = get_server_instance();
        if (server) {
            static_cast<void>(server->stop());
            server.reset();
        }

        if (raft_manager) {
            raft_manager->stop();
        }

        if (rpc_server) {
            rpc_server->stop();
        }

        log_manager->stop_flush_thread();

        std::cout << "Goodbye!" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown fatal error" << std::endl;
        return 1;
    }

    return 0;
}
