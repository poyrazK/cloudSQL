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
#include <vector>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "network/server.hpp"
#include "recovery/log_manager.hpp"
#include "recovery/recovery_manager.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/storage_manager.hpp"

namespace {

/**
 * @brief Async-safe shutdown flag
 */
std::atomic<bool> shutdown_requested{false};

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
    std::cout << "  -p, --port PORT     Port to listen on (default: 5432)\n";
    std::cout << "  -d, --data DIR      Data directory (default: ./data)\n";
    std::cout << "  -c, --config FILE   Configuration file (optional)\n";
    std::cout << "  -m, --mode MODE     Run mode: embedded or distributed (default: embedded)\n";
    std::cout << "  -h, --help          Show this help message\n";
    std::cout << "  -v, --version       Show version information\n";
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
        cloudsql::config::Config config;

        /* Convert argv to vector of strings for safer parsing */
        const std::vector<std::string> args(argv, argv + argc);

        /* Parse command line arguments */
        for (size_t i = 1; i < args.size(); ++i) {
            const std::string& arg = args[i];
            if (arg == "-h" || arg == "--help") {
                if (!args.empty()) {
                    print_usage(args[0].c_str());
                }
                return 0;
            }
            if (arg == "-v" || arg == "--version") {
                print_version();
                return 0;
            }
            if ((arg == "-p" || arg == "--port") && i + 1 < args.size()) {
                try {
                    const std::string& port_str = args[++i];
                    const unsigned long port_val = std::stoul(port_str);
                    if (port_val > 65535) {
                        throw std::out_of_range("Port out of range");
                    }
                    config.port = static_cast<uint16_t>(port_val);
                } catch (const std::exception& e) {
                    std::cerr << "Invalid port: " << args[i] << " (" << e.what() << ")\n";
                    return 1;
                }
            } else if ((arg == "-d" || arg == "--data") && i + 1 < args.size()) {
                config.data_dir = args[++i];
            } else if ((arg == "-c" || arg == "--config") && i + 1 < args.size()) {
                config.config_file = args[++i];
                static_cast<void>(config.load(config.config_file));
            } else if ((arg == "-m" || arg == "--mode") && i + 1 < args.size()) {
                const std::string& mode = args[++i];
                if (mode == "distributed") {
                    config.mode = cloudsql::config::RunMode::Distributed;
                } else {
                    config.mode = cloudsql::config::RunMode::Embedded;
                }
            } else {
                std::cerr << "Unknown option: " << arg << "\n";
                if (!args.empty()) {
                    print_usage(args[0].c_str());
                }
                return 1;
            }
        }

        std::cout << "=== SQL Engine ===\n";
        std::cout << "Version: 0.2.0\n";
        std::cout << "Mode: "
                  << (config.mode == cloudsql::config::RunMode::Distributed ? "distributed"
                                                                            : "embedded")
                  << "\n";
        std::cout << "Data directory: " << config.data_dir << "\n";
        std::cout << "Port: " << config.port << "\n\n";

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
            std::cerr << "Failed to initialize catalog\n";
            return 1;
        }

        /* Initialize log manager & run recovery */
        auto log_manager =
            std::make_unique<cloudsql::recovery::LogManager>(config.data_dir + "/wal.log");

        std::cout << "Running Crash Recovery...\n";
        cloudsql::recovery::RecoveryManager rm(*bpm, *catalog, *log_manager);
        if (!rm.recover()) {
            std::cerr << "Crash recovery failed. Restarting anyway.\n";
        }
        log_manager->run_flush_thread();

        /* Initialize server */
        auto& server = get_server_instance();
        server = cloudsql::network::Server::create(config.port, *catalog, *bpm);
        if (!server) {
            std::cerr << "Failed to create server\n";
            log_manager->stop_flush_thread();
            return 1;
        }

        /* Start server */
        std::cout << "Starting server...\n";
        if (!server->start()) {
            std::cerr << "Failed to start server\n";
            log_manager->stop_flush_thread();
            return 1;
        }

        std::cout << "Server running. Press Ctrl+C to stop.\n";

        /* Monitor shutdown flag */
        while (!shutdown_requested.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        /* Cleanup */
        std::cout << "\nShutting down...\n";
        static_cast<void>(server->stop());
        server.reset();

        log_manager->stop_flush_thread();

        std::cout << "Goodbye!\n";
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << "Unknown fatal error\n";
        return 1;
    }

    return 0;
}
