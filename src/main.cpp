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

#include <csignal>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "network/server.hpp"
#include "storage/storage_manager.hpp"

namespace {

/* Global server instance for signal handling */
std::unique_ptr<cloudsql::network::Server> g_server = nullptr; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

/**
 * Signal handler for graceful shutdown
 */
void signal_handler(int sig) {
    std::cout << "\nReceived signal " << sig << ", shutting down...\n";
    if (g_server != nullptr) {
        static_cast<void>(g_server->stop());
    }
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
int main(int argc, char* argv[]) { // NOLINT(bugprone-exception-escape)
    cloudsql::config::Config config;

    /* Convert argv to vector of strings for safer parsing */
    const std::vector<std::string> args(argv, argv + argc);

    /* Parse command line arguments */
    for (size_t i = 1; i < args.size(); ++i) {
        const std::string& arg = args[i];
        if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
        if (arg == "-v" || arg == "--version") {
            print_version();
            return 0;
        }
        if ((arg == "-p" || arg == "--port") && i + 1 < args.size()) {
            config.port = static_cast<uint16_t>(std::stoi(args[++i]));
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
            print_usage(argv[0]);
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

    try {
        /* Initialize storage manager */
        const auto storage_manager = std::make_unique<cloudsql::storage::StorageManager>(config.data_dir);

        /* Initialize catalog */
        const auto catalog = cloudsql::Catalog::create();
        if (!catalog) {
            std::cerr << "Failed to initialize catalog\n";
            return 1;
        }

        /* Initialize server */
        g_server = cloudsql::network::Server::create(config.port, *catalog, *storage_manager);
        if (!g_server) {
            std::cerr << "Failed to create server\n";
            return 1;
        }

        /* Start server */
        std::cout << "Starting server...\n";
        if (!g_server->start()) {
            std::cerr << "Failed to start server\n";
            return 1;
        }

        std::cout << "Server running. Press Ctrl+C to stop.\n";
        g_server->wait();

        /* Cleanup */
        std::cout << "Shutting down...\n";
        static_cast<void>(g_server->stop());
        g_server.reset();

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
