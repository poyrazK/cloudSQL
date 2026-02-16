/**
 * @file main.cpp
 * @brief SQL Engine - Main Entry Point (C++ Edition)
 *
 * A lightweight, distributed SQL database engine for cloud platforms.
 *
 * @author SQL Engine Team
 * @version 0.2.0
 * @date 2024
 *
 * This file is part of SQL Engine, a PostgreSQL-compatible distributed
 * database designed for cloud environments. It's built with educational
 * value and production readiness in mind.
 */

#include <csignal>
#include <cstring>
#include <iostream>
#include <memory>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "network/server.hpp"
#include "storage/storage_manager.hpp"

/* Global server instance for signal handling */
static std::unique_ptr<cloudsql::network::Server> g_server = nullptr;

/**
 * Signal handler for graceful shutdown
 */
static void signal_handler(int sig) {
    std::cout << "\nReceived signal " << sig << ", shutting down...\n";
    if (g_server != nullptr) {
        g_server->stop();
    }
}

/**
 * Print usage information
 */
static void print_usage(const char* prog) {
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
static void print_version() {
    std::cout << "SQL Engine 0.2.0 (C++ Edition)\n";
    std::cout << "A lightweight PostgreSQL-compatible distributed database\n\n";
    std::cout << "Copyright (c) 2024 SQL Engine Team\n";
    std::cout << "License: MIT\n";
}

/**
 * Main entry point
 */
int main(int argc, char* argv[]) {
    cloudsql::config::Config config;

    /* Parse command line arguments */
    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], "-h") == 0 || std::strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (std::strcmp(argv[i], "-v") == 0 || std::strcmp(argv[i], "--version") == 0) {
            print_version();
            return 0;
        } else if ((std::strcmp(argv[i], "-p") == 0 || std::strcmp(argv[i], "--port") == 0) &&
                   i + 1 < argc) {
            config.port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if ((std::strcmp(argv[i], "-d") == 0 || std::strcmp(argv[i], "--data") == 0) &&
                   i + 1 < argc) {
            config.data_dir = argv[++i];
        } else if ((std::strcmp(argv[i], "-c") == 0 || std::strcmp(argv[i], "--config") == 0) &&
                   i + 1 < argc) {
            config.config_file = argv[++i];
            config.load(config.config_file);
        } else if ((std::strcmp(argv[i], "-m") == 0 || std::strcmp(argv[i], "--mode") == 0) &&
                   i + 1 < argc) {
            if (std::strcmp(argv[++i], "distributed") == 0) {
                config.mode = cloudsql::config::RunMode::Distributed;
            } else {
                config.mode = cloudsql::config::RunMode::Embedded;
            }
        } else {
            std::cerr << "Unknown option: " << argv[i] << "\n";
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
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    /* Initialize storage manager */
    auto storage_manager = std::make_unique<cloudsql::storage::StorageManager>(config.data_dir);

    /* Initialize catalog */
    auto catalog = cloudsql::Catalog::create();
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
    g_server->stop();
    g_server.reset();

    std::cout << "Goodbye!\n";
    return 0;
}
