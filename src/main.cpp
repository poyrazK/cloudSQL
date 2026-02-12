#include <iostream>
#include <csignal>
#include <memory>
#include <cstring>
#include "common/config.hpp"
#include "network/server.hpp"
#include "catalog/catalog.hpp"

// Global for signal handling
std::unique_ptr<cloudsql::network::Server> g_server;

void signal_handler(int sig) {
    std::cout << "
Received signal " << sig << ", shutting down...
";
    if (g_server) {
        g_server->stop();
    }
}

static void print_usage(const char *prog) {
    std::cout << "Usage: " << prog << " [OPTIONS]

";
    std::cout << "Options:
";
    std::cout << "  -p, --port PORT     Port to listen on (default: 5432)
";
    std::cout << "  -d, --data DIR      Data directory (default: ./data)
";
    std::cout << "  -c, --config FILE   Configuration file (optional)
";
    std::cout << "  -m, --mode MODE     Run mode: embedded or distributed (default: embedded)
";
    std::cout << "  -h, --help          Show this help message
";
    std::cout << "  -v, --version       Show version information
";
}

static void print_version() {
    std::cout << "SQL Engine 0.2.0 (C++ Edition)
";
    std::cout << "A lightweight PostgreSQL-compatible distributed database

";
    std::cout << "Copyright (c) 2024 SQL Engine Team
";
    std::cout << "License: MIT
";
}

int main(int argc, char* argv[]) {
    cloudsql::config::Config config;
    
    /* Parse command line arguments */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--version") == 0) {
            print_version();
            return 0;
        } else if ((strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--port") == 0) && i + 1 < argc) {
            config.port = static_cast<uint16_t>(atoi(argv[++i]));
        } else if ((strcmp(argv[i], "-d") == 0 || strcmp(argv[i], "--data") == 0) && i + 1 < argc) {
            config.data_dir = argv[++i];
        } else if ((strcmp(argv[i], "-c") == 0 || strcmp(argv[i], "--config") == 0) && i + 1 < argc) {
            config.config_file = argv[++i];
            config.load(config.config_file);
        } else if ((strcmp(argv[i], "-m") == 0 || strcmp(argv[i], "--mode") == 0) && i + 1 < argc) {
            if (strcmp(argv[++i], "distributed") == 0) {
                config.mode = cloudsql::config::RunMode::Distributed;
            } else {
                config.mode = cloudsql::config::RunMode::Embedded;
            }
        } else {
            std::cerr << "Unknown option: " << argv[i] << "
";
            print_usage(argv[0]);
            return 1;
        }
    }
    
    std::cout << "=== SQL Engine ===
";
    std::cout << "Mode: " << (config.mode == cloudsql::config::RunMode::Distributed ? "distributed" : "embedded") << "
";
    std::cout << "Data directory: " << config.data_dir << "
";
    std::cout << "Port: " << config.port << "

";
    
    /* Set up signal handlers */
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    
    // Create Catalog (TODO: Pass storage manager)
    auto catalog = cloudsql::Catalog::create();
    if (!catalog) {
        std::cerr << "Failed to initialize catalog
";
        return 1;
    }
    
    // Create and Start Server
    g_server = cloudsql::network::Server::create(config.port);
    if (!g_server) {
        std::cerr << "Failed to create server
";
        return 1;
    }
    
    std::cout << "Starting server...
";
    if (!g_server->start()) {
        std::cerr << "Failed to start server
";
        return 1;
    }
    
    std::cout << "Server running. Press Ctrl+C to stop.
";
    g_server->wait();
    
    std::cout << "Shutting down...
";
    g_server->stop();
    g_server.reset();
    
    std::cout << "Goodbye!
";
    return 0;
}
