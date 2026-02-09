/**
 * @file main.c
 * @brief SQL Engine - Main Entry Point
 *
 * A lightweight, distributed SQL database engine for cloud platforms.
 *
 * @author SQL Engine Team
 * @version 0.1.0
 * @date 2024
 *
 * This file is part of SQL Engine, a PostgreSQL-compatible distributed
 * database designed for cloud environments. It's built with educational
 * value and production readiness in mind.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

#include "common/config.h"
#include "network/server.h"
#include "storage/manager.h"
#include "catalog/catalog.h"

/* Global server instance for signal handling */
static server_t *g_server = NULL;

/**
 * Signal handler for graceful shutdown
 */
static void signal_handler(int sig) {
    printf("\nReceived signal %d, shutting down...\n", sig);
    if (g_server != NULL) {
        server_stop(g_server);
    }
}

/**
 * Print usage information
 */
static void print_usage(const char *prog) {
    printf("Usage: %s [OPTIONS]\n\n", prog);
    printf("Options:\n");
    printf("  -p, --port PORT     Port to listen on (default: 5432)\n");
    printf("  -d, --data DIR      Data directory (default: ./data)\n");
    printf("  -c, --config FILE   Configuration file (optional)\n");
    printf("  -m, --mode MODE     Run mode: embedded or distributed (default: embedded)\n");
    printf("  -h, --help          Show this help message\n");
    printf("  -v, --version       Show version information\n");
}

/**
 * Print version information
 */
static void print_version(void) {
    printf("SQL Engine %s\n", SQL_ENGINE_VERSION);
    printf("A lightweight PostgreSQL-compatible distributed database\n\n");
    printf("Copyright (c) 2024 SQL Engine Team\n");
    printf("License: MIT\n");
}

/**
 * Main entry point
 */
int main(int argc, char *argv[]) {
    int exit_code = 0;
    config_t config = CONFIG_DEFAULT;
    
    /* Parse command line arguments */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--version") == 0) {
            print_version();
            return 0;
        } else if ((strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--port") == 0) && i + 1 < argc) {
            config.port = atoi(argv[++i]);
        } else if ((strcmp(argv[i], "-d") == 0 || strcmp(argv[i], "--data") == 0) && i + 1 < argc) {
            strncpy(config.data_dir, argv[++i], sizeof(config.data_dir) - 1);
        } else if ((strcmp(argv[i], "-c") == 0 || strcmp(argv[i], "--config") == 0) && i + 1 < argc) {
            strncpy(config.config_file, argv[++i], sizeof(config.config_file) - 1);
        } else if ((strcmp(argv[i], "-m") == 0 || strcmp(argv[i], "--mode") == 0) && i + 1 < argc) {
            if (strcmp(argv[++i], "distributed") == 0) {
                config.mode = MODE_DISTRIBUTED;
            } else {
                config.mode = MODE_EMBEDDED;
            }
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }
    
    printf("=== SQL Engine ===\n");
    printf("Version: %s\n", SQL_ENGINE_VERSION);
    printf("Mode: %s\n", config.mode == MODE_DISTRIBUTED ? "distributed" : "embedded");
    printf("Data directory: %s\n", config.data_dir);
    printf("Port: %d\n\n", config.port);
    
    /* Set up signal handlers */
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGHUP, signal_handler);
    
    /* Initialize storage manager */
    storage_manager_t *storage = storage_manager_create(config.data_dir);
    if (storage == NULL) {
        fprintf(stderr, "Failed to initialize storage manager\n");
        return 1;
    }
    
    /* Initialize catalog */
    catalog_t *catalog = catalog_create(storage);
    if (catalog == NULL) {
        fprintf(stderr, "Failed to initialize catalog\n");
        storage_manager_destroy(storage);
        return 1;
    }
    
    /* Initialize server */
    g_server = server_create(&config, catalog);
    if (g_server == NULL) {
        fprintf(stderr, "Failed to create server\n");
        catalog_destroy(catalog);
        storage_manager_destroy(storage);
        return 1;
    }
    
    /* Start server */
    printf("Starting server...\n");
    if (server_start(g_server) != 0) {
        fprintf(stderr, "Failed to start server\n");
        exit_code = 1;
    } else {
        printf("Server running. Press Ctrl+C to stop.\n");
        server_wait(g_server);
    }
    
    /* Cleanup */
    printf("Shutting down...\n");
    server_destroy(g_server);
    catalog_destroy(catalog);
    storage_manager_destroy(storage);
    
    printf("Goodbye!\n");
    return exit_code;
}
