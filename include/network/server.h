/**
 * @file server.h
 * @brief Network server for PostgreSQL wire protocol
 *
 * @defgroup server Network Server
 * @{
 */

#ifndef SQL_ENGINE_NETWORK_SERVER_H
#define SQL_ENGINE_NETWORK_SERVER_H

#include <stdbool.h>
#include <stdint.h>

#include "common/config.h"
#include "catalog/catalog.h"

/* Forward declarations */
struct server_t;

/**
 * @brief Server statistics
 */
typedef struct {
    uint64_t connections_accepted;
    uint64_t connections_active;
    uint64_t queries_executed;
    uint64_t bytes_received;
    uint64_t bytes_sent;
    uint64_t uptime_seconds;
} server_stats_t;

/**
 * @brief Server callback functions
 */
typedef struct {
    /** Called when a new connection is accepted */
    int (*on_connect)(struct server_t *server, int client_fd);
    
    /** Called when a query is received */
    int (*on_query)(struct server_t *server, int client_fd, const char *sql);
    
    /** Called when a connection is closed */
    int (*on_disconnect)(struct server_t *server, int client_fd);
} server_callbacks_t;

/**
 * @brief Server instance structure
 */
typedef struct server_t {
    /** Server configuration */
    config_t *config;
    
    /** Catalog reference */
    catalog_t *catalog;
    
 /** Server socket file descriptor */
    int listen_fd;
    
    /** Flag indicating if server is running */
    bool running;
    
    /** Flag indicating if server is stopping */
    bool stopping;
    
    /** Server callbacks */
    server_callbacks_t callbacks;
    
    /** Server statistics */
    server_stats_t stats;
    
    /** User-defined context */
    void *context;
} server_t;

/**
 * @brief Create a new server instance
 * @param config Server configuration
 * @param catalog Catalog instance
 * @return New server instance or NULL on error
 */
server_t *server_create(config_t *config, catalog_t *catalog);

/**
 * @brief Destroy a server instance
 * @param server Server instance
 */
void server_destroy(server_t *server);

/**
 * @brief Set server callbacks
 * @param server Server instance
 * @param callbacks Callback functions
 */
void server_set_callbacks(server_t *server, server_callbacks_t *callbacks);

/**
 * @brief Start the server
 * @param server Server instance
 * @return 0 on success, -1 on error
 */
int server_start(server_t *server);

/**
 * @brief Stop the server
 * @param server Server instance
 * @return 0 on success, -1 on error
 */
int server_stop(server_t *server);

/**
 * @brief Wait for server to stop
 * @param server Server instance
 */
void server_wait(server_t *server);

/**
 * @brief Get server statistics
 * @param server Server instance
 * @return Statistics structure
 */
server_stats_t server_get_stats(server_t *server);

/**
 * @brief Get server status as string
 * @param server Server instance
 * @return Status string
 */
const char *server_get_status(server_t *server);

#endif /* SQL_ENGINE_NETWORK_SERVER_H */

/** @} */ /* server */
