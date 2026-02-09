/**
 * @file config.h
 * @brief Configuration structures for SQL Engine
 *
 * @defgroup config Configuration
 * @{
 */

#ifndef SQL_ENGINE_COMMON_CONFIG_H
#define SQL_ENGINE_COMMON_CONFIG_H

#include <stdint.h>
#include <stdbool.h>
#include <limits.h>

/* Version information */
#define SQL_ENGINE_VERSION "0.1.0"
#define SQL_ENGINE_NAME "SQL Engine"

/* Default configuration values */
#define DEFAULT_PORT 5432
#define DEFAULT_DATA_DIR "./data"
#define DEFAULT_MAX_CONNECTIONS 100
#define DEFAULT_BUFFER_POOL_SIZE 128
#define DEFAULT_PAGE_SIZE 8192

/* Run modes */
typedef enum {
    MODE_EMBEDDED = 0,
    MODE_DISTRIBUTED = 1
} run_mode_t;

/**
 * @brief Server configuration structure
 */
typedef struct {
    /** Server port */
    uint16_t port;
    
    /** Data directory path */
    char data_dir[PATH_MAX];
    
    /** Configuration file path */
    char config_file[PATH_MAX];
    
    /** Run mode (embedded or distributed) */
    run_mode_t mode;
    
    /** Maximum number of connections */
    int max_connections;
    
    /** Buffer pool size in pages */
    int buffer_pool_size;
    
    /** Page size in bytes */
    int page_size;
    
    /** Enable debug logging */
    bool debug;
    
    /** Enable verbose output */
    bool verbose;
} config_t;

/**
 * @brief Default configuration
 */
extern const config_t CONFIG_DEFAULT;

/**
 * @brief Initialize configuration with defaults
 * @param config Pointer to configuration structure
 */
void config_init(config_t *config);

/**
 * @brief Load configuration from file
 * @param config Pointer to configuration structure
 * @param filename Configuration file path
 * @return 0 on success, -1 on error
 */
int config_load(config_t *config, const char *filename);

/**
 * @brief Save configuration to file
 * @param config Pointer to configuration structure
 * @param filename Configuration file path
 * @return 0 on success, -1 on error
 */
int config_save(config_t *config, const char *filename);

/**
 * @brief Validate configuration
 * @param config Pointer to configuration structure
 * @return 0 if valid, -1 if invalid
 */
int config_validate(config_t *config);

/**
 * @brief Print configuration to stdout
 * @param config Pointer to configuration structure
 */
void config_print(config_t *config);

#endif /* SQL_ENGINE_COMMON_CONFIG_H */

/** @} */ /* config */
