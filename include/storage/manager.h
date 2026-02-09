/**
 * @file manager.h
 * @brief Storage manager for database files
 *
 * @defgroup storage Storage Manager
 * @{
 */

#ifndef SQL_ENGINE_STORAGE_MANAGER_H
#define SQL_ENGINE_STORAGE_MANAGER_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "common/types.h"

/* Forward declarations */
struct storage_manager_t;
struct buffer_pool_t;

/**
 * @brief Storage statistics
 */
typedef struct {
    uint64_t pages_read;
    uint64_t pages_written;
    uint64_t files_opened;
    uint64_t bytes_read;
    uint64_t bytes_written;
} storage_stats_t;

/**
 * @brief Storage manager instance
 */
typedef struct storage_manager_t {
    /** Data directory path */
    char data_dir[PATH_MAX];
    
    /** Open file handles */
    FILE **open_files;
    
    /** Number of open files */
    int num_files;
    
    /** Maximum open files */
    int max_files;
    
    /** Buffer pool instance */
    struct buffer_pool_t *buffer_pool;
    
    /** Statistics */
    storage_stats_t stats;
} storage_manager_t;

/**
 * @brief Create a new storage manager
 * @param data_dir Data directory path
 * @return New storage manager or NULL on error
 */
storage_manager_t *storage_manager_create(const char *data_dir);

/**
 * @brief Destroy a storage manager
 * @param manager Storage manager
 */
void storage_manager_destroy(storage_manager_t *manager);

/**
 * @brief Open a database file
 * @param manager Storage manager
 * @param filename File name
 * @return File handle or NULL on error
 */
FILE *storage_open_file(storage_manager_t *manager, const char *filename);

/**
 * @brief Close a database file
 * @param manager Storage manager
 * @param file File handle
 * @return 0 on success, -1 on error
 */
int storage_close_file(storage_manager_t *manager, FILE *file);

/**
 * @brief Read a page from storage
 * @param manager Storage manager
 * @param file File handle
 * @param page_num Page number
 * @param buffer Buffer to read into
 * @return 0 on success, -1 on error
 */
int storage_read_page(storage_manager_t *manager, FILE *file, uint32_t page_num, page_t *buffer);

/**
 * @brief Write a page to storage
 * @param manager Storage manager
 * @param file File handle
 * @param page_num Page number
 * @param buffer Buffer to write from
 * @return 0 on success, -1 on error
 */
int storage_write_page(storage_manager_t *manager, FILE *file, uint32_t page_num, page_t *buffer);

/**
 * @brief Get storage statistics
 * @param manager Storage manager
 * @return Statistics structure
 */
storage_stats_t storage_get_stats(storage_manager_t *manager);

/**
 * @brief Check if data directory exists
 * @param data_dir Data directory path
 * @return true if exists, false otherwise
 */
bool storage_dir_exists(const char *data_dir);

/**
 * @brief Create data directory
 * @param data_dir Data directory path
 * @return 0 on success, -1 on error
 */
int storage_create_dir(const char *data_dir);

#endif /* SQL_ENGINE_STORAGE_MANAGER_H */

/** @} */ /* storage */
