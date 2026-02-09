/**
 * @file manager.c
 * @brief Storage manager implementation
 *
 * @defgroup storage Storage Manager
 * @{
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <unistd.h>

#include "storage/manager.h"

/**
 * @brief Create a new storage manager
 */
storage_manager_t *storage_manager_create(const char *data_dir) {
    storage_manager_t *manager;
    
    if (data_dir == NULL) {
        return NULL;
    }
    
    manager = ALLOC_ZERO(sizeof(storage_manager_t));
    if (manager == NULL) {
        return NULL;
    }
    
    /* Copy data directory */
    strncpy(manager->data_dir, data_dir, sizeof(manager->data_dir) - 1);
    
    /* Initialize file handle array */
    manager->open_files = NULL;
    manager->num_files = 0;
    manager->max_files = 16;
    
    manager->open_files = ALLOC(sizeof(FILE *) * manager->max_files);
    if (manager->open_files == NULL) {
        FREE(manager);
        return NULL;
    }
    
    memset(manager->open_files, 0, sizeof(FILE *) * manager->max_files);
    
    /* Initialize statistics */
    memset(&manager->stats, 0, sizeof(manager->stats));
    
    /* Create data directory if it doesn't exist */
    if (!storage_dir_exists(data_dir)) {
        if (storage_create_dir(data_dir) != 0) {
            FREE(manager->open_files);
            FREE(manager);
            return NULL;
        }
    }
    
    return manager;
}

/**
 * @brief Destroy a storage manager
 */
void storage_manager_destroy(storage_manager_t *manager) {
    if (manager == NULL) {
        return;
    }
    
    /* Close all open files */
    for (int i = 0; i < manager->num_files; i++) {
        if (manager->open_files[i] != NULL) {
            fclose(manager->open_files[i]);
        }
    }
    
    /* Free resources */
    FREE(manager->open_files);
    FREE(manager);
}

/**
 * @brief Open a database file
 */
FILE *storage_open_file(storage_manager_t *manager, const char *filename) {
    char filepath[PATH_MAX];
    FILE *file;
    int slot;
    
    if (manager == NULL || filename == NULL) {
        return NULL;
    }
    
    /* Check if file is already open */
    for (int i = 0; i < manager->num_files; i++) {
        if (manager->open_files[i] != NULL) {
            /* TODO: Track file names to avoid duplicates */
        }
    }
    
    /* Find available slot */
    slot = -1;
    for (int i = 0; i < manager->max_files; i++) {
        if (manager->open_files[i] == NULL) {
            slot = i;
            break;
        }
    }
    
    if (slot == -1) {
        /* Expand file handle array */
        int new_max = manager->max_files * 2;
        FILE **new_files = REALLOC(manager->open_files, sizeof(FILE *) * new_max);
        if (new_files == NULL) {
            return NULL;
        }
        
        manager->open_files = new_files;
        for (int i = manager->max_files; i < new_max; i++) {
            manager->open_files[i] = NULL;
        }
        slot = manager->max_files;
        manager->max_files = new_max;
    }
    
    /* Build full path */
    snprintf(filepath, sizeof(filepath), "%s/%s", manager->data_dir, filename);
    
    /* Open file */
    file = fopen(filepath, "a+b");
    if (file == NULL) {
        fprintf(stderr, "Cannot open file: %s (%s)\n", filepath, strerror(errno));
        return NULL;
    }
    
    /* Store file handle */
    manager->open_files[slot] = file;
    if (slot >= manager->num_files) {
        manager->num_files = slot + 1;
    }
    
    manager->stats.files_opened++;
    
    return file;
}

/**
 * @brief Close a database file
 */
int storage_close_file(storage_manager_t *manager, FILE *file) {
    if (manager == NULL || file == NULL) {
        return -1;
    }
    
    for (int i = 0; i < manager->num_files; i++) {
        if (manager->open_files[i] == file) {
            fclose(file);
            manager->open_files[i] = NULL;
            return 0;
        }
    }
    
    return -1;
}

/**
 * @brief Read a page from storage
 */
int storage_read_page(storage_manager_t *manager, FILE *file, uint32_t page_num, page_t *buffer) {
    size_t bytes_read;
    off_t offset;
    
    if (manager == NULL || file == NULL || buffer == NULL) {
        return -1;
    }
    
    /* Calculate offset */
    offset = (off_t)page_num * PAGE_SIZE;
    
    /* Seek to page */
    if (fseeko(file, offset, SEEK_SET) != 0) {
        return -1;
    }
    
    /* Read page */
    bytes_read = fread(buffer, 1, PAGE_SIZE, file);
    if (bytes_read != PAGE_SIZE) {
        /* File might be smaller than expected */
        if (feof(file)) {
            /* Zero out buffer for non-existent page */
            memset(buffer, 0, PAGE_SIZE);
            return 0;
        }
        return -1;
    }
    
    manager->stats.pages_read++;
    manager->stats.bytes_read += bytes_read;
    
    return 0;
}

/**
 * @brief Write a page to storage
 */
int storage_write_page(storage_manager_t *manager, FILE *file, uint32_t page_num, page_t *buffer) {
    size_t bytes_written;
    off_t offset;
    
    if (manager == NULL || file == NULL || buffer == NULL) {
        return -1;
    }
    
    /* Calculate offset */
    offset = (off_t)page_num * PAGE_SIZE;
    
    /* Seek to page */
    if (fseeko(file, offset, SEEK_SET) != 0) {
        return -1;
    }
    
    /* Write page */
    bytes_written = fwrite(buffer, 1, PAGE_SIZE, file);
    if (bytes_written != PAGE_SIZE) {
        return -1;
    }
    
    /* Flush to disk */
    fflush(file);
    
    manager->stats.pages_written++;
    manager->stats.bytes_written += bytes_written;
    
    return 0;
}

/**
 * @brief Get storage statistics
 */
storage_stats_t storage_get_stats(storage_manager_t *manager) {
    storage_stats_t stats = {0};
    
    if (manager != NULL) {
        stats = manager->stats;
    }
    
    return stats;
}

/**
 * @brief Check if data directory exists
 */
bool storage_dir_exists(const char *data_dir) {
    struct stat st;
    
    if (data_dir == NULL) {
        return false;
    }
    
    return (stat(data_dir, &st) == 0 && S_ISDIR(st.st_mode));
}

/**
 * @brief Create data directory
 */
int storage_create_dir(const char *data_dir) {
    if (data_dir == NULL) {
        return -1;
    }
    
    if (mkdir(data_dir, 0755) != 0) {
        if (errno != EEXIST) {
            fprintf(stderr, "Cannot create directory: %s (%s)\n", 
                    data_dir, strerror(errno));
            return -1;
        }
    }
    
    return 0;
}

/** @} */ /* storage */
