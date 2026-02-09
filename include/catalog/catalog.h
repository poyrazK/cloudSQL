/**
 * @file catalog.h
 * @brief System catalog for database metadata
 *
 * @defgroup catalog System Catalog
 * @{
 */

#ifndef SQL_ENGINE_CATALOG_CATALOG_H
#define SQL_ENGINE_CATALOG_CATALOG_H

#include <stdbool.h>
#include <stdint.h>

#include "common/types.h"
#include "storage/manager.h"

/* Forward declarations */
struct catalog_t;
struct table_info_t;
struct column_info_t;
struct index_info_t;

/**
 * @brief Table information structure
 */
typedef struct table_info_t {
    /** Table OID */
    oid_t table_id;
    
    /** Table name */
    char name[NAME_MAX];
    
    /** Number of columns */
    uint16_t num_columns;
    
    /** Column information array */
    struct column_info_t *columns;
    
    /** Number of indexes */
    uint16_t num_indexes;
    
    /** Index information array */
    struct index_info_t *indexes;
    
    /** Number of rows (approximate) */
    uint64_t num_rows;
    
    /** Table file name */
    char filename[PATH_MAX];
    
    /** Table flags */
    uint32_t flags;
    
    /** Creation time */
    uint64_t created_at;
    
    /** Last modification time */
    uint64_t modified_at;
} table_info_t;

/**
 * @brief Column information structure
 */
typedef struct column_info_t {
    /** Column name */
    char name[NAME_MAX];
    
    /** Column type */
    value_type_t type;
    
    /** Column position (1-based) */
    uint16_t position;
    
    /** Maximum length for varchar/text */
    uint32_t max_length;
    
    /** Is column nullable */
    bool nullable;
    
    /** Is column part of primary key */
    bool is_primary_key;
    
    /** Default value expression (optional) */
    char *default_value;
    
    /** Column flags */
    uint32_t flags;
} column_info_t;

/**
 * @brief Index information structure
 */
typedef struct index_info_t {
    /** Index OID */
    oid_t index_id;
    
    /** Index name */
    char name[NAME_MAX];
    
    /** Parent table OID */
    oid_t table_id;
    
    /** Indexed column positions */
    uint16_t *column_positions;
    
    /** Number of indexed columns */
    uint16_t num_columns;
    
    /** Index type (B-tree, hash, etc.) */
    uint8_t index_type;
    
    /** Index file name */
    char filename[PATH_MAX];
    
    /** Is unique index */
    bool is_unique;
    
    /** Is primary key index */
    bool is_primary;
    
    /** Index flags */
    uint32_t flags;
} index_info_t;

/**
 * @brief Database information structure
 */
typedef struct {
    /** Database OID */
    oid_t database_id;
    
    /** Database name */
    char name[NAME_MAX];
    
    /** Database encoding */
    uint32_t encoding;
    
    /** Database collation */
    char collation[NAME_MAX];
    
    /** Number of tables */
    uint16_t num_tables;
    
    /** Table OIDs */
    oid_t *table_ids;
    
    /** Creation time */
    uint64_t created_at;
} database_info_t;

/**
 * @brief Catalog instance
 */
typedef struct catalog_t {
    /** Storage manager reference */
    storage_manager_t *storage;
    
    /** Catalog database file */
    FILE *catalog_file;
    
    /** Database information */
    database_info_t database;
    
    /** Number of tables */
    uint16_t num_tables;
    
    /** Tables hash map (key: table OID) */
    table_info_t **tables;
    
    /** Indexes hash map (key: index OID) */
    index_info_t **indexes;
    
    /** Catalog version */
    uint64_t version;
    
    /** Last catalog update time */
    uint64_t last_update;
} catalog_t;

/**
 * @brief Create a new catalog
 * @param storage Storage manager
 * @return New catalog or NULL on error
 */
catalog_t *catalog_create(storage_manager_t *storage);

/**
 * @brief Destroy a catalog
 * @param catalog Catalog instance
 */
void catalog_destroy(catalog_t *catalog);

/**
 * @brief Load catalog from storage
 * @param catalog Catalog instance
 * @return 0 on success, -1 on error
 */
int catalog_load(catalog_t *catalog);

/**
 * @brief Save catalog to storage
 * @param catalog Catalog instance
 * @return 0 on success, -1 on error
 */
int catalog_save(catalog_t *catalog);

/**
 * @brief Create a new table
 * @param catalog Catalog instance
 * @param name Table name
 * @param columns Column definitions
 * @param num_columns Number of columns
 * @return Table OID or 0 on error
 */
oid_t catalog_create_table(catalog_t *catalog, const char *name, 
                           column_info_t *columns, int num_columns);

/**
 * @brief Drop a table
 * @param catalog Catalog instance
 * @param table_id Table OID
 * @return 0 on success, -1 on error
 */
int catalog_drop_table(catalog_t *catalog, oid_t table_id);

/**
 * @brief Get table information
 * @param catalog Catalog instance
 * @param table_id Table OID
 * @return Table info or NULL if not found
 */
table_info_t *catalog_get_table(catalog_t *catalog, oid_t table_id);

/**
 * @brief Get table by name
 * @param catalog Catalog instance
 * @param name Table name
 * @return Table info or NULL if not found
 */
table_info_t *catalog_get_table_by_name(catalog_t *catalog, const char *name);

/**
 * @brief Get all tables
 * @param catalog Catalog instance
 * @param num_tables Output: number of tables
 * @return Array of table info (caller must free)
 */
table_info_t **catalog_get_all_tables(catalog_t *catalog, int *num_tables);

/**
 * @brief Create an index
 * @param catalog Catalog instance
 * @param name Index name
 * @param table_id Table OID
 * @param column_positions Column positions to index
 * @param num_columns Number of indexed columns
 * @param index_type Index type
 * @param is_unique Is unique index
 * @return Index OID or 0 on error
 */
oid_t catalog_create_index(catalog_t *catalog, const char *name, oid_t table_id,
                           uint16_t *column_positions, int num_columns,
                           uint8_t index_type, bool is_unique);

/**
 * @brief Drop an index
 * @param catalog Catalog instance
 * @param index_id Index OID
 * @return 0 on success, -1 on error
 */
int catalog_drop_index(catalog_t *catalog, oid_t index_id);

/**
 * @brief Get index information
 * @param catalog Catalog instance
 * @param index_id Index OID
 * @return Index info or NULL if not found
 */
index_info_t *catalog_get_index(catalog_t *catalog, oid_t index_id);

/**
 * @brief Get indexes for a table
 * @param catalog Catalog instance
 * @param table_id Table OID
 * @param num_indexes Output: number of indexes
 * @return Array of index info (caller must free)
 */
index_info_t **catalog_get_table_indexes(catalog_t *catalog, oid_t table_id, int *num_indexes);

/**
 * @brief Update table statistics
 * @param catalog Catalog instance
 * @param table_id Table OID
 * @param num_rows New row count
 * @return 0 on success, -1 on error
 */
int catalog_update_table_stats(catalog_t *catalog, oid_t table_id, uint64_t num_rows);

/**
 * @brief Create a new database
 * @param catalog Catalog instance
 * @param name Database name
 * @param encoding Character encoding
 * @param collation Collation
 * @return 0 on success, -1 on error
 */
int catalog_create_database(catalog_t *catalog, const char *name, 
                           uint32_t encoding, const char *collation);

/**
 * @brief Get database information
 * @param catalog Catalog instance
 * @return Database info
 */
database_info_t *catalog_get_database(catalog_t *catalog);

/**
 * @brief Check if table exists
 * @param catalog Catalog instance
 * @param table_id Table OID
 * @return true if exists, false otherwise
 */
bool catalog_table_exists(catalog_t *catalog, oid_t table_id);

/**
 * @brief Check if table exists by name
 * @param catalog Catalog instance
 * @param name Table name
 * @return true if exists, false otherwise
 */
bool catalog_table_exists_by_name(catalog_t *catalog, const char *name);

/**
 * @brief Print catalog contents
 * @param catalog Catalog instance
 */
void catalog_print(catalog_t *catalog);

#endif /* SQL_ENGINE_CATALOG_CATALOG_H */

/** @} */ /* catalog */
