/**
 * @file types.h
 * @brief Common type definitions for SQL Engine
 *
 * @defgroup types Common Types
 * @{
 */

#ifndef SQL_ENGINE_COMMON_TYPES_H
#define SQL_ENGINE_COMMON_TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <limits.h>

/* Page size - 8KB default ( PostgreSQL compatible) */
#define PAGE_SIZE 8192
#define PAGE_SIZE_SHIFT 13

/* Name limits */
#define NAME_MAX 64
#define PATH_MAX 256

/* OID (Object Identifier) type */
typedef uint32_t oid_t;

/* Transaction ID type */
typedef uint64_t transaction_id_t;

/* Page number type */
typedef uint32_t page_num_t;

/* Tuple ID type */
typedef struct {
    page_num_t page_num;
    uint16_t slot_num;
} tuple_id_t;

/**
 * @brief Page header structure
 */
typedef struct {
    /** Page LSN (Log Sequence Number) */
    uint64_t lsn;
    
    /** Previous page LSN */
    uint64_t prev_lsn;
    
    /** Transaction ID that created this page */
    transaction_id_t transaction_id;
    
    /** Page type (heap, index, etc.) */
    uint8_t page_type;
    
    /** Page flags */
    uint8_t flags;
    
    /** Lower bound of free space on page */
    uint16_t lower;
    
    /** Upper bound of free space on page */
    uint16_t upper;
    
    /** Special section offset */
    uint16_t special;
    
    /** Number of items on page */
    uint16_t num_items;
    
    /** Number of line pointers (including unused) */
    uint16_t num_line_pointers;
} page_header_t;

/**
 * @brief Line pointer (item pointer)
 */
typedef struct {
    /** Offset to item start */
    uint16_t offset;
    
    /** Item length */
    uint16_t length;
    
    /** Transaction ID for MVCC */
    transaction_id_t transaction_id;
    
    /** Flags */
    uint8_t flags;
} line_pointer_t;

/**
 * @brief Data page structure
 */
typedef struct {
    /** Page header */
    page_header_t header;
    
    /** Line pointers array */
    line_pointer_t line_pointers[1];
    
    /** Data starts here (variable length) */
    char data[PAGE_SIZE - sizeof(page_header_t) - sizeof(line_pointer_t)];
} page_t;

/**
 * @brief Value types supported by the database
 */
typedef enum {
    TYPE_NULL = 0,
    TYPE_BOOL = 1,
    TYPE_INT8 = 2,
    TYPE_INT16 = 3,
    TYPE_INT32 = 4,
    TYPE_INT64 = 5,
    TYPE_FLOAT32 = 6,
    TYPE_FLOAT64 = 7,
    TYPE_DECIMAL = 8,
    TYPE_CHAR = 9,
    TYPE_VARCHAR = 10,
    TYPE_TEXT = 11,
    TYPE_DATE = 12,
    TYPE_TIME = 13,
    TYPE_TIMESTAMP = 14,
    TYPE_JSON = 15,
    TYPE_BLOB = 16
} value_type_t;

/**
 * @brief Value structure (for passing query parameters)
 */
typedef struct {
    value_type_t type;
    bool is_null;
    union {
        bool bool_val;
        int8_t int8_val;
        int16_t int16_val;
        int32_t int32_val;
        int64_t int64_val;
        float float32_val;
        double float64_val;
        char *string_val;
    } value;
} value_t;

/**
 * @brief Tuple (row) structure
 */
typedef struct {
    /** Tuple ID */
    tuple_id_t tuple_id;
    
    /** Number of attributes */
    uint16_t num_attrs;
    
    /** Attribute values */
    value_t *values;
    
    /** Tuple flags */
    uint8_t flags;
} tuple_t;

/**
 * @brief Result structure for query execution
 */
typedef struct {
    /** Number of columns */
    uint16_t num_columns;
    
    /** Column names */
    char **column_names;
    
    /** Column types */
    value_type_t *column_types;
    
    /** Number of rows */
    uint64_t num_rows;
    
    /** Row data */
    tuple_t **rows;
    
    /** Execution time in microseconds */
    uint64_t execution_time;
    
    /** Number of rows affected */
    uint64_t rows_affected;
    
    /** Error message (NULL if successful) */
    char *error_message;
} query_result_t;

/**
 * @brief Comparison operators
 */
typedef enum {
    CMP_EQ = 0,
    CMP_NE = 1,
    CMP_LT = 2,
    CMP_LE = 3,
    CMP_GT = 4,
    CMP_GE = 5,
    CMP_LIKE = 6,
    CMP_IN = 7,
    CMP_IS_NULL = 8,
    CMP_IS_NOT_NULL = 9
} comparison_operator_t;

/**
 * @brief Boolean operators
 */
typedef enum {
    BOOL_AND = 0,
    BOOL_OR = 1,
    BOOL_NOT = 2
} boolean_operator_t;

/**
 * @brief Aggregate functions
 */
typedef enum {
    AGG_NONE = 0,
    AGG_COUNT = 1,
    AGG_SUM = 2,
    AGG_AVG = 3,
    AGG_MIN = 4,
    AGG_MAX = 5
} aggregate_type_t;

/**
 * @brief Join types
 */
typedef enum {
    JOIN_INNER = 0,
    JOIN_LEFT = 1,
    JOIN_RIGHT = 2,
    JOIN_FULL = 3,
    JOIN_CROSS = 4
} join_type_t;

/**
 * @brief Order by direction
 */
typedef enum {
    ORDER_ASC = 0,
    ORDER_DESC = 1
} order_direction_t;

/**
 * @brief Allocation macros
 */
#define ALLOC(size) malloc(size)
#define FREE(ptr) free(ptr)

/**
 * @brief Memory allocation with zero-initialization
 */
#define ALLOC_ZERO(size) calloc(1, size)

/**
 * @brief Reallocate memory
 */
#define REALLOC(ptr, size) realloc(ptr, size)

/**
 * @brief Safe string duplication
 */
#define STRDUP(str) strdup(str)

/**
 * @brief Assert with error handling
 */
#ifdef NDEBUG
#define ASSERT(condition, message) ((void)0)
#else
#define ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            fprintf(stderr, "Assertion failed: %s\n", message); \
            abort(); \
        } \
    } while (0)
#endif

#endif /* SQL_ENGINE_COMMON_TYPES_H */

/** @} */ /* types */
