/**
 * @file executor.h
 * @brief Query Executor - Volcano-style iterator model
 *
 * @defgroup executor Query Executor
 * @{
 */

#ifndef SQL_ENGINE_EXECUTOR_EXECUTOR_H
#define SQL_ENGINE_EXECUTOR_EXECUTOR_H

#include <stdbool.h>
#include <stdint.h>

#include "common/types.h"
#include "parser/ast.h"
#include "catalog/catalog.h"

/* Forward declarations */
struct executor_state_t;
struct operator_t;

/**
 * @brief Operator types
 */
typedef enum {
    OPERATOR_SEQ_SCAN,
    OPERATOR_INDEX_SCAN,
    OPERATOR_FILTER,
    OPERATOR_PROJECT,
    OPERATOR_NESTED_LOOP_JOIN,
    OPERATOR_HASH_JOIN,
    OPERATOR_SORT,
    OPERATOR_AGGREGATE,
    OPERATOR_HASH_AGGREGATE,
    OPERATOR_LIMIT,
    OPERATOR_SORT_MERGE_JOIN,
    OPERATOR_MATERIALIZE
} operator_type_t;

/**
 * @brief Execution state
 */
typedef enum {
    EXEC_STATE_INIT,
    EXEC_STATE_OPEN,
    EXEC_STATE_EXECUTING,
    EXEC_STATE_DONE,
    EXEC_STATE_ERROR
} exec_state_t;

/**
 * @brief Operator output mode
 */
typedef enum {
    OUTPUT_MODE_TUPLE,
    OUTPUT_MODE_EOF
} output_mode_t;

/**
 * @brief Operator structure
 */
typedef struct operator_t {
    /** Operator type */
    operator_type_t type;
    
    /** Operator-specific state */
    void *state;
    
    /** Child operators */
    struct operator_t **children;
    int num_children;
    
    /** Schema (output columns) */
    char **column_names;
    value_type_t *column_types;
    int num_columns;
    
    /** Current execution state */
    exec_state_t state;
    
    /**
     * @brief Initialize operator
     * @param op Operator
     * @param ast AST node
     * @param catalog Catalog
     * @return 0 on success, -1 on error
     */
    int (*init)(struct operator_t *op, ast_node_t *ast, catalog_t *catalog);
    
    /**
     * @brief Open operator
     * @param op Operator
     * @return 0 on success, -1 on error
     */
    int (*open)(struct operator_t *op);
    
    /**
     * @brief Get next tuple
     * @param op Operator
     * @return Tuple or NULL if done
     */
    tuple_t *(*next)(struct operator_t *op);
    
    /**
     * @brief Close operator
     * @param op Operator
     */
    void (*close)(struct operator_t *op);
    
    /**
     * @brief Destroy operator
     * @param op Operator
     */
    void (*destroy)(struct operator_t *op);
} operator_t;

/**
 * @brief Executor instance
 */
typedef struct executor_state_t {
    /** Root operator */
    operator_t *root;
    
    /** Current tuple */
    tuple_t *current_tuple;
    
    /** Execution state */
    exec_state_t state;
    
    /** Catalog reference */
    catalog_t *catalog;
    
    /** Number of rows produced */
    uint64_t rows_produced;
    
    /** Execution time in microseconds */
    uint64_t execution_time;
} executor_state_t;

/**
 * @brief Create a new executor
 * @param catalog Catalog instance
 * @return New executor or NULL on error
 */
executor_state_t *executor_create(catalog_t *catalog);

/**
 * @brief Destroy an executor
 * @param executor Executor instance
 */
void executor_destroy(executor_state_t *executor);

/**
 * @brief Execute a query
 * @param executor Executor instance
 * @param ast AST root node
 * @return Query result
 */
query_result_t *executor_execute(executor_state_t *executor, ast_node_t *ast);

/**
 * @brief Get next tuple from query
 * @param executor Executor instance
 * @return Tuple or NULL if done
 */
tuple_t *executor_next(executor_state_t *executor);

/**
 * @brief Close executor
 * @param executor Executor instance
 */
void executor_close(executor_state_t *executor);

/**
 * @brief Create an operator from AST node
 * @param ast AST node
 * @param catalog Catalog
 * @return New operator or NULL on error
 */
operator_t *executor_create_operator(ast_node_t *ast, catalog_t *catalog);

/**
 * @brief Destroy an operator
 * @param op Operator
 */
void executor_destroy_operator(operator_t *op);

/**
 * @brief Print execution plan
 * @param op Operator
 * @param depth Indentation depth
 */
void executor_print_plan(operator_t *op, int depth);

/* Operator implementations */

/**
 * @brief Create sequential scan operator
 * @param table_name Table name
 * @param catalog Catalog
 * @return New operator
 */
operator_t *executor_create_seq_scan(const char *table_name, catalog_t *catalog);

/**
 * @brief Create filter operator
 * @param child Child operator
 * @param condition Filter condition expression
 * @return New operator
 */
operator_t *executor_create_filter(operator_t *child, ast_expression_t *condition);

/**
 * @brief Create project operator
 * @param child Child operator
 * @param columns Column expressions
 * @param num_columns Number of columns
 * @return New operator
 */
operator_t *executor_create_project(operator_t *child, ast_expression_t **columns, int num_columns);

/**
 * @brief Create hash join operator
 * @param left Left child
 * @param right Right child
 * @param left_key Left join key
 * @param right_key Right join key
 * @return New operator
 */
operator_t *executor_create_hash_join(operator_t *left, operator_t *right,
                                       ast_expression_t *left_key, 
                                       ast_expression_t *right_key);

/**
 * @brief Create limit operator
 * @param child Child operator
 * @param limit Limit value
 * @param offset Offset value
 * @return New operator
 */
operator_t *executor_create_limit(operator_t *child, uint64_t limit, uint64_t offset);

/**
 * @brief Create aggregate operator
 * @param child Child operator
 * @param group_by Group by expressions
 * @param num_groups Number of group by expressions
 * @param aggregates Aggregate expressions
 * @param num_aggregates Number of aggregates
 * @return New operator
 */
operator_t *executor_create_aggregate(operator_t *child, 
                                        ast_expression_t **group_by, int num_groups,
                                        ast_expression_t **aggregates, int num_aggregates);

/**
 * @brief Create sort operator
 * @param child Child operator
 * @param sort_by Sort expressions
 * @param num_sort Number of sort expressions
 * @return New operator
 */
operator_t *executor_create_sort(operator_t *child, 
                                  ast_expression_t **sort_by, int num_sort);

#endif /* SQL_ENGINE_EXECUTOR_EXECUTOR_H */

/** @} */ /* executor */
