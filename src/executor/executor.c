/**
 * @file executor.c
 * @brief Query Executor implementation
 *
 * @defgroup executor Query Executor
 * @{
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "executor/executor.h"
#include "storage/manager.h"

/**
 * @brief Create a new executor
 */
executor_state_t *executor_create(catalog_t *catalog) {
    executor_state_t *executor;
    
    if (catalog == NULL) {
        return NULL;
    }
    
    executor = ALLOC_ZERO(sizeof(executor_state_t));
    if (executor == NULL) {
        return NULL;
    }
    
    executor->catalog = catalog;
    executor->state = EXEC_STATE_INIT;
    executor->rows_produced = 0;
    
    return executor;
}

/**
 * @brief Destroy an executor
 */
void executor_destroy(executor_state_t *executor) {
    if (executor == NULL) {
        return;
    }
    
    if (executor->root != NULL) {
        executor_destroy_operator(executor->root);
    }
    
    if (executor->current_tuple != NULL) {
        FREE(executor->current_tuple);
    }
    
    FREE(executor);
}

/**
 * @brief Execute a query
 */
query_result_t *executor_execute(executor_state_t *executor, ast_node_t *ast) {
    query_result_t *result;
    uint64_t start_time;
    
    if (executor == NULL || ast == NULL) {
        return NULL;
    }
    
    start_time = clock();
    
    /* Create execution plan from AST */
    executor->root = executor_create_operator(ast, executor->catalog);
    if (executor->root == NULL) {
        result = ALLOC_ZERO(sizeof(query_result_t));
        result->error_message = "Failed to create execution plan";
        return result;
    }
    
    /* Open the execution plan */
    if (executor->root->open(executor->root) != 0) {
        result = ALLOC_ZERO(sizeof(query_result_t));
        result->error_message = "Failed to open execution plan";
        executor_destroy_operator(executor->root);
        return result;
    }
    
    executor->state = EXEC_STATE_EXECUTING;
    
    /* Execute and collect results */
    result = ALLOC_ZERO(sizeof(query_result_t));
    result->rows = NULL;
    result->num_rows = 0;
    
    tuple_t *tuple;
    while ((tuple = executor_next(executor)) != NULL) {
        result->rows = REALLOC(result->rows, sizeof(tuple_t *) * (result->num_rows + 1));
        result->rows[result->num_rows++] = tuple;
    }
    
    executor->execution_time = (clock() - start_time) / (CLOCKS_PER_SEC / 1000000);
    result->execution_time = executor->execution_time;
    result->rows_affected = executor->rows_produced;
    
    return result;
}

/**
 * @brief Get next tuple
 */
tuple_t *executor_next(executor_state_t *executor) {
    if (executor == NULL || executor->root == NULL) {
        return NULL;
    }
    
    if (executor->state != EXEC_STATE_EXECUTING) {
        return NULL;
    }
    
    tuple_t *tuple = executor->root->next(executor->root);
    if (tuple != NULL) {
        executor->rows_produced++;
    }
    
    return tuple;
}

/**
 * @brief Close executor
 */
void executor_close(executor_state_t *executor) {
    if (executor == NULL) {
        return;
    }
    
    if (executor->root != NULL) {
        executor->root->close(executor->root);
    }
    
    executor->state = EXEC_STATE_DONE;
}

/**
 * @brief Create an operator from AST node
 */
operator_t *executor_create_operator(ast_node_t *ast, catalog_t *catalog) {
    operator_t *op;
    
    if (ast == NULL) {
        return NULL;
    }
    
    op = ALLOC_ZERO(sizeof(operator_t));
    if (op == NULL) {
        return NULL;
    }
    
    switch (ast->type) {
        case AST_SELECT:
            op->type = OPERATOR_PROJECT;
            /* TODO: Build full execution plan */
            break;
        case AST_CREATE_TABLE:
            /* DDL - handle separately */
            FREE(op);
            return NULL;
        case AST_DROP_TABLE:
            /* DDL - handle separately */
            FREE(op);
            return NULL;
        default:
            FREE(op);
            return NULL;
    }
    
    return op;
}

/**
 * @brief Destroy an operator
 */
void executor_destroy_operator(operator_t *op) {
    if (op == NULL) {
        return;
    }
    
    /* Destroy children */
    for (int i = 0; i < op->num_children; i++) {
        if (op->children[i] != NULL) {
            executor_destroy_operator(op->children[i]);
        }
    }
    FREE(op->children);
    
    /* Free column metadata */
    for (int i = 0; i < op->num_columns; i++) {
        FREE(op->column_names[i]);
    }
    FREE(op->column_names);
    FREE(op->column_types);
    
    /* Call destroy function */
    if (op->destroy != NULL) {
        op->destroy(op);
    } else {
        FREE(op);
    }
}

/**
 * @brief Print execution plan
 */
void executor_print_plan(operator_t *op, int depth) {
    if (op == NULL) {
        return;
    }
    
    for (int i = 0; i < depth; i++) {
        printf("  ");
    }
    
    const char *op_names[] = {
        "SEQ_SCAN", "INDEX_SCAN", "FILTER", "PROJECT",
        "NESTED_LOOP_JOIN", "HASH_JOIN", "SORT", "AGGREGATE",
        "HASH_AGGREGATE", "LIMIT", "SORT_MERGE_JOIN", "MATERIALIZE"
    };
    
    printf("%s\n", op_names[op->type]);
    
    for (int i = 0; i < op->num_children; i++) {
        executor_print_plan(op->children[i], depth + 1);
    }
}

/* Sequential Scan Operator */

/**
 * @brief Seq scan state
 */
typedef struct {
    const char *table_name;
    table_info_t *table;
    FILE *heap_file;
    uint32_t current_page;
    uint32_t current_slot;
    page_t page;
} seq_scan_state_t;

/**
 * @brief Initialize sequential scan
 */
static int seq_scan_init(operator_t *op, ast_node_t *ast, catalog_t *catalog) {
    seq_scan_state_t *state;
    select_stmt_t *select;
    
    if (ast->type != AST_SELECT) {
        return -1;
    }
    
    select = ast->stmt.select;
    if (select->from_clause == NULL || select->from_clause->type != EXPR_COLUMN) {
        return -1;
    }
    
    state = ALLOC_ZERO(sizeof(seq_scan_state_t));
    if (state == NULL) {
        return -1;
    }
    
    state->table_name = select->from_clause->column_name;
    state->table = catalog_get_table_by_name(catalog, state->table_name);
    if (state->table == NULL) {
        FREE(state);
        return -1;
    }
    
    state->heap_file = NULL;
    state->current_page = 0;
    state->current_slot = 0;
    
    op->state = state;
    
    /* Set output schema */
    op->num_columns = state->table->num_columns;
    op->column_names = ALLOC(sizeof(char *) * op->num_columns);
    op->column_types = ALLOC(sizeof(value_type_t) * op->num_columns);
    
    for (int i = 0; i < state->table->num_columns; i++) {
        op->column_names[i] = STRDUP(state->table->columns[i].name);
        op->column_types[i] = state->table->columns[i].type;
    }
    
    return 0;
}

/**
 * @brief Open sequential scan
 */
static int seq_scan_open(operator_t *op) {
    seq_scan_state_t *state = (seq_scan_state_t *)op->state;
    
    if (state->table == NULL) {
        return -1;
    }
    
    /* TODO: Open heap file through storage manager */
    state->heap_file = fopen(state->table->filename, "rb");
    if (state->heap_file == NULL) {
        /* Table might be empty, that's OK */
        return 0;
    }
    
    return 0;
}

/**
 * @brief Get next tuple from sequential scan
 */
static tuple_t *seq_scan_next(operator_t *op) {
    seq_scan_state_t *state = (seq_scan_state_t *)op->state;
    
    if (state->heap_file == NULL) {
        return NULL;  /* Table is empty */
    }
    
    /* TODO: Implement proper heap tuple extraction */
    
    /* Placeholder: return NULL for now */
    return NULL;
}

/**
 * @brief Close sequential scan
 */
static void seq_scan_close(operator_t *op) {
    seq_scan_state_t *state = (seq_scan_state_t *)op->state;
    
    if (state->heap_file != NULL) {
        fclose(state->heap_file);
        state->heap_file = NULL;
    }
}

/**
 * @brief Destroy sequential scan
 */
static void seq_scan_destroy(operator_t *op) {
    FREE(op->state);
    FREE(op);
}

/**
 * @brief Create sequential scan operator
 */
operator_t *executor_create_seq_scan(const char *table_name, catalog_t *catalog) {
    operator_t *op;
    
    op = ALLOC_ZERO(sizeof(operator_t));
    if (op == NULL) {
        return NULL;
    }
    
    op->type = OPERATOR_SEQ_SCAN;
    op->init = seq_scan_init;
    op->open = seq_scan_open;
    op->next = seq_scan_next;
    op->close = seq_scan_close;
    op->destroy = seq_scan_destroy;
    op->num_children = 0;
    
    return op;
}

/* Filter Operator */

/**
 * @brief Filter state
 */
typedef struct {
    operator_t *child;
    ast_expression_t *condition;
} filter_state_t;

/**
 * @brief Initialize filter
 */
static int filter_init(operator_t *op, ast_node_t *ast, catalog_t *catalog) {
    filter_state_t *state;
    
    state = ALLOC_ZERO(sizeof(filter_state_t));
    if (state == NULL) {
        return -1;
    }
    
    /* TODO: Extract filter condition from AST */
    state->condition = NULL;
    
    op->state = state;
    
    return 0;
}

/**
 * @brief Open filter
 */
static int filter_open(operator_t *op) {
    filter_state_t *state = (filter_state_t *)op->state;
    
    if (state->child != NULL) {
        return state->child->open(state->child);
    }
    
    return 0;
}

/**
 * @brief Get next tuple from filter
 */
static tuple_t *filter_next(operator_t *op) {
    filter_state_t *state = (filter_state_t *)op->state;
    
    while (state->child != NULL) {
        tuple_t *tuple = state->child->next(state->child);
        if (tuple == NULL) {
            return NULL;
        }
        
        /* TODO: Evaluate condition */
        
        return tuple;
    }
    
    return NULL;
}

/**
 * @brief Close filter
 */
static void filter_close(operator_t *op) {
    filter_state_t *state = (filter_state_t *)op->state;
    
    if (state->child != NULL) {
        state->child->close(state->child);
    }
}

/**
 * @brief Destroy filter
 */
static void filter_destroy(operator_t *op) {
    filter_state_t *state = (filter_state_t *)op->state;
    
    if (state->condition != NULL) {
        ast_free_expression(state->condition);
    }
    
    FREE(op->state);
    FREE(op);
}

/**
 * @brief Create filter operator
 */
operator_t *executor_create_filter(operator_t *child, ast_expression_t *condition) {
    operator_t *op;
    
    op = ALLOC_ZERO(sizeof(operator_t));
    if (op == NULL) {
        return NULL;
    }
    
    op->type = OPERATOR_FILTER;
    op->init = filter_init;
    op->open = filter_open;
    op->next = filter_next;
    op->close = filter_close;
    op->destroy = filter_destroy;
    
    op->children = ALLOC(sizeof(operator_t *));
    op->children[0] = child;
    op->num_children = 1;
    
    /* Inherit schema from child */
    op->num_columns = child->num_columns;
    op->column_names = child->column_names;
    op->column_types = child->column_types;
    
    return op;
}

/* Project Operator */

/**
 * @brief Project state
 */
typedef struct {
    operator_t *child;
    ast_expression_t **columns;
    int num_columns;
} project_state_t;

/**
 * @brief Initialize project
 */
static int project_init(operator_t *op, ast_node_t *ast, catalog_t *catalog) {
    project_state_t *state;
    
    state = ALLOC_ZERO(sizeof(project_state_t));
    if (state == NULL) {
        return -1;
    }
    
    /* TODO: Extract column expressions from AST */
    state->columns = NULL;
    state->num_columns = 0;
    
    op->state = state;
    
    return 0;
}

/**
 * @brief Open project
 */
static int project_open(operator_t *op) {
    project_state_t *state = (project_state_t *)op->state;
    
    if (state->child != NULL) {
        return state->child->open(state->child);
    }
    
    return 0;
}

/**
 * @brief Get next tuple from project
 */
static tuple_t *project_next(operator_t *op) {
    project_state_t *state = (project_state_t *)op->state;
    
    if (state->child != NULL) {
        return state->child->next(state->child);
    }
    
    return NULL;
}

/**
 * @brief Close project
 */
static void project_close(operator_t *op) {
    project_state_t *state = (project_state_t *)op->state;
    
    if (state->child != NULL) {
        state->child->close(state->child);
    }
}

/**
 * @brief Destroy project
 */
static void project_destroy(operator_t *op) {
    project_state_t *state = (project_state_t *)op->state;
    
    for (int i = 0; i < state->num_columns; i++) {
        if (state->columns[i] != NULL) {
            ast_free_expression(state->columns[i]);
        }
    }
    FREE(state->columns);
    
    FREE(op->state);
    FREE(op);
}

/**
 * @brief Create project operator
 */
operator_t *executor_create_project(operator_t *child, ast_expression_t **columns, int num_columns) {
    operator_t *op;
    
    op = ALLOC_ZERO(sizeof(operator_t));
    if (op == NULL) {
        return NULL;
    }
    
    op->type = OPERATOR_PROJECT;
    op->init = project_init;
    op->open = project_open;
    op->next = project_next;
    op->close = project_close;
    op->destroy = project_destroy;
    
    op->children = ALLOC(sizeof(operator_t *));
    op->children[0] = child;
    op->num_children = 1;
    
    return op;
}

/* Hash Join Operator */

/**
 * @brief Hash join state
 */
typedef struct {
    operator_t *left;
    operator_t *right;
    ast_expression_t *left_key;
    ast_expression_t *right_key;
    
    /* Hash table for left side */
    void *hash_table;
    uint64_t hash_table_size;
    
    /* Build phase state */
    bool build_complete;
    
    /* Probe phase state */
    void *probe_state;
} hash_join_state_t;

/**
 * @brief Create hash join operator
 */
operator_t *executor_create_hash_join(operator_t *left, operator_t *right,
                                       ast_expression_t *left_key, 
                                       ast_expression_t *right_key) {
    operator_t *op;
    
    op = ALLOC_ZERO(sizeof(operator_t));
    if (op == NULL) {
        return NULL;
    }
    
    op->type = OPERATOR_HASH_JOIN;
    
    op->children = ALLOC(sizeof(operator_t *) * 2);
    op->children[0] = left;
    op->children[1] = right;
    op->num_children = 2;
    
    return op;
}

/* Limit Operator */

/**
 * @brief Create limit operator
 */
operator_t *executor_create_limit(operator_t *child, uint64_t limit, uint64_t offset) {
    operator_t *op;
    
    op = ALLOC_ZERO(sizeof(operator_t));
    if (op == NULL) {
        return NULL;
    }
    
    op->type = OPERATOR_LIMIT;
    
    op->children = ALLOC(sizeof(operator_t *));
    op->children[0] = child;
    op->num_children = 1;
    
    return op;
}

/* Aggregate Operator */

/**
 * @brief Create aggregate operator
 */
operator_t *executor_create_aggregate(operator_t *child, 
                                        ast_expression_t **group_by, int num_groups,
                                        ast_expression_t **aggregates, int num_aggregates) {
    operator_t *op;
    
    op = ALLOC_ZERO(sizeof(operator_t));
    if (op == NULL) {
        return NULL;
    }
    
    op->type = OPERATOR_AGGREGATE;
    
    op->children = ALLOC(sizeof(operator_t *));
    op->children[0] = child;
    op->num_children = 1;
    
    return op;
}

/* Sort Operator */

/**
 * @brief Create sort operator
 */
operator_t *executor_create_sort(operator_t *child, 
                                  ast_expression_t **sort_by, int num_sort) {
    operator_t *op;
    
    op = ALLOC_ZERO(sizeof(operator_t));
    if (op == NULL) {
        return NULL;
    }
    
    op->type = OPERATOR_SORT;
    
    op->children = ALLOC(sizeof(operator_t *));
    op->children[0] = child;
    op->num_children = 1;
    
    return op;
}

/** @} */ /* executor */
