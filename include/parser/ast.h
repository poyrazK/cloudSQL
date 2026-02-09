/**
 * @file ast.h
 * @brief Abstract Syntax Tree for SQL queries
 *
 * @defgroup ast Abstract Syntax Tree
 * @{
 */

#ifndef SQL_ENGINE_PARSER_AST_H
#define SQL_ENGINE_PARSER_AST_H

#include <stdbool.h>
#include <stdint.h>

#include "common/types.h"
#include "parser/lexer.h"

/* Forward declarations */
struct ast_node_t;
struct select_stmt_t;
struct insert_stmt_t;
struct update_stmt_t;
struct delete_stmt_t;
struct create_table_stmt_t;
struct drop_table_stmt_t;

/**
 * @brief AST node types
 */
typedef enum {
    AST_SELECT,
    AST_INSERT,
    AST_UPDATE,
    AST_DELETE,
    AST_CREATE_TABLE,
    AST_DROP_TABLE,
    AST_CREATE_INDEX,
    AST_DROP_INDEX,
    AST_EXPRESSION,
    AST_BINARY_OP,
    AST_UNARY_OP,
    AST_FUNCTION_CALL,
    AST_COLUMN_REF,
    AST_CONSTANT,
    AST_TABLE_REF,
    AST_JOIN,
    AST_ORDER_BY,
    AST_GROUP_BY
} ast_node_type_t;

/**
 * @brief Expression types
 */
typedef enum {
    EXPR_BINARY,
    EXPR_UNARY,
    EXPR_COLUMN,
    EXPR_CONSTANT,
    EXPR_FUNCTION,
    EXPR_SUBQUERY,
    EXPR_IN,
    EXPR_LIKE,
    EXPR_BETWEEN,
    EXPR_IS_NULL,
    EXPR_CASE
} expr_type_t;

/**
 * @brief Expression node
 */
typedef struct ast_expression_t {
    expr_type_t type;
    struct ast_expression_t *left;
    struct ast_expression_t *right;
    token_type_t op;           /* For binary/unary */
    struct ast_expression_t *expr;  /* For unary */
    char *column_name;         /* For column references */
    value_t value;             /* For constants */
    char *func_name;           /* For function calls */
    struct ast_expression_t **func_args;
    int num_args;
    struct ast_expression_t **list;  /* For IN, BETWEEN */
    bool not_flag;             /* For NOT IN, NOT LIKE, etc. */
} ast_expression_t;

/**
 * @brief Sort specification (ORDER BY)
 */
typedef struct sort_spec_t {
    char *column_name;
    struct ast_expression_t *expr;
    order_direction_t direction;
} sort_spec_t;

/**
 * @brief SELECT statement
 */
typedef struct select_stmt_t {
    bool distinct;
    ast_expression_t **columns;
    int num_columns;
    char **column_aliases;
    int num_aliases;
    ast_expression_t *from_clause;
    ast_expression_t *where_clause;
    sort_spec_t **order_by;
    int num_order_by;
    struct ast_expression_t *group_by;
    struct ast_expression_t *having;
    uint64_t limit;
    uint64_t offset;
    bool is_subquery;
    struct ast_node_t *subquery;
} select_stmt_t;

/**
 * @brief INSERT statement
 */
typedef struct insert_stmt_t {
    char *table_name;
    char **columns;
    int num_columns;
    struct ast_expression_t **values;
    int num_values;
    struct ast_node_t *select_stmt;
} insert_stmt_t;

/**
 * @brief UPDATE statement
 */
typedef struct update_stmt_t {
    char *table_name;
    struct {
        char *column;
        struct ast_expression_t *value;
    } *assignments;
    int num_assignments;
    struct ast_expression_t *where_clause;
} update_stmt_t;

/**
 * @brief DELETE statement
 */
typedef struct delete_stmt_t {
    char *table_name;
    struct ast_expression_t *where_clause;
} delete_stmt_t;

/**
 * @brief Column definition for CREATE TABLE
 */
typedef struct {
    char *name;
    value_type_t type;
    uint32_t max_length;
    bool nullable;
    bool is_primary_key;
    bool is_unique;
    bool is_foreign_key;
    char *foreign_table;
    char *foreign_column;
    struct ast_expression_t *default_value;
} column_def_t;

/**
 * @brief CREATE TABLE statement
 */
typedef struct create_table_stmt_t {
    char *table_name;
    column_def_t *columns;
    int num_columns;
    bool if_not_exists;
} create_table_stmt_t;

/**
 * @brief DROP TABLE statement
 */
typedef struct drop_table_stmt_t {
    char *table_name;
    bool if_exists;
    bool cascade;
} drop_table_stmt_t;

/**
 * @brief CREATE INDEX statement
 */
typedef struct {
    char *index_name;
    char *table_name;
    char **columns;
    int num_columns;
    bool is_unique;
    bool is_primary;
} create_index_stmt_t;

/**
 * @brief DROP INDEX statement
 */
typedef struct {
    char *index_name;
    char *table_name;
} drop_index_stmt_t;

/**
 * @brief Main AST node
 */
typedef struct ast_node_t {
    ast_node_type_t type;
    union {
        select_stmt_t *select;
        insert_stmt_t *insert;
        update_stmt_t *update;
        delete_stmt_t *del;
        create_table_stmt_t *create_table;
        drop_table_stmt_t *drop_table;
        create_index_stmt_t *create_index;
        drop_index_stmt_t *drop_index;
        ast_expression_t *expr;
    } stmt;
    uint32_t line;
    uint32_t column;
} ast_node_t;

/**
 * @brief Parser instance
 */
typedef struct {
    lexer_t *lexer;
    token_t current_token;
    bool has_current;
    char *error_message;
} parser_t;

/**
 * @brief Create a new parser
 * @param sql SQL query string
 * @return New parser or NULL on error
 */
parser_t *parser_create(const char *sql);

/**
 * @brief Destroy a parser
 * @param parser Parser instance
 */
void parser_destroy(parser_t *parser);

/**
 * @brief Parse SQL statement
 * @param parser Parser instance
 * @return AST root node or NULL on error
 */
ast_node_t *parser_parse(parser_t *parser);

/**
 * @brief Parse a single statement
 * @param parser Parser instance
 * @return AST node or NULL on error
 */
ast_node_t *parser_parse_statement(parser_t *parser);

/**
 * @brief Parse SELECT statement
 * @param parser Parser instance
 * @return SELECT AST node
 */
ast_node_t *parser_parse_select(parser_t *parser);

/**
 * @brief Parse INSERT statement
 * @param parser Parser instance
 * @return INSERT AST node
 */
ast_node_t *parser_parse_insert(parser_t *parser);

/**
 * @brief Parse UPDATE statement
 * @param parser Parser instance
 * @return UPDATE AST node
 */
ast_node_t *parser_parse_update(parser_t *parser);

/**
 * @brief Parse DELETE statement
 * @param parser Parser instance
 * @return DELETE AST node
 */
ast_node_t *parser_parse_delete(parser_t *parser);

/**
 * @brief Parse CREATE TABLE statement
 * @param parser Parser instance
 * @return CREATE TABLE AST node
 */
ast_node_t *parser_parse_create_table(parser_t *parser);

/**
 * @brief Parse DROP TABLE statement
 * @param parser Parser instance
 * @return DROP TABLE AST node
 */
ast_node_t *parser_parse_drop_table(parser_t *parser);

/**
 * @brief Parse expression
 * @param parser Parser instance
 * @return Expression AST node
 */
ast_expression_t *parser_parse_expression(parser_t *parser);

/**
 * @brief Parse WHERE clause
 * @param parser Parser instance
 * @return Expression AST node
 */
ast_expression_t *parser_parse_where(parser_t *parser);

/**
 * @brief Get parser error message
 * @param parser Parser instance
 * @return Error message or NULL
 */
const char *parser_get_error(parser_t *parser);

/**
 * @brief Free AST node
 * @param node AST node to free
 */
void ast_free(ast_node_t *node);

/**
 * @brief Free expression
 * @param expr Expression to free
 */
void ast_free_expression(ast_expression_t *expr);

/**
 * @brief Print AST for debugging
 * @param node AST node
 * @param indent Indentation level
 */
void ast_print(ast_node_t *node, int indent);

/**
 * @brief Print expression for debugging
 * @param expr Expression
 * @param indent Indentation level
 */
void ast_print_expression(ast_expression_t *expr, int indent);

#endif /* SQL_ENGINE_PARSER_AST_H */

/** @} */ /* ast */
