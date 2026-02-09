/**
 * @file ast.c
 * @brief AST implementation
 *
 * @defgroup ast Abstract Syntax Tree
 * @{
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "parser/ast.h"
#include "parser/lexer.h"

/**
 * @brief Get next token (consuming current)
 */
static token_t next_token(parser_t *parser) {
    if (parser->has_current) {
        parser->has_current = false;
        return parser->current_token;
    }
    return lexer_next_token(parser->lexer);
}

/**
 * @brief Peek at current token
 */
static token_t *peek_token(parser_t *parser) {
    if (parser->has_current) {
        return &parser->current_token;
    }
    parser->current_token = lexer_next_token(parser->lexer);
    parser->has_current = true;
    return &parser->current_token;
}

/**
 * @brief Consume expected token
 */
static bool consume_token(parser_t *parser, token_type_t expected) {
    token_t token = next_token(parser);
    if (token.type != expected) {
        parser->error_message = "Unexpected token";
        token_free(&token);
        return false;
    }
    return true;
}

/**
 * @brief Create a new parser
 */
parser_t *parser_create(const char *sql) {
    parser_t *parser;
    
    if (sql == NULL) {
        return NULL;
    }
    
    parser = ALLOC_ZERO(sizeof(parser_t));
    if (parser == NULL) {
        return NULL;
    }
    
    parser->lexer = lexer_create(sql, strlen(sql));
    if (parser->lexer == NULL) {
        FREE(parser);
        return NULL;
    }
    
    parser->has_current = false;
    parser->error_message = NULL;
    
    return parser;
}

/**
 * @brief Destroy a parser
 */
void parser_destroy(parser_t *parser) {
    if (parser == NULL) {
        return;
    }
    
    if (parser->has_current) {
        token_free(&parser->current_token);
    }
    
    lexer_destroy(parser->lexer);
    FREE(parser);
}

/**
 * @brief Parse expression (lowest precedence - OR)
 */
static ast_expression_t *parse_or(parser_t *parser) {
    ast_expression_t *left = parse_and(parser);
    
    while (true) {
        token_t *tok = peek_token(parser);
        if (tok->type == TOKEN_OR) {
            next_token(parser);  /* Consume OR */
            ast_expression_t *right = parse_and(parser);
            ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
            node->type = EXPR_BINARY;
            node->op = TOKEN_OR;
            node->left = left;
            node->right = right;
            left = node;
        } else {
            break;
        }
    }
    
    return left;
}

/**
 * @brief Parse AND
 */
static ast_expression_t *parse_and(parser_t *parser) {
    ast_expression_t *left = parse_not(parser);
    
    while (true) {
        token_t *tok = peek_token(parser);
        if (tok->type == TOKEN_AND) {
            next_token(parser);  /* Consume AND */
            ast_expression_t *right = parse_not(parser);
            ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
            node->type = EXPR_BINARY;
            node->op = TOKEN_AND;
            node->left = left;
            node->right = right;
            left = node;
        } else {
            break;
        }
    }
    
    return left;
}

/**
 * @brief Parse NOT
 */
static ast_expression_t *parse_not(parser_t *parser) {
    token_t *tok = peek_token(parser);
    
    if (tok->type == TOKEN_NOT) {
        next_token(parser);  /* Consume NOT */
        ast_expression_t *expr = parse_not(parser);
        ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
        node->type = EXPR_UNARY;
        node->op = TOKEN_NOT;
        node->expr = expr;
        return node;
    }
    
    return parse_compare(parser);
}

/**
 * @brief Parse comparison operators
 */
static ast_expression_t *parse_compare(parser_t *parser) {
    ast_expression_t *left = parse_add_sub(parser);
    
    while (true) {
        token_t *tok = peek_token(parser);
        
        if (tok->type == TOKEN_EQ || tok->type == TOKEN_NE ||
            tok->type == TOKEN_LT || tok->type == TOKEN_LE ||
            tok->type == TOKEN_GT || tok->type == TOKEN_GE) {
            
            token_type_t op = tok->type;
            next_token(parser);
            
            ast_expression_t *right = parse_add_sub(parser);
            ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
            node->type = EXPR_BINARY;
            node->op = op;
            node->left = left;
            node->right = right;
            left = node;
        } else if (tok->type == TOKEN_IS) {
            next_token(parser);
            bool is_not = false;
            token_t *next = peek_token(parser);
            if (next->type == TOKEN_NOT) {
                next_token(parser);
                is_not = true;
            }
            
            ast_expression_t *right = parse_add_sub(parser);
            ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
            node->type = EXPR_IS_NULL;
            node->expr = right;
            node->not_flag = is_not;
            left = node;
        } else if (tok->type == TOKEN_LIKE) {
            next_token(parser);
            ast_expression_t *right = parse_add_sub(parser);
            ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
            node->type = EXPR_LIKE;
            node->left = left;
            node->right = right;
            return node;
        } else if (tok->type == TOKEN_IN) {
            next_token(parser);
            bool is_not = false;
            token_t *next = peek_token(parser);
            if (next->type == TOKEN_NOT) {
                next_token(parser);
                is_not = true;
            }
            
            /* Parse parenthesized list or subquery */
            token_t *peek = peek_token(parser);
            if (peek->type == TOKEN_LPAREN) {
                next_token(parser);
                /* Parse value list */
                ast_expression_t **values = NULL;
                int num_values = 0;
                
                while (true) {
                    ast_expression_t *val = parse_add_sub(parser);
                    values = REALLOC(values, sizeof(ast_expression_t *) * (num_values + 1));
                    values[num_values++] = val;
                    
                    token_t *t = peek_token(parser);
                    if (t->type == TOKEN_COMMA) {
                        next_token(parser);
                    } else {
                        break;
                    }
                }
                
                consume_token(parser, TOKEN_RPAREN);
                
                ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
                node->type = EXPR_IN;
                node->expr = left;
                node->list = values;
                node->num_args = num_values;
                node->not_flag = is_not;
                left = node;
            }
        } else {
            break;
        }
    }
    
    return left;
}

/**
 * @brief Parse addition/subtraction
 */
static ast_expression_t *parse_add_sub(parser_t *parser) {
    ast_expression_t *left = parse_mul_div(parser);
    
    while (true) {
        token_t *tok = peek_token(parser);
        
        if (tok->type == TOKEN_PLUS || tok->type == TOKEN_MINUS) {
            token_type_t op = tok->type;
            next_token(parser);
            
            ast_expression_t *right = parse_mul_div(parser);
            ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
            node->type = EXPR_BINARY;
            node->op = op;
            node->left = left;
            node->right = right;
            left = node;
        } else {
            break;
        }
    }
    
    return left;
}

/**
 * @brief Parse multiplication/division
 */
static ast_expression_t *parse_mul_div(parser_t *parser) {
    ast_expression_t *left = parse_unary(parser);
    
    while (true) {
        token_t *tok = peek_token(parser);
        
        if (tok->type == TOKEN_STAR || tok->type == TOKEN_SLASH || tok->type == TOKEN_PERCENT) {
            token_type_t op = tok->type;
            next_token(parser);
            
            ast_expression_t *right = parse_unary(parser);
            ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
            node->type = EXPR_BINARY;
            node->op = op;
            node->left = left;
            node->right = right;
            left = node;
        } else {
            break;
        }
    }
    
    return left;
}

/**
 * @brief Parse unary operators
 */
static ast_expression_t *parse_unary(parser_t *parser) {
    token_t *tok = peek_token(parser);
    
    if (tok->type == TOKEN_MINUS || tok->type == TOKEN_PLUS) {
        next_token(parser);
        ast_expression_t *expr = parse_unary(parser);
        ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
        node->type = EXPR_UNARY;
        node->op = tok->type;
        node->expr = expr;
        return node;
    }
    
    return parse_primary(parser);
}

/**
 * @brief Parse primary expressions
 */
static ast_expression_t *parse_primary(parser_t *parser) {
    token_t *tok = peek_token(parser);
    
    /* NULL */
    if (tok->type == TOKEN_NULL) {
        next_token(parser);
        ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
        node->type = EXPR_CONSTANT;
        node->value.type = TYPE_NULL;
        node->value.is_null = true;
        return node;
    }
    
    /* Number */
    if (tok->type == TOKEN_NUMBER) {
        next_token(parser);
        ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
        node->type = EXPR_CONSTANT;
        
        if (tok->value.float_val != (double)(int64_t)tok->value.float_val) {
            node->value.type = TYPE_FLOAT64;
            node->value.float64_val = tok->value.float_val;
        } else {
            node->value.type = TYPE_INT64;
            node->value.int64_val = tok->value.int_val;
        }
        
        return node;
    }
    
    /* String */
    if (tok->type == TOKEN_STRING) {
        next_token(parser);
        ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
        node->type = EXPR_CONSTANT;
        node->value.type = TYPE_TEXT;
        node->value.value.string_val = tok->value.str_val;
        return node;
    }
    
    /* Parenthesized expression or subquery */
    if (tok->type == TOKEN_LPAREN) {
        next_token(parser);
        
        /* Check for subquery */
        token_t *next = peek_token(parser);
        if (next->type == TOKEN_SELECT || next->type == TOKEN_LPAREN) {
            /* Subquery - TODO */
            consume_token(parser, TOKEN_RPAREN);
            return NULL;
        }
        
        ast_expression_t *expr = parse_or(parser);
        consume_token(parser, TOKEN_RPAREN);
        return expr;
    }
    
    /* Identifier (column reference or function call) */
    if (tok->type == TOKEN_IDENTIFIER) {
        char *name = STRDUP(tok->lexeme);
        next_token(parser);
        
        token_t *peek = peek_token(parser);
        if (peek->type == TOKEN_LPAREN) {
            /* Function call */
            next_token(parser);  /* Consume LPAREN */
            
            ast_expression_t **args = NULL;
            int num_args = 0;
            
            if (peek_token(parser)->type != TOKEN_RPAREN) {
                while (true) {
                    ast_expression_t *arg = parse_or(parser);
                    args = REALLOC(args, sizeof(ast_expression_t *) * (num_args + 1));
                    args[num_args++] = arg;
                    
                    token_t *t = peek_token(parser);
                    if (t->type == TOKEN_COMMA) {
                        next_token(parser);
                    } else {
                        break;
                    }
                }
            }
            
            consume_token(parser, TOKEN_RPAREN);
            
            ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
            node->type = EXPR_FUNCTION;
            node->func_name = name;
            node->func_args = args;
            node->num_args = num_args;
            return node;
        }
        
        /* Column reference */
        ast_expression_t *node = ALLOC_ZERO(sizeof(ast_expression_t));
        node->type = EXPR_COLUMN;
        node->column_name = name;
        return node;
    }
    
    return NULL;
}

/**
 * @brief Parse expression
 */
ast_expression_t *parser_parse_expression(parser_t *parser) {
    return parse_or(parser);
}

/**
 * @brief Parse WHERE clause
 */
ast_expression_t *parser_parse_where(parser_t *parser) {
    token_t *tok = peek_token(parser);
    
    if (tok->type == TOKEN_WHERE) {
        next_token(parser);  /* Consume WHERE */
        return parse_or(parser);
    }
    
    return NULL;
}

/**
 * @brief Parse SELECT statement
 */
static ast_node_t *parse_select(parser_t *parser) {
    select_stmt_t *select = ALLOC_ZERO(sizeof(select_stmt_t));
    token_t *tok;
    
    /* SELECT */
    next_token(parser);
    
    /* DISTINCT */
    tok = peek_token(parser);
    if (tok->type == TOKEN_DISTINCT) {
        next_token(parser);
        select->distinct = true;
    }
    
    /* Columns */
    select->columns = NULL;
    select->num_columns = 0;
    
    if (peek_token(parser)->type != TOKEN_FROM) {
        while (true) {
            ast_expression_t *col = parse_or(parser);
            select->columns = REALLOC(select->columns, sizeof(ast_expression_t *) * (select->num_columns + 1));
            select->columns[select->num_columns++] = col;
            
            tok = peek_token(parser);
            if (tok->type == TOKEN_COMMA) {
                next_token(parser);
            } else {
                break;
            }
        }
    }
    
    /* FROM */
    if (!consume_token(parser, TOKEN_FROM)) {
        FREE(select);
        return NULL;
    }
    
    /* Table */
    tok = next_token(parser);
    if (tok->type != TOKEN_IDENTIFIER) {
        parser->error_message = "Expected table name";
        FREE(select);
        return NULL;
    }
    select->from_clause = ALLOC_ZERO(sizeof(ast_expression_t));
    select->from_clause->type = EXPR_COLUMN;
    select->from_clause->column_name = STRDUP(tok->lexeme);
    
    /* WHERE */
    select->where_clause = parser_parse_where(parser);
    
    /* ORDER BY - TODO */
    
    /* LIMIT - TODO */
    
    /* OFFSET - TODO */
    
    ast_node_t *node = ALLOC_ZERO(sizeof(ast_node_t));
    node->type = AST_SELECT;
    node->stmt.select = select;
    node->line = 1;
    
    return node;
}

/**
 * @brief Parse column definition
 */
static column_def_t *parse_column_def(parser_t *parser) {
    token_t *tok = next_token(parser);
    
    if (tok->type != TOKEN_IDENTIFIER) {
        return NULL;
    }
    
    column_def_t *col = ALLOC_ZERO(sizeof(column_def_t));
    col->name = STRDUP(tok->lexeme);
    col->nullable = true;
    col->is_primary_key = false;
    col->is_unique = false;
    col->is_foreign_key = false;
    
    /* Data type */
    tok = next_token(parser);
    if (tok->type == TOKEN_IDENTIFIER || tok->type == TOKEN_VARCHAR) {
        if (strcasecmp(tok->lexeme, "INT") == 0 || 
            strcasecmp(tok->lexeme, "INTEGER") == 0) {
            col->type = TYPE_INT32;
        } else if (strcasecmp(tok->lexeme, "BIGINT") == 0) {
            col->type = TYPE_INT64;
        } else if (strcasecmp(tok->lexeme, "SMALLINT") == 0) {
            col->type = TYPE_INT16;
        } else if (strcasecmp(tok->lexeme, "FLOAT") == 0 || 
                   strcasecmp(tok->lexeme, "DOUBLE") == 0) {
            col->type = TYPE_FLOAT64;
        } else if (strcasecmp(tok->lexeme, "VARCHAR") == 0) {
            col->type = TYPE_VARCHAR;
            consume_token(parser, TOKEN_LPAREN);
            tok = next_token(parser);
            if (tok->type == TOKEN_NUMBER) {
                col->max_length = tok->value.int_val;
            }
            consume_token(parser, TOKEN_RPAREN);
        } else if (strcasecmp(tok->lexeme, "TEXT") == 0) {
            col->type = TYPE_TEXT;
        } else if (strcasecmp(tok->lexeme, "BOOLEAN") == 0 || 
                   strcasecmp(tok->lexeme, "BOOL") == 0) {
            col->type = TYPE_BOOL;
        } else if (strcasecmp(tok->lexeme, "DATE") == 0) {
            col->type = TYPE_DATE;
        } else if (strcasecmp(tok->lexeme, "TIMESTAMP") == 0) {
            col->type = TYPE_TIMESTAMP;
        } else if (strcasecmp(tok->lexeme, "JSON") == 0) {
            col->type = TYPE_JSON;
        } else {
            col->type = TYPE_TEXT;  /* Default */
        }
    }
    
    /* Parse constraints */
    while (true) {
        tok = peek_token(parser);
        
        if (tok->type == TOKEN_PRIMARY) {
            next_token(parser);
            consume_token(parser, TOKEN_KEY);
            col->is_primary_key = true;
            col->nullable = false;
        } else if (tok->type == TOKEN_NOT) {
            next_token(parser);
            tok = next_token(parser);
            if (tok->type == TOKEN_NULL) {
                col->nullable = false;
            }
        } else if (tok->type == TOKEN_DEFAULT) {
            next_token(parser);
            ast_expression_t *def = parse_or(parser);
            col->default_value = def;
        } else if (tok->type == TOKEN_COMMA || tok->type == TOKEN_RPAREN) {
            break;
        } else {
            break;
        }
    }
    
    return col;
}

/**
 * @brief Parse CREATE TABLE statement
 */
static ast_node_t *parse_create_table(parser_t *parser) {
    create_table_stmt_t *create = ALLOC_ZERO(sizeof(create_table_stmt_t));
    
    /* CREATE */
    next_token(parser);
    
    /* TABLE */
    if (!consume_token(parser, TOKEN_TABLE)) {
        FREE(create);
        return NULL;
    }
    
    /* IF NOT EXISTS */
    token_t *tok = peek_token(parser);
    if (tok->type == TOKEN_NOT) {
        next_token(parser);
        tok = peek_token(parser);
        if (tok->type == TOKEN_EXISTS) {
            next_token(parser);
            create->if_not_exists = true;
        }
    }
    
    /* Table name */
    tok = next_token(parser);
    if (tok->type != TOKEN_IDENTIFIER) {
        FREE(create);
        return NULL;
    }
    create->table_name = STRDUP(tok->lexeme);
    
    /* Columns */
    consume_token(parser, TOKEN_LPAREN);
    
    create->columns = NULL;
    create->num_columns = 0;
    
    while (true) {
        column_def_t *col = parse_column_def(parser);
        if (col != NULL) {
            create->columns = REALLOC(create->columns, sizeof(column_def_t) * (create->num_columns + 1));
            create->columns[create->num_columns++] = *col;
            FREE(col);
        }
        
        tok = peek_token(parser);
        if (tok->type == TOKEN_COMMA) {
            next_token(parser);
        } else {
            break;
        }
    }
    
    consume_token(parser, TOKEN_RPAREN);
    
    ast_node_t *node = ALLOC_ZERO(sizeof(ast_node_t));
    node->type = AST_CREATE_TABLE;
    node->stmt.create_table = create;
    
    return node;
}

/**
 * @brief Parse DROP TABLE statement
 */
static ast_node_t *parse_drop_table(parser_t *parser) {
    drop_table_stmt_t *drop = ALLOC_ZERO(sizeof(drop_table_stmt_t));
    
    /* DROP */
    next_token(parser);
    
    /* TABLE */
    if (!consume_token(parser, TOKEN_TABLE)) {
        FREE(drop);
        return NULL;
    }
    
    /* IF EXISTS */
    token_t *tok = peek_token(parser);
    if (tok->type == TOKEN_EXISTS) {
        next_token(parser);
        drop->if_exists = true;
    }
    
    /* Table name */
    tok = next_token(parser);
    if (tok->type != TOKEN_IDENTIFIER) {
        FREE(drop);
        return NULL;
    }
    drop->table_name = STRDUP(tok->lexeme);
    
    ast_node_t *node = ALLOC_ZERO(sizeof(ast_node_t));
    node->type = AST_DROP_TABLE;
    node->stmt.drop_table = drop;
    
    return node;
}

/**
 * @brief Parse statement
 */
ast_node_t *parser_parse_statement(parser_t *parser) {
    token_t *tok = peek_token(parser);
    
    switch (tok->type) {
        case TOKEN_SELECT:
            return parse_select(parser);
        case TOKEN_INSERT:
            /* TODO */
            break;
        case TOKEN_UPDATE:
            /* TODO */
            break;
        case TOKEN_DELETE:
            /* TODO */
            break;
        case TOKEN_CREATE:
            next_token(parser);  /* Consume CREATE */
            tok = peek_token(parser);
            if (tok->type == TOKEN_TABLE) {
                return parse_create_table(parser);
            }
            break;
        case TOKEN_DROP:
            next_token(parser);  /* Consume DROP */
            tok = peek_token(parser);
            if (tok->type == TOKEN_TABLE) {
                return parse_drop_table(parser);
            }
            break;
        default:
            break;
    }
    
    return NULL;
}

/**
 * @brief Parse SQL statement
 */
ast_node_t *parser_parse(parser_t *parser) {
    ast_node_t *node = parser_parse_statement(parser);
    
    /* Check for semicolon */
    token_t *tok = peek_token(parser);
    if (tok->type == TOKEN_SEMICOLON) {
        next_token(parser);
    }
    
    return node;
}

/**
 * @brief Get parser error message
 */
const char *parser_get_error(parser_t *parser) {
    return parser->error_message;
}

/**
 * @brief Free expression
 */
void ast_free_expression(ast_expression_t *expr) {
    if (expr == NULL) {
        return;
    }
    
    FREE(expr->column_name);
    
    if (expr->type == EXPR_CONSTANT && expr->value.type == TYPE_TEXT) {
        FREE(expr->value.value.string_val);
    }
    
    if (expr->left) {
        ast_free_expression(expr->left);
    }
    if (expr->right) {
        ast_free_expression(expr->right);
    }
    if (expr->expr) {
        ast_free_expression(expr->expr);
    }
    
    if (expr->func_args) {
        for (int i = 0; i < expr->num_args; i++) {
            ast_free_expression(expr->func_args[i]);
        }
        FREE(expr->func_args);
    }
    
    FREE(expr);
}

/**
 * @brief Free AST node
 */
void ast_free(ast_node_t *node) {
    if (node == NULL) {
        return;
    }
    
    switch (node->type) {
        case AST_SELECT:
            /* TODO: Free select statement */
            break;
        case AST_CREATE_TABLE:
            /* TODO: Free create table statement */
            break;
        case AST_DROP_TABLE:
            /* TODO: Free drop table statement */
            break;
        default:
            break;
    }
    
    FREE(node);
}

/**
 * @brief Print expression
 */
void ast_print_expression(ast_expression_t *expr, int indent) {
    if (expr == NULL) {
        return;
    }
    
    for (int i = 0; i < indent; i++) printf("  ");
    
    switch (expr->type) {
        case EXPR_COLUMN:
            printf("COLUMN: %s\n", expr->column_name);
            break;
        case EXPR_CONSTANT:
            printf("CONSTANT: ");
            switch (expr->value.type) {
                case TYPE_INT64:
                    printf("%ld\n", expr->value.int64_val);
                    break;
                case TYPE_FLOAT64:
                    printf("%f\n", expr->value.float64_val);
                    break;
                case TYPE_TEXT:
                    printf("'%s'\n", expr->value.value.string_val);
                    break;
                case TYPE_NULL:
                    printf("NULL\n");
                    break;
                default:
                    printf("?\n");
            }
            break;
        case EXPR_BINARY:
            printf("BINARY OP: %s\n", token_type_to_string(expr->op));
            ast_print_expression(expr->left, indent + 1);
            ast_print_expression(expr->right, indent + 1);
            break;
        case EXPR_UNARY:
            printf("UNARY OP: %s\n", token_type_to_string(expr->op));
            ast_print_expression(expr->expr, indent + 1);
            break;
        case EXPR_FUNCTION:
            printf("FUNCTION: %s\n", expr->func_name);
            for (int i = 0; i < expr->num_args; i++) {
                ast_print_expression(expr->func_args[i], indent + 1);
            }
            break;
        default:
            printf("EXPR TYPE: %d\n", expr->type);
    }
}

/**
 * @brief Print AST
 */
void ast_print(ast_node_t *node, int indent) {
    if (node == NULL) {
        return;
    }
    
    for (int i = 0; i < indent; i++) printf("  ");
    
    switch (node->type) {
        case AST_SELECT:
            printf("SELECT\n");
            for (int j = 0; j < node->stmt.select->num_columns; j++) {
                ast_print_expression(node->stmt.select->columns[j], indent + 1);
            }
            if (node->stmt.select->where_clause) {
                for (int j = 0; j < indent + 1; j++) printf("  ");
                printf("WHERE:\n");
                ast_print_expression(node->stmt.select->where_clause, indent + 2);
            }
            break;
        case AST_CREATE_TABLE:
            printf("CREATE TABLE: %s\n", node->stmt.create_table->table_name);
            for (int j = 0; j < node->stmt.create_table->num_columns; j++) {
                for (int k = 0; k < indent + 1; k++) printf("  ");
                printf("COLUMN: %s\n", node->stmt.create_table->columns[j].name);
            }
            break;
        case AST_DROP_TABLE:
            printf("DROP TABLE: %s\n", node->stmt.drop_table->table_name);
            break;
        default:
            printf("NODE TYPE: %d\n", node->type);
    }
}

/** @} */ /* ast */
