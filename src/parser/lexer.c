/**
 * @file lexer.c
 * @brief SQL Lexer implementation
 *
 * @defgroup lexer Lexer Implementation
 * @{
 */

#include "../include/parser/lexer.h"
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>

/* Keyword lookup table */
static const struct {
    const char *keyword;
    token_type_t token;
    bool is_reserved;
} keywords[] = {
    {"SELECT", TOKEN_SELECT, true},
    {"FROM", TOKEN_FROM, true},
    {"WHERE", TOKEN_WHERE, true},
    {"INSERT", TOKEN_INSERT, true},
    {"INTO", TOKEN_INTO, true},
    {"VALUES", TOKEN_VALUES, true},
    {"UPDATE", TOKEN_UPDATE, true},
    {"SET", TOKEN_SET, true},
    {"DELETE", TOKEN_DELETE, true},
    {"CREATE", TOKEN_CREATE, true},
    {"TABLE", TOKEN_TABLE, true},
    {"DROP", TOKEN_DROP, true},
    {"INDEX", TOKEN_INDEX, true},
    {"ON", TOKEN_ON, true},
    {"AND", TOKEN_AND, true},
    {"OR", TOKEN_OR, true},
    {"NOT", TOKEN_NOT, true},
    {"IN", TOKEN_IN, true},
    {"LIKE", TOKEN_LIKE, true},
    {"IS", TOKEN_IS, true},
    {"NULL", TOKEN_NULL, true},
    {"PRIMARY", TOKEN_PRIMARY, true},
    {"KEY", TOKEN_KEY, true},
    {"FOREIGN", TOKEN_FOREIGN, true},
    {"REFERENCES", TOKEN_REFERENCES, true},
    {"JOIN", TOKEN_JOIN, true},
    {"LEFT", TOKEN_LEFT, true},
    {"RIGHT", TOKEN_RIGHT, true},
    {"INNER", TOKEN_INNER, true},
    {"OUTER", TOKEN_OUTER, true},
    {"ORDER", TOKEN_ORDER, true},
    {"BY", TOKEN_BY, true},
    {"ASC", TOKEN_ASC, true},
    {"DESC", TOKEN_DESC, true},
    {"GROUP", TOKEN_GROUP, true},
    {"HAVING", TOKEN_HAVING, true},
    {"LIMIT", TOKEN_LIMIT, true},
    {"OFFSET", TOKEN_OFFSET, true},
    {"AS", TOKEN_AS, true},
    {"DISTINCT", TOKEN_DISTINCT, true},
    {"COUNT", TOKEN_COUNT, false},
    {"SUM", TOKEN_SUM, false},
    {"AVG", TOKEN_AVG, false},
    {"MIN", TOKEN_MIN, false},
    {"MAX", TOKEN_MAX, false},
    {"BEGIN", TOKEN_BEGIN, true},
    {"COMMIT", TOKEN_COMMIT, true},
    {"ROLLBACK", TOKEN_ROLLBACK, true},
    {"TRUNCATE", TOKEN_TRUNCATE, true},
    {"ALTER", TOKEN_ALTER, true},
    {"ADD", TOKEN_ADD, true},
    {"COLUMN", TOKEN_COLUMN, true},
    {"TYPE", TOKEN_TYPE, true},
    {"CONSTRAINT", TOKEN_CONSTRAINT, true},
    {"UNIQUE", TOKEN_UNIQUE, true},
    {"CHECK", TOKEN_CHECK, true},
    {"DEFAULT", TOKEN_DEFAULT, true},
    {"EXISTS", TOKEN_EXISTS, true},
    {"VARCHAR", TOKEN_VARCHAR, true},
    {NULL, TOKEN_EOF, false}
};

/* Forward declarations */
static void lexer_skip_whitespace(lexer_t *lexer);
static int lexer_is_alpha(char c);
static int lexer_is_digit(char c);
static int lexer_is_alphanumeric(char c);
static token_t lexer_scan_identifier(lexer_t *lexer);
static token_t lexer_scan_string(lexer_t *lexer);
static token_t lexer_scan_number(lexer_t *lexer);
static token_t lexer_scan_operator(lexer_t *lexer);
static token_type_t lexer_lookup_keyword(const char *lexeme);
static token_t create_token(token_type_t type, lexer_t *lexer, size_t length);

/**
 * @brief Create a new lexer instance
 */
lexer_t *lexer_create(const char *input, size_t length) {
    if (input == NULL || length == 0) {
        return NULL;
    }
    
    lexer_t *lexer = (lexer_t *)malloc(sizeof(lexer_t));
    if (lexer == NULL) {
        return NULL;
    }
    
    lexer->input = input;
    lexer->input_length = length;
    lexer->position = 0;
    lexer->line = 1;
    lexer->column = 1;
    lexer->current_char = (length > 0) ? input[0] : '\0';
    
    return lexer;
}

/**
 * @brief Destroy a lexer instance
 */
void lexer_destroy(lexer_t *lexer) {
    if (lexer != NULL) {
        free(lexer);
    }
}

/**
 * @brief Advance to the next character
 */
static void lexer_advance(lexer_t *lexer) {
    if (lexer->current_char == '\n') {
        lexer->line++;
        lexer->column = 1;
    } else {
        lexer->column++;
    }
    
    lexer->position++;
    if (lexer->position < lexer->input_length) {
        lexer->current_char = lexer->input[lexer->position];
    } else {
        lexer->current_char = '\0';
    }
}

/**
 * @brief Skip whitespace characters
 */
static void lexer_skip_whitespace(lexer_t *lexer) {
    while (lexer->current_char != '\0' && 
           (lexer->current_char == ' ' || 
            lexer->current_char == '\n' || 
            lexer->current_char == '\r' || 
            lexer->current_char == '\t' ||
            lexer->current_char == '\v' ||
            lexer->current_char == '\f')) {
        lexer_advance(lexer);
    }
}

/**
 * @brief Check if character is alphabetic
 */
static int lexer_is_alpha(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
}

/**
 * @brief Check if character is a digit
 */
static int lexer_is_digit(char c) {
    return (c >= '0' && c <= '9');
}

/**
 * @brief Check if character is alphanumeric
 */
static int lexer_is_alphanumeric(char c) {
    return lexer_is_alpha(c) || lexer_is_digit(c);
}

/**
 * @brief Scan and create an identifier token
 */
static token_t lexer_scan_identifier(lexer_t *lexer) {
    size_t start = lexer->position;
    
    while (lexer->current_char != '\0' && lexer_is_alphanumeric(lexer->current_char)) {
        lexer_advance(lexer);
    }
    
    size_t length = lexer->position - start;
    char *lexeme = (char *)malloc(length + 1);
    if (lexeme == NULL) {
        return create_token(TOKEN_ERROR, lexer, 0);
    }
    
    memcpy(lexeme, &lexer->input[start], length);
    lexeme[length] = '\0';
    
    /* Convert to uppercase for keyword lookup */
    for (size_t i = 0; i < length; i++) {
        lexeme[i] = toupper((unsigned char)lexeme[i]);
    }
    
    token_type_t type = lexer_lookup_keyword(lexeme);
    
    token_t token;
    token.type = type;
    token.lexeme = lexeme;
    token.length = length;
    token.line = lexer->line;
    token.column = lexer->column - length;
    
    return token;
}

/**
 * @brief Scan and create a string literal token
 */
static token_t lexer_scan_string(lexer_t *lexer) {
    char quote_char = lexer->current_char;
    lexer_advance(lexer); /* Skip opening quote */
    
    size_t start = lexer->position;
    size_t length = 0;
    
    while (lexer->current_char != '\0' && lexer->current_char != quote_char) {
        /* Handle escape sequences */
        if (lexer->current_char == '\\' && lexer->position + 1 < lexer->input_length) {
            lexer_advance(lexer);
        }
        lexer_advance(lexer);
        length++;
    }
    
    if (lexer->current_char != quote_char) {
        /* Unterminated string */
        char *lexeme = (char *)malloc(length + 1);
        if (lexeme == NULL) {
            return create_token(TOKEN_ERROR, lexer, 0);
        }
        memcpy(lexeme, &lexer->input[start], length);
        lexeme[length] = '\0';
        
        token_t token;
        token.type = TOKEN_ERROR;
        token.lexeme = lexeme;
        token.length = length;
        token.line = lexer->line;
        token.column = lexer->column - length;
        return token;
    }
    
    lexer_advance(lexer); /* Skip closing quote */
    
    char *lexeme = (char *)malloc(length + 1);
    if (lexeme == NULL) {
        return create_token(TOKEN_ERROR, lexer, 0);
    }
    
    /* Copy and handle escape sequences */
    size_t j = 0;
    for (size_t i = 0; i < length; i++) {
        if (start + i < lexer->input_length) {
            char c = lexer->input[start + i];
            if (c == '\\' && i + 1 < length) {
                char next = lexer->input[start + i + 1];
                switch (next) {
                    case 'n': lexeme[j++] = '\n'; i++; break;
                    case 't': lexeme[j++] = '\t'; i++; break;
                    case 'r': lexeme[j++] = '\r'; i++; break;
                    case '\\': lexeme[j++] = '\\'; i++; break;
                    case '\'': lexeme[j++] = '\''; i++; break;
                    case '"': lexeme[j++] = '"'; i++; break;
                    default: lexeme[j++] = c; break;
                }
            } else {
                lexeme[j++] = c;
            }
        }
    }
    lexeme[j] = '\0';
    
    token_t token;
    token.type = TOKEN_STRING;
    token.lexeme = lexeme;
    token.length = j;
    token.value.str_val = lexeme;
    token.line = lexer->line;
    token.column = lexer->column - length - 2;
    
    return token;
}

/**
 * @brief Scan and create a number token
 */
static token_t lexer_scan_number(lexer_t *lexer) {
    size_t start = lexer->position;
    int has_decimal = 0;
    
    while (lexer->current_char != '\0' && 
           (lexer_is_digit(lexer->current_char) || 
            (lexer->current_char == '.' && !has_decimal))) {
        if (lexer->current_char == '.') {
            has_decimal = 1;
        }
        lexer_advance(lexer);
    }
    
    size_t length = lexer->position - start;
    char *lexeme = (char *)malloc(length + 1);
    if (lexeme == NULL) {
        return create_token(TOKEN_ERROR, lexer, 0);
    }
    
    memcpy(lexeme, &lexer->input[start], length);
    lexeme[length] = '\0';
    
    token_t token;
    token.lexeme = lexeme;
    token.length = length;
    token.line = lexer->line;
    token.column = lexer->column - length;
    
    if (has_decimal) {
        token.type = TOKEN_NUMBER;
        token.value.float_val = strtod(lexeme, NULL);
    } else {
        token.type = TOKEN_NUMBER;
        token.value.int_val = strtoll(lexeme, NULL, 10);
    }
    
    return token;
}

/**
 * @brief Scan and create an operator token
 */
static token_t lexer_scan_operator(lexer_t *lexer) {
    char op[3] = {lexer->current_char, '\0', '\0'};
    token_type_t type = TOKEN_ERROR;
    size_t length = 1;
    
    switch (lexer->current_char) {
        case '=':
            lexer_advance(lexer);
            type = TOKEN_EQ;
            break;
        case '<':
            lexer_advance(lexer);
            if (lexer->current_char == '=') {
                type = TOKEN_LE;
                op[1] = '=';
                length = 2;
            } else if (lexer->current_char == '>') {
                type = TOKEN_NE;
                op[1] = '>';
                length = 2;
            } else if (lexer->current_char == '<') {
                /* << operator - shift left */
                op[1] = '<';
                length = 2;
                type = TOKEN_ERROR; /* Not supported yet */
            } else {
                /* Just '<' */
                type = TOKEN_LT;
            }
            break;
        case '>':
            lexer_advance(lexer);
            if (lexer->current_char == '=') {
                type = TOKEN_GE;
                op[1] = '=';
                length = 2;
            } else if (lexer->current_char == '>') {
                /* >> operator - shift right */
                op[1] = '>';
                length = 2;
                type = TOKEN_ERROR; /* Not supported yet */
            } else {
                /* Just '>' */
                type = TOKEN_GT;
            }
            break;
        case '!':
            lexer_advance(lexer);
            if (lexer->current_char == '=') {
                type = TOKEN_NE;
                op[1] = '=';
                length = 2;
            } else {
                /* Just '!' */
                type = TOKEN_ERROR;
            }
            break;
        case '+':
            lexer_advance(lexer);
            type = TOKEN_PLUS;
            break;
        case '-':
            lexer_advance(lexer);
            type = TOKEN_MINUS;
            break;
        case '*':
            lexer_advance(lexer);
            type = TOKEN_STAR;
            break;
        case '/':
            lexer_advance(lexer);
            type = TOKEN_SLASH;
            break;
        case '%':
            lexer_advance(lexer);
            type = TOKEN_PERCENT;
            break;
        case '(':
            lexer_advance(lexer);
            type = TOKEN_LPAREN;
            break;
        case ')':
            lexer_advance(lexer);
            type = TOKEN_RPAREN;
            break;
        case ',':
            lexer_advance(lexer);
            type = TOKEN_COMMA;
            break;
        case ';':
            lexer_advance(lexer);
            type = TOKEN_SEMICOLON;
            break;
        case '.':
            lexer_advance(lexer);
            type = TOKEN_DOT;
            break;
        case ':':
            lexer_advance(lexer);
            type = TOKEN_COLON;
            break;
        case '|':
            lexer_advance(lexer);
            if (lexer->current_char == '|') {
                type = TOKEN_CONCAT;
                op[1] = '|';
                length = 2;
            }
            break;
        default:
            lexer_advance(lexer);
            return create_token(TOKEN_ERROR, lexer, 1);
    }
    
    return create_token(type, lexer, length);
}

/**
 * @brief Look up a keyword
 */
static token_type_t lexer_lookup_keyword(const char *lexeme) {
    for (int i = 0; keywords[i].keyword != NULL; i++) {
        if (strcmp(keywords[i].keyword, lexeme) == 0) {
            return keywords[i].token;
        }
    }
    return TOKEN_IDENTIFIER;
}

/**
 * @brief Create a token
 */
static token_t create_token(token_type_t type, lexer_t *lexer, size_t length) {
    token_t token;
    token.type = type;
    token.lexeme = NULL;
    token.length = 0;
    token.line = lexer->line;
    token.column = lexer->column;
    
    if (length > 0 && lexer->position - length >= 0) {
        size_t start = lexer->position - length;
        token.lexeme = (char *)malloc(length + 1);
        if (token.lexeme != NULL) {
            memcpy(token.lexeme, &lexer->input[start], length);
            token.lexeme[length] = '\0';
            token.length = length;
            token.column = lexer->column - length + 1;
        }
    }
    
    return token;
}

/**
 * @brief Get the next token
 */
token_t lexer_next_token(lexer_t *lexer) {
    if (lexer == NULL || lexer->input == NULL) {
        token_t error_token;
        error_token.type = TOKEN_ERROR;
        error_token.lexeme = NULL;
        error_token.length = 0;
        error_token.line = 1;
        error_token.column = 1;
        return error_token;
    }
    
    /* Skip whitespace and comments */
    while (lexer->current_char != '\0') {
        lexer_skip_whitespace(lexer);
        
        /* Skip single-line comments (--) */
        if (lexer->current_char == '-' && 
            lexer->position + 1 < lexer->input_length &&
            lexer->input[lexer->position + 1] == '-') {
            while (lexer->current_char != '\0' && lexer->current_char != '\n') {
                lexer_advance(lexer);
            }
            continue;
        }
        
        /* Skip multi-line comments (/* ... * /) */
        if (lexer->current_char == '/' && 
            lexer->position + 1 < lexer->input_length &&
            lexer->input[lexer->position + 1] == '*') {
            lexer_advance(lexer); /* Skip / */
            lexer_advance(lexer); /* Skip * */
            while (lexer->current_char != '\0') {
                if (lexer->current_char == '*' && 
                    lexer->position + 1 < lexer->input_length &&
                    lexer->input[lexer->position + 1] == '/') {
                    lexer_advance(lexer); /* Skip * */
                    lexer_advance(lexer); /* Skip / */
                    break;
                }
                lexer_advance(lexer);
            }
            continue;
        }
        
        break;
    }
    
    /* Check for end of input */
    if (lexer->current_char == '\0') {
        token_t token;
        token.type = TOKEN_EOF;
        token.lexeme = NULL;
        token.length = 0;
        token.line = lexer->line;
        token.column = lexer->column;
        return token;
    }
    
    /* Scan based on first character */
    if (lexer_is_alpha(lexer->current_char)) {
        return lexer_scan_identifier(lexer);
    } else if (lexer_is_digit(lexer->current_char)) {
        return lexer_scan_number(lexer);
    } else if (lexer->current_char == '\'' || lexer->current_char == '"') {
        return lexer_scan_string(lexer);
    } else if (lexer->current_char == '$' && 
               lexer->position + 1 < lexer->input_length &&
               lexer_is_digit(lexer->input[lexer->position + 1])) {
        /* Parameter placeholder like $1 */
        lexer_advance(lexer);
        size_t start = lexer->position;
        while (lexer->current_char != '\0' && lexer_is_digit(lexer->current_char)) {
            lexer_advance(lexer);
        }
        size_t length = lexer->position - start;
        char *lexeme = (char *)malloc(length + 2);
        if (lexeme != NULL) {
            lexeme[0] = '$';
            memcpy(&lexeme[1], &lexer->input[start], length);
            lexeme[length + 1] = '\0';
        }
        token_t token;
        token.type = TOKEN_PARAM;
        token.lexeme = lexeme;
        token.length = length + 1;
        token.value.int_val = strtoll(&lexer->input[start], NULL, 10);
        token.line = lexer->line;
        token.column = lexer->column - length;
        return token;
    } else {
        return lexer_scan_operator(lexer);
    }
}

/**
 * @brief Peek at the next token without consuming it
 */
token_t *lexer_peek_token(lexer_t *lexer) {
    if (lexer == NULL) {
        return NULL;
    }
    
    size_t saved_position = lexer->position;
    uint32_t saved_line = lexer->line;
    uint32_t saved_column = lexer->column;
    char saved_char = lexer->current_char;
    
    token_t *token = (token_t *)malloc(sizeof(token_t));
    if (token == NULL) {
        return NULL;
    }
    
    *token = lexer_next_token(lexer);
    
    /* Restore lexer state */
    lexer->position = saved_position;
    lexer->line = saved_line;
    lexer->column = saved_column;
    lexer->current_char = saved_char;
    
    return token;
}

/**
 * @brief Consume a specific token
 */
token_t lexer_consume(lexer_t *lexer, token_type_t expected_type) {
    token_t token = lexer_next_token(lexer);
    
    if (token.type != expected_type) {
        /* Return error token but keep lexeme for error reporting */
        token.type = TOKEN_ERROR;
    }
    
    return token;
}

/**
 * @brief Get the current token type (lookahead)
 */
token_type_t lexer_current_token(lexer_t *lexer) {
    token_t *token = lexer_peek_token(lexer);
    if (token == NULL) {
        return TOKEN_ERROR;
    }
    token_type_t type = token->type;
    free(token->lexeme);
    free(token);
    return type;
}

/**
 * @brief Check if current token matches type
 */
bool lexer_match(lexer_t *lexer, token_type_t type) {
    token_type_t current = lexer_current_token(lexer);
    return current == type;
}

/**
 * @brief Check if current token matches any of the given types
 */
bool lexer_match_any(lexer_t *lexer, token_type_t *types, size_t num_types) {
    token_type_t current = lexer_current_token(lexer);
    for (size_t i = 0; i < num_types; i++) {
        if (current == types[i]) {
            return true;
        }
    }
    return false;
}

/**
 * @brief Get token type as string
 */
const char *token_type_to_string(token_type_t type) {
    switch (type) {
        case TOKEN_EOF: return "EOF";
        case TOKEN_SELECT: return "SELECT";
        case TOKEN_FROM: return "FROM";
        case TOKEN_WHERE: return "WHERE";
        case TOKEN_INSERT: return "INSERT";
        case TOKEN_INTO: return "INTO";
        case TOKEN_VALUES: return "VALUES";
        case TOKEN_UPDATE: return "UPDATE";
        case TOKEN_SET: return "SET";
        case TOKEN_DELETE: return "DELETE";
        case TOKEN_CREATE: return "CREATE";
        case TOKEN_TABLE: return "TABLE";
        case TOKEN_DROP: return "DROP";
        case TOKEN_INDEX: return "INDEX";
        case TOKEN_ON: return "ON";
        case TOKEN_AND: return "AND";
        case TOKEN_OR: return "OR";
        case TOKEN_NOT: return "NOT";
        case TOKEN_IN: return "IN";
        case TOKEN_LIKE: return "LIKE";
        case TOKEN_IS: return "IS";
        case TOKEN_NULL: return "NULL";
        case TOKEN_PRIMARY: return "PRIMARY";
        case TOKEN_KEY: return "KEY";
        case TOKEN_FOREIGN: return "FOREIGN";
        case TOKEN_REFERENCES: return "REFERENCES";
        case TOKEN_JOIN: return "JOIN";
        case TOKEN_LEFT: return "LEFT";
        case TOKEN_RIGHT: return "RIGHT";
        case TOKEN_INNER: return "INNER";
        case TOKEN_OUTER: return "OUTER";
        case TOKEN_ORDER: return "ORDER";
        case TOKEN_BY: return "BY";
        case TOKEN_ASC: return "ASC";
        case TOKEN_DESC: return "DESC";
        case TOKEN_GROUP: return "GROUP";
        case TOKEN_HAVING: return "HAVING";
        case TOKEN_LIMIT: return "LIMIT";
        case TOKEN_OFFSET: return "OFFSET";
        case TOKEN_AS: return "AS";
        case TOKEN_DISTINCT: return "DISTINCT";
        case TOKEN_COUNT: return "COUNT";
        case TOKEN_SUM: return "SUM";
        case TOKEN_AVG: return "AVG";
        case TOKEN_MIN: return "MIN";
        case TOKEN_MAX: return "MAX";
        case TOKEN_BEGIN: return "BEGIN";
        case TOKEN_COMMIT: return "COMMIT";
        case TOKEN_ROLLBACK: return "ROLLBACK";
        case TOKEN_TRUNCATE: return "TRUNCATE";
        case TOKEN_ALTER: return "ALTER";
        case TOKEN_ADD: return "ADD";
        case TOKEN_COLUMN: return "COLUMN";
        case TOKEN_TYPE: return "TYPE";
        case TOKEN_CONSTRAINT: return "CONSTRAINT";
        case TOKEN_UNIQUE: return "UNIQUE";
        case TOKEN_CHECK: return "CHECK";
        case TOKEN_DEFAULT: return "DEFAULT";
        case TOKEN_EXISTS: return "EXISTS";
        case TOKEN_VARCHAR: return "VARCHAR";
        case TOKEN_IDENTIFIER: return "IDENTIFIER";
        case TOKEN_STRING: return "STRING";
        case TOKEN_NUMBER: return "NUMBER";
        case TOKEN_PARAM: return "PARAM";
        case TOKEN_EQ: return "=";
        case TOKEN_NE: return "<>";
        case TOKEN_LT: return "<";
        case TOKEN_LE: return "<=";
        case TOKEN_GT: return ">";
        case TOKEN_GE: return ">=";
        case TOKEN_PLUS: return "+";
        case TOKEN_MINUS: return "-";
        case TOKEN_STAR: return "*";
        case TOKEN_SLASH: return "/";
        case TOKEN_PERCENT: return "%";
        case TOKEN_CONCAT: return "||";
        case TOKEN_LPAREN: return "(";
        case TOKEN_RPAREN: return ")";
        case TOKEN_COMMA: return ",";
        case TOKEN_SEMICOLON: return ";";
        case TOKEN_DOT: return ".";
        case TOKEN_COLON: return ":";
        case TOKEN_ERROR: return "ERROR";
        default: return "UNKNOWN";
    }
}

/**
 * @brief Free a token
 */
void token_free(token_t *token) {
    if (token != NULL && token->lexeme != NULL) {
        free(token->lexeme);
        token->lexeme = NULL;
    }
}

/** @} */ /* lexer */
