/**
 * @file test_lexer.c
 * @brief Test the SQL lexer
 */

#include <stdio.h>
#include <string.h>
#include "common/common.h"
#include "parser/lexer.h"

int main() {
    const char *sql1 = "SELECT id, name FROM users WHERE email = 'test@example.com'";
    const char *sql2 = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))";
    const char *sql3 = "SELECT COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 10";
    
    printf("=== Testing SQL Lexer ===\n\n");
    
    /* Test 1: SELECT with WHERE */
    printf("Test 1: %s\n\n", sql1);
    lexer_t *lexer = lexer_create(sql1, strlen(sql1));
    if (lexer) {
        token_t token;
        do {
            token = lexer_next_token(lexer);
            printf("  %s: '%s'\n", 
                   token_type_to_string(token.type),
                   token.lexeme ? token.lexeme : "(null)");
            FREE(token.lexeme);
        } while (token.type != TOKEN_EOF);
        lexer_destroy(lexer);
    }
    printf("\n");
    
    /* Test 2: CREATE TABLE */
    printf("Test 2: %s\n\n", sql2);
    lexer = lexer_create(sql2, strlen(sql2));
    if (lexer) {
        token_t token;
        do {
            token = lexer_next_token(lexer);
            printf("  %s: '%s'\n", 
                   token_type_to_string(token.type),
                   token.lexeme ? token.lexeme : "(null)");
            FREE(token.lexeme);
        } while (token.type != TOKEN_EOF);
        lexer_destroy(lexer);
    }
    printf("\n");
    
    /* Test 3: Aggregates */
    printf("Test 3: %s\n\n", sql3);
    lexer = lexer_create(sql3, strlen(sql3));
    if (lexer) {
        token_t token;
        do {
            token = lexer_next_token(lexer);
            printf("  %s: '%s'\n", 
                   token_type_to_string(token.type),
                   token.lexeme ? token.lexeme : "(null)");
            FREE(token.lexeme);
        } while (token.type != TOKEN_EOF);
        lexer_destroy(lexer);
    }
    printf("\n");
    
    printf("=== All tests completed! ===\n");
    return 0;
}
