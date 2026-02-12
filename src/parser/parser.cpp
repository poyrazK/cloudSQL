/**
 * @file parser.cpp
 * @brief SQL Parser implementation
 *
 * @defgroup parser SQL Parser
 * @{
 */

#include "parser/parser.hpp"
#include <iostream>

namespace cloudsql {
namespace parser {

/**
 * @brief Construct a new Parser
 */
Parser::Parser(std::unique_ptr<Lexer> lexer) 
    : lexer_(std::move(lexer)) {}

/**
 * @brief Parse a single SQL statement
 */
std::unique_ptr<Statement> Parser::parse_statement() {
    Token tok = peek_token();
    
    switch (tok.type()) {
        case TokenType::Select:
            return parse_select();
        case TokenType::Create:
            next_token(); // consume CREATE
            if (peek_token().type() == TokenType::Table) {
                return parse_create_table();
            }
            break;
        case TokenType::Insert:
            return parse_insert();
        default:
            break;
    }
    return nullptr;
}

/**
 * @brief Parse SELECT statement
 */
std::unique_ptr<Statement> Parser::parse_select() {
    auto stmt = std::make_unique<SelectStatement>();
    consume(TokenType::Select);
    
    /* DISTINCT */
    if (peek_token().type() == TokenType::Distinct) {
        consume(TokenType::Distinct);
        stmt->set_distinct(true);
    }
    
    /* Columns */
    bool first = true;
    while (first || consume(TokenType::Comma)) {
        first = false;
        stmt->add_column(parse_expression());
    }
    
    /* FROM */
    if (consume(TokenType::From)) {
        stmt->add_from(parse_expression());
    }
    
    /* WHERE */
    if (consume(TokenType::Where)) {
        stmt->set_where(parse_expression());
    }
    
    /* GROUP BY */
    if (consume(TokenType::Group) && consume(TokenType::By)) {
        bool g_first = true;
        while (g_first || consume(TokenType::Comma)) {
            g_first = false;
            stmt->add_group_by(parse_expression());
        }
    }
    
    /* HAVING */
    if (consume(TokenType::Having)) {
        stmt->set_having(parse_expression());
    }
    
    /* ORDER BY */
    if (consume(TokenType::Order) && consume(TokenType::By)) {
        bool o_first = true;
        while (o_first || consume(TokenType::Comma)) {
            o_first = false;
            stmt->add_order_by(parse_expression());
        }
    }
    
    /* LIMIT */
    if (consume(TokenType::Limit)) {
        Token val = next_token();
        if (val.type() == TokenType::Number) {
            stmt->set_limit(val.as_int64());
        }
    }
    
    /* OFFSET */
    if (consume(TokenType::Offset)) {
        Token val = next_token();
        if (val.type() == TokenType::Number) {
            stmt->set_offset(val.as_int64());
        }
    }
    
    return stmt;
}

/**
 * @brief Parse CREATE TABLE statement
 */
std::unique_ptr<Statement> Parser::parse_create_table() {
    auto stmt = std::make_unique<CreateTableStatement>();
    consume(TokenType::Table);
    
    /* IF NOT EXISTS */
    if (peek_token().type() == TokenType::Not) {
        consume(TokenType::Not);
        consume(TokenType::Exists);
    }
    
    /* Table name */
    Token name = next_token();
    if (name.type() != TokenType::Identifier) {
        return nullptr;
    }
    stmt->set_table_name(name.lexeme());
    
    consume(TokenType::LParen);
    
    /* Columns */
    bool first = true;
    while (first || consume(TokenType::Comma)) {
        first = false;
        Token col_name = next_token();
        Token col_type = next_token();
        
        if (col_name.type() != TokenType::Identifier) break;
        
        std::string type_str = col_type.lexeme();
        if (col_type.type() == TokenType::Varchar) {
             if (consume(TokenType::LParen)) {
                 Token len = next_token();
                 consume(TokenType::RParen);
                 type_str += "(" + len.lexeme() + ")";
             }
        }
        
        stmt->add_column(col_name.lexeme(), type_str);
        
        /* Constraints */
        while (true) {
            if (peek_token().type() == TokenType::Primary) {
                consume(TokenType::Primary);
                consume(TokenType::Key);
            } else if (peek_token().type() == TokenType::Not) {
                consume(TokenType::Not);
                consume(TokenType::Null);
            } else {
                break;
            }
        }
        
        if (peek_token().type() == TokenType::RParen) break;
    }
    
    consume(TokenType::RParen);
    return stmt;
}

/**
 * @brief Parse INSERT statement
 */
std::unique_ptr<Statement> Parser::parse_insert() {
    auto stmt = std::make_unique<InsertStatement>();
    consume(TokenType::Insert);
    consume(TokenType::Into);
    
    /* Table name */
    stmt->set_table(parse_primary());
    
    /* Optional columns: (id, name) */
    if (consume(TokenType::LParen)) {
        bool first = true;
        while (first || consume(TokenType::Comma)) {
            first = false;
            stmt->add_column(parse_primary());
        }
        consume(TokenType::RParen);
    }
    
    consume(TokenType::Values);
    
    /* Rows: (1, 'Alice'), (2, 'Bob') */
    bool first_row = true;
    while (first_row || consume(TokenType::Comma)) {
        first_row = false;
        consume(TokenType::LParen);
        
        std::vector<std::unique_ptr<Expression>> row;
        bool first_val = true;
        while (first_val || consume(TokenType::Comma)) {
            first_val = false;
            row.push_back(parse_expression());
        }
        stmt->add_row(std::move(row));
        consume(TokenType::RParen);
    }
    
    return stmt;
}

/**
 * @brief Parse expression (Precedence Climbing)
 */
std::unique_ptr<Expression> Parser::parse_expression() {
    return parse_or();
}

/**
 * @brief Parse OR expressions
 */
std::unique_ptr<Expression> Parser::parse_or() {
    auto left = parse_and();
    while (consume(TokenType::Or)) {
        auto right = parse_and();
        left = std::make_unique<BinaryExpr>(std::move(left), TokenType::Or, std::move(right));
    }
    return left;
}

/**
 * @brief Parse AND expressions
 */
std::unique_ptr<Expression> Parser::parse_and() {
    auto left = parse_not();
    while (consume(TokenType::And)) {
        auto right = parse_not();
        left = std::make_unique<BinaryExpr>(std::move(left), TokenType::And, std::move(right));
    }
    return left;
}

/**
 * @brief Parse NOT expressions
 */
std::unique_ptr<Expression> Parser::parse_not() {
    if (consume(TokenType::Not)) {
        return std::make_unique<UnaryExpr>(TokenType::Not, parse_not());
    }
    return parse_compare();
}

/**
 * @brief Parse comparison expressions
 */
std::unique_ptr<Expression> Parser::parse_compare() {
    auto left = parse_add_sub();
    
    Token tok = peek_token();
    if (tok.type() == TokenType::Eq || tok.type() == TokenType::Ne ||
        tok.type() == TokenType::Lt || tok.type() == TokenType::Le ||
        tok.type() == TokenType::Gt || tok.type() == TokenType::Ge) {
        next_token();
        auto right = parse_add_sub();
        return std::make_unique<BinaryExpr>(std::move(left), tok.type(), std::move(right));
    }
    
    return left;
}

/**
 * @brief Parse additive expressions
 */
std::unique_ptr<Expression> Parser::parse_add_sub() {
    auto left = parse_mul_div();
    while (peek_token().type() == TokenType::Plus || peek_token().type() == TokenType::Minus) {
        Token op = next_token();
        auto right = parse_mul_div();
        left = std::make_unique<BinaryExpr>(std::move(left), op.type(), std::move(right));
    }
    return left;
}

/**
 * @brief Parse multiplicative expressions
 */
std::unique_ptr<Expression> Parser::parse_mul_div() {
    auto left = parse_unary();
    while (peek_token().type() == TokenType::Star || peek_token().type() == TokenType::Slash) {
        Token op = next_token();
        auto right = parse_unary();
        left = std::make_unique<BinaryExpr>(std::move(left), op.type(), std::move(right));
    }
    return left;
}

/**
 * @brief Parse unary expressions
 */
std::unique_ptr<Expression> Parser::parse_unary() {
    if (peek_token().type() == TokenType::Minus || peek_token().type() == TokenType::Plus) {
        Token op = next_token();
        return std::make_unique<UnaryExpr>(op.type(), parse_unary());
    }
    return parse_primary();
}

/**
 * @brief Parse primary expressions (literals, identifiers, subqueries)
 */
std::unique_ptr<Expression> Parser::parse_primary() {
    Token tok = peek_token();
    
    /* Numbers */
    if (tok.type() == TokenType::Number) {
        next_token();
        if (tok.lexeme().find('.') != std::string::npos) {
            return std::make_unique<ConstantExpr>(common::Value::make_float64(tok.as_double()));
        } else {
            return std::make_unique<ConstantExpr>(common::Value::make_int64(tok.as_int64()));
        }
    } 
    /* Strings */
    else if (tok.type() == TokenType::String) {
        next_token();
        return std::make_unique<ConstantExpr>(common::Value::make_text(tok.as_string()));
    } 
    /* Identifiers (Columns) */
    else if (tok.type() == TokenType::Identifier) {
        next_token();
        return std::make_unique<ColumnExpr>(tok.lexeme());
    } 
    /* Parenthesized Expressions */
    else if (consume(TokenType::LParen)) {
        auto expr = parse_expression();
        consume(TokenType::RParen);
        return expr;
    }
    
    return nullptr;
}

/**
 * @brief Get next token from lexer
 */
Token Parser::next_token() {
    if (has_current_) {
        has_current_ = false;
        return current_token_;
    }
    return lexer_->next_token();
}

/**
 * @brief Peek at current token without consuming it
 */
Token Parser::peek_token() {
    if (!has_current_) {
        current_token_ = lexer_->next_token();
        has_current_ = true;
    }
    return current_token_;
}

/**
 * @brief Consume token if it matches expected type
 */
bool Parser::consume(TokenType type) {
    if (peek_token().type() == type) {
        next_token();
        return true;
    }
    return false;
}

} // namespace parser
} // namespace cloudsql

/** @} */ /* parser */
