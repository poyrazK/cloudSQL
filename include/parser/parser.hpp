#ifndef CLOUDSQL_PARSER_PARSER_HPP
#define CLOUDSQL_PARSER_PARSER_HPP

#include <memory>

#include "parser/expression.hpp"
#include "parser/lexer.hpp"
#include "parser/statement.hpp"

namespace cloudsql::parser {

class Parser {
   public:
    explicit Parser(std::unique_ptr<Lexer> lexer);
    std::unique_ptr<Statement> parse_statement();

   private:
    std::unique_ptr<Lexer> lexer_;
    Token current_token_;
    bool has_current_ = false;

    Token next_token();
    Token peek_token();
    bool consume(TokenType type);

    std::unique_ptr<Statement> parse_select();
    std::unique_ptr<Statement> parse_create_table();
    std::unique_ptr<Statement> parse_insert();
    std::unique_ptr<Statement> parse_update();
    std::unique_ptr<Statement> parse_delete();
    std::unique_ptr<Statement> parse_drop();

    std::unique_ptr<Expression> parse_expression();
    std::unique_ptr<Expression> parse_or();
    std::unique_ptr<Expression> parse_and();
    std::unique_ptr<Expression> parse_not();
    std::unique_ptr<Expression> parse_compare();
    std::unique_ptr<Expression> parse_add_sub();
    std::unique_ptr<Expression> parse_mul_div();
    std::unique_ptr<Expression> parse_unary();
    std::unique_ptr<Expression> parse_primary();
};



}  // namespace cloudsql::parser

#endif  // CLOUDSQL_PARSER_PARSER_HPP
