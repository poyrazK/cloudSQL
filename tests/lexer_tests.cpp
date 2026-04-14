/**
 * @file lexer_tests.cpp
 * @brief Unit tests for Lexer - SQL tokenization
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "parser/lexer.hpp"
#include "parser/token.hpp"

using namespace cloudsql::parser;

namespace {

// Helper to create a simple lexer
static Lexer make_lexer(const std::string& input) {
    return Lexer(input);
}

// Helper to get all tokens from a lexer
static std::vector<Token> tokenize(const std::string& input) {
    Lexer lexer(input);
    std::vector<Token> tokens;
    while (true) {
        Token token = lexer.next_token();
        tokens.push_back(token);
        if (token.type() == TokenType::End) {
            break;
        }
        if (lexer.is_at_end()) {
            // Input exhausted but last token wasn't End - add End token
            tokens.push_back(lexer.next_token());
            break;
        }
    }
    return tokens;
}

// ============= Basic Tokenization Tests =============

TEST(LexerTests, EmptyInput) {
    Lexer lexer("");
    EXPECT_TRUE(lexer.is_at_end());
    EXPECT_EQ(lexer.next_token().type(), TokenType::End);
}

TEST(LexerTests, BasicSelect) {
    auto lexer = make_lexer("SELECT * FROM users");
    Token token = lexer.next_token();
    EXPECT_EQ(token.type(), TokenType::Select);
    EXPECT_EQ(token.lexeme(), "SELECT");

    token = lexer.next_token();
    EXPECT_EQ(token.type(), TokenType::Star);

    token = lexer.next_token();
    EXPECT_EQ(token.type(), TokenType::From);

    token = lexer.next_token();
    EXPECT_EQ(token.type(), TokenType::Identifier);
    EXPECT_EQ(token.lexeme(), "users");

    EXPECT_TRUE(lexer.is_at_end());
}

TEST(LexerTests, SelectAllColumns) {
    auto tokens = tokenize("SELECT * FROM t");
    ASSERT_EQ(tokens.size(), 5);  // SELECT, *, FROM, t, End
    EXPECT_EQ(tokens[0].type(), TokenType::Select);
    EXPECT_EQ(tokens[1].type(), TokenType::Star);
    EXPECT_EQ(tokens[2].type(), TokenType::From);
    EXPECT_EQ(tokens[3].type(), TokenType::Identifier);
    EXPECT_EQ(tokens[4].type(), TokenType::End);
}

// ============= Keyword Tests =============

TEST(LexerTests, KeywordsCaseInsensitive) {
    auto tokens1 = tokenize("select * from users");
    auto tokens2 = tokenize("SELECT * FROM users");
    auto tokens3 = tokenize("Select * From Users");

    EXPECT_EQ(tokens1[0].type(), TokenType::Select);
    EXPECT_EQ(tokens2[0].type(), TokenType::Select);
    EXPECT_EQ(tokens3[0].type(), TokenType::Select);
}

TEST(LexerTests, KeywordsVariety) {
    auto tokens = tokenize("SELECT DISTINCT id, name FROM users WHERE age > 18 ORDER BY name ASC");
    ASSERT_GE(tokens.size(), 15);

    EXPECT_EQ(tokens[0].type(), TokenType::Select);
    EXPECT_EQ(tokens[1].type(), TokenType::Distinct);
    EXPECT_EQ(tokens[2].type(), TokenType::Identifier);  // id
    EXPECT_EQ(tokens[3].type(), TokenType::Comma);
    EXPECT_EQ(tokens[4].type(), TokenType::Identifier);  // name
    EXPECT_EQ(tokens[5].type(), TokenType::From);
    EXPECT_EQ(tokens[6].type(), TokenType::Identifier);  // users
    EXPECT_EQ(tokens[7].type(), TokenType::Where);
    EXPECT_EQ(tokens[8].type(), TokenType::Identifier);  // age
    EXPECT_EQ(tokens[9].type(), TokenType::Gt);
    EXPECT_EQ(tokens[10].type(), TokenType::Number);  // 18
    EXPECT_EQ(tokens[11].type(), TokenType::Order);
    EXPECT_EQ(tokens[12].type(), TokenType::By);
    EXPECT_EQ(tokens[13].type(), TokenType::Identifier);  // name
    EXPECT_EQ(tokens[14].type(), TokenType::Asc);
}

// ============= Identifier Tests =============

TEST(LexerTests, SimpleIdentifier) {
    auto tokens = tokenize("my_table");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::Identifier);
    EXPECT_EQ(tokens[0].lexeme(), "my_table");
}

TEST(LexerTests, IdentifierWithUnderscore) {
    auto tokens = tokenize("user_123_table");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::Identifier);
    EXPECT_EQ(tokens[0].lexeme(), "user_123_table");
}

TEST(LexerTests, IdentifiersWithNumbers) {
    auto tokens = tokenize("table99");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::Identifier);
    EXPECT_EQ(tokens[0].lexeme(), "table99");
}

// ============= Number Tests =============

TEST(LexerTests, SimpleInteger) {
    auto tokens = tokenize("42");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::Number);
    EXPECT_EQ(tokens[0].as_int64(), 42);
}

TEST(LexerTests, FloatNumber) {
    auto tokens = tokenize("3.14");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::Number);
    EXPECT_DOUBLE_EQ(tokens[0].as_double(), 3.14);
}

TEST(LexerTests, LargeNumber) {
    auto tokens = tokenize("12345678901234");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::Number);
    EXPECT_EQ(tokens[0].as_int64(), 12345678901234);
}

TEST(LexerTests, NegativeNumber) {
    auto tokens = tokenize("-42");
    ASSERT_EQ(tokens.size(), 3);  // -, 42, End
    EXPECT_EQ(tokens[0].type(), TokenType::Minus);
    EXPECT_EQ(tokens[1].type(), TokenType::Number);
    EXPECT_EQ(tokens[1].as_int64(), 42);
}

// ============= String Tests =============

TEST(LexerTests, SimpleString) {
    auto tokens = tokenize("'hello world'");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::String);
    EXPECT_EQ(tokens[0].as_string(), "hello world");
}

TEST(LexerTests, EmptyString) {
    auto tokens = tokenize("''");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::String);
    EXPECT_EQ(tokens[0].as_string(), "");
}

TEST(LexerTests, StringWithEscapedQuote) {
    // Note: Lexer does NOT handle SQL-style '' escaping
    // 'it''s cool' is parsed as two strings: 'it' and 's cool'
    auto tokens = tokenize("'it''s cool'");
    // First token is the string 'it' (lexer stops at second ')
    ASSERT_EQ(tokens.size(), 3);  // 'it', 's cool', End
    EXPECT_EQ(tokens[0].type(), TokenType::String);
    EXPECT_EQ(tokens[0].as_string(), "it");
    EXPECT_EQ(tokens[1].type(), TokenType::String);
    EXPECT_EQ(tokens[1].as_string(), "s cool");
}

// ============= Operator Tests =============

TEST(LexerTests, EqualsOperator) {
    auto tokens = tokenize("=");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::Eq);
}

TEST(LexerTests, NotEqualsOperator) {
    auto tokens1 = tokenize("<>");
    auto tokens2 = tokenize("!=");
    ASSERT_EQ(tokens1.size(), 2);
    ASSERT_EQ(tokens2.size(), 2);
    EXPECT_EQ(tokens1[0].type(), TokenType::Ne);
    EXPECT_EQ(tokens2[0].type(), TokenType::Ne);
}

TEST(LexerTests, ComparisonOperators) {
    auto tokens = tokenize("< <= > >=");
    ASSERT_EQ(tokens.size(), 5);
    EXPECT_EQ(tokens[0].type(), TokenType::Lt);
    EXPECT_EQ(tokens[1].type(), TokenType::Le);
    EXPECT_EQ(tokens[2].type(), TokenType::Gt);
    EXPECT_EQ(tokens[3].type(), TokenType::Ge);
}

TEST(LexerTests, ArithmeticOperators) {
    auto tokens = tokenize("+ - * /");
    ASSERT_EQ(tokens.size(), 5);
    EXPECT_EQ(tokens[0].type(), TokenType::Plus);
    EXPECT_EQ(tokens[1].type(), TokenType::Minus);
    EXPECT_EQ(tokens[2].type(), TokenType::Star);
    EXPECT_EQ(tokens[3].type(), TokenType::Slash);
}

TEST(LexerTests, ComplexExpression) {
    auto tokens = tokenize("1 + 2 * 3 - 4 / 5");
    ASSERT_EQ(tokens.size(), 10);
    EXPECT_EQ(tokens[0].type(), TokenType::Number);
    EXPECT_EQ(tokens[1].type(), TokenType::Plus);
    EXPECT_EQ(tokens[2].type(), TokenType::Number);
    EXPECT_EQ(tokens[3].type(), TokenType::Star);
    EXPECT_EQ(tokens[4].type(), TokenType::Number);
}

// ============= Delimiter Tests =============

TEST(LexerTests, Parentheses) {
    auto tokens = tokenize("( )");
    ASSERT_EQ(tokens.size(), 3);
    EXPECT_EQ(tokens[0].type(), TokenType::LParen);
    EXPECT_EQ(tokens[1].type(), TokenType::RParen);
}

TEST(LexerTests, CommaAndSemicolon) {
    auto tokens = tokenize("a, b;");
    ASSERT_EQ(tokens.size(), 5);
    EXPECT_EQ(tokens[0].type(), TokenType::Identifier);
    EXPECT_EQ(tokens[1].type(), TokenType::Comma);
    EXPECT_EQ(tokens[2].type(), TokenType::Identifier);
    EXPECT_EQ(tokens[3].type(), TokenType::Semicolon);
}

TEST(LexerTests, DotForQualifiedNames) {
    auto tokens = tokenize("users.id");
    ASSERT_EQ(tokens.size(), 4);
    EXPECT_EQ(tokens[0].type(), TokenType::Identifier);  // users
    EXPECT_EQ(tokens[1].type(), TokenType::Dot);
    EXPECT_EQ(tokens[2].type(), TokenType::Identifier);  // id
}

// ============= Whitespace Tests =============

TEST(LexerTests, MultipleSpaces) {
    auto tokens = tokenize("SELECT    *    FROM    t");
    ASSERT_EQ(tokens.size(), 5);
    EXPECT_EQ(tokens[0].type(), TokenType::Select);
    EXPECT_EQ(tokens[1].type(), TokenType::Star);
    EXPECT_EQ(tokens[2].type(), TokenType::From);
    EXPECT_EQ(tokens[3].type(), TokenType::Identifier);
}

TEST(LexerTests, TabsAndNewlines) {
    auto tokens = tokenize("SELECT\n*\nFROM\nt");
    ASSERT_EQ(tokens.size(), 5);
    EXPECT_EQ(tokens[0].type(), TokenType::Select);
    EXPECT_EQ(tokens[2].type(), TokenType::From);
}

// ============= SQL Comment Tests =============

TEST(LexerTests, SingleLineComment) {
    auto tokens = tokenize("SELECT * -- this is a comment\nFROM t");
    ASSERT_EQ(tokens.size(), 5);
    EXPECT_EQ(tokens[0].type(), TokenType::Select);
    EXPECT_EQ(tokens[1].type(), TokenType::Star);
    EXPECT_EQ(tokens[2].type(), TokenType::From);  // Comment should be skipped
    EXPECT_EQ(tokens[3].type(), TokenType::Identifier);
}

TEST(LexerTests, CommentOnly) {
    auto tokens = tokenize("-- just a comment\nSELECT");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::Select);  // Comment skipped
}

// ============= Boolean Tests =============

TEST(LexerTests, TrueKeyword) {
    auto tokens = tokenize("TRUE");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::True);
    EXPECT_TRUE(tokens[0].as_bool());
}

TEST(LexerTests, FalseKeyword) {
    auto tokens = tokenize("FALSE");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::False);
    EXPECT_FALSE(tokens[0].as_bool());
}

TEST(LexerTests, NullKeyword) {
    auto tokens = tokenize("NULL");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::Null);
}

// ============= Position Tracking Tests =============

TEST(LexerTests, PositionAfterToken) {
    Lexer lexer("SELECT *");
    EXPECT_EQ(lexer.line(), 1);
    EXPECT_EQ(lexer.column(), 1);

    lexer.next_token();  // SELECT
    // Position moves past "SELECT"
    EXPECT_GE(lexer.column(), 7);  // "SELECT" is 6 chars
}

TEST(LexerTests, LineTracking) {
    Lexer lexer("SELECT\n*\nFROM");
    EXPECT_EQ(lexer.line(), 1);
    lexer.next_token();  // SELECT
    lexer.next_token();  // * (still on line 1)
    lexer.next_token();  // newline - should increment line
    EXPECT_GE(lexer.line(), 2);
    lexer.next_token();  // FROM
}

// ============= Peek Tests =============

TEST(LexerTests, PeekDoesNotConsume) {
    Lexer lexer("SELECT *");
    Token peeked = lexer.peek_token();
    EXPECT_EQ(peeked.type(), TokenType::Select);

    // Should still get SELECT on next_token
    Token actual = lexer.next_token();
    EXPECT_EQ(actual.type(), TokenType::Select);
}

TEST(LexerTests, MultiplePeekReturnsSame) {
    Lexer lexer("SELECT");
    Token peek1 = lexer.peek_token();
    Token peek2 = lexer.peek_token();
    EXPECT_EQ(peek1.type(), TokenType::Select);
    EXPECT_EQ(peek2.type(), TokenType::Select);
}

// ============= Error Handling Tests =============

TEST(LexerTests, UnterminatedString) {
    auto tokens = tokenize("'unclosed string");
    ASSERT_FALSE(tokens.empty());
    EXPECT_EQ(tokens[0].type(), TokenType::Error);
}

TEST(LexerTests, InvalidCharacter) {
    auto tokens = tokenize("@invalid");
    ASSERT_FALSE(tokens.empty());
    EXPECT_EQ(tokens[0].type(), TokenType::Error);
}

TEST(LexerTests, BangAlone) {
    auto tokens = tokenize("!");
    ASSERT_FALSE(tokens.empty());
    EXPECT_EQ(tokens[0].type(), TokenType::Error);
}

// ============= is_at_end Tests =============

TEST(LexerTests, IsAtEndEmpty) {
    Lexer lexer("");
    EXPECT_TRUE(lexer.is_at_end());
}

TEST(LexerTests, IsAtEndAfterConsuming) {
    Lexer lexer("A");
    EXPECT_FALSE(lexer.is_at_end());
    lexer.next_token();
    EXPECT_TRUE(lexer.is_at_end());
}

TEST(LexerTests, IsAtEndWithWhitespace) {
    // is_at_end() doesn't skip whitespace - use next_token() to consume it
    Lexer lexer("   ");
    EXPECT_FALSE(lexer.is_at_end());
    lexer.next_token();  // Returns End after consuming whitespace
    EXPECT_TRUE(lexer.is_at_end());
}

// ============= Data Types Tests =============

TEST(LexerTests, IntType) {
    auto tokens = tokenize("INT");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::TypeInt);
}

TEST(LexerTests, TextType) {
    auto tokens = tokenize("TEXT");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::TypeText);
}

TEST(LexerTests, BoolType) {
    auto tokens = tokenize("BOOL");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::TypeBool);
}

// ============= Complex SQL Statements Tests =============

TEST(LexerTests, CreateTable) {
    auto tokens = tokenize("CREATE TABLE users (id INT, name TEXT)");
    // Tokens: CREATE, TABLE, users, (, id, INT, ,, name, TEXT, ), End = 11
    ASSERT_EQ(tokens.size(), 11);
    EXPECT_EQ(tokens[0].type(), TokenType::Create);
    EXPECT_EQ(tokens[1].type(), TokenType::Table);
    EXPECT_EQ(tokens[2].type(), TokenType::Identifier);  // users
    EXPECT_EQ(tokens[3].type(), TokenType::LParen);
    EXPECT_EQ(tokens[4].type(), TokenType::Identifier);  // id
    EXPECT_EQ(tokens[5].type(), TokenType::TypeInt);
    EXPECT_EQ(tokens[6].type(), TokenType::Comma);
}

TEST(LexerTests, InsertValues) {
    auto tokens = tokenize("INSERT INTO users VALUES (1, 'Alice')");
    ASSERT_GE(tokens.size(), 10);
    EXPECT_EQ(tokens[0].type(), TokenType::Insert);
    EXPECT_EQ(tokens[1].type(), TokenType::Into);
    EXPECT_EQ(tokens[2].type(), TokenType::Identifier);  // users
    EXPECT_EQ(tokens[3].type(), TokenType::Values);
}

TEST(LexerTests, UpdateStatement) {
    auto tokens = tokenize("UPDATE users SET name = 'Bob' WHERE id = 1");
    // Tokens: UPDATE, users, SET, name, =, 'Bob', WHERE, id, =, 1, End = 11
    ASSERT_EQ(tokens.size(), 11);
    EXPECT_EQ(tokens[0].type(), TokenType::Update);
    EXPECT_EQ(tokens[1].type(), TokenType::Identifier);  // users
    EXPECT_EQ(tokens[2].type(), TokenType::Set);
}

TEST(LexerTests, DeleteStatement) {
    auto tokens = tokenize("DELETE FROM users WHERE id = 1");
    ASSERT_GE(tokens.size(), 8);
    EXPECT_EQ(tokens[0].type(), TokenType::Delete);
    EXPECT_EQ(tokens[1].type(), TokenType::From);
    EXPECT_EQ(tokens[2].type(), TokenType::Identifier);  // users
    EXPECT_EQ(tokens[3].type(), TokenType::Where);
}

TEST(LexerTests, JoinStatement) {
    auto tokens = tokenize("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    // Verify SELECT, JOIN, and ON are present
    bool has_select = false, has_join = false, has_on = false;
    for (const auto& t : tokens) {
        if (t.type() == TokenType::Select) has_select = true;
        if (t.type() == TokenType::Join) has_join = true;
        if (t.type() == TokenType::On) has_on = true;
    }
    EXPECT_TRUE(has_select);
    EXPECT_TRUE(has_join);
    EXPECT_TRUE(has_on);
}

TEST(LexerTests, GroupByHaving) {
    auto tokens =
        tokenize("SELECT department FROM employees GROUP BY department HAVING COUNT(*) > 5");
    // Verify GROUP BY and HAVING are present
    bool has_group = false, has_by = false, has_having = false;
    for (const auto& t : tokens) {
        if (t.type() == TokenType::Group) has_group = true;
        if (t.type() == TokenType::By) has_by = true;
        if (t.type() == TokenType::Having) has_having = true;
    }
    EXPECT_TRUE(has_group);
    EXPECT_TRUE(has_by);
    EXPECT_TRUE(has_having);
}

// ============= Limit Offset Tests =============

TEST(LexerTests, LimitOffset) {
    auto tokens = tokenize("SELECT * FROM t LIMIT 10 OFFSET 20");
    // Tokens: SELECT, *, FROM, t, LIMIT, 10, OFFSET, 20, End = 9
    ASSERT_EQ(tokens.size(), 9);
    EXPECT_EQ(tokens[0].type(), TokenType::Select);
    EXPECT_EQ(tokens[4].type(), TokenType::Limit);
    EXPECT_EQ(tokens[5].type(), TokenType::Number);  // 10
    EXPECT_EQ(tokens[6].type(), TokenType::Offset);
    EXPECT_EQ(tokens[7].type(), TokenType::Number);  // 20
}

// ============= And Or Not Tests =============

TEST(LexerTests, AndOperator) {
    auto tokens = tokenize("AND");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::And);
}

TEST(LexerTests, OrOperator) {
    auto tokens = tokenize("OR");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::Or);
}

TEST(LexerTests, NotOperator) {
    auto tokens = tokenize("NOT");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].type(), TokenType::Not);
}

TEST(LexerTests, ComplexWhere) {
    auto tokens = tokenize("WHERE age > 18 AND name = 'John' OR status = FALSE");
    // Verify key operators are present
    bool has_where = false, has_and = false, has_or = false, has_gt = false, has_eq = false;
    for (const auto& t : tokens) {
        if (t.type() == TokenType::Where) has_where = true;
        if (t.type() == TokenType::And) has_and = true;
        if (t.type() == TokenType::Or) has_or = true;
        if (t.type() == TokenType::Gt) has_gt = true;
        if (t.type() == TokenType::Eq) has_eq = true;
    }
    EXPECT_TRUE(has_where);
    EXPECT_TRUE(has_and);
    EXPECT_TRUE(has_or);
    EXPECT_TRUE(has_gt);
    EXPECT_TRUE(has_eq);
}

}  // namespace
