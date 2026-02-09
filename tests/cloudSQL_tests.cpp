/**
 * @file cloudSQL_tests.cpp
 * @brief Simple test suite for cloudSQL C++ implementation
 */

#include <iostream>
#include <string>
#include <memory>
#include <vector>
#include <cassert>
#include "common/value.hpp"
#include "parser/token.hpp"
#include "parser/lexer.hpp"
#include "parser/expression.hpp"
#include "common/config.hpp"
#include "catalog/catalog.hpp"
#include "network/server.hpp"

using namespace cloudsql;
using namespace cloudsql::common;
using namespace cloudsql::parser;

// Simple test framework
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) void test_##name()
#define RUN_TEST(name) do { \
    std::cout << "  " << #name << "... "; \
    try { \
        test_##name(); \
        std::cout << "PASSED" << std::endl; \
        tests_passed++; \
    } catch (const std::exception& e) { \
        std::cout << "FAILED: " << e.what() << std::endl; \
        tests_failed++; \
    } \
} while(0)

#define EXPECT_EQ(a, b) do { \
    auto _a = (a); auto _b = (b); \
    if (_a != _b) { \
        throw std::runtime_error("Expected " + std::to_string(static_cast<int>(_b)) + " but got " + std::to_string(static_cast<int>(_a))); \
    } \
} while(0)

#define EXPECT_TRUE(a) do { \
    if (!(a)) { \
        throw std::runtime_error("Expected true but got false"); \
    } \
} while(0)

#define EXPECT_FALSE(a) do { \
    if (a) { \
        throw std::runtime_error("Expected false but got true"); \
    } \
} while(0)

#define EXPECT_STREQ(a, b) do { \
    std::string _a = (a); std::string _b = (b); \
    if (_a != _b) { \
        throw std::runtime_error("Expected '" + _b + "' but got '" + _a + "'"); \
    } \
} while(0)

#define EXPECT_GE(a, b) do { \
    auto _a = (a); auto _b = (b); \
    if (_a < _b) { \
        throw std::runtime_error("Expected " + std::to_string(_a) + " to be >= " + std::to_string(_b)); \
    } \
} while(0)

// ============= Value Tests =============

TEST(ValueTest_IntegerOperations) {
    auto val = Value::make_int64(42);
    EXPECT_EQ(val.type(), TYPE_INT64);
    EXPECT_EQ(val.to_int64(), 42);
    EXPECT_FALSE(val.is_null());
}

TEST(ValueTest_StringOperations) {
    auto val = Value::make_text("hello");
    EXPECT_EQ(val.type(), TYPE_TEXT);
    EXPECT_STREQ(val.as_text().c_str(), "hello");
    EXPECT_FALSE(val.is_null());
}

TEST(ValueTest_NullValue) {
    auto val = Value::make_null();
    EXPECT_EQ(val.type(), TYPE_NULL);
    EXPECT_TRUE(val.is_null());
}

TEST(ValueTest_FloatOperations) {
    auto val = Value::make_float64(3.14);
    EXPECT_EQ(val.type(), TYPE_FLOAT64);
    EXPECT_TRUE(val.to_float64() > 3.13 && val.to_float64() < 3.15);
}

TEST(ValueTest_CopyOperations) {
    auto val1 = Value::make_int64(100);
    auto val2 = val1;  // Copy constructor
    EXPECT_EQ(val2.to_int64(), 100);
    
    auto val3 = std::move(val1);  // Move constructor
    EXPECT_EQ(val3.to_int64(), 100);
}

// ============= Token Tests =============

TEST(TokenTest_BasicTokens) {
    Token tok1(TokenType::Select);
    EXPECT_EQ(tok1.type(), TokenType::Select);
    
    Token tok2(TokenType::Number, "123");
    EXPECT_EQ(tok2.type(), TokenType::Number);
    EXPECT_STREQ(tok2.lexeme().c_str(), "123");
}

TEST(TokenTest_IdentifierToken) {
    Token tok(TokenType::Identifier, "users");
    EXPECT_EQ(tok.type(), TokenType::Identifier);
    EXPECT_STREQ(tok.lexeme().c_str(), "users");
}

TEST(TokenTest_Equality) {
    Token tok1(TokenType::From, "FROM");
    Token tok2(TokenType::From, "FROM");
    Token tok3(TokenType::Where, "WHERE");
    
    EXPECT_TRUE(tok1.type() == tok2.type());
    EXPECT_FALSE(tok1.type() == tok3.type());
}

// ============= Lexer Tests =============

TEST(LexerTest_SelectKeyword) {
    Lexer lexer("SELECT * FROM users");
    std::vector<Token> tokens;
    while (!lexer.is_at_end()) {
        tokens.push_back(lexer.next_token());
    }
    
    EXPECT_EQ(tokens.size(), static_cast<size_t>(4));
    EXPECT_EQ(tokens[0].type(), TokenType::Select);
    EXPECT_EQ(tokens[1].type(), TokenType::Star);
    EXPECT_EQ(tokens[2].type(), TokenType::From);
    EXPECT_EQ(tokens[3].type(), TokenType::Identifier);
}

TEST(LexerTest_Numbers) {
    Lexer lexer("SELECT 123, 456 FROM users");
    std::vector<Token> tokens;
    while (!lexer.is_at_end()) {
        tokens.push_back(lexer.next_token());
    }
    
    EXPECT_EQ(tokens[1].type(), TokenType::Number);
    EXPECT_STREQ(tokens[1].lexeme().c_str(), "123");
    EXPECT_EQ(tokens[2].type(), TokenType::Comma);
    EXPECT_EQ(tokens[3].type(), TokenType::Number);
    EXPECT_STREQ(tokens[3].lexeme().c_str(), "456");
}

TEST(LexerTest_Strings) {
    Lexer lexer("SELECT 'hello world' FROM users");
    std::vector<Token> tokens;
    while (!lexer.is_at_end()) {
        tokens.push_back(lexer.next_token());
    }
    
    EXPECT_EQ(tokens[1].type(), TokenType::String);
}

TEST(LexerTest_Operators) {
    Lexer lexer("WHERE age = 25 AND status = 'active'");
    std::vector<Token> tokens;
    while (!lexer.is_at_end()) {
        tokens.push_back(lexer.next_token());
    }
    
    EXPECT_EQ(tokens[0].type(), TokenType::Where);
    EXPECT_EQ(tokens[1].type(), TokenType::Identifier);
    EXPECT_EQ(tokens[2].type(), TokenType::Eq);
    EXPECT_EQ(tokens[3].type(), TokenType::Number);
    EXPECT_EQ(tokens[4].type(), TokenType::And);
    EXPECT_EQ(tokens[5].type(), TokenType::Identifier);
    EXPECT_EQ(tokens[6].type(), TokenType::Eq);
    EXPECT_EQ(tokens[7].type(), TokenType::String);
}

TEST(LexerTest_ComplexQuery) {
    std::string query = 
        "SELECT id, name, age "
        "FROM users "
        "WHERE age > 18 AND status = 'active' "
        "ORDER BY name ASC "
        "LIMIT 10";
    
    Lexer lexer(query);
    std::vector<Token> tokens;
    while (!lexer.is_at_end()) {
        tokens.push_back(lexer.next_token());
    }
    
    // Basic validation - tokens should be generated
    EXPECT_GE(tokens.size(), static_cast<size_t>(10));
    EXPECT_EQ(tokens[0].type(), TokenType::Select);
    EXPECT_EQ(tokens[1].type(), TokenType::Identifier);  // id
    EXPECT_EQ(tokens[2].type(), TokenType::Comma);
    EXPECT_EQ(tokens[3].type(), TokenType::Identifier);  // name
    EXPECT_EQ(tokens[4].type(), TokenType::Comma);
    EXPECT_EQ(tokens[5].type(), TokenType::Identifier);  // age
}

// ============= Expression Tests =============

TEST(ExpressionTest_ConstantExpression) {
    auto expr = std::make_unique<ConstantExpr>(Value::make_int64(42));
    EXPECT_EQ(expr->type(), ExprType::Constant);
}

TEST(ExpressionTest_ColumnExpression) {
    auto expr = std::make_unique<ColumnExpr>("users", "name");
    EXPECT_EQ(expr->type(), ExprType::Column);
    EXPECT_STREQ(expr->table().c_str(), "users");
    EXPECT_STREQ(expr->name().c_str(), "name");
}

TEST(ExpressionTest_BinaryExpression) {
    auto left = std::make_unique<ColumnExpr>("", "age");
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(18));
    
    auto expr = std::make_unique<BinaryExpr>(
        std::move(left), 
        TokenType::Gt, 
        std::move(right)
    );
    
    EXPECT_EQ(expr->type(), ExprType::Binary);
}

TEST(ExpressionTest_UnaryExpression) {
    auto expr = std::make_unique<UnaryExpr>(
        TokenType::Not,
        std::make_unique<ColumnExpr>("", "active")
    );
    
    EXPECT_EQ(expr->type(), ExprType::Unary);
}

// ============= Config Tests =============

TEST(ConfigTest_DefaultValues) {
    config::Config config;
    EXPECT_EQ(config.port, config::Config::DEFAULT_PORT);
    EXPECT_STREQ(config.data_dir.c_str(), config::Config::DEFAULT_DATA_DIR);
    EXPECT_EQ(config.mode, config::RunMode::Embedded);
    EXPECT_EQ(config.max_connections, config::Config::DEFAULT_MAX_CONNECTIONS);
}

TEST(ConfigTest_Setters) {
    config::Config config;
    config.port = 8080;
    config.data_dir = "/var/data";
    config.mode = config::RunMode::Distributed;
    config.debug = true;
    
    EXPECT_EQ(config.port, static_cast<uint16_t>(8080));
    EXPECT_STREQ(config.data_dir.c_str(), "/var/data");
    EXPECT_EQ(config.mode, config::RunMode::Distributed);
    EXPECT_TRUE(config.debug);
}

TEST(ConfigTest_Validate) {
    config::Config config;
    EXPECT_TRUE(config.validate());
    
    config.port = 0;  // Invalid port
    EXPECT_FALSE(config.validate());
    
    config.port = 8080;
    config.data_dir = "";  // Empty data dir
    EXPECT_FALSE(config.validate());
}

// ============= Catalog Tests =============

TEST(CatalogTest_CreateTable) {
    auto catalog = cloudsql::Catalog::create();
    
    std::vector<cloudsql::ColumnInfo> columns;
    columns.push_back(cloudsql::ColumnInfo("id", cloudsql::ValueType::Int64, 1));
    columns.push_back(cloudsql::ColumnInfo("name", cloudsql::ValueType::Text, 2));
    
    auto table_id = catalog->create_table("users", std::move(columns));
    EXPECT_TRUE(table_id > 0);
    
    auto table = catalog->get_table(table_id);
    EXPECT_TRUE(table.has_value());
    EXPECT_STREQ((*table)->name.c_str(), "users");
    EXPECT_EQ((*table)->num_columns(), static_cast<uint16_t>(2));
}

TEST(CatalogTest_GetTableByName) {
    auto catalog = cloudsql::Catalog::create();
    
    std::vector<cloudsql::ColumnInfo> columns;
    columns.push_back(cloudsql::ColumnInfo("id", cloudsql::ValueType::Int64, 1));
    
    catalog->create_table("products", std::move(columns));
    
    auto table = catalog->get_table_by_name("products");
    EXPECT_TRUE(table.has_value());
    EXPECT_STREQ((*table)->name.c_str(), "products");
}

TEST(CatalogTest_DropTable) {
    auto catalog = cloudsql::Catalog::create();
    
    std::vector<cloudsql::ColumnInfo> columns;
    columns.push_back(cloudsql::ColumnInfo("id", cloudsql::ValueType::Int64, 1));
    
    auto table_id = catalog->create_table("temp", std::move(columns));
    EXPECT_TRUE(catalog->table_exists(table_id));
    
    catalog->drop_table(table_id);
    EXPECT_FALSE(catalog->table_exists(table_id));
}

TEST(CatalogTest_CreateIndex) {
    auto catalog = cloudsql::Catalog::create();
    
    std::vector<cloudsql::ColumnInfo> columns;
    columns.push_back(cloudsql::ColumnInfo("id", cloudsql::ValueType::Int64, 1));
    columns.push_back(cloudsql::ColumnInfo("email", cloudsql::ValueType::Text, 2));
    
    auto table_id = catalog->create_table("accounts", std::move(columns));
    
    auto index_id = catalog->create_index("idx_email", table_id, {2}, 
                                           cloudsql::IndexType::BTree, true);
    EXPECT_TRUE(index_id > 0);
    
    auto indexes = catalog->get_table_indexes(table_id);
    EXPECT_EQ(indexes.size(), static_cast<size_t>(1));
}

// ============= Server Tests =============

TEST(ServerTest_CreateServer) {
    auto server = cloudsql::network::Server::create(5432);
    EXPECT_TRUE(server != nullptr);
    EXPECT_EQ(server->get_port(), static_cast<uint16_t>(5432));
    EXPECT_FALSE(server->is_running());
    EXPECT_EQ(server->get_status(), cloudsql::network::ServerStatus::Stopped);
}

TEST(ServerTest_StatusString) {
    auto server = cloudsql::network::Server::create(5433);
    EXPECT_STREQ(server->get_status_string().c_str(), "Stopped");
}

int main() {
    std::cout << "cloudSQL C++ Test Suite" << std::endl;
    std::cout << "========================" << std::endl << std::endl;
    
    std::cout << "Value Tests:" << std::endl;
    RUN_TEST(ValueTest_IntegerOperations);
    RUN_TEST(ValueTest_StringOperations);
    RUN_TEST(ValueTest_NullValue);
    RUN_TEST(ValueTest_FloatOperations);
    RUN_TEST(ValueTest_CopyOperations);
    std::cout << std::endl;
    
    std::cout << "Token Tests:" << std::endl;
    RUN_TEST(TokenTest_BasicTokens);
    RUN_TEST(TokenTest_IdentifierToken);
    RUN_TEST(TokenTest_Equality);
    std::cout << std::endl;
    
    std::cout << "Lexer Tests:" << std::endl;
    RUN_TEST(LexerTest_SelectKeyword);
    RUN_TEST(LexerTest_Numbers);
    RUN_TEST(LexerTest_Strings);
    RUN_TEST(LexerTest_Operators);
    RUN_TEST(LexerTest_ComplexQuery);
    std::cout << std::endl;
    
    std::cout << "Expression Tests:" << std::endl;
    RUN_TEST(ExpressionTest_ConstantExpression);
    RUN_TEST(ExpressionTest_ColumnExpression);
    RUN_TEST(ExpressionTest_BinaryExpression);
    RUN_TEST(ExpressionTest_UnaryExpression);
    std::cout << std::endl;
    
    std::cout << "Config Tests:" << std::endl;
    RUN_TEST(ConfigTest_DefaultValues);
    RUN_TEST(ConfigTest_Setters);
    RUN_TEST(ConfigTest_Validate);
    std::cout << std::endl;
    
    std::cout << "Catalog Tests:" << std::endl;
    RUN_TEST(CatalogTest_CreateTable);
    RUN_TEST(CatalogTest_GetTableByName);
    RUN_TEST(CatalogTest_DropTable);
    RUN_TEST(CatalogTest_CreateIndex);
    std::cout << std::endl;
    
    std::cout << "Server Tests:" << std::endl;
    RUN_TEST(ServerTest_CreateServer);
    RUN_TEST(ServerTest_StatusString);
    std::cout << std::endl;
    
    std::cout << "========================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed" << std::endl;
    
    if (tests_failed > 0) {
        std::cout << std::endl << "SOME TESTS FAILED!" << std::endl;
        return 1;
    }
    
    std::cout << std::endl << "All tests passed!" << std::endl;
    return 0;
}
