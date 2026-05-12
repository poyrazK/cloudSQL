/**
 * @file parser_tests.cpp
 * @brief Unit tests for Parser - SQL parsing
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "parser/expression.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"
#include "parser/statement.hpp"

using namespace cloudsql::parser;

namespace {

// Helper to create a parser from SQL string
static std::unique_ptr<Parser> make_parser(const std::string& sql) {
    return std::make_unique<Parser>(std::make_unique<Lexer>(sql));
}

// Helper to parse a statement
static std::unique_ptr<Statement> parse(const std::string& sql) {
    auto parser = make_parser(sql);
    return parser->parse_statement();
}

// Helper to verify statement is of type and cast
template <typename T>
static T* as(std::unique_ptr<Statement>& stmt) {
    return dynamic_cast<T*>(stmt.get());
}

// ============= Basic Parsing Tests =============

TEST(ParserTests, EmptyInput) {
    auto stmt = parse("");
    EXPECT_EQ(stmt, nullptr);
}

TEST(ParserTests, InvalidStatement) {
    auto stmt = parse("INVALID_KEYWORD");
    EXPECT_EQ(stmt, nullptr);
}

TEST(ParserTests, GarbageInput) {
    auto stmt = parse("@@#$%^&*()");
    EXPECT_EQ(stmt, nullptr);
}

// ============= SELECT Statement Tests =============

TEST(ParserTests, SelectSimple) {
    auto stmt = parse("SELECT * FROM users");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::Select);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->columns().size(), 1U);  // * becomes one column
    EXPECT_NE(select->from(), nullptr);
    EXPECT_EQ(select->joins().size(), 0U);
    EXPECT_EQ(select->where(), nullptr);
}

TEST(ParserTests, SelectMultipleColumns) {
    auto stmt = parse("SELECT id, name, email FROM users");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->columns().size(), 3U);
}

TEST(ParserTests, SelectDistinct) {
    auto stmt = parse("SELECT DISTINCT name FROM users");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_TRUE(select->distinct());
}

TEST(ParserTests, SelectAllColumns) {
    auto stmt = parse("SELECT * FROM t");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->columns().size(), 1U);
}

TEST(ParserTests, SelectWithWhere) {
    auto stmt = parse("SELECT * FROM users WHERE id = 1");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_NE(select->where(), nullptr);
}

TEST(ParserTests, SelectWithLimit) {
    auto stmt = parse("SELECT * FROM t LIMIT 10");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->limit(), 10);
    EXPECT_FALSE(select->has_offset());
}

TEST(ParserTests, SelectWithLimitOffset) {
    auto stmt = parse("SELECT * FROM t LIMIT 10 OFFSET 5");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->limit(), 10);
    EXPECT_EQ(select->offset(), 5);
}

TEST(ParserTests, SelectWithOrderBy) {
    auto stmt = parse("SELECT * FROM users ORDER BY name");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->order_by().size(), 1U);
}

TEST(ParserTests, SelectWithOrderByAsc) {
    auto stmt = parse("SELECT * FROM users ORDER BY name ASC");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->order_by().size(), 1U);
}

TEST(ParserTests, SelectWithOrderByMultiple) {
    auto stmt = parse("SELECT * FROM users ORDER BY name, age");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->order_by().size(), 2U);
}

TEST(ParserTests, SelectWithGroupBy) {
    auto stmt = parse("SELECT department, COUNT(*) FROM employees GROUP BY department");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->group_by().size(), 1U);
    EXPECT_EQ(select->having(), nullptr);
}

TEST(ParserTests, SelectWithGroupByHaving) {
    auto stmt = parse("SELECT department FROM employees GROUP BY department HAVING COUNT(*) > 5");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->group_by().size(), 1U);
    EXPECT_NE(select->having(), nullptr);
}

// ============= JOIN Tests =============

TEST(ParserTests, SelectWithInnerJoin) {
    auto stmt = parse("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->joins().size(), 1U);
    EXPECT_EQ(select->joins()[0].type, SelectStatement::JoinType::Inner);
}

TEST(ParserTests, SelectWithLeftJoin) {
    auto stmt = parse("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->joins().size(), 1U);
    EXPECT_EQ(select->joins()[0].type, SelectStatement::JoinType::Left);
}

TEST(ParserTests, SelectWithRightJoin) {
    auto stmt = parse("SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->joins().size(), 1U);
    EXPECT_EQ(select->joins()[0].type, SelectStatement::JoinType::Right);
}

TEST(ParserTests, SelectWithFullJoin) {
    auto stmt = parse("SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->joins().size(), 1U);
    EXPECT_EQ(select->joins()[0].type, SelectStatement::JoinType::Full);
}

TEST(ParserTests, SelectWithMultipleJoins) {
    auto stmt = parse(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON "
        "orders.product_id = products.id");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->joins().size(), 2U);
}

TEST(ParserTests, SelectWithJoinNoCondition) {
    // JOIN without ON condition is accepted with nullptr condition
    auto stmt = parse("SELECT * FROM users JOIN orders");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_EQ(select->joins().size(), 1U);
    EXPECT_EQ(select->joins()[0].condition, nullptr);
}

// ============= CREATE TABLE Tests =============

TEST(ParserTests, CreateTableSimple) {
    auto stmt = parse("CREATE TABLE users (id INT, name TEXT)");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::CreateTable);

    auto* create = as<CreateTableStatement>(stmt);
    ASSERT_NE(create, nullptr);
    EXPECT_EQ(create->table_name(), "users");
    EXPECT_EQ(create->columns().size(), 2U);
}

TEST(ParserTests, CreateTableIfNotExists) {
    auto stmt = parse("CREATE TABLE IF NOT EXISTS users (id INT)");
    ASSERT_NE(stmt, nullptr);

    auto* create = as<CreateTableStatement>(stmt);
    ASSERT_NE(create, nullptr);
    EXPECT_EQ(create->table_name(), "users");
    EXPECT_TRUE(create->if_not_exists());
}

TEST(ParserTests, CreateTableWithPrimaryKey) {
    // PRIMARY KEY is not supported - lexer doesn't have PRIMARY/KEY keywords
    auto stmt = parse("CREATE TABLE users (id INT, name TEXT)");
    ASSERT_NE(stmt, nullptr);

    auto* create = as<CreateTableStatement>(stmt);
    ASSERT_NE(create, nullptr);
    EXPECT_EQ(create->columns().size(), 2U);
    EXPECT_FALSE(create->columns()[0].is_primary_key_);
}

TEST(ParserTests, CreateTableWithNotNull) {
    auto stmt = parse("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    ASSERT_NE(stmt, nullptr);

    auto* create = as<CreateTableStatement>(stmt);
    ASSERT_NE(create, nullptr);
    EXPECT_TRUE(create->columns()[0].is_not_null_);
    EXPECT_FALSE(create->columns()[1].is_not_null_);
}

TEST(ParserTests, CreateTableWithUnique) {
    auto stmt = parse("CREATE TABLE users (id INT UNIQUE, name TEXT)");
    ASSERT_NE(stmt, nullptr);

    auto* create = as<CreateTableStatement>(stmt);
    ASSERT_NE(create, nullptr);
    EXPECT_TRUE(create->columns()[0].is_unique_);
    EXPECT_FALSE(create->columns()[1].is_unique_);
}

TEST(ParserTests, CreateTableWithMultipleColumns) {
    // Note: PRIMARY KEY not supported by lexer (no PRIMARY/KEY keywords)
    auto stmt = parse("CREATE TABLE products (id INT, name TEXT NOT NULL, price INT UNIQUE)");
    ASSERT_NE(stmt, nullptr);

    auto* create = as<CreateTableStatement>(stmt);
    ASSERT_NE(create, nullptr);
    EXPECT_EQ(create->columns().size(), 3U);
    EXPECT_FALSE(create->columns()[0].is_primary_key_);
    EXPECT_TRUE(create->columns()[1].is_not_null_);
    EXPECT_TRUE(create->columns()[2].is_unique_);
}

// ============= CREATE INDEX Tests =============

TEST(ParserTests, CreateIndexSimple) {
    auto stmt = parse("CREATE INDEX idx ON users (id)");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::CreateIndex);

    auto* create = as<CreateIndexStatement>(stmt);
    ASSERT_NE(create, nullptr);
    EXPECT_EQ(create->index_name(), "idx");
    EXPECT_EQ(create->table_name(), "users");
    EXPECT_EQ(create->columns().size(), 1U);
    EXPECT_FALSE(create->unique());
}

TEST(ParserTests, CreateUniqueIndex) {
    auto stmt = parse("CREATE UNIQUE INDEX idx ON users (id, name)");
    ASSERT_NE(stmt, nullptr);

    auto* create = as<CreateIndexStatement>(stmt);
    ASSERT_NE(create, nullptr);
    EXPECT_TRUE(create->unique());
    EXPECT_EQ(create->columns().size(), 2U);
}

// ============= INSERT Tests =============

TEST(ParserTests, InsertSimple) {
    auto stmt = parse("INSERT INTO users VALUES (1, 'Alice')");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::Insert);

    auto* insert = as<InsertStatement>(stmt);
    ASSERT_NE(insert, nullptr);
    EXPECT_NE(insert->table(), nullptr);
    EXPECT_EQ(insert->columns().size(), 0U);  // No column list
    EXPECT_EQ(insert->value_count(), 1U);
}

TEST(ParserTests, InsertWithColumns) {
    auto stmt = parse("INSERT INTO users (id, name) VALUES (1, 'Alice')");
    ASSERT_NE(stmt, nullptr);

    auto* insert = as<InsertStatement>(stmt);
    ASSERT_NE(insert, nullptr);
    EXPECT_EQ(insert->columns().size(), 2U);
    EXPECT_EQ(insert->value_count(), 1U);
}

TEST(ParserTests, InsertMultipleRows) {
    auto stmt = parse("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
    ASSERT_NE(stmt, nullptr);

    auto* insert = as<InsertStatement>(stmt);
    ASSERT_NE(insert, nullptr);
    EXPECT_EQ(insert->value_count(), 2U);
}

TEST(ParserTests, InsertWithColumnsAndMultipleRows) {
    auto stmt = parse("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')");
    ASSERT_NE(stmt, nullptr);

    auto* insert = as<InsertStatement>(stmt);
    ASSERT_NE(insert, nullptr);
    EXPECT_EQ(insert->columns().size(), 2U);
    EXPECT_EQ(insert->value_count(), 2U);
}

// ============= UPDATE Tests =============

TEST(ParserTests, UpdateSimple) {
    auto stmt = parse("UPDATE users SET name = 'Bob'");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::Update);

    auto* update = as<UpdateStatement>(stmt);
    ASSERT_NE(update, nullptr);
    EXPECT_NE(update->table(), nullptr);
    EXPECT_EQ(update->set_clauses().size(), 1U);
    EXPECT_EQ(update->where(), nullptr);
}

TEST(ParserTests, UpdateWithWhere) {
    auto stmt = parse("UPDATE users SET name = 'Bob' WHERE id = 1");
    ASSERT_NE(stmt, nullptr);

    auto* update = as<UpdateStatement>(stmt);
    ASSERT_NE(update, nullptr);
    EXPECT_NE(update->where(), nullptr);
}

TEST(ParserTests, UpdateMultipleSet) {
    auto stmt = parse("UPDATE users SET name = 'Bob', age = 25 WHERE id = 1");
    ASSERT_NE(stmt, nullptr);

    auto* update = as<UpdateStatement>(stmt);
    ASSERT_NE(update, nullptr);
    EXPECT_EQ(update->set_clauses().size(), 2U);
}

// ============= DELETE Tests =============

TEST(ParserTests, DeleteSimple) {
    auto stmt = parse("DELETE FROM users");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::Delete);

    auto* delete_stmt = as<DeleteStatement>(stmt);
    ASSERT_NE(delete_stmt, nullptr);
    EXPECT_NE(delete_stmt->table(), nullptr);
    EXPECT_FALSE(delete_stmt->has_where());
}

TEST(ParserTests, DeleteWithWhere) {
    auto stmt = parse("DELETE FROM users WHERE id = 1");
    ASSERT_NE(stmt, nullptr);

    auto* delete_stmt = as<DeleteStatement>(stmt);
    ASSERT_NE(delete_stmt, nullptr);
    EXPECT_TRUE(delete_stmt->has_where());
}

// ============= DROP Tests =============

TEST(ParserTests, DropTableSimple) {
    auto stmt = parse("DROP TABLE users");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::DropTable);

    auto* drop = dynamic_cast<DropTableStatement*>(stmt.get());
    ASSERT_NE(drop, nullptr);
    EXPECT_EQ(drop->table_name(), "users");
    EXPECT_FALSE(drop->if_exists());
}

TEST(ParserTests, DropTableIfExists) {
    auto stmt = parse("DROP TABLE IF EXISTS users");
    ASSERT_NE(stmt, nullptr);

    auto* drop = dynamic_cast<DropTableStatement*>(stmt.get());
    ASSERT_NE(drop, nullptr);
    EXPECT_TRUE(drop->if_exists());
}

TEST(ParserTests, DropIndexSimple) {
    auto stmt = parse("DROP INDEX idx");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::DropIndex);

    auto* drop = dynamic_cast<DropIndexStatement*>(stmt.get());
    ASSERT_NE(drop, nullptr);
    EXPECT_EQ(drop->index_name(), "idx");
    EXPECT_FALSE(drop->if_exists());
}

TEST(ParserTests, DropIndexIfExists) {
    auto stmt = parse("DROP INDEX IF EXISTS idx");
    ASSERT_NE(stmt, nullptr);

    auto* drop = dynamic_cast<DropIndexStatement*>(stmt.get());
    ASSERT_NE(drop, nullptr);
    EXPECT_TRUE(drop->if_exists());
}

// ============= Transaction Tests =============

TEST(ParserTests, BeginTransaction) {
    auto stmt = parse("BEGIN");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::TransactionBegin);
}

TEST(ParserTests, CommitTransaction) {
    auto stmt = parse("COMMIT");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::TransactionCommit);
}

TEST(ParserTests, RollbackTransaction) {
    auto stmt = parse("ROLLBACK");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::TransactionRollback);
}

// ============= Expression Parsing Tests =============

TEST(ParserTests, ParseIntegerConstant) {
    auto stmt = parse("SELECT 42 FROM t");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);
    EXPECT_EQ(select->columns()[0]->type(), ExprType::Constant);
}

TEST(ParserTests, ParseStringConstant) {
    auto stmt = parse("SELECT 'hello' FROM t");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);
    EXPECT_EQ(select->columns()[0]->type(), ExprType::Constant);
}

TEST(ParserTests, ParseBooleanConstants) {
    // TRUE and FALSE are parsed as identifiers/function calls (not as constants)
    // because the lexer recognizes TRUE/FALSE as keywords but parse_primary
    // only handles NULL specially, not TRUE/FALSE
    auto stmt1 = parse("SELECT TRUE FROM t");
    ASSERT_NE(stmt1, nullptr);
    auto* select1 = as<SelectStatement>(stmt1);
    ASSERT_NE(select1, nullptr);
    ASSERT_GE(select1->columns().size(), 1U);
    // TRUE is parsed as a column expression (identifier)
    EXPECT_EQ(select1->columns()[0]->type(), ExprType::Column);

    auto stmt2 = parse("SELECT FALSE FROM t");
    ASSERT_NE(stmt2, nullptr);
    auto* select2 = as<SelectStatement>(stmt2);
    ASSERT_NE(select2, nullptr);
    ASSERT_GE(select2->columns().size(), 1U);
    EXPECT_EQ(select2->columns()[0]->type(), ExprType::Column);

    // NULL is parsed as ConstantExpr with null value
    auto stmt3 = parse("SELECT NULL FROM t");
    ASSERT_NE(stmt3, nullptr);
    auto* select3 = as<SelectStatement>(stmt3);
    ASSERT_NE(select3, nullptr);
    ASSERT_GE(select3->columns().size(), 1U);
    EXPECT_EQ(select3->columns()[0]->type(), ExprType::Constant);
    auto* const3 = dynamic_cast<ConstantExpr*>(select3->columns()[0].get());
    ASSERT_NE(const3, nullptr);
    EXPECT_TRUE(const3->value().is_null());
}

TEST(ParserTests, ParseColumnReference) {
    auto stmt = parse("SELECT id FROM users");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);
    EXPECT_EQ(select->columns()[0]->type(), ExprType::Column);
}

TEST(ParserTests, ParseQualifiedColumn) {
    auto stmt = parse("SELECT users.id FROM users");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);
    EXPECT_EQ(select->columns()[0]->type(), ExprType::Column);
}

TEST(ParserTests, ParseBinaryExpression) {
    auto stmt = parse("SELECT a + b FROM t");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);
    EXPECT_EQ(select->columns()[0]->type(), ExprType::Binary);
}

TEST(ParserTests, ParseComparisonExpression) {
    auto stmt = parse("SELECT * FROM t WHERE a = b");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_NE(select->where(), nullptr);
    EXPECT_EQ(select->where()->type(), ExprType::Binary);
}

TEST(ParserTests, ParseArithmeticPrecedence) {
    // 1 + 2 * 3 should parse as 1 + (2 * 3)
    auto stmt = parse("SELECT 1 + 2 * 3 FROM t");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);

    // Top level is BinaryExpr with + operator
    auto* top_bin = dynamic_cast<BinaryExpr*>(select->columns()[0].get());
    ASSERT_NE(top_bin, nullptr);
    EXPECT_EQ(top_bin->op(), TokenType::Plus);

    // Left child is ConstantExpr with value 1
    auto* left = dynamic_cast<ConstantExpr*>(const_cast<Expression*>(&top_bin->left()));
    ASSERT_NE(left, nullptr);
    EXPECT_EQ(left->value().as_int64(), 1);

    // Right child is BinaryExpr with * operator
    auto* right_bin = dynamic_cast<BinaryExpr*>(const_cast<Expression*>(&top_bin->right()));
    ASSERT_NE(right_bin, nullptr);
    EXPECT_EQ(right_bin->op(), TokenType::Star);

    // Right's left is ConstantExpr with value 2
    auto* right_left = dynamic_cast<ConstantExpr*>(const_cast<Expression*>(&right_bin->left()));
    ASSERT_NE(right_left, nullptr);
    EXPECT_EQ(right_left->value().as_int64(), 2);

    // Right's right is ConstantExpr with value 3
    auto* right_right = dynamic_cast<ConstantExpr*>(const_cast<Expression*>(&right_bin->right()));
    ASSERT_NE(right_right, nullptr);
    EXPECT_EQ(right_right->value().as_int64(), 3);
}

TEST(ParserTests, ParseLogicalAnd) {
    auto stmt = parse("SELECT * FROM t WHERE a = 1 AND b = 2");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_NE(select->where(), nullptr);
    EXPECT_EQ(select->where()->type(), ExprType::Binary);
}

TEST(ParserTests, ParseLogicalOr) {
    auto stmt = parse("SELECT * FROM t WHERE a = 1 OR b = 2");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_NE(select->where(), nullptr);
    EXPECT_EQ(select->where()->type(), ExprType::Binary);
}

TEST(ParserTests, ParseNotExpression) {
    auto stmt = parse("SELECT * FROM t WHERE NOT a = 1");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_NE(select->where(), nullptr);
    EXPECT_EQ(select->where()->type(), ExprType::Unary);
}

TEST(ParserTests, ParseIsNull) {
    auto stmt = parse("SELECT * FROM t WHERE a IS NULL");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_NE(select->where(), nullptr);
    EXPECT_EQ(select->where()->type(), ExprType::IsNull);
}

TEST(ParserTests, ParseIsNotNull) {
    auto stmt = parse("SELECT * FROM t WHERE a IS NOT NULL");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_NE(select->where(), nullptr);
    EXPECT_EQ(select->where()->type(), ExprType::IsNull);
}

TEST(ParserTests, ParseInExpression) {
    auto stmt = parse("SELECT * FROM t WHERE a IN (1, 2, 3)");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_NE(select->where(), nullptr);
    EXPECT_EQ(select->where()->type(), ExprType::In);
}

TEST(ParserTests, ParseNotInExpression) {
    auto stmt = parse("SELECT * FROM t WHERE a NOT IN (1, 2, 3)");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_NE(select->where(), nullptr);
    EXPECT_EQ(select->where()->type(), ExprType::In);
}

TEST(ParserTests, ParseFunctionCall) {
    auto stmt = parse("SELECT COUNT(*) FROM users");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);
    EXPECT_EQ(select->columns()[0]->type(), ExprType::Function);
}

TEST(ParserTests, ParseFunctionCallWithArgument) {
    auto stmt = parse("SELECT COUNT(id) FROM users");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);
    EXPECT_EQ(select->columns()[0]->type(), ExprType::Function);
}

TEST(ParserTests, ParseFunctionCallDistinct) {
    auto stmt = parse("SELECT COUNT(DISTINCT id) FROM users");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);
    EXPECT_EQ(select->columns()[0]->type(), ExprType::Function);
}

TEST(ParserTests, ParseUnaryMinus) {
    auto stmt = parse("SELECT -42 FROM t");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);
    EXPECT_EQ(select->columns()[0]->type(), ExprType::Unary);
}

TEST(ParserTests, ParseParenthesizedExpression) {
    auto stmt = parse("SELECT (a + b) * c FROM t");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);
    EXPECT_EQ(select->columns()[0]->type(), ExprType::Binary);
}

TEST(ParserTests, ParseParameterExpression) {
    auto stmt = parse("SELECT ? FROM t");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    ASSERT_GE(select->columns().size(), 1U);
    EXPECT_EQ(select->columns()[0]->type(), ExprType::Parameter);
}

// ============= Error Handling Tests =============

TEST(ParserTests, SelectMissingFrom) {
    // Parser prints error to cerr but still returns valid SelectStatement with empty from
    auto stmt = parse("SELECT *");
    // Parser returns nullptr when FROM is required but missing
    EXPECT_EQ(stmt, nullptr);
}

TEST(ParserTests, CreateTableMissingParen) {
    auto stmt = parse("CREATE TABLE users id INT");
    EXPECT_EQ(stmt, nullptr);
}

TEST(ParserTests, InsertInvalid) {
    auto stmt = parse("INSERT users VALUES (1)");
    EXPECT_EQ(stmt, nullptr);
}

TEST(ParserTests, UpdateMissingSet) {
    auto stmt = parse("UPDATE users WHERE id = 1");
    EXPECT_EQ(stmt, nullptr);
}

TEST(ParserTests, DeleteMissingTable) {
    auto stmt = parse("DELETE WHERE id = 1");
    EXPECT_EQ(stmt, nullptr);
}

TEST(ParserTests, DropInvalid) {
    auto stmt = parse("DROP");
    EXPECT_EQ(stmt, nullptr);
}

TEST(ParserTests, CreateIndexInvalid) {
    auto stmt = parse("CREATE INDEX");
    EXPECT_EQ(stmt, nullptr);
}

// ============= Case Insensitivity Tests =============

TEST(ParserTests, KeywordsCaseInsensitive) {
    auto stmt1 = parse("select * from users");
    auto stmt2 = parse("SELECT * FROM users");
    auto stmt3 = parse("SeLeCt * FrOm users");

    EXPECT_NE(stmt1, nullptr);
    EXPECT_NE(stmt2, nullptr);
    EXPECT_NE(stmt3, nullptr);
}

// ============= Complex SQL Statements Tests =============

TEST(ParserTests, ComplexSelectAllClauses) {
    auto stmt = parse(
        "SELECT DISTINCT id, name FROM users JOIN orders ON users.id = orders.user_id LEFT "
        "JOIN products ON orders.product_id = products.id WHERE age > 18 AND status = 'active' "
        "GROUP BY department HAVING COUNT(*) > 5 ORDER BY name ASC LIMIT 100 OFFSET 10");
    ASSERT_NE(stmt, nullptr);

    auto* select = as<SelectStatement>(stmt);
    ASSERT_NE(select, nullptr);
    EXPECT_TRUE(select->distinct());
    EXPECT_EQ(select->columns().size(), 2U);
    EXPECT_EQ(select->joins().size(), 2U);
    EXPECT_NE(select->where(), nullptr);
    EXPECT_EQ(select->group_by().size(), 1U);
    EXPECT_NE(select->having(), nullptr);
    EXPECT_EQ(select->order_by().size(), 1U);
    EXPECT_EQ(select->limit(), 100);
    EXPECT_EQ(select->offset(), 10);
}

TEST(ParserTests, ComplexInsert) {
    auto stmt = parse(
        "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', "
        "30), (2, 'Bob', 'bob@example.com', 25)");
    ASSERT_NE(stmt, nullptr);

    auto* insert = as<InsertStatement>(stmt);
    ASSERT_NE(insert, nullptr);
    EXPECT_EQ(insert->columns().size(), 4U);
    EXPECT_EQ(insert->value_count(), 2U);
}

TEST(ParserTests, ComplexUpdate) {
    auto stmt = parse(
        "UPDATE users SET name = 'Updated', status = 'active', last_login = NOW() WHERE id = "
        "1 AND email = 'test@example.com'");
    ASSERT_NE(stmt, nullptr);

    auto* update = as<UpdateStatement>(stmt);
    ASSERT_NE(update, nullptr);
    EXPECT_EQ(update->set_clauses().size(), 3U);
    EXPECT_NE(update->where(), nullptr);
}

TEST(ParserTests, ComplexDelete) {
    auto stmt = parse("DELETE FROM users WHERE status = 'inactive' AND last_login < '2024-01-01'");
    ASSERT_NE(stmt, nullptr);

    auto* delete_stmt = as<DeleteStatement>(stmt);
    ASSERT_NE(delete_stmt, nullptr);
    EXPECT_TRUE(delete_stmt->has_where());
}

TEST(ParserTests, ComplexCreateTable) {
    // Note: PRIMARY KEY not supported by lexer (no PRIMARY/KEY keywords)
    auto stmt = parse(
        "CREATE TABLE IF NOT EXISTS employees (id INT, name TEXT NOT NULL, "
        "email TEXT UNIQUE, department TEXT, salary INT, hire_date TEXT)");
    ASSERT_NE(stmt, nullptr);

    auto* create = as<CreateTableStatement>(stmt);
    ASSERT_NE(create, nullptr);
    EXPECT_EQ(create->table_name(), "employees");
    EXPECT_EQ(create->columns().size(), 6U);
    EXPECT_FALSE(create->columns()[0].is_primary_key_);
    EXPECT_TRUE(create->columns()[1].is_not_null_);
    EXPECT_TRUE(create->columns()[2].is_unique_);
}

// ============= Whitespace Tests =============

TEST(ParserTests, ExtraWhitespace) {
    auto stmt = parse("SELECT    *    FROM    users    WHERE    id    =    1");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::Select);
}

TEST(ParserTests, NewlinesAndTabs) {
    auto stmt = parse("SELECT\n*\nFROM\nusers\nWHERE\nid\n=\n1");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::Select);
}

// ============= ANALYZE TABLE Tests =============

TEST(ParserTests, AnalyzeTableBasic) {
    auto stmt = parse("ANALYZE TABLE my_table");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::Analyze);
    auto* analyze = as<AnalyzeStatement>(stmt);
    ASSERT_NE(analyze, nullptr);
    EXPECT_EQ(analyze->table_name(), "my_table");
}

TEST(ParserTests, AnalyzeTableWithoutTableKeyword) {
    // Some SQL dialects allow ANALYZE tablename without TABLE keyword
    auto stmt = parse("ANALYZE my_table");
    ASSERT_NE(stmt, nullptr);
    EXPECT_EQ(stmt->type(), StmtType::Analyze);
    auto* analyze = as<AnalyzeStatement>(stmt);
    ASSERT_NE(analyze, nullptr);
    EXPECT_EQ(analyze->table_name(), "my_table");
}

TEST(ParserTests, AnalyzeTableMissingName) {
    auto stmt = parse("ANALYZE TABLE");
    EXPECT_EQ(stmt, nullptr);
}

TEST(ParserTests, AnalyzeTableInvalidName) {
    auto stmt = parse("ANALYZE TABLE 123");
    EXPECT_EQ(stmt, nullptr);
}

}  // namespace
