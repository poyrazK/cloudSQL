/**
 * @file statement_tests.cpp
 * @brief Unit tests for SQL Statement serialization
 */

#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "common/value.hpp"
#include "parser/expression.hpp"
#include "parser/statement.hpp"

using namespace cloudsql;
using namespace cloudsql::parser;
using namespace cloudsql::common;

// Simple test framework
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) void test_##name()
#define RUN_TEST(name)                                        \
    do {                                                      \
        std::cout << "  " << #name << "... ";                 \
        try {                                                 \
            test_##name();                                    \
            std::cout << "PASSED" << std::endl;               \
            tests_passed++;                                   \
        } catch (const std::exception& e) {                   \
            std::cout << "FAILED: " << e.what() << std::endl; \
            tests_failed++;                                   \
        }                                                     \
    } while (0)

#define EXPECT_STREQ(a, b)                                                          \
    do {                                                                            \
        std::string _a = (a);                                                       \
        std::string _b = (b);                                                       \
        if (_a != _b) {                                                             \
            throw std::runtime_error("Expected '" + _b + "' but got '" + _a + "'"); \
        }                                                                           \
    } while (0)

TEST(SelectStatement_Complex) {
    auto stmt = std::make_unique<SelectStatement>();
    stmt->set_distinct(true);
    stmt->add_column(std::make_unique<ColumnExpr>("id"));
    stmt->add_column(std::make_unique<ColumnExpr>("name"));

    stmt->add_from(std::make_unique<ColumnExpr>("users"));

    // JOIN orders ON users.id = orders.user_id
    auto join_cond =
        std::make_unique<BinaryExpr>(std::make_unique<ColumnExpr>("users", "id"), TokenType::Eq,
                                     std::make_unique<ColumnExpr>("orders", "user_id"));
    stmt->add_join(SelectStatement::JoinType::Inner, std::make_unique<ColumnExpr>("orders"),
                   std::move(join_cond));

    // LEFT JOIN metadata (no condition for test simplicity, though invalid SQL usually)
    stmt->add_join(SelectStatement::JoinType::Left, std::make_unique<ColumnExpr>("metadata"),
                   nullptr);

    // WHERE age > 18
    stmt->set_where(
        std::make_unique<BinaryExpr>(std::make_unique<ColumnExpr>("age"), TokenType::Gt,
                                     std::make_unique<ConstantExpr>(Value::make_int64(18))));

    // GROUP BY age
    stmt->add_group_by(std::make_unique<ColumnExpr>("age"));

    // HAVING COUNT(*) > 5
    auto count_func = std::make_unique<FunctionExpr>("COUNT");
    stmt->set_having(
        std::make_unique<BinaryExpr>(std::move(count_func), TokenType::Gt,
                                     std::make_unique<ConstantExpr>(Value::make_int64(5))));

    // ORDER BY name DESC (using simplified check since we don't have DESC enum exposed in
    // expression easily here)
    stmt->add_order_by(std::make_unique<ColumnExpr>("name"));

    stmt->set_limit(10);
    stmt->set_offset(5);

    std::string sql = stmt->to_string();
    // Check key parts presence (exact string match might be brittle due to internal spacing)
    // "SELECT DISTINCT id, name FROM users JOIN orders ON users.id = orders.user_id LEFT JOIN
    // metadata WHERE age > 18 GROUP BY age HAVING COUNT(*) > 5 ORDER BY name LIMIT 10 OFFSET 5"

    EXPECT_STREQ(
        sql,
        "SELECT DISTINCT id, name FROM users JOIN orders ON users.id = orders.user_id LEFT JOIN "
        "metadata WHERE age > 18 GROUP BY age HAVING COUNT(*) > 5 ORDER BY name LIMIT 10 OFFSET 5");
}

TEST(InsertStatement_MultiRow) {
    auto stmt = std::make_unique<InsertStatement>();
    stmt->set_table(std::make_unique<ColumnExpr>("users"));
    stmt->add_column(std::make_unique<ColumnExpr>("id"));
    stmt->add_column(std::make_unique<ColumnExpr>("val"));

    std::vector<std::unique_ptr<Expression>> row1;
    row1.push_back(std::make_unique<ConstantExpr>(Value::make_int64(1)));
    row1.push_back(std::make_unique<ConstantExpr>(Value::make_text("A")));
    stmt->add_row(std::move(row1));

    std::vector<std::unique_ptr<Expression>> row2;
    row2.push_back(std::make_unique<ConstantExpr>(Value::make_int64(2)));
    row2.push_back(std::make_unique<ConstantExpr>(Value::make_text("B")));
    stmt->add_row(std::move(row2));

    EXPECT_STREQ(stmt->to_string(), "INSERT INTO users (id, val) VALUES (1, 'A'), (2, 'B')");
}

TEST(UpdateStatement_Basic) {
    auto stmt = std::make_unique<UpdateStatement>();
    stmt->set_table(std::make_unique<ColumnExpr>("products"));

    stmt->add_set(std::make_unique<ColumnExpr>("price"),
                  std::make_unique<ConstantExpr>(Value::make_int64(100)));
    stmt->add_set(std::make_unique<ColumnExpr>("stock"),
                  std::make_unique<ConstantExpr>(Value::make_int64(50)));

    stmt->set_where(
        std::make_unique<BinaryExpr>(std::make_unique<ColumnExpr>("id"), TokenType::Eq,
                                     std::make_unique<ConstantExpr>(Value::make_int64(1))));

    // Map iteration order is unspecified, so check substrings
    std::string sql = stmt->to_string();
    bool has_price = sql.find("price = 100") != std::string::npos;
    bool has_stock = sql.find("stock = 50") != std::string::npos;

    if (!has_price || !has_stock) {
        throw std::runtime_error("Update string missing set clauses: " + sql);
    }
}

TEST(DeleteStatement_Basic) {
    auto stmt = std::make_unique<DeleteStatement>();
    stmt->set_table(std::make_unique<ColumnExpr>("users"));
    stmt->set_where(
        std::make_unique<BinaryExpr>(std::make_unique<ColumnExpr>("id"), TokenType::Lt,
                                     std::make_unique<ConstantExpr>(Value::make_int64(0))));

    EXPECT_STREQ(stmt->to_string(), "DELETE FROM users WHERE id < 0");
}

TEST(CreateTableStatement_Complex) {
    auto stmt = std::make_unique<CreateTableStatement>();
    stmt->set_table_name("complex_table");

    stmt->add_column("id", "INT");
    stmt->get_last_column().is_primary_key_ = true;

    stmt->add_column("name", "TEXT");
    stmt->get_last_column().is_not_null_ = true;
    stmt->get_last_column().is_unique_ = true;

    EXPECT_STREQ(stmt->to_string(),
                 "CREATE TABLE complex_table (id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE)");
}

int main() {
    std::cout << "Statement Serialization Tests" << std::endl;
    std::cout << "=============================" << std::endl;

    RUN_TEST(SelectStatement_Complex);
    RUN_TEST(InsertStatement_MultiRow);
    RUN_TEST(UpdateStatement_Basic);
    RUN_TEST(DeleteStatement_Basic);
    RUN_TEST(CreateTableStatement_Complex);

    std::cout << std::endl
              << "Results: " << tests_passed << " passed, " << tests_failed << " failed"
              << std::endl;
    return (tests_failed > 0);
}
