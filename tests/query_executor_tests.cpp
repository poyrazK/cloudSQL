/**
 * @file query_executor_tests.cpp
 * @brief Unit tests for QueryExecutor - direct method testing
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "executor/query_executor.hpp"
#include "executor/types.hpp"
#include "parser/expression.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"
#include "parser/statement.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::common;
using namespace cloudsql::parser;
using namespace cloudsql::executor;
using namespace cloudsql::storage;
using namespace cloudsql::transaction;

namespace {

// Helper to create a test environment
struct TestEnvironment {
    StorageManager disk_manager;
    BufferPoolManager bpm;
    std::shared_ptr<Catalog> catalog;
    LockManager lock_manager;
    TransactionManager txn_manager;
    QueryExecutor executor;

    TestEnvironment()
        : disk_manager("./test_data"),
          bpm(config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager),
          catalog(Catalog::create()),
          lock_manager(),
          txn_manager(lock_manager, *catalog, bpm, bpm.get_log_manager()),
          executor(*catalog, bpm, lock_manager, txn_manager) {
        disk_manager.create_dir_if_not_exists();
    }

    ~TestEnvironment() {
        // Cleanup test tables
        std::remove("./test_data/test_table.heap");
        std::remove("./test_data/table_a.heap");
        std::remove("./test_data/table_b.heap");
    }
};

// Helper to execute SQL and get result
QueryResult execute_sql(QueryExecutor& exec, const char* sql) {
    auto lexer = std::make_unique<Lexer>(sql);
    auto stmt = Parser(std::move(lexer)).parse_statement();
    if (!stmt) {
        QueryResult res;
        res.set_error("Parse error: invalid SQL");
        return res;
    }
    return exec.execute(*stmt);
}

class QueryExecutorTests : public ::testing::Test {
   protected:
    void SetUp() override {}
    void TearDown() override {}
};

// ============= Constructor and Setup Tests =============

TEST_F(QueryExecutorTests, ConstructorBasic) {
    TestEnvironment env;
    EXPECT_NE(&env.executor, nullptr);
}

TEST_F(QueryExecutorTests, SetContextId) {
    TestEnvironment env;
    env.executor.set_context_id("test_context");
    SUCCEED();  // No exception thrown
}

TEST_F(QueryExecutorTests, SetLocalOnlyMode) {
    TestEnvironment env;
    env.executor.set_local_only(true);
    SUCCEED();  // No exception thrown
}

// ============= CREATE TABLE Tests =============

TEST_F(QueryExecutorTests, CreateTableBasic) {
    TestEnvironment env;
    const auto res = execute_sql(env.executor, "CREATE TABLE test_table (id INT, name TEXT)");
    EXPECT_TRUE(res.success());
    EXPECT_TRUE(env.catalog->table_exists_by_name("test_table"));
}

TEST_F(QueryExecutorTests, CreateTableWithVariousTypes) {
    TestEnvironment env;
    const auto res = execute_sql(
        env.executor,
        "CREATE TABLE test_table (id INT, bigid BIGINT, val DOUBLE, flag BOOL, data TEXT)");
    EXPECT_TRUE(res.success());
    EXPECT_TRUE(env.catalog->table_exists_by_name("test_table"));
}

TEST_F(QueryExecutorTests, CreateTableDuplicate) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT)");
    const auto res = execute_sql(env.executor, "CREATE TABLE test_table (id INT)");
    EXPECT_FALSE(res.success());
}

TEST_F(QueryExecutorTests, DropTableBasic) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT)");
    const auto res = execute_sql(env.executor, "DROP TABLE test_table");
    EXPECT_TRUE(res.success());
    EXPECT_FALSE(env.catalog->table_exists_by_name("test_table"));
}

TEST_F(QueryExecutorTests, DropTableIfExists) {
    TestEnvironment env;
    // Should succeed even if table doesn't exist
    const auto res = execute_sql(env.executor, "DROP TABLE IF EXISTS nonexistent_table");
    EXPECT_TRUE(res.success());
}

TEST_F(QueryExecutorTests, DropNonExistentTable) {
    TestEnvironment env;
    const auto res = execute_sql(env.executor, "DROP TABLE nonexistent_table");
    EXPECT_FALSE(res.success());
}

// ============= INSERT Tests =============

TEST_F(QueryExecutorTests, InsertSingleRow) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    const auto res = execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 100)");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.rows_affected(), 1U);
}

TEST_F(QueryExecutorTests, InsertMultipleRows) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    const auto res =
        execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 30)");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.rows_affected(), 3U);
}

TEST_F(QueryExecutorTests, InsertWithColumnList) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT, name TEXT)");
    const auto res = execute_sql(env.executor, "INSERT INTO test_table (id, val) VALUES (1, 100)");
    EXPECT_TRUE(res.success());
}

TEST_F(QueryExecutorTests, InsertIntoNonExistentTable) {
    TestEnvironment env;
    const auto res = execute_sql(env.executor, "INSERT INTO nonexistent VALUES (1)");
    EXPECT_FALSE(res.success());
}

// ============= SELECT Tests =============

TEST_F(QueryExecutorTests, SelectStarFromEmptyTable) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT)");
    const auto res = execute_sql(env.executor, "SELECT * FROM test_table");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 0U);
}

TEST_F(QueryExecutorTests, SelectAllRows) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 30)");
    const auto res = execute_sql(env.executor, "SELECT * FROM test_table");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 3U);
}

TEST_F(QueryExecutorTests, SelectSpecificColumns) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT, extra TEXT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 10, 'A'), (2, 20, 'B')");
    const auto res = execute_sql(env.executor, "SELECT id, val FROM test_table");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 2U);
    EXPECT_EQ(res.rows()[0].size(), 2U);  // Only 2 columns projected
}

TEST_F(QueryExecutorTests, SelectWithWhereCondition) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 30)");
    const auto res = execute_sql(env.executor, "SELECT * FROM test_table WHERE val > 15");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 2U);  // id=2 and id=3
}

TEST_F(QueryExecutorTests, SelectWithOrderBy) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (3, 30), (1, 10), (2, 20)");
    const auto res = execute_sql(env.executor, "SELECT val FROM test_table ORDER BY val");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 3U);
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "10");
    EXPECT_STREQ(res.rows()[1].get(0).to_string().c_str(), "20");
    EXPECT_STREQ(res.rows()[2].get(0).to_string().c_str(), "30");
}

TEST_F(QueryExecutorTests, SelectWithLimit) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1), (2), (3), (4), (5)");
    const auto res = execute_sql(env.executor, "SELECT * FROM test_table LIMIT 3");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 3U);
}

TEST_F(QueryExecutorTests, SelectWithLimitAndOffset) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1), (2), (3), (4), (5)");
    const auto res = execute_sql(env.executor, "SELECT * FROM test_table LIMIT 2 OFFSET 2");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 2U);
}

TEST_F(QueryExecutorTests, SelectWithAggregateCount) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 30)");
    const auto res = execute_sql(env.executor, "SELECT COUNT(val) FROM test_table");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 1U);
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "3");
}

TEST_F(QueryExecutorTests, SelectWithAggregateSum) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 30)");
    const auto res = execute_sql(env.executor, "SELECT SUM(val) FROM test_table");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 1U);
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "60");
}

TEST_F(QueryExecutorTests, SelectWithGroupBy) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (cat TEXT, val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES ('A', 10), ('A', 20), ('B', 5)");
    const auto res = execute_sql(env.executor, "SELECT cat, SUM(val) FROM test_table GROUP BY cat");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 2U);
    // Verify actual aggregated values (A=30, B=5)
    bool found_a = false, found_b = false;
    for (size_t i = 0; i < res.row_count(); ++i) {
        std::string cat_val = res.rows()[i].get(0).to_string();
        int sum_val = std::stoi(res.rows()[i].get(1).to_string());
        if (cat_val == "A") {
            EXPECT_EQ(sum_val, 30);
            found_a = true;
        } else if (cat_val == "B") {
            EXPECT_EQ(sum_val, 5);
            found_b = true;
        }
    }
    EXPECT_TRUE(found_a);
    EXPECT_TRUE(found_b);
}

TEST_F(QueryExecutorTests, SelectWithGroupByCount) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (cat TEXT, val INT)");
    execute_sql(env.executor,
               "INSERT INTO test_table VALUES ('A', 10), ('A', 20), ('B', 5)");
    const auto res = execute_sql(
        env.executor, "SELECT cat, COUNT(val) FROM test_table GROUP BY cat");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 2U);
    // Verify counts (A=2, B=1)
    bool found_a = false, found_b = false;
    for (size_t i = 0; i < res.row_count(); ++i) {
        std::string cat_val = res.rows()[i].get(0).to_string();
        int cnt_val = std::stoi(res.rows()[i].get(1).to_string());
        if (cat_val == "A") {
            EXPECT_EQ(cnt_val, 2);
            found_a = true;
        } else if (cat_val == "B") {
            EXPECT_EQ(cnt_val, 1);
            found_b = true;
        }
    }
    EXPECT_TRUE(found_a);
    EXPECT_TRUE(found_b);
}

TEST_F(QueryExecutorTests, SelectWithGroupByMinMax) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (cat TEXT, val INT)");
    execute_sql(env.executor,
               "INSERT INTO test_table VALUES ('A', 10), ('A', 20), ('B', 5), ('B', 15)");
    const auto res = execute_sql(
        env.executor, "SELECT cat, MIN(val), MAX(val) FROM test_table GROUP BY cat");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 2U);
    // Verify A: min=10, max=20; B: min=5, max=15
    bool found_a = false, found_b = false;
    for (size_t i = 0; i < res.row_count(); ++i) {
        std::string cat_val = res.rows()[i].get(0).to_string();
        int min_val = std::stoi(res.rows()[i].get(1).to_string());
        int max_val = std::stoi(res.rows()[i].get(2).to_string());
        if (cat_val == "A") {
            EXPECT_EQ(min_val, 10);
            EXPECT_EQ(max_val, 20);
            found_a = true;
        } else if (cat_val == "B") {
            EXPECT_EQ(min_val, 5);
            EXPECT_EQ(max_val, 15);
            found_b = true;
        }
    }
    EXPECT_TRUE(found_a);
    EXPECT_TRUE(found_b);
}

TEST_F(QueryExecutorTests, SelectWithGroupByMultipleColumns) {
    TestEnvironment env;
    execute_sql(env.executor,
               "CREATE TABLE test_table (cat1 TEXT, cat2 TEXT, val INT)");
    // 4 groups: (A,X), (A,Y), (B,X), (B,Y)
    execute_sql(env.executor,
               "INSERT INTO test_table VALUES ('A', 'X', 10), ('A', 'Y', 20), "
               "('A', 'X', 5), ('A', 'Y', 15), ('B', 'X', 10), ('B', 'Y', 20)");
    const auto res = execute_sql(env.executor,
                                 "SELECT cat1, cat2, SUM(val) FROM test_table GROUP BY "
                                 "cat1, cat2 ORDER BY cat1, cat2");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 4U);
    // Verify sums: (A,X)=15, (A,Y)=35, (B,X)=10, (B,Y)=20
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "A");
    EXPECT_STREQ(res.rows()[0].get(1).to_string().c_str(), "X");
    EXPECT_EQ(std::stoi(res.rows()[0].get(2).to_string()), 15);
    EXPECT_STREQ(res.rows()[1].get(0).to_string().c_str(), "A");
    EXPECT_STREQ(res.rows()[1].get(1).to_string().c_str(), "Y");
    EXPECT_EQ(std::stoi(res.rows()[1].get(2).to_string()), 35);
    EXPECT_STREQ(res.rows()[2].get(0).to_string().c_str(), "B");
    EXPECT_STREQ(res.rows()[2].get(1).to_string().c_str(), "X");
    EXPECT_EQ(std::stoi(res.rows()[2].get(2).to_string()), 10);
    EXPECT_STREQ(res.rows()[3].get(0).to_string().c_str(), "B");
    EXPECT_STREQ(res.rows()[3].get(1).to_string().c_str(), "Y");
    EXPECT_EQ(std::stoi(res.rows()[3].get(2).to_string()), 20);
}

TEST_F(QueryExecutorTests, SelectNonExistentTable) {
    TestEnvironment env;
    const auto res = execute_sql(env.executor, "SELECT * FROM nonexistent");
    EXPECT_FALSE(res.success());
}

TEST_F(QueryExecutorTests, SelectNonExistentColumn) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT)");
    const auto res = execute_sql(env.executor, "SELECT nonexistent FROM test_table");
    // Implementation returns success with empty result for non-existent column
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 0U);
}

// ============= UPDATE Tests =============

TEST_F(QueryExecutorTests, UpdateWithCondition) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 30)");
    const auto res = execute_sql(env.executor, "UPDATE test_table SET val = 100 WHERE id = 2");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.rows_affected(), 1U);
}

TEST_F(QueryExecutorTests, UpdateAllRows) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 10), (2, 20)");
    const auto res = execute_sql(env.executor, "UPDATE test_table SET val = 99");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.rows_affected(), 2U);
}

TEST_F(QueryExecutorTests, UpdateNonExistentTable) {
    TestEnvironment env;
    const auto res = execute_sql(env.executor, "UPDATE nonexistent SET val = 1");
    EXPECT_FALSE(res.success());
}

// ============= DELETE Tests =============

TEST_F(QueryExecutorTests, DeleteWithCondition) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 10), (2, 20), (3, 30)");
    const auto res = execute_sql(env.executor, "DELETE FROM test_table WHERE id = 2");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.rows_affected(), 1U);
}

TEST_F(QueryExecutorTests, DeleteAllRows) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1), (2), (3)");
    const auto res = execute_sql(env.executor, "DELETE FROM test_table");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.rows_affected(), 3U);
}

TEST_F(QueryExecutorTests, DeleteNonExistentTable) {
    TestEnvironment env;
    const auto res = execute_sql(env.executor, "DELETE FROM nonexistent");
    EXPECT_FALSE(res.success());
}

// ============= Transaction Tests =============

TEST_F(QueryExecutorTests, TransactionBegin) {
    TestEnvironment env;
    const auto res = execute_sql(env.executor, "BEGIN");
    EXPECT_TRUE(res.success());
}

TEST_F(QueryExecutorTests, TransactionCommit) {
    TestEnvironment env;
    execute_sql(env.executor, "BEGIN");
    execute_sql(env.executor, "CREATE TABLE test_table (id INT)");
    const auto res = execute_sql(env.executor, "COMMIT");
    EXPECT_TRUE(res.success());
    EXPECT_TRUE(env.catalog->table_exists_by_name("test_table"));
}

TEST_F(QueryExecutorTests, TransactionRollback) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (100)");
    execute_sql(env.executor, "BEGIN");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (200)");
    // Verify second insert is visible within transaction
    const auto res_internal = execute_sql(env.executor, "SELECT val FROM test_table");
    EXPECT_EQ(res_internal.row_count(), 2U);
    // Rollback
    execute_sql(env.executor, "ROLLBACK");
    // Should be back to 1 row
    const auto res_after = execute_sql(env.executor, "SELECT val FROM test_table");
    EXPECT_EQ(res_after.row_count(), 1U);
}

TEST_F(QueryExecutorTests, TransactionIsolationReadBeforeCommit) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 10)");

    // Use same executor for transaction
    execute_sql(env.executor, "BEGIN");
    execute_sql(env.executor, "UPDATE test_table SET val = 99 WHERE id = 1");

    // Create a new executor (same catalog) to verify isolation
    QueryExecutor exec2(*env.catalog, env.bpm, env.lock_manager, env.txn_manager);
    const auto res = execute_sql(exec2, "SELECT val FROM test_table WHERE id = 1");
    EXPECT_TRUE(res.success());
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "10");
}

// ============= CREATE INDEX Tests =============

TEST_F(QueryExecutorTests, CreateIndexBasic) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1, 10), (2, 20)");
    const auto res = execute_sql(env.executor, "CREATE INDEX idx_test ON test_table (val)");
    EXPECT_TRUE(res.success());
}

TEST_F(QueryExecutorTests, DropIndexBasic) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT, val INT)");
    execute_sql(env.executor, "CREATE INDEX idx_test ON test_table (val)");
    const auto res = execute_sql(env.executor, "DROP INDEX idx_test");
    EXPECT_TRUE(res.success());
}

// ============= JOIN Tests =============

TEST_F(QueryExecutorTests, InnerJoin) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE table_a (id INT, name TEXT)");
    execute_sql(env.executor, "CREATE TABLE table_b (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO table_a VALUES (1, 'Alice'), (2, 'Bob')");
    execute_sql(env.executor, "INSERT INTO table_b VALUES (1, 100), (2, 200), (3, 300)");

    const auto res = execute_sql(
        env.executor,
        "SELECT table_a.name, table_b.val FROM table_a JOIN table_b ON table_a.id = table_b.id");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 2U);
}

TEST_F(QueryExecutorTests, LeftJoin) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE table_a (id INT, name TEXT)");
    execute_sql(env.executor, "CREATE TABLE table_b (id INT, val INT)");
    execute_sql(env.executor, "INSERT INTO table_a VALUES (1, 'Alice'), (2, 'Bob')");
    execute_sql(env.executor, "INSERT INTO table_b VALUES (1, 100), (2, 200)");

    const auto res = execute_sql(env.executor,
                                 "SELECT table_a.name, table_b.val FROM table_a LEFT JOIN table_b "
                                 "ON table_a.id = table_b.id");
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 2U);
}

// ============= Error Handling Tests =============

TEST_F(QueryExecutorTests, InvalidSQLSyntax) {
    TestEnvironment env;
    // Test malformed SQL with parser error - parser returns nullptr
    auto lexer = std::make_unique<Lexer>("SELECT * FROM");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    // Parser should return nullptr for malformed SQL
    EXPECT_EQ(stmt, nullptr);
}

TEST_F(QueryExecutorTests, DivisionByZero) {
    TestEnvironment env;
    execute_sql(env.executor, "CREATE TABLE test_table (id INT)");
    execute_sql(env.executor, "INSERT INTO test_table VALUES (1)");
    const auto res = execute_sql(env.executor, "SELECT 10 / 0 FROM test_table");
    // Division by zero succeeds with result (implementation-dependent behavior)
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.row_count(), 1U);
}

}  // namespace
