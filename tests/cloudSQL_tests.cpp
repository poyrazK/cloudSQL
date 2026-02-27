/**
 * @file cloudSQL_tests.cpp
 * @brief Comprehensive test suite for cloudSQL C++ implementation
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
#include "common/value.hpp"
#include "executor/query_executor.hpp"
#include "executor/types.hpp"
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

constexpr int64_t VAL_42 = 42;
constexpr double PI_LOWER = 3.14;
constexpr double PI_UPPER = 3.15;
constexpr int64_t VAL_10 = 10;
constexpr int64_t VAL_25 = 25;
constexpr uint64_t STATS_100 = 100;
constexpr uint16_t PORT_5432 = 5432;
constexpr uint16_t PORT_9999 = 9999;

// ============= Value Tests =============

TEST(ValueTests, Basic) {
    const auto val = Value::make_int64(VAL_42);
    EXPECT_EQ(val.to_int64(), VAL_42);
}

TEST(ValueTests, TypeVariety) {
    const Value b(true);
    EXPECT_TRUE(b.as_bool());
    EXPECT_STREQ(b.to_string().c_str(), "TRUE");

    const Value f(3.14159);
    EXPECT_GT(f.as_float64(), PI_LOWER);
    EXPECT_LT(f.as_float64(), PI_UPPER);

    const Value s("cloudSQL");
    EXPECT_STREQ(s.as_text().c_str(), "cloudSQL");
}

// ============= Parser Tests =============

TEST(ParserTests, Expressions) {
    auto lexer = std::make_unique<Lexer>("SELECT 1 + 2 * 3 FROM dual");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);
    const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
    ASSERT_NE(select, nullptr);
    EXPECT_STREQ(select->columns()[0]->to_string().c_str(), "1 + 2 * 3");
}

TEST(ParserTests, ComplexExpressions) {
    {
        auto lexer = std::make_unique<Lexer>("SELECT (1 > 0 AND 5 <= 2) OR NOT (1 = 1) FROM dual");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        ASSERT_TRUE(stmt != nullptr);
        const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
        ASSERT_NE(select, nullptr);
        const auto val = select->columns()[0]->evaluate();
        EXPECT_FALSE(val.as_bool());
    }
    {
        auto lexer = std::make_unique<Lexer>("SELECT -10 + 20, 5 * (2 + 3) FROM dual");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        ASSERT_TRUE(stmt != nullptr);
        const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
        ASSERT_NE(select, nullptr);
        EXPECT_EQ(select->columns()[0]->evaluate().to_int64(), VAL_10);
        EXPECT_EQ(select->columns()[1]->evaluate().to_int64(), VAL_25);
    }
}

TEST(ParserTests, SelectVariants) {
    auto lexer = std::make_unique<Lexer>("SELECT DISTINCT name FROM users LIMIT 10 OFFSET 20");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);
    const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
    ASSERT_NE(select, nullptr);
    EXPECT_TRUE(select->distinct());
    EXPECT_EQ(select->limit(), VAL_10);
    EXPECT_EQ(select->offset(), 20);
}

// ============= Catalog Tests =============

TEST(CatalogTests, FullLifecycle) {
    auto catalog = Catalog::create();

    const std::vector<ColumnInfo> cols = {{"id", ValueType::TYPE_INT64, 0},
                                          {"name", ValueType::TYPE_TEXT, 1}};

    const oid_t table_id = catalog->create_table("test_table", cols);
    EXPECT_TRUE(table_id > 0);
    EXPECT_TRUE(catalog->table_exists(table_id));

    auto table = catalog->get_table(table_id);
    EXPECT_TRUE(table.has_value());
    if (table.has_value()) {
        EXPECT_STREQ(table.value()->name.c_str(), "test_table");
    }

    catalog->update_table_stats(table_id, STATS_100);
    if (table.has_value()) {
        EXPECT_EQ(table.value()->num_rows, STATS_100);
    }

    EXPECT_TRUE(catalog->drop_table(table_id));
    EXPECT_FALSE(catalog->table_exists(table_id));
}

// ============= Config Tests =============

TEST(ConfigTests, Basic) {
    config::Config cfg;
    EXPECT_EQ(cfg.port, PORT_5432);

    cfg.port = PORT_9999;
    cfg.data_dir = "./tmp_data";

    EXPECT_TRUE(cfg.validate());

    const std::string cfg_file = "test_config.conf";
    EXPECT_TRUE(cfg.save(cfg_file));

    config::Config cfg2;
    EXPECT_TRUE(cfg2.load(cfg_file));
    EXPECT_EQ(cfg2.port, PORT_9999);
    EXPECT_STREQ(cfg2.data_dir.c_str(), "./tmp_data");

    static_cast<void>(std::remove(cfg_file.c_str()));
}

// ============= Storage Tests =============

TEST(StorageTests, Persistence) {
    const std::string filename = "persist_test";
    static_cast<void>(std::remove("./test_data/persist_test.heap"));
    Schema schema;
    schema.add_column("data", ValueType::TYPE_TEXT);
    {
        StorageManager disk_manager("./test_data");
        BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
        HeapTable table(filename, sm, schema);
        static_cast<void>(table.create());
        static_cast<void>(table.insert(Tuple({Value::make_text("Persistent data")})));
    }
    {
        StorageManager disk_manager("./test_data");
        BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
        HeapTable table(filename, sm, schema);
        auto iter = table.scan();
        Tuple t;
        EXPECT_TRUE(iter.next(t));
        EXPECT_STREQ(t.get(0).as_text().c_str(), "Persistent data");
    }
}

// ============= Execution Tests =============

TEST(ExecutionTests, EndToEnd) {
    static_cast<void>(std::remove("./test_data/users.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);
    QueryExecutor exec(*catalog, sm, lm, tm);

    {
        auto lexer = std::make_unique<Lexer>("CREATE TABLE users (id BIGINT, age BIGINT)");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        ASSERT_NE(stmt, nullptr);
        const auto res = exec.execute(*stmt);
        EXPECT_TRUE(res.success());
    }
    {
        auto lexer =
            std::make_unique<Lexer>("INSERT INTO users (id, age) VALUES (1, 20), (2, 30), (3, 40)");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        ASSERT_NE(stmt, nullptr);
        const auto res = exec.execute(*stmt);
        EXPECT_TRUE(res.success());
    }
    {
        auto lexer = std::make_unique<Lexer>("SELECT id FROM users WHERE age > 25");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        ASSERT_NE(stmt, nullptr);
        const auto res = exec.execute(*stmt);
        EXPECT_TRUE(res.success());
        EXPECT_EQ(res.row_count(), 2);
    }
}

}  // namespace
