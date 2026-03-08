/**
 * @file cloudSQL_tests.cpp
 * @brief Comprehensive test suite for cloudSQL implementation
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <exception>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "common/value.hpp"
#include "executor/query_executor.hpp"
#include "executor/types.hpp"
#include "parser/expression.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"
#include "parser/statement.hpp"
#include "parser/token.hpp"
#include "storage/btree_index.hpp"
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
constexpr int64_t VAL_1 = 1;
constexpr int64_t VAL_2 = 2;
constexpr int64_t VAL_10 = 10;
constexpr int64_t VAL_20 = 20;
constexpr int64_t VAL_25 = 25;
constexpr uint64_t STATS_100 = 100;
constexpr uint16_t PORT_5432 = 5432;
constexpr uint16_t PORT_9999 = 9999;
constexpr int64_t VAL_123 = 123;
constexpr double VAL_1_5 = 1.5;
constexpr oid_t TABLE_9999 = 9999;
constexpr oid_t INDEX_8888 = 8888;

// ============= Value Tests =============

TEST(CloudSQLTests, ValueBasic) {
    const auto val = Value::make_int64(VAL_42);
    EXPECT_EQ(val.to_int64(), VAL_42);
}

TEST(CloudSQLTests, ValueTypeVariety) {
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

TEST(CloudSQLTests, ParserExpressions) {
    auto lexer = std::make_unique<Lexer>("SELECT 1 + 2 * 3 FROM dual");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    EXPECT_TRUE(stmt != nullptr);
    const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
    ASSERT_NE(select, nullptr);
    EXPECT_STREQ(select->columns()[0]->to_string().c_str(), "1 + 2 * 3");
}

TEST(CloudSQLTests, ExpressionComplex) {
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

TEST(CloudSQLTests, ParserSelectVariants) {
    auto lexer = std::make_unique<Lexer>("SELECT DISTINCT name FROM users LIMIT 10 OFFSET 20");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);
    const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
    ASSERT_NE(select, nullptr);
    EXPECT_TRUE(select->distinct());
    EXPECT_EQ(select->limit(), VAL_10);
    EXPECT_EQ(select->offset(), VAL_20);
}

TEST(CloudSQLTests, ParserErrors) {
    {
        auto lexer = std::make_unique<Lexer>("SELECT FROM users");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        EXPECT_TRUE(stmt == nullptr);
    }
}

// ============= Catalog Tests =============

TEST(CloudSQLTests, CatalogFullLifecycle) {
    auto catalog = Catalog::create();

    const std::vector<ColumnInfo> cols = {{"id", ValueType::TYPE_INT64, 0},
                                          {"name", ValueType::TYPE_TEXT, 1}};

    const oid_t table_id = catalog->create_table("test_table", cols);
    EXPECT_TRUE(table_id > 0);
    EXPECT_TRUE(catalog->table_exists(table_id));
    EXPECT_TRUE(catalog->table_exists_by_name("test_table"));

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

TEST(CloudSQLTests, ConfigBasic) {
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

TEST(CloudSQLTests, StoragePersistence) {
    const std::string filename = "persist_test";
    const std::string filepath = "./test_data/" + filename + ".heap";
    static_cast<void>(std::remove(filepath.c_str()));
    Schema schema;
    schema.add_column("data", ValueType::TYPE_TEXT);
    {
        StorageManager disk_manager("./test_data");
        BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
        HeapTable table(filename, sm, schema);
        static_cast<void>(table.create());
        static_cast<void>(table.insert(Tuple({Value::make_text("Persistent data")})));
        sm.flush_all_pages();
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
    static_cast<void>(std::remove(filepath.c_str()));
}

TEST(CloudSQLTests, StorageDelete) {
    const std::string filename = "delete_test";
    const std::string filepath = "./test_data/" + filename + ".heap";
    static_cast<void>(std::remove(filepath.c_str()));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    Schema schema;
    schema.add_column("id", ValueType::TYPE_INT64);
    HeapTable table(filename, sm, schema);
    EXPECT_TRUE(table.create());

    static_cast<void>(table.insert(Tuple({Value::make_int64(VAL_1)})));
    const auto tid2 = table.insert(Tuple({Value::make_int64(VAL_2)}));

    EXPECT_EQ(table.tuple_count(), 2U);
    EXPECT_TRUE(table.remove(tid2, 100));  // Logically delete with xmax=100
    EXPECT_EQ(table.tuple_count(), 1U);

    auto iter = table.scan();
    Tuple t;
    EXPECT_TRUE(iter.next(t));
    EXPECT_EQ(t.get(0).to_int64(), 1);
    EXPECT_FALSE(iter.next(t));
    static_cast<void>(std::remove(filepath.c_str()));
}

// ============= Index Tests =============

TEST(IndexTests, BTreeBasic) {
    static_cast<void>(std::remove("./test_data/idx_test.idx"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    BTreeIndex idx("idx_test", sm, ValueType::TYPE_INT64);
    static_cast<void>(idx.create());
    static_cast<void>(idx.insert(Value::make_int64(VAL_10), HeapTable::TupleId(1, 1)));
    static_cast<void>(idx.insert(Value::make_int64(VAL_20), HeapTable::TupleId(1, 2)));
    static_cast<void>(idx.insert(Value::make_int64(VAL_10), HeapTable::TupleId(2, 1)));
    const auto res = idx.search(Value::make_int64(VAL_10));
    EXPECT_EQ(res.size(), 2U);
    static_cast<void>(idx.drop());
}

TEST(IndexTests, Scan) {
    static_cast<void>(std::remove("./test_data/scan_test.idx"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    BTreeIndex idx("scan_test", sm, ValueType::TYPE_INT64);
    static_cast<void>(idx.create());
    static_cast<void>(idx.insert(Value::make_int64(VAL_1), HeapTable::TupleId(1, 1)));
    static_cast<void>(idx.insert(Value::make_int64(VAL_2), HeapTable::TupleId(1, 2)));

    auto iter = idx.scan();
    BTreeIndex::Entry entry;
    EXPECT_TRUE(iter.next(entry));
    EXPECT_EQ(entry.key.to_int64(), 1);
    EXPECT_TRUE(iter.next(entry));
    EXPECT_EQ(entry.key.to_int64(), 2);
    EXPECT_FALSE(iter.next(entry));
    static_cast<void>(idx.drop());
}

// ============= Execution Tests =============

TEST(ExecutionTests, EndToEnd) {
    static_cast<void>(std::remove("./test_data/users_e2e.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());
    QueryExecutor exec(*catalog, sm, lm, tm);

    {
        auto lexer = std::make_unique<Lexer>("CREATE TABLE users_e2e (id BIGINT, age BIGINT)");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        const auto res = exec.execute(*stmt);
        EXPECT_TRUE(res.success());
    }
    {
        auto lexer = std::make_unique<Lexer>(
            "INSERT INTO users_e2e (id, age) VALUES (1, 20), (2, 30), (3, 40)");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        const auto res = exec.execute(*stmt);
        EXPECT_TRUE(res.success());
    }
    {
        auto lexer = std::make_unique<Lexer>("SELECT id FROM users_e2e WHERE age > 25");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        const auto res = exec.execute(*stmt);
        EXPECT_TRUE(res.success());
        EXPECT_EQ(res.row_count(), 2U);
    }
    static_cast<void>(std::remove("./test_data/users_e2e.heap"));
}

TEST(ExecutionTests, Sort) {
    static_cast<void>(std::remove("./test_data/sort_test.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE sort_test (val INT)")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO sort_test VALUES (30), (10), (20)"))
             .parse_statement()));

    const auto res =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT val FROM sort_test ORDER BY val"))
                          .parse_statement());
    ASSERT_EQ(res.row_count(), 3U);
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "10");
    EXPECT_STREQ(res.rows()[1].get(0).to_string().c_str(), "20");
    EXPECT_STREQ(res.rows()[2].get(0).to_string().c_str(), "30");
    static_cast<void>(std::remove("./test_data/sort_test.heap"));
}

TEST(ExecutionTests, Aggregate) {
    static_cast<void>(std::remove("./test_data/agg_test.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE agg_test (cat TEXT, val INT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>(
                                 "INSERT INTO agg_test VALUES ('A', 10), ('A', 20), ('B', 5)"))
                          .parse_statement()));

    auto lex =
        std::make_unique<Lexer>("SELECT cat, COUNT(val), SUM(val) FROM agg_test GROUP BY cat");
    auto stmt = Parser(std::move(lex)).parse_statement();
    ASSERT_NE(stmt, nullptr);

    const auto res = exec.execute(*stmt);
    EXPECT_TRUE(res.success());

    ASSERT_EQ(res.row_count(), 2U);
    /* Row 0: 'A', 2, 30 */
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "A");
    EXPECT_STREQ(res.rows()[0].get(1).to_string().c_str(), "2");
    EXPECT_STREQ(res.rows()[0].get(2).to_string().c_str(), "30");
    static_cast<void>(std::remove("./test_data/agg_test.heap"));
}

TEST(ExecutionTests, AggregateAdvanced) {
    static_cast<void>(std::remove("./test_data/adv_agg.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE adv_agg (val INT)")).parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO adv_agg VALUES (10), (20), (30)"))
                          .parse_statement()));

    const auto res = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT MIN(val), MAX(val), AVG(val) FROM adv_agg"))
             .parse_statement());
    EXPECT_TRUE(res.success());

    ASSERT_EQ(res.row_count(), 1U);
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "10");
    EXPECT_STREQ(res.rows()[0].get(1).to_string().c_str(), "30");
    EXPECT_STREQ(res.rows()[0].get(2).to_string().c_str(), "20");
    static_cast<void>(std::remove("./test_data/adv_agg.heap"));
}

TEST(ExecutionTests, AggregateDistinct) {
    static_cast<void>(std::remove("./test_data/dist_agg.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE dist_agg (val INT)")).parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>(
                                 "INSERT INTO dist_agg VALUES (10), (10), (20), (30), (30), (30)"))
                          .parse_statement()));

    const auto res =
        exec.execute(*Parser(std::make_unique<Lexer>(
                                 "SELECT COUNT(DISTINCT val), SUM(DISTINCT val) FROM dist_agg"))
                          .parse_statement());
    EXPECT_TRUE(res.success());

    ASSERT_EQ(res.row_count(), 1U);
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "3");
    EXPECT_STREQ(res.rows()[0].get(1).to_string().c_str(), "60");
    static_cast<void>(std::remove("./test_data/dist_agg.heap"));
}

TEST(ExecutionTests, Transaction) {
    static_cast<void>(std::remove("./test_data/txn_test.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());

    QueryExecutor qexec1(*catalog, sm, lm, tm);
    static_cast<void>(
        qexec1.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE txn_test (id INT, val INT)"))
                            .parse_statement()));

    static_cast<void>(qexec1.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(
        qexec1.execute(*Parser(std::make_unique<Lexer>("INSERT INTO txn_test VALUES (1, 100)"))
                            .parse_statement()));

    QueryExecutor qexec2(*catalog, sm, lm, tm);

    const auto res_commit =
        qexec1.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement());
    EXPECT_TRUE(res_commit.success());

    const auto res_select =
        qexec2.execute(*Parser(std::make_unique<Lexer>("SELECT val FROM txn_test WHERE id = 1"))
                            .parse_statement());
    ASSERT_EQ(res_select.row_count(), 1U);
    EXPECT_STREQ(res_select.rows()[0].get(0).to_string().c_str(), "100");
    static_cast<void>(std::remove("./test_data/txn_test.heap"));
}

TEST(ExecutionTests, Rollback) {
    static_cast<void>(std::remove("./test_data/rollback_test.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE rollback_test (val INT)"))
                          .parse_statement()));

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO rollback_test VALUES (100)"))
                          .parse_statement()));

    const auto res_internal = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM rollback_test")).parse_statement());
    EXPECT_EQ(res_internal.row_count(), 1U);

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));

    const auto res_after = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM rollback_test")).parse_statement());
    EXPECT_EQ(res_after.row_count(), 0U);
    static_cast<void>(std::remove("./test_data/rollback_test.heap"));
}

TEST(ExecutionTests, UpdateDelete) {
    static_cast<void>(std::remove("./test_data/upd_test.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE upd_test (id INT, val TEXT)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO upd_test VALUES (1, 'old'), (2, 'stay')"))
             .parse_statement()));

    /* Test UPDATE */
    const auto res_upd = exec.execute(
        *Parser(std::make_unique<Lexer>("UPDATE upd_test SET val = 'new' WHERE id = 1"))
             .parse_statement());
    EXPECT_EQ(res_upd.rows_affected(), 1U);

    const auto res_sel =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT val FROM upd_test WHERE id = 1"))
                          .parse_statement());
    ASSERT_EQ(res_sel.row_count(), 1U);
    EXPECT_STREQ(res_sel.rows()[0].get(0).to_string().c_str(), "new");

    /* Test DELETE */
    const auto res_del = exec.execute(
        *Parser(std::make_unique<Lexer>("DELETE FROM upd_test WHERE id = 2")).parse_statement());
    EXPECT_EQ(res_del.rows_affected(), 1U);

    const auto res_sel2 =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT id FROM upd_test")).parse_statement());
    EXPECT_EQ(res_sel2.row_count(), 1U);  // Only ID 1 remains
    static_cast<void>(std::remove("./test_data/upd_test.heap"));
}

TEST(ExecutionTests, MVCC) {
    static_cast<void>(std::remove("./test_data/mvcc_test.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());

    QueryExecutor qexec1(*catalog, sm, lm, tm);
    static_cast<void>(qexec1.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE mvcc_test (val INT)")).parse_statement()));

    /* Start T1 and Insert */
    static_cast<void>(qexec1.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(qexec1.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO mvcc_test VALUES (10)")).parse_statement()));

    /* Session 2 should see nothing yet (atomic snapshot) */
    QueryExecutor qexec2(*catalog, sm, lm, tm);
    const auto res2_pre = qexec2.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM mvcc_test")).parse_statement());
    EXPECT_EQ(res2_pre.row_count(), 0U);

    /* T1 updates row */
    static_cast<void>(qexec1.execute(
        *Parser(std::make_unique<Lexer>("UPDATE mvcc_test SET val = 20")).parse_statement()));

    /* T1 sees new value */
    const auto res1 = qexec1.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM mvcc_test")).parse_statement());
    ASSERT_EQ(res1.row_count(), 1U);
    EXPECT_STREQ(res1.rows()[0].get(0).to_string().c_str(), "20");

    static_cast<void>(qexec1.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    /* After commit, Session 2 sees the latest value */
    const auto res2_post = qexec2.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM mvcc_test")).parse_statement());
    ASSERT_EQ(res2_post.row_count(), 1U);
    EXPECT_STREQ(res2_post.rows()[0].get(0).to_string().c_str(), "20");
    static_cast<void>(std::remove("./test_data/mvcc_test.heap"));
}

TEST(ExecutionTests, Join) {
    static_cast<void>(std::remove("./test_data/users_join.heap"));
    static_cast<void>(std::remove("./test_data/orders_join.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE users_join (id INT, name TEXT)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE orders_join (id "
                                                                   "INT, user_id INT, amount "
                                                                   "DOUBLE)"))
                                        .parse_statement()));

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO users_join VALUES (1, 'Alice'), (2, 'Bob')"))
             .parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO orders_join VALUES "
                                                                   "(101, 1, 50.5), (102, 1, "
                                                                   "25.0), (103, 2, 100.0)"))
                                        .parse_statement()));

    /* Test: INNER JOIN with sorting */
    const auto result = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT users_join.name, orders_join.amount FROM "
                                        "users_join JOIN orders_join "
                                        "ON users_join.id = orders_join.user_id ORDER BY "
                                        "orders_join.amount"))
             .parse_statement());

    ASSERT_EQ(result.row_count(), 3U);

    /* 25.0 (Alice), 50.5 (Alice), 100.0 (Bob) */
    EXPECT_STREQ(result.rows()[0].get(0).to_string().c_str(), "Alice");
    EXPECT_STREQ(result.rows()[0].get(1).to_string().c_str(), "25");
    EXPECT_STREQ(result.rows()[2].get(0).to_string().c_str(), "Bob");
    EXPECT_STREQ(result.rows()[2].get(1).to_string().c_str(), "100");
    static_cast<void>(std::remove("./test_data/users_join.heap"));
    static_cast<void>(std::remove("./test_data/orders_join.heap"));
}

TEST(ExecutionTests, DDL) {
    static_cast<void>(std::remove("./test_data/ddl_test.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());
    QueryExecutor exec(*catalog, sm, lm, tm);

    /* 1. Create and then Drop Table */
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE ddl_test (id INT)")).parse_statement()));
    EXPECT_TRUE(catalog->table_exists_by_name("ddl_test"));

    const auto res_drop =
        exec.execute(*Parser(std::make_unique<Lexer>("DROP TABLE ddl_test")).parse_statement());
    EXPECT_TRUE(res_drop.success());
    EXPECT_FALSE(catalog->table_exists_by_name("ddl_test"));

    /* 2. IF EXISTS */
    const auto res_drop_none = exec.execute(
        *Parser(std::make_unique<Lexer>("DROP TABLE IF EXISTS non_existent")).parse_statement());
    EXPECT_TRUE(res_drop_none.success());

    /* 3. Create Index and then Drop Index */
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE ddl_test (id INT)")).parse_statement()));
    auto table_opt = catalog->get_table_by_name("ddl_test");
    if (table_opt) {
        const oid_t tid = (*table_opt)->table_id;
        static_cast<void>(catalog->create_index("idx_ddl", tid, {0}, IndexType::BTree, true));
    }

    const auto res_drop_idx =
        exec.execute(*Parser(std::make_unique<Lexer>("DROP INDEX idx_ddl")).parse_statement());
    EXPECT_TRUE(res_drop_idx.success());
    static_cast<void>(std::remove("./test_data/ddl_test.heap"));
}

TEST(LexerTests, Advanced) {
    /* 1. Test comments and line tracking */
    {
        const std::string sql = "SELECT -- comment here\n* FROM users";
        Lexer lexer(sql);
        const auto t1 = lexer.next_token();
        EXPECT_EQ(static_cast<int>(t1.type()), static_cast<int>(TokenType::Select));
        const auto t2 = lexer.next_token();  // Should skip comment and newline
        EXPECT_STREQ(t2.lexeme().c_str(), "*");
        EXPECT_EQ(t2.line(), 2U);
    }
    /* 2. Test Error and Unknown operators */
    {
        Lexer lexer("@");
        const auto t = lexer.next_token();
        EXPECT_EQ(static_cast<int>(t.type()), static_cast<int>(TokenType::Error));
    }
}

TEST(ExecutionTests, Expressions) {
    static_cast<void>(std::remove("./test_data/expr_test.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, sm.get_log_manager());
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE expr_test (id INT, val DOUBLE, str TEXT)"))
             .parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>(
                    "INSERT INTO expr_test VALUES (1, 10.5, 'A'), (2, NULL, 'B'), (3, 20.0, 'C')"))
             .parse_statement()));

    /* 1. Test IS NULL / IS NOT NULL */
    {
        const auto res = exec.execute(
            *Parser(std::make_unique<Lexer>("SELECT id FROM expr_test WHERE val IS NULL"))
                 .parse_statement());
        ASSERT_EQ(res.row_count(), 1U);
        EXPECT_EQ(res.rows()[0].get(0).to_int64(), 2);

        const auto res2 = exec.execute(
            *Parser(std::make_unique<Lexer>("SELECT id FROM expr_test WHERE val IS NOT NULL"))
                 .parse_statement());
        EXPECT_EQ(res2.row_count(), 2U);
    }

    /* 2. Test IN / NOT IN */
    {
        const auto res = exec.execute(
            *Parser(std::make_unique<Lexer>("SELECT id FROM expr_test WHERE id IN (1, 3)"))
                 .parse_statement());
        EXPECT_EQ(res.row_count(), 2U);

        const auto res2 = exec.execute(
            *Parser(std::make_unique<Lexer>("SELECT id FROM expr_test WHERE str NOT IN ('A', 'C')"))
                 .parse_statement());
        ASSERT_EQ(res2.row_count(), 1U);
        EXPECT_EQ(res2.rows()[0].get(0).to_int64(), 2);
    }

    /* 3. Test Arithmetic and Complex Binary */
    {
        const auto res = exec.execute(
            *Parser(std::make_unique<Lexer>(
                        "SELECT id, val * 2 + 10, val / 2, val - 5 FROM expr_test WHERE id = 1"))
                 .parse_statement());
        ASSERT_EQ(res.row_count(), 1U);
        EXPECT_DOUBLE_EQ(res.rows()[0].get(1).to_float64(), 31.0);
        EXPECT_DOUBLE_EQ(res.rows()[0].get(2).to_float64(), 5.25);
        EXPECT_DOUBLE_EQ(res.rows()[0].get(3).to_float64(), 5.5);
    }
    static_cast<void>(std::remove("./test_data/expr_test.heap"));
}

TEST(CloudSQLTests, ExpressionTypes) {
    /* Test ConstantExpr with various types for coverage */
    {
        const ConstantExpr c_bool(Value::make_bool(true));
        EXPECT_TRUE(c_bool.evaluate().as_bool());

        const ConstantExpr c_int(Value::make_int64(VAL_123));
        EXPECT_EQ(c_int.evaluate().to_int64(), VAL_123);

        const ConstantExpr c_float(Value::make_float64(VAL_1_5));
        EXPECT_DOUBLE_EQ(c_float.evaluate().to_float64(), VAL_1_5);

        const ConstantExpr c_null(Value::make_null());
        EXPECT_TRUE(c_null.evaluate().is_null());
    }
}

TEST(CatalogTests, Errors) {
    auto catalog = Catalog::create();
    const std::vector<ColumnInfo> cols = {{"id", ValueType::TYPE_INT64, 0}};

    static_cast<void>(catalog->create_table("fail_test", cols));
    /* Duplicate table */
    EXPECT_THROW(catalog->create_table("fail_test", cols), std::exception);

    /* Missing table */
    EXPECT_FALSE(catalog->table_exists(TABLE_9999));
    EXPECT_FALSE(catalog->get_table(TABLE_9999).has_value());
    EXPECT_FALSE(catalog->table_exists_by_name("non_existent"));

    /* Duplicate index */
    const oid_t tid = catalog->create_table("idx_fail", cols);
    static_cast<void>(catalog->create_index("my_idx", tid, {0}, IndexType::BTree, true));
    EXPECT_THROW(catalog->create_index("my_idx", tid, {0}, IndexType::BTree, true), std::exception);

    /* Missing index */
    EXPECT_FALSE(catalog->get_index(INDEX_8888).has_value());
    EXPECT_FALSE(catalog->drop_index(INDEX_8888));
}

TEST(CatalogTests, Stats) {
    auto catalog = Catalog::create();
    const std::vector<ColumnInfo> cols = {{"id", ValueType::TYPE_INT64, 0}};
    const oid_t tid = catalog->create_table("stats_test", cols);

    EXPECT_TRUE(catalog->update_table_stats(tid, 500U));
    auto tinfo = catalog->get_table(tid);
    if (tinfo) {
        EXPECT_EQ((*tinfo)->num_rows, 500U);
    }

    /* Cover print() */
    catalog->print();
}

// ============= Parser Advanced Tests =============

TEST(ParserAdvanced, JoinAndComplexSelect) {
    /* 1. Left Join and multiple joins */
    {
        auto lexer = std::make_unique<Lexer>(
            "SELECT a.id, b.val FROM t1 LEFT JOIN t2 ON a.id = b.id JOIN t3 ON b.x = t3.x WHERE "
            "a.id > 10");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        ASSERT_NE(stmt, nullptr);
        const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
        ASSERT_NE(select, nullptr);
        EXPECT_EQ(select->joins().size(), 2U);
        EXPECT_EQ(select->joins()[0].type, SelectStatement::JoinType::Left);
        EXPECT_EQ(select->joins()[1].type, SelectStatement::JoinType::Inner);
    }

    /* 2. Group By and Having */
    {
        auto lexer = std::make_unique<Lexer>(
            "SELECT cat, SUM(val) FROM items GROUP BY cat HAVING SUM(val) > 1000 ORDER BY cat "
            "DESC");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        ASSERT_NE(stmt, nullptr);
        const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
        ASSERT_NE(select, nullptr);
        EXPECT_EQ(select->group_by().size(), 1U);
        ASSERT_NE(select->having(), nullptr);
        EXPECT_EQ(select->order_by().size(), 1U);
    }

    /* 3. Transaction Statements */
    {
        auto lexer = std::make_unique<Lexer>("BEGIN");
        Parser parser(std::move(lexer));
        auto s1 = parser.parse_statement();
        ASSERT_NE(s1, nullptr);
        EXPECT_EQ(s1->type(), StmtType::TransactionBegin);

        auto lexer2 = std::make_unique<Lexer>("COMMIT");
        Parser parser2(std::move(lexer2));
        auto s2 = parser2.parse_statement();
        ASSERT_NE(s2, nullptr);
        EXPECT_EQ(s2->type(), StmtType::TransactionCommit);

        auto lexer3 = std::make_unique<Lexer>("ROLLBACK");
        Parser parser3(std::move(lexer3));
        auto s3 = parser3.parse_statement();
        ASSERT_NE(s3, nullptr);
        EXPECT_EQ(s3->type(), StmtType::TransactionRollback);
    }
}

TEST(ParserAdvanced, ParserErrorPaths) {
    /* Invalid CREATE syntax */
    {
        auto lexer = std::make_unique<Lexer>("CREATE TABLE (id INT)");  // Missing table name
        Parser parser(std::move(lexer));
        EXPECT_EQ(parser.parse_statement(), nullptr);
    }
    /* Invalid JOIN syntax */
    {
        auto lexer = std::make_unique<Lexer>("SELECT * FROM t1 LEFT t2");  // Missing JOIN keyword
        Parser parser(std::move(lexer));
        EXPECT_EQ(parser.parse_statement(), nullptr);
    }
    /* Invalid GROUP BY syntax */
    {
        auto lexer = std::make_unique<Lexer>("SELECT * FROM t1 GROUP cat");  // Missing BY keyword
        Parser parser(std::move(lexer));
        EXPECT_EQ(parser.parse_statement(), nullptr);
    }
}

// ============= Execution Advanced Tests =============

TEST(ExecutionTests, AggregationHaving) {
    static_cast<void>(std::remove("./test_data/having_test.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(128, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, nullptr);
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE having_test (grp INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO having_test VALUES (1, 10), (1, 20), (2, 5)"))
             .parse_statement()));

    // SELECT grp, SUM(val) FROM having_test GROUP BY grp HAVING SUM(val) > 10
    auto res = exec.execute(
        *Parser(std::make_unique<Lexer>(
                    "SELECT grp, SUM(val) FROM having_test GROUP BY grp HAVING SUM(val) > 10"))
             .parse_statement());

    EXPECT_TRUE(res.success());
    ASSERT_EQ(res.row_count(), 1U);  // Only group 1 should pass (sum=30)
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "1");
    static_cast<void>(std::remove("./test_data/having_test.heap"));
}

TEST(OperatorTests, AggregateTypes) {
    static_cast<void>(std::remove("./test_data/agg_types.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(128, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, nullptr);
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE agg_types (val DOUBLE)")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO agg_types VALUES (10.0), (20.0), (30.0)"))
             .parse_statement()));

    auto res = exec.execute(
        *Parser(std::make_unique<Lexer>(
                    "SELECT MIN(val), MAX(val), AVG(val), SUM(val), COUNT(val) FROM agg_types"))
             .parse_statement());
    EXPECT_TRUE(res.success());
    ASSERT_EQ(res.row_count(), 1U);
    EXPECT_DOUBLE_EQ(res.rows()[0].get(0).to_float64(), 10.0);
    EXPECT_DOUBLE_EQ(res.rows()[0].get(1).to_float64(), 30.0);
    EXPECT_DOUBLE_EQ(res.rows()[0].get(2).to_float64(), 20.0);
    EXPECT_DOUBLE_EQ(res.rows()[0].get(3).to_float64(), 60.0);
    EXPECT_EQ(res.rows()[0].get(4).to_int64(), 3);
    static_cast<void>(std::remove("./test_data/agg_types.heap"));
}

TEST(OperatorTests, LimitOffset) {
    static_cast<void>(std::remove("./test_data/lim_off.heap"));
    StorageManager disk_manager("./test_data");
    BufferPoolManager sm(128, disk_manager);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, nullptr);
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE lim_off (val INT)")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO lim_off VALUES (1), (2), (3), (4), (5)"))
             .parse_statement()));

    auto res = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM lim_off ORDER BY val LIMIT 2 OFFSET 2"))
             .parse_statement());
    EXPECT_TRUE(res.success());
    ASSERT_EQ(res.row_count(), 2U);
    EXPECT_EQ(res.rows()[0].get(0).to_int64(), 3);
    EXPECT_EQ(res.rows()[1].get(0).to_int64(), 4);
    static_cast<void>(std::remove("./test_data/lim_off.heap"));
}

TEST(OperatorTests, SeqScanVisibility) {
    StorageManager storage("./test_data");
    BufferPoolManager sm(128, storage);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm, nullptr);
    Schema schema;
    schema.add_column("v", ValueType::TYPE_INT64);

    HeapTable table("vis_test", sm, schema);
    table.create();

    // Use a transaction to insert, ensuring xmin > 0
    auto* txn_setup = tm.begin();
    table.insert(Tuple({Value::make_int64(1)}), txn_setup->get_id());
    tm.commit(txn_setup);

    auto* txn = tm.begin();
    SeqScanOperator scan(std::make_unique<HeapTable>("vis_test", sm, schema), txn, nullptr);
    scan.init();
    scan.open();

    Tuple t;
    int count = 0;
    while (scan.next(t)) {
        count++;
    }
    EXPECT_GE(count, 1);

    static_cast<void>(std::remove("./test_data/vis_test.heap"));
}

TEST(ParserTests, CreateIndexAndAlter) {
    {
        auto lexer = std::make_unique<Lexer>("CREATE INDEX idx_name ON users (col1)");
        Parser parser(std::move(lexer));
    }
    {
        auto lexer =
            std::make_unique<Lexer>("SELECT * FROM t WHERE col IS NOT NULL AND id IN (1, 2, 3)");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        ASSERT_NE(stmt, nullptr);
    }
}

}  // namespace
