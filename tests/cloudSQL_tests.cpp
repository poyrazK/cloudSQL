/**
 * @file cloudSQL_tests.cpp
 * @brief Comprehensive test suite for cloudSQL C++ implementation
 */

#include <arpa/inet.h>   // IWYU pragma: keep
#include <netinet/in.h>  // IWYU pragma: keep
#include <sys/socket.h>  // IWYU pragma: keep
#include <sys/types.h>   // IWYU pragma: keep
#include <unistd.h>      // IWYU pragma: keep

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <exception>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "common/value.hpp"
#include "executor/query_executor.hpp"
#include "executor/types.hpp"
#include "network/server.hpp" // IWYU pragma: keep
#include "parser/expression.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"
#include "parser/statement.hpp"
#include "parser/token.hpp"
#include "storage/btree_index.hpp"
#include "storage/heap_table.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"
#include "test_utils.hpp"

using namespace cloudsql;
using namespace cloudsql::common;
using namespace cloudsql::parser;
using namespace cloudsql::executor;
using namespace cloudsql::storage;
using namespace cloudsql::transaction;

namespace {

// Using common test counters
using cloudsql::tests::tests_passed;
using cloudsql::tests::tests_failed;

constexpr int64_t VAL_42 = 42;
constexpr double PI_LOWER = 3.14;
constexpr double PI_UPPER = 3.15;
constexpr int64_t VAL_10 = 10;
constexpr int64_t VAL_25 = 25;
constexpr uint64_t STATS_100 = 100;
constexpr uint16_t PORT_9999 = 9999;
constexpr uint64_t XMAX_100 = 100;
constexpr int64_t BTREE_VAL_10 = 10;
constexpr int64_t BTREE_VAL_20 = 20;
constexpr uint64_t STATS_500 = 500;

constexpr uint16_t PORT_5432 = 5432;
constexpr int64_t VAL_123 = 123;
constexpr double VAL_1_5 = 1.5;
constexpr oid_t TABLE_9999 = 9999;
constexpr oid_t INDEX_8888 = 8888;

// ============= Value Tests =============

TEST(ValueTest_Basic) {
    const auto val = Value::make_int64(VAL_42);
    EXPECT_EQ(val.to_int64(), VAL_42);
}

TEST(ValueTest_TypeVariety) {
    const Value b(true);
    EXPECT_TRUE(b.as_bool());
    EXPECT_STREQ(b.to_string(), "TRUE");

    const Value f(3.14159);
    EXPECT_GT(f.as_float64(), PI_LOWER);
    EXPECT_LT(f.as_float64(), PI_UPPER);

    const Value s("cloudSQL");
    EXPECT_STREQ(s.as_text(), "cloudSQL");
}

// ============= Parser Tests =============

TEST(ParserTest_Expressions) {
    {
        auto lexer = std::make_unique<Lexer>("SELECT 1 + 2 * 3 FROM dual");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        EXPECT_TRUE(stmt != nullptr);
        const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
        EXPECT_STREQ(select->columns()[0]->to_string(), "1 + 2 * 3");
    }
}

TEST(ExpressionTest_Complex) {
    {
        auto lexer = std::make_unique<Lexer>("SELECT (1 > 0 AND 5 <= 2) OR NOT (1 = 1) FROM dual");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        if (!stmt) { throw std::runtime_error("ExpressionTest_Complex: Parser failed on query 1"); }
        const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
        const auto val = select->columns()[0]->evaluate();
        EXPECT_FALSE(val.as_bool());
    }
    {
        auto lexer = std::make_unique<Lexer>("SELECT -10 + 20, 5 * (2 + 3) FROM dual");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        if (!stmt) { throw std::runtime_error("ExpressionTest_Complex: Parser failed on query 2"); }
        const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
        EXPECT_EQ(select->columns()[0]->evaluate().to_int64(), VAL_10);
        EXPECT_EQ(select->columns()[1]->evaluate().to_int64(), VAL_25);
    }
    {
        auto lexer = std::make_unique<Lexer>("SELECT 5.5 FROM dual");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        if (!stmt) { throw std::runtime_error("ExpressionTest_Complex: Parser failed on query 3a"); }
        const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
        EXPECT_TRUE(select->columns()[0]->evaluate().to_float64() == 5.5); // NOLINT(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    }
    {
        auto lexer = std::make_unique<Lexer>("SELECT 10 / 2 FROM dual");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        if (!stmt) { throw std::runtime_error("ExpressionTest_Complex: Parser failed on query 3b"); }
        const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
        EXPECT_TRUE(select->columns()[0]->evaluate().to_float64() == 5.0); // NOLINT(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    }
}

TEST(ParserTest_SelectVariants) {
    {
        auto lexer = std::make_unique<Lexer>("SELECT DISTINCT name FROM users LIMIT 10 OFFSET 20");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
        EXPECT_TRUE(select->distinct());
        EXPECT_EQ(select->limit(), VAL_10);
        EXPECT_EQ(select->offset(), static_cast<int64_t>(20)); // NOLINT(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    }
    {
        auto lexer =
            std::make_unique<Lexer>("SELECT age, cnt FROM users GROUP BY age ORDER BY age");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        const auto* const select = dynamic_cast<const SelectStatement*>(stmt.get());
        EXPECT_EQ(select->group_by().size(), static_cast<size_t>(1));
        EXPECT_EQ(select->order_by().size(), static_cast<size_t>(1));
    }
}

TEST(ParserTest_Errors) {
    {
        auto lexer = std::make_unique<Lexer>("SELECT FROM users");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        EXPECT_TRUE(stmt == nullptr);
    }
}

// ============= Catalog Tests =============

TEST(CatalogTest_FullLifecycle) {
    auto catalog = Catalog::create();

    const std::vector<ColumnInfo> cols = {{"id", ValueType::TYPE_INT64, 0}, {"name", ValueType::TYPE_TEXT, 1}};

    const oid_t table_id = catalog->create_table("test_table", cols);
    EXPECT_TRUE(table_id > 0);
    EXPECT_TRUE(catalog->table_exists(table_id));
    EXPECT_TRUE(catalog->table_exists_by_name("test_table"));

    auto table = catalog->get_table(table_id);
    EXPECT_TRUE(table.has_value());
    EXPECT_STREQ((*table)->name, "test_table");

    catalog->update_table_stats(table_id, STATS_100);
    EXPECT_EQ((*table)->num_rows, STATS_100);

    const oid_t idx_id = catalog->create_index("test_idx", table_id, {0}, IndexType::BTree, true);
    EXPECT_TRUE(idx_id > 0);
    EXPECT_EQ(catalog->get_table_indexes(table_id).size(), static_cast<size_t>(1));

    auto idx_pair = catalog->get_index(idx_id);
    EXPECT_TRUE(idx_pair.has_value());
    EXPECT_STREQ(idx_pair->second->name, "test_idx");

    EXPECT_TRUE(catalog->drop_index(idx_id));
    EXPECT_EQ(catalog->get_table_indexes(table_id).size(), static_cast<size_t>(0));

    EXPECT_TRUE(catalog->drop_table(table_id));
    EXPECT_FALSE(catalog->table_exists(table_id));
}

// ============= Config Tests =============

TEST(ConfigTest_Basic) {
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
    EXPECT_STREQ(cfg2.data_dir, "./tmp_data");

    static_cast<void>(std::remove(cfg_file.c_str()));
}

// ============= Statement Tests =============

TEST(StatementTest_ToString) {
    const TransactionBeginStatement begin;
    EXPECT_STREQ(begin.to_string(), "BEGIN");

    const TransactionCommitStatement commit;
    EXPECT_STREQ(commit.to_string(), "COMMIT");

    const TransactionRollbackStatement rollback;
    EXPECT_STREQ(rollback.to_string(), "ROLLBACK");
}

TEST(StatementTest_Serialization) {
    {
        auto lexer = std::make_unique<Lexer>(
            "SELECT name, age FROM users WHERE age > 18 ORDER BY age LIMIT 10 OFFSET 5");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        EXPECT_STREQ(stmt->to_string(),
                     "SELECT name, age FROM users WHERE age > 18 ORDER BY age LIMIT 10 OFFSET 5");
    }
    {
        auto lexer =
            std::make_unique<Lexer>("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        EXPECT_STREQ(stmt->to_string(),
                     "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')");
    }
}

// ============= Storage Tests =============

TEST(StorageTest_Persistence) {
    const std::string filename = "persist_test";
    static_cast<void>(std::remove("./test_data/persist_test.heap"));
    Schema schema;
    schema.add_column("data", ValueType::TYPE_TEXT);
    {
        StorageManager sm("./test_data");
        HeapTable table(filename, sm, schema);
        static_cast<void>(table.create());
        static_cast<void>(table.insert(Tuple({Value::make_text("Persistent data")})));
    }
    {
        StorageManager sm("./test_data");
        HeapTable table(filename, sm, schema);
        auto iter = table.scan();
        Tuple t;
        EXPECT_TRUE(iter.next(t));
        EXPECT_STREQ(t.get(0).as_text(), "Persistent data");
    }
}

TEST(StorageTest_Delete) {
    const std::string filename = "delete_test";
    static_cast<void>(std::remove("./test_data/delete_test.heap"));
    StorageManager sm("./test_data");
    Schema schema;
    schema.add_column("id", ValueType::TYPE_INT64);
    HeapTable table(filename, sm, schema);
    EXPECT_TRUE(table.create());

    static_cast<void>(table.insert(Tuple({Value::make_int64(1)})));
    const auto tid2 = table.insert(Tuple({Value::make_int64(2)}));

    EXPECT_EQ(table.tuple_count(), static_cast<uint64_t>(2));
    EXPECT_TRUE(table.remove(tid2, XMAX_100));  // Logically delete with xmax=100
    EXPECT_EQ(table.tuple_count(), static_cast<uint64_t>(1));

    auto iter = table.scan();
    Tuple t;
    EXPECT_TRUE(iter.next(t));
    EXPECT_EQ(t.get(0).to_int64(), 1);
    EXPECT_FALSE(iter.next(t));
}

// ============= Index Tests =============

TEST(IndexTest_BTreeBasic) {
    static_cast<void>(std::remove("./test_data/idx_test.idx"));
    StorageManager sm("./test_data");
    BTreeIndex idx("idx_test", sm, ValueType::TYPE_INT64);
    static_cast<void>(idx.create());
    static_cast<void>(idx.insert(Value::make_int64(BTREE_VAL_10), HeapTable::TupleId(1, 1)));
    static_cast<void>(idx.insert(Value::make_int64(BTREE_VAL_20), HeapTable::TupleId(1, 2)));
    static_cast<void>(idx.insert(Value::make_int64(BTREE_VAL_10), HeapTable::TupleId(2, 1)));
    const auto res = idx.search(Value::make_int64(BTREE_VAL_10));
    EXPECT_EQ(res.size(), static_cast<size_t>(2));
    static_cast<void>(idx.drop());
}

TEST(IndexTest_Scan) {
    static_cast<void>(std::remove("./test_data/scan_test.idx"));
    StorageManager sm("./test_data");
    BTreeIndex idx("scan_test", sm, ValueType::TYPE_INT64);
    static_cast<void>(idx.create());
    static_cast<void>(idx.insert(Value::make_int64(1), HeapTable::TupleId(1, 1)));
    static_cast<void>(idx.insert(Value::make_int64(2), HeapTable::TupleId(1, 2)));

    auto iter = idx.scan();
    BTreeIndex::Entry entry;
    EXPECT_TRUE(iter.next(entry));
    EXPECT_EQ(entry.key.to_int64(), 1);
    EXPECT_TRUE(iter.next(entry));
    EXPECT_EQ(entry.key.to_int64(), 2);
    EXPECT_FALSE(iter.next(entry));
}

// ============= Network Tests =============

#if 0
TEST(NetworkTest_Handshake) {
    const uint16_t port = 5438;
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    auto server = network::Server::create(port, *catalog, sm);

    std::thread server_thread([&]() { static_cast<void>(server->start()); });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        throw std::runtime_error("Failed to create socket in NetworkTest_Handshake");
    }
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    static_cast<void>(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr));

    if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
        const uint32_t ssl_req[] = {htonl(8), htonl(80877103)};
        static_cast<void>(send(sock, ssl_req, 8, 0));
        char response{};
        static_cast<void>(recv(sock, &response, 1, 0));
        EXPECT_EQ(static_cast<int>(response), static_cast<int>('N'));

        const uint32_t startup[] = {htonl(8), htonl(196608)};
        static_cast<void>(send(sock, startup, 8, 0));
        char type{};
        static_cast<void>(recv(sock, &type, 1, 0));
        EXPECT_EQ(static_cast<int>(type), static_cast<int>('R'));
    }

    static_cast<void>(close(sock));
    /* Ensure server finishes handling the connection before stopping */
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    static_cast<void>(server->stop());
    if (server_thread.joinable()) { server_thread.join(); }
}

TEST(NetworkTest_MultiClient) {
    const uint16_t port = 5439;
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    auto server = network::Server::create(port, *catalog, sm);

    std::thread server_thread([&]() { static_cast<void>(server->start()); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const int num_clients = 5;
    std::vector<std::thread> clients;
    std::atomic<int> success_count{0};

    for (int i = 0; i < num_clients; ++i) {
        clients.emplace_back([&success_count]() {
            const int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) { return; }
            struct sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(5439);
            static_cast<void>(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr));

            if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
                const uint32_t startup[] = {htonl(8), htonl(196608)};
                static_cast<void>(send(sock, startup, 8, 0));
                char type{};
                if (recv(sock, &type, 1, 0) > 0 && type == 'R') {
                    success_count++;
                }
            }
            static_cast<void>(close(sock));
        });
    }

    for (auto& t : clients) { t.join(); }
    EXPECT_EQ(success_count.load(), num_clients);

    static_cast<void>(server->stop());
    if (server_thread.joinable()) { server_thread.join(); }
}
#endif

// ============= Execution Tests =============

TEST(ExecutionTest_EndToEnd) {
    static_cast<void>(std::remove("./test_data/users.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);
    QueryExecutor exec(*catalog, sm, lm, tm);

    {
        auto lexer = std::make_unique<Lexer>("CREATE TABLE users (id BIGINT, age BIGINT)");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        const auto res = exec.execute(*stmt);
        if (!res.success()) { throw std::runtime_error("CREATE failed: " + res.error()); }
    }
    {
        auto lexer =
            std::make_unique<Lexer>("INSERT INTO users (id, age) VALUES (1, 20), (2, 30), (3, 40)");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        const auto res = exec.execute(*stmt);
        if (!res.success()) { throw std::runtime_error("INSERT failed: " + res.error()); }
    }
    {
        auto lexer = std::make_unique<Lexer>("SELECT id FROM users WHERE age > 25");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        const auto res = exec.execute(*stmt);
        if (!res.success()) { throw std::runtime_error("SELECT failed: " + res.error()); }
        EXPECT_EQ(res.row_count(), static_cast<size_t>(2));
    }
}

TEST(ExecutionTest_Sort) {
    static_cast<void>(std::remove("./test_data/sort_test.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE sort_test (val INT)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO sort_test VALUES (30), (10), (20)"))
                      .parse_statement()));

    const auto res =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT val FROM sort_test ORDER BY val"))
                          .parse_statement());
    EXPECT_EQ(res.row_count(), static_cast<size_t>(3));
    EXPECT_STREQ(res.rows()[0].get(0).to_string(), "10");
    EXPECT_STREQ(res.rows()[1].get(0).to_string(), "20");
    EXPECT_STREQ(res.rows()[2].get(0).to_string(), "30");
}

TEST(ExecutionTest_Aggregate) {
    static_cast<void>(std::remove("./test_data/agg_test.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE agg_test (cat TEXT, val INT)"))
                      .parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>(
                             "INSERT INTO agg_test VALUES ('A', 10), ('A', 20), ('B', 5)"))
                      .parse_statement()));

    auto lex =
        std::make_unique<Lexer>("SELECT cat, COUNT(val), SUM(val) FROM agg_test GROUP BY cat");
    auto stmt = Parser(std::move(lex)).parse_statement();
    if (!stmt) { throw std::runtime_error("Parser failed for aggregate query"); }

    const auto res = exec.execute(*stmt);
    if (!res.success()) { throw std::runtime_error("Execution failed: " + res.error()); }

    EXPECT_EQ(res.row_count(), static_cast<size_t>(2));
    /* Row 0: 'A', 2, 30 */
    EXPECT_STREQ(res.rows()[0].get(0).to_string(), "A");
    EXPECT_STREQ(res.rows()[0].get(1).to_string(), "2");
    EXPECT_STREQ(res.rows()[0].get(2).to_string(), "30");
}

TEST(ExecutionTest_AggregateAdvanced) {
    static_cast<void>(std::remove("./test_data/adv_agg.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE adv_agg (val INT)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO adv_agg VALUES (10), (20), (30)"))
                      .parse_statement()));

    const auto res = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT MIN(val), MAX(val), AVG(val) FROM adv_agg"))
             .parse_statement());
    if (!res.success()) { throw std::runtime_error("Execution failed: " + res.error()); }

    EXPECT_EQ(res.row_count(), static_cast<size_t>(1));
    EXPECT_STREQ(res.rows()[0].get(0).to_string(), "10");
    EXPECT_STREQ(res.rows()[0].get(1).to_string(), "30");
    EXPECT_STREQ(res.rows()[0].get(2).to_string(), "20");
}

TEST(ExecutionTest_AggregateDistinct) {
    static_cast<void>(std::remove("./test_data/dist_agg.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE dist_agg (val INT)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>(
                             "INSERT INTO dist_agg VALUES (10), (10), (20), (30), (30), (30)"))
                      .parse_statement()));

    const auto res =
        exec.execute(*Parser(std::make_unique<Lexer>(
                                 "SELECT COUNT(DISTINCT val), SUM(DISTINCT val) FROM dist_agg"))
                          .parse_statement());
    if (!res.success()) { throw std::runtime_error("Execution failed: " + res.error()); }

    EXPECT_EQ(res.row_count(), static_cast<size_t>(1));
    EXPECT_STREQ(res.rows()[0].get(0).to_string(), "3");
    EXPECT_STREQ(res.rows()[0].get(1).to_string(), "60");
}

TEST(ExecutionTest_Transaction) {
    static_cast<void>(std::remove("./test_data/txn_test.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);

    QueryExecutor qexec1(*catalog, sm, lm, tm);
    static_cast<void>(qexec1.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE txn_test (id INT, val INT)"))
                       .parse_statement()));

    static_cast<void>(qexec1.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(qexec1.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO txn_test VALUES (1, 100)")).parse_statement()));

    QueryExecutor qexec2(*catalog, sm, lm, tm);

    const auto res_commit = qexec1.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement());
    EXPECT_TRUE(res_commit.success());

    const auto res_select =
        qexec2.execute(*Parser(std::make_unique<Lexer>("SELECT val FROM txn_test WHERE id = 1"))
                           .parse_statement());
    EXPECT_EQ(res_select.row_count(), static_cast<size_t>(1));
    EXPECT_STREQ(res_select.rows()[0].get(0).to_string(), "100");
}

TEST(ExecutionTest_Rollback) {
    static_cast<void>(std::remove("./test_data/rollback_test.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE rollback_test (val INT)")).parse_statement()));

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO rollback_test VALUES (100)"))
                      .parse_statement()));

    const auto res_internal = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM rollback_test")).parse_statement());
    EXPECT_EQ(res_internal.row_count(), static_cast<size_t>(1));

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));

    const auto res_after = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM rollback_test")).parse_statement());
    EXPECT_EQ(res_after.row_count(), static_cast<size_t>(0));
}

TEST(ExecutionTest_UpdateDelete) {
    static_cast<void>(std::remove("./test_data/upd_test.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE upd_test (id INT, val TEXT)"))
                      .parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO upd_test VALUES (1, 'old'), (2, 'stay')"))
             .parse_statement()));

    /* Test UPDATE */
    const auto res_upd = exec.execute(
        *Parser(std::make_unique<Lexer>("UPDATE upd_test SET val = 'new' WHERE id = 1"))
             .parse_statement());
    EXPECT_EQ(res_upd.rows_affected(), static_cast<uint64_t>(1));

    const auto res_sel =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT val FROM upd_test WHERE id = 1"))
                          .parse_statement());
    EXPECT_EQ(res_sel.row_count(), static_cast<size_t>(1));
    EXPECT_STREQ(res_sel.rows()[0].get(0).to_string(), "new");

    /* Test DELETE */
    const auto res_del = exec.execute(
        *Parser(std::make_unique<Lexer>("DELETE FROM upd_test WHERE id = 2")).parse_statement());
    EXPECT_EQ(res_del.rows_affected(), static_cast<uint64_t>(1));

    const auto res_sel2 =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT id FROM upd_test")).parse_statement());
    EXPECT_EQ(res_sel2.row_count(), static_cast<size_t>(1));  // Only ID 1 remains
}

TEST(ExecutionTest_MVCC) {
    static_cast<void>(std::remove("./test_data/mvcc_test.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);

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
    EXPECT_EQ(res2_pre.row_count(), static_cast<size_t>(0));

    /* T1 updates row */
    static_cast<void>(qexec1.execute(
        *Parser(std::make_unique<Lexer>("UPDATE mvcc_test SET val = 20")).parse_statement()));

    /* T1 sees new value */
    const auto res1 = qexec1.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM mvcc_test")).parse_statement());
    EXPECT_EQ(res1.row_count(), static_cast<size_t>(1));
    EXPECT_STREQ(res1.rows()[0].get(0).to_string(), "20");

    static_cast<void>(qexec1.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    /* After commit, Session 2 sees the latest value */
    const auto res2_post = qexec2.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM mvcc_test")).parse_statement());
    EXPECT_EQ(res2_post.row_count(), static_cast<size_t>(1));
    EXPECT_STREQ(res2_post.rows()[0].get(0).to_string(), "20");
}

TEST(ExecutionTest_Join) {
    static_cast<void>(std::remove("./test_data/users.heap"));
    static_cast<void>(std::remove("./test_data/orders.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);
    QueryExecutor exec(*catalog, sm, lm, tm);

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE users (id INT, name TEXT)"))
                      .parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE orders (id INT, user_id INT, amount DOUBLE)"))
             .parse_statement()));

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"))
             .parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>(
                    "INSERT INTO orders VALUES (101, 1, 50.5), (102, 1, 25.0), (103, 2, 100.0)"))
             .parse_statement()));

    /* Test: INNER JOIN with sorting */
    const auto result = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT users.name, orders.amount FROM users JOIN orders "
                                        "ON users.id = orders.user_id ORDER BY orders.amount"))
             .parse_statement());

    EXPECT_EQ(result.row_count(), static_cast<size_t>(3));

    /* 25.0 (Alice), 50.5 (Alice), 100.0 (Bob) */
    EXPECT_STREQ(result.rows()[0].get(0).to_string(), "Alice");
    EXPECT_STREQ(result.rows()[0].get(1).to_string(), "25");
    EXPECT_STREQ(result.rows()[2].get(0).to_string(), "Bob");
    EXPECT_STREQ(result.rows()[2].get(1).to_string(), "100");
}

TEST(ExecutionTest_DDL) {
    static_cast<void>(std::remove("./test_data/ddl_test.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);
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
    // Note: Our system doesn't have a direct "CREATE INDEX" statement parsing yet,
    // but the catalog supports it. For now we just test that DROP INDEX works if index exists.
    auto table_opt = catalog->get_table_by_name("ddl_test");
    if (table_opt) {
        const oid_t tid = (*table_opt)->table_id;
        static_cast<void>(catalog->create_index("idx_ddl", tid, {0}, IndexType::BTree, true));
    }

    const auto res_drop_idx =
        exec.execute(*Parser(std::make_unique<Lexer>("DROP INDEX idx_ddl")).parse_statement());
    EXPECT_TRUE(res_drop_idx.success());
}

TEST(LexerTest_Advanced) {
    /* 1. Test comments and line tracking */
    {
        const std::string sql = "SELECT -- comment here\n* FROM users";
        Lexer lexer(sql);
        const auto t1 = lexer.next_token();
        EXPECT_EQ(static_cast<int>(t1.type()), static_cast<int>(TokenType::Select));
        const auto t2 = lexer.next_token();  // Should skip comment and newline
        EXPECT_STREQ(t2.lexeme(), "*");
        EXPECT_EQ(t2.line(), static_cast<uint32_t>(2));
    }
    /* 2. Test Error and Unknown operators */
    {
        Lexer lexer("@");
        const auto t = lexer.next_token();
        EXPECT_EQ(static_cast<int>(t.type()), static_cast<int>(TokenType::Error));
    }
}

TEST(ExecutionTest_Expressions) {
    static_cast<void>(std::remove("./test_data/expr_test.heap"));
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, sm);
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
        EXPECT_EQ(res.row_count(), static_cast<size_t>(1));
        EXPECT_EQ(res.rows()[0].get(0).to_int64(), static_cast<int64_t>(2)); // NOLINT(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

        const auto res2 = exec.execute(
            *Parser(std::make_unique<Lexer>("SELECT id FROM expr_test WHERE val IS NOT NULL"))
                 .parse_statement());
        EXPECT_EQ(res2.row_count(), static_cast<size_t>(2));
    }

    /* 2. Test IN / NOT IN */
    {
        const auto res = exec.execute(
            *Parser(std::make_unique<Lexer>("SELECT id FROM expr_test WHERE id IN (1, 3)"))
                 .parse_statement());
        EXPECT_EQ(res.row_count(), static_cast<size_t>(2));

        const auto res2 = exec.execute(
            *Parser(std::make_unique<Lexer>("SELECT id FROM expr_test WHERE str NOT IN ('A', 'C')"))
                 .parse_statement());
        EXPECT_EQ(res2.row_count(), static_cast<size_t>(1));
        EXPECT_EQ(res2.rows()[0].get(0).to_int64(), static_cast<int64_t>(2)); // NOLINT(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    }

    /* 3. Test Arithmetic and Complex Binary */
    {
        const auto res = exec.execute(
            *Parser(std::make_unique<Lexer>(
                        "SELECT id, val * 2 + 10, val / 2, val - 5 FROM expr_test WHERE id = 1"))
                 .parse_statement());
        EXPECT_DOUBLE_EQ(res.rows()[0].get(1).to_float64(), 31.0); // NOLINT(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
        EXPECT_DOUBLE_EQ(res.rows()[0].get(2).to_float64(), 5.25); // NOLINT(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
        EXPECT_DOUBLE_EQ(res.rows()[0].get(3).to_float64(), 5.5);  // NOLINT(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    }
}

TEST(ExpressionTest_Types) {
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

TEST(CatalogTest_Errors) {
    auto catalog = Catalog::create();
    const std::vector<ColumnInfo> cols = {{"id", ValueType::TYPE_INT64, 0}};

    static_cast<void>(catalog->create_table("fail_test", cols));
    /* Duplicate table */
    try {
        static_cast<void>(catalog->create_table("fail_test", cols));
    } catch (const std::exception& e) {
        static_cast<void>(e.what());
    }

    /* Missing table */
    EXPECT_FALSE(catalog->table_exists(TABLE_9999));
    EXPECT_FALSE(catalog->get_table(TABLE_9999).has_value());
    EXPECT_FALSE(catalog->table_exists_by_name("non_existent"));

    /* Duplicate index */
    const oid_t tid = catalog->create_table("idx_fail", cols);
    static_cast<void>(catalog->create_index("my_idx", tid, {0}, IndexType::BTree, true));
    try {
        static_cast<void>(catalog->create_index("my_idx", tid, {0}, IndexType::BTree, true));
    } catch (const std::exception& e) {
        static_cast<void>(e.what());
    }

    /* Missing index */
    EXPECT_FALSE(catalog->get_index(INDEX_8888).has_value());
    EXPECT_FALSE(catalog->drop_index(INDEX_8888));
}

TEST(CatalogTest_Stats) {
    auto catalog = Catalog::create();
    const std::vector<ColumnInfo> cols = {{"id", ValueType::TYPE_INT64, 0}};
    const oid_t tid = catalog->create_table("stats_test", cols);

    EXPECT_TRUE(catalog->update_table_stats(tid, STATS_500));
    auto tinfo = catalog->get_table(tid);
    if (tinfo) {
        EXPECT_EQ((*tinfo)->num_rows, STATS_500);
    }

    /* Cover print() */
    catalog->print();
}

}  // namespace

int main() {
    std::cout << "cloudSQL C++ Test Suite\n";
    std::cout << "========================\n\n";

    RUN_TEST(ValueTest_Basic);
    RUN_TEST(ValueTest_TypeVariety);
    RUN_TEST(ParserTest_Expressions);
    RUN_TEST(LexerTest_Advanced);
    RUN_TEST(ExpressionTest_Complex);
    RUN_TEST(ParserTest_SelectVariants);
    RUN_TEST(ParserTest_Errors);
    RUN_TEST(CatalogTest_FullLifecycle);
    RUN_TEST(CatalogTest_Errors);
    RUN_TEST(ConfigTest_Basic);
    RUN_TEST(StatementTest_ToString);
    RUN_TEST(StatementTest_Serialization);
    RUN_TEST(StorageTest_Persistence);
    RUN_TEST(StorageTest_Delete);
    RUN_TEST(IndexTest_BTreeBasic);
    RUN_TEST(IndexTest_Scan);
    RUN_TEST(ExecutionTest_EndToEnd);
    RUN_TEST(ExecutionTest_Sort);
    RUN_TEST(ExecutionTest_Aggregate);
    RUN_TEST(ExecutionTest_AggregateAdvanced);
    RUN_TEST(ExecutionTest_AggregateDistinct);
    RUN_TEST(ExecutionTest_Transaction);
    RUN_TEST(ExecutionTest_Rollback);
    RUN_TEST(ExecutionTest_UpdateDelete);
    RUN_TEST(ExecutionTest_MVCC);
    RUN_TEST(ExecutionTest_Join);
    RUN_TEST(ExecutionTest_DDL);
    RUN_TEST(ExecutionTest_Expressions);
    RUN_TEST(ExpressionTest_Types);
    RUN_TEST(CatalogTest_Stats);

    std::cout << "\n========================\n";
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed\n";

    return (tests_failed > 0);
}
