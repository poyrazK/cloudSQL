/**
 * @file duckdb_comparison_bench.cpp
 * @brief Performance comparison between cloudSQL and DuckDB
 */

#include <benchmark/benchmark.h>
#include <duckdb.hpp>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "executor/query_executor.hpp"
#include "parser/parser.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::storage;
using namespace cloudsql::executor;
using namespace cloudsql::parser;

namespace {

// Helper to parse SQL string into a Statement
std::unique_ptr<Statement> ParseSQL(const std::string& sql) {
    auto lexer = std::make_unique<Lexer>(sql);
    Parser parser(std::move(lexer));
    return parser.parse_statement();
}

// --- cloudSQL Setup ---
struct CloudSQLContext {
    std::string test_dir;
    std::unique_ptr<StorageManager> storage;
    std::unique_ptr<BufferPoolManager> bpm;
    std::unique_ptr<Catalog> catalog;
    std::unique_ptr<transaction::LockManager> lock_manager;
    std::unique_ptr<transaction::TransactionManager> txn_manager;
    std::unique_ptr<QueryExecutor> executor;

    CloudSQLContext(const std::string& dir) : test_dir(dir) {
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);
        storage = std::make_unique<StorageManager>(test_dir);
        bpm = std::make_unique<BufferPoolManager>(4096, *storage);
        catalog = std::make_unique<Catalog>();
        lock_manager = std::make_unique<transaction::LockManager>();
        txn_manager = std::make_unique<transaction::TransactionManager>(*lock_manager, *catalog, *bpm);
        executor = std::make_unique<QueryExecutor>(*catalog, *bpm, *lock_manager, *txn_manager);
        executor->set_local_only(true);

        // Create lineitem table (TPC-H schema, simplified)
        CreateTableStatement create_stmt;
        create_stmt.set_table_name("lineitem");
        create_stmt.add_column("l_orderkey", "BIGINT");
        create_stmt.add_column("l_partkey", "BIGINT");
        create_stmt.add_column("l_quantity", "INT");
        create_stmt.add_column("l_extendedprice", "DOUBLE");
        create_stmt.add_column("l_discount", "DOUBLE");
        create_stmt.add_column("l_tax", "DOUBLE");
        executor->execute(create_stmt);
    }

    ~CloudSQLContext() {
        executor.reset();
        txn_manager.reset();
        lock_manager.reset();
        catalog.reset();
        bpm.reset();
        storage.reset();
        std::filesystem::remove_all(test_dir);
    }
};

// --- DuckDB Setup ---
struct DuckDBContext {
    duckdb::DuckDB db;
    duckdb::Connection conn;

    DuckDBContext() : db(":memory:"), conn(db) {
        conn.Query(
            "CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey BIGINT, l_quantity INT, "
            "l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE)");
    }

    ~DuckDBContext() {}
};

}  // anonymous namespace

// --- Benchmark 1: cloudSQL Lineitem Aggregation (Q1-like) ---
static void BM_CloudSQL_Q1(benchmark::State& state) {
    const int num_rows = state.range(0);
    CloudSQLContext ctx("./bench_cloudsql_q1_" + std::to_string(state.thread_index()));

    // Populate
    ctx.executor->execute("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.executor->execute(*ParseSQL(
            "INSERT INTO lineitem VALUES (" + std::to_string(i % 1000) + ", " +
            std::to_string(i % 100) + ", " + std::to_string(1 + (i % 10)) + ", " +
            "1000.0, 0.05, 0.02);"));
    }
    ctx.executor->execute("COMMIT");

    for (auto _ : state) {
        auto result = ctx.executor->execute(
            *ParseSQL("SELECT l_quantity, SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY "
                      "l_quantity"));
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_CloudSQL_Q1)->Arg(10000)->Arg(100000);

// --- Benchmark 2: DuckDB Lineitem Aggregation (Q1-like) ---
static void BM_DuckDB_Q1(benchmark::State& state) {
    const int num_rows = state.range(0);
    DuckDBContext ctx;

    // Populate
    for (int i = 0; i < num_rows; ++i) {
        ctx.conn.Query("INSERT INTO lineitem VALUES (" + std::to_string(i % 1000) + ", " +
                      std::to_string(i % 100) + ", " + std::to_string(1 + (i % 10)) + ", " +
                      "1000.0, 0.05, 0.02)");
    }

    for (auto _ : state) {
        auto result = ctx.conn.Query(
            "SELECT l_quantity, SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY "
            "l_quantity");
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_DuckDB_Q1)->Arg(10000)->Arg(100000);

// --- Benchmark 3: cloudSQL Scan with Filter (Q6-like) ---
static void BM_CloudSQL_Q6(benchmark::State& state) {
    const int num_rows = state.range(0);
    CloudSQLContext ctx("./bench_cloudsql_q6_" + std::to_string(state.thread_index()));

    // Populate
    ctx.executor->execute("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.executor->execute(*ParseSQL(
            "INSERT INTO lineitem VALUES (" + std::to_string(i) + ", " +
            std::to_string(i % 100) + ", " + std::to_string(1 + (i % 10)) + ", " +
            "1000.0, 0.05, 0.02);"));
    }
    ctx.executor->execute("COMMIT");

    for (auto _ : state) {
        auto result = ctx.executor->execute(*ParseSQL(
            "SELECT SUM(l_extendedprice) FROM lineitem WHERE l_discount BETWEEN 0.04 AND 0.06 AND "
            "l_quantity < 25"));
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_CloudSQL_Q6)->Arg(10000)->Arg(100000);

// --- Benchmark 4: DuckDB Scan with Filter (Q6-like) ---
static void BM_DuckDB_Q6(benchmark::State& state) {
    const int num_rows = state.range(0);
    DuckDBContext ctx;

    // Populate
    for (int i = 0; i < num_rows; ++i) {
        ctx.conn.Query("INSERT INTO lineitem VALUES (" + std::to_string(i) + ", " +
                      std::to_string(i % 100) + ", " + std::to_string(1 + (i % 10)) + ", " +
                      "1000.0, 0.05, 0.02)");
    }

    for (auto _ : state) {
        auto result = ctx.conn.Query(
            "SELECT SUM(l_extendedprice) FROM lineitem WHERE l_discount BETWEEN 0.04 AND 0.06 AND "
            "l_quantity < 25");
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_DuckDB_Q6)->Arg(10000)->Arg(100000);

// --- Benchmark 5: cloudSQL Simple Join (simplified Q3-like) ---
static void BM_CloudSQL_Join(benchmark::State& state) {
    const int num_rows = state.range(0);
    CloudSQLContext ctx("./bench_cloudsql_join_" + std::to_string(state.thread_index()));

    // Create orders table
    ctx.executor->execute(*ParseSQL("CREATE TABLE orders (o_orderkey BIGINT, o_custkey BIGINT, "
                                    "o_orderdate TEXT)"));

    // Populate orders
    ctx.executor->execute("BEGIN");
    for (int i = 0; i < num_rows / 10; ++i) {
        ctx.executor->execute(*ParseSQL("INSERT INTO orders VALUES (" + std::to_string(i) +
                                        ", " + std::to_string(i % 100) + ", '2024-01-01')"));
    }
    // Populate lineitem
    for (int i = 0; i < num_rows; ++i) {
        ctx.executor->execute(*ParseSQL("INSERT INTO lineitem VALUES (" +
                                        std::to_string(i % (num_rows / 10)) + ", " +
                                        std::to_string(i % 100) + ", " +
                                        std::to_string(1 + (i % 10)) + ", " +
                                        "1000.0, 0.05, 0.02)"));
    }
    ctx.executor->execute("COMMIT");

    for (auto _ : state) {
        auto result = ctx.executor->execute(*ParseSQL(
            "SELECT o.o_orderkey, SUM(l.l_extendedprice) FROM orders o JOIN lineitem l ON "
            "o.o_orderkey = l.l_orderkey GROUP BY o.o_orderkey"));
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_CloudSQL_Join)->Arg(10000)->Arg(50000);

// --- Benchmark 6: DuckDB Simple Join (simplified Q3-like) ---
static void BM_DuckDB_Join(benchmark::State& state) {
    const int num_rows = state.range(0);
    DuckDBContext ctx;

    // Create orders table
    ctx.conn.Query(
        "CREATE TABLE orders (o_orderkey BIGINT, o_custkey BIGINT, o_orderdate TEXT)");

    // Populate orders
    for (int i = 0; i < num_rows / 10; ++i) {
        ctx.conn.Query("INSERT INTO orders VALUES (" + std::to_string(i) + ", " +
                       std::to_string(i % 100) + ", '2024-01-01')");
    }
    // Populate lineitem
    for (int i = 0; i < num_rows; ++i) {
        ctx.conn.Query("INSERT INTO lineitem VALUES (" + std::to_string(i % (num_rows / 10)) + ", " +
                       std::to_string(i % 100) + ", " + std::to_string(1 + (i % 10)) + ", " +
                       "1000.0, 0.05, 0.02)");
    }

    for (auto _ : state) {
        auto result = ctx.conn.Query(
            "SELECT o.o_orderkey, SUM(l.l_extendedprice) FROM orders o JOIN lineitem l ON "
            "o.o_orderkey = l.l_orderkey GROUP BY o.o_orderkey");
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_DuckDB_Join)->Arg(10000)->Arg(50000);

// BENCHMARK_MAIN() is provided by benchmark::benchmark_main (linked via benchmark_main)
BENCHMARK_MAIN();