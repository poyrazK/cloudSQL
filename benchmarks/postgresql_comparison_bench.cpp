/**
 * @file postgresql_comparison_bench.cpp
 * @brief Performance comparison between cloudSQL and PostgreSQL
 */

#include <benchmark/benchmark.h>
#include <filesystem>
#include <libpq-fe.h>
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

// --- PostgreSQL Connection Context ---
struct PostgreSQLContext {
    PGconn* conn;

    PostgreSQLContext() {
        const char* host = std::getenv("PGHOST") ? std::getenv("PGHOST") : "localhost";
        const char* port = std::getenv("PGPORT") ? std::getenv("PGPORT") : "5432";
        const char* dbname = std::getenv("PGDATABASE") ? std::getenv("PGDATABASE") : "postgres";
        const char* user = std::getenv("PGUSER") ? std::getenv("PGUSER") : "postgres";

        std::string conninfo = "host=" + std::string(host) + " port=" + std::string(port) +
                              " dbname=" + std::string(dbname) + " user=" + std::string(user);
        conn = PQconnectdb(conninfo.c_str());

        if (PQstatus(conn) != CONNECTION_OK) {
            fprintf(stderr, "PostgreSQL connection failed: %s\n", PQerrorMessage(conn));
            PQfinish(conn);
            conn = nullptr;
        }
    }

    ~PostgreSQLContext() {
        if (conn) {
            PQfinish(conn);
        }
    }

    void create_tables() {
        if (!conn) return;
        PGresult* r = PQexec(conn, "SET max_parallel_workers_per_gather = 0");
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            fprintf(stderr, "SET max_parallel_workers_per_gather failed: %s\n", PQerrorMessage(conn));
        }
        PQclear(r);
        r = PQexec(conn, "SET max_parallel_workers = 0");
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            fprintf(stderr, "SET max_parallel_workers failed: %s\n", PQerrorMessage(conn));
        }
        PQclear(r);
        r = PQexec(conn, "SET max_parallel_maintenance_workers = 0");
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            fprintf(stderr, "SET max_parallel_maintenance_workers failed: %s\n", PQerrorMessage(conn));
        }
        PQclear(r);
        r = PQexec(conn, "DROP TABLE IF EXISTS lineitem");
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            fprintf(stderr, "DROP TABLE lineitem failed: %s\n", PQerrorMessage(conn));
        }
        PQclear(r);
        r = PQexec(conn, "DROP TABLE IF EXISTS orders");
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            fprintf(stderr, "DROP TABLE orders failed: %s\n", PQerrorMessage(conn));
        }
        PQclear(r);
        r = PQexec(conn,
               "CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey BIGINT, "
               "l_quantity INT, l_extendedprice DOUBLE PRECISION, l_discount DOUBLE PRECISION, "
               "l_tax DOUBLE PRECISION)");
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            fprintf(stderr, "CREATE TABLE lineitem failed: %s\n", PQerrorMessage(conn));
        }
        PQclear(r);
        r = PQexec(conn,
               "CREATE TABLE orders (o_orderkey BIGINT, o_custkey BIGINT, "
               "o_orderdate TEXT)");
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            fprintf(stderr, "CREATE TABLE orders failed: %s\n", PQerrorMessage(conn));
        }
        PQclear(r);
    }

    void execute_sql(const std::string& sql) {
        if (!conn) return;
        PGresult* res = PQexec(conn, sql.c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "SQL execution failed: %s\n", PQerrorMessage(conn));
        }
        PQclear(res);
    }
};

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
        executor->set_storage_manager(storage.get());

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

        // Create orders table
        CreateTableStatement orders_stmt;
        orders_stmt.set_table_name("orders");
        orders_stmt.add_column("o_orderkey", "BIGINT");
        orders_stmt.add_column("o_custkey", "BIGINT");
        orders_stmt.add_column("o_orderdate", "TEXT");
        executor->execute(orders_stmt);
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

}  // anonymous namespace

// ============== OLTP BENCHMARKS ==============

// --- Benchmark: PostgreSQL INSERT ---
static void BM_PostgreSQL_Insert(benchmark::State& state) {
    const int num_rows = state.range(0);
    PostgreSQLContext ctx;

    if (!ctx.conn) {
        state.SkipWithError("PostgreSQL not available");
        return;
    }

    ctx.create_tables();

    for (auto _ : state) {
        // Clear table at start of each iteration to measure insert throughput
        // without accumulation effects
        ctx.execute_sql("TRUNCATE TABLE lineitem");
        ctx.execute_sql("BEGIN");
        for (int i = 0; i < num_rows; ++i) {
            std::string sql = "INSERT INTO lineitem VALUES (" + std::to_string(i) + ", " +
                             std::to_string(i % 100) + ", " + std::to_string(1 + (i % 10)) +
                             ", 1000.0, 0.05, 0.02)";
            ctx.execute_sql(sql);
        }
        ctx.execute_sql("COMMIT");
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_PostgreSQL_Insert)->Arg(1000)->Arg(10000);

// --- Benchmark: cloudSQL INSERT ---
static void BM_CloudSQL_Insert(benchmark::State& state) {
    const int num_rows = state.range(0);
    CloudSQLContext ctx("./bench_pg_insert_" + std::to_string(state.thread_index()));

    for (auto _ : state) {
        // Clear table at start of each iteration to measure insert throughput
        // without accumulation effects
        ctx.executor->execute(*ParseSQL("TRUNCATE TABLE lineitem"));
        ctx.executor->execute("BEGIN");
        for (int i = 0; i < num_rows; ++i) {
            ctx.executor->execute(*ParseSQL("INSERT INTO lineitem VALUES (" + std::to_string(i) +
                                           ", " + std::to_string(i % 100) + ", " +
                                           std::to_string(1 + (i % 10)) + ", 1000.0, 0.05, 0.02)"));
        }
        ctx.executor->execute("COMMIT");
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_CloudSQL_Insert)->Arg(1000)->Arg(10000);

// --- Benchmark: PostgreSQL UPDATE ---
static void BM_PostgreSQL_Update(benchmark::State& state) {
    const int num_rows = state.range(0);
    PostgreSQLContext ctx;

    if (!ctx.conn) {
        state.SkipWithError("PostgreSQL not available");
        return;
    }

    ctx.create_tables();

    // Populate first
    ctx.execute_sql("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.execute_sql("INSERT INTO lineitem VALUES (" + std::to_string(i) + ", " +
                        std::to_string(i % 100) + ", " + std::to_string(1 + (i % 10)) +
                        ", 1000.0, 0.05, 0.02)");
    }
    ctx.execute_sql("COMMIT");

    for (auto _ : state) {
        ctx.execute_sql("BEGIN");
        for (int i = 0; i < num_rows; ++i) {
            ctx.execute_sql("UPDATE lineitem SET l_quantity = " + std::to_string(i % 20) +
                           " WHERE l_orderkey = " + std::to_string(i));
        }
        ctx.execute_sql("COMMIT");
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_PostgreSQL_Update)->Arg(1000)->Arg(10000);

// --- Benchmark: cloudSQL UPDATE ---
static void BM_CloudSQL_Update(benchmark::State& state) {
    const int num_rows = state.range(0);
    CloudSQLContext ctx("./bench_pg_update_" + std::to_string(state.thread_index()));

    // Populate first
    ctx.executor->execute("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.executor->execute(*ParseSQL("INSERT INTO lineitem VALUES (" + std::to_string(i) +
                                        ", " + std::to_string(i % 100) + ", " +
                                        std::to_string(1 + (i % 10)) + ", 1000.0, 0.05, 0.02)"));
    }
    ctx.executor->execute("COMMIT");

    for (auto _ : state) {
        ctx.executor->execute("BEGIN");
        for (int i = 0; i < num_rows; ++i) {
            ctx.executor->execute(*ParseSQL("UPDATE lineitem SET l_quantity = " +
                                            std::to_string(i % 20) + " WHERE l_orderkey = " +
                                            std::to_string(i)));
        }
        ctx.executor->execute("COMMIT");
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_CloudSQL_Update)->Arg(1000)->Arg(10000);

// --- Benchmark: PostgreSQL Point SELECT ---
static void BM_PostgreSQL_PointSelect(benchmark::State& state) {
    const int num_rows = state.range(0);
    PostgreSQLContext ctx;

    if (!ctx.conn) {
        state.SkipWithError("PostgreSQL not available");
        return;
    }

    ctx.create_tables();

    // Populate
    ctx.execute_sql("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.execute_sql("INSERT INTO lineitem VALUES (" + std::to_string(i) + ", " +
                        std::to_string(i % 100) + ", " + std::to_string(1 + (i % 10)) +
                        ", 1000.0, 0.05, 0.02)");
    }
    ctx.execute_sql("COMMIT");

    for (auto _ : state) {
        for (int i = 0; i < num_rows; ++i) {
            std::string sql = "SELECT * FROM lineitem WHERE l_orderkey = " + std::to_string(i);
            ctx.execute_sql(sql);
        }
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_PostgreSQL_PointSelect)->Arg(1000)->Arg(10000);

// --- Benchmark: cloudSQL Point SELECT ---
static void BM_CloudSQL_PointSelect(benchmark::State& state) {
    const int num_rows = state.range(0);
    CloudSQLContext ctx("./bench_pg_point_" + std::to_string(state.thread_index()));

    // Populate
    ctx.executor->execute("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.executor->execute(*ParseSQL("INSERT INTO lineitem VALUES (" + std::to_string(i) +
                                        ", " + std::to_string(i % 100) + ", " +
                                        std::to_string(1 + (i % 10)) + ", 1000.0, 0.05, 0.02)"));
    }
    ctx.executor->execute("COMMIT");

    for (auto _ : state) {
        for (int i = 0; i < num_rows; ++i) {
            ctx.executor->execute(*ParseSQL("SELECT * FROM lineitem WHERE l_orderkey = " +
                                            std::to_string(i)));
        }
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_CloudSQL_PointSelect)->Arg(1000)->Arg(10000);

// ============== ANALYTICAL BENCHMARKS ==============

// --- Benchmark: PostgreSQL Full Scan ---
static void BM_PostgreSQL_FullScan(benchmark::State& state) {
    const int num_rows = state.range(0);
    PostgreSQLContext ctx;

    if (!ctx.conn) {
        state.SkipWithError("PostgreSQL not available");
        return;
    }

    ctx.create_tables();

    // Populate
    ctx.execute_sql("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.execute_sql("INSERT INTO lineitem VALUES (" + std::to_string(i) + ", " +
                        std::to_string(i % 100) + ", " + std::to_string(1 + (i % 10)) +
                        ", 1000.0, 0.05, 0.02)");
    }
    ctx.execute_sql("COMMIT");

    for (auto _ : state) {
        ctx.execute_sql("SELECT * FROM lineitem");
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_PostgreSQL_FullScan)->Arg(10000)->Arg(100000);

// --- Benchmark: cloudSQL Full Scan ---
static void BM_CloudSQL_FullScan(benchmark::State& state) {
    const int num_rows = state.range(0);
    CloudSQLContext ctx("./bench_pg_fullscan_" + std::to_string(state.thread_index()));

    // Populate
    ctx.executor->execute("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.executor->execute(*ParseSQL("INSERT INTO lineitem VALUES (" + std::to_string(i) +
                                        ", " + std::to_string(i % 100) + ", " +
                                        std::to_string(1 + (i % 10)) + ", 1000.0, 0.05, 0.02)"));
    }
    ctx.executor->execute("COMMIT");

    for (auto _ : state) {
        ctx.executor->execute(*ParseSQL("SELECT * FROM lineitem"));
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_CloudSQL_FullScan)->Arg(10000)->Arg(100000);

// --- Benchmark: PostgreSQL GROUP BY ---
static void BM_PostgreSQL_GroupBy(benchmark::State& state) {
    const int num_rows = state.range(0);
    PostgreSQLContext ctx;

    if (!ctx.conn) {
        state.SkipWithError("PostgreSQL not available");
        return;
    }

    ctx.create_tables();

    // Populate
    ctx.execute_sql("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.execute_sql("INSERT INTO lineitem VALUES (" + std::to_string(i) + ", " +
                        std::to_string(i % 100) + ", " + std::to_string(1 + (i % 10)) +
                        ", 1000.0, 0.05, 0.02)");
    }
    ctx.execute_sql("COMMIT");

    for (auto _ : state) {
        ctx.execute_sql("SELECT l_quantity, SUM(l_extendedprice) FROM lineitem GROUP BY l_quantity");
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_PostgreSQL_GroupBy)->Arg(10000)->Arg(100000);

// --- Benchmark: cloudSQL GROUP BY ---
static void BM_CloudSQL_GroupBy(benchmark::State& state) {
    const int num_rows = state.range(0);
    CloudSQLContext ctx("./bench_pg_groupby_" + std::to_string(state.thread_index()));

    // Populate
    ctx.executor->execute("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.executor->execute(*ParseSQL("INSERT INTO lineitem VALUES (" + std::to_string(i) +
                                        ", " + std::to_string(i % 100) + ", " +
                                        std::to_string(1 + (i % 10)) + ", 1000.0, 0.05, 0.02)"));
    }
    ctx.executor->execute("COMMIT");

    // Prepare the query once to test plan caching
    auto prepared = ctx.executor->prepare(
        "SELECT l_quantity, SUM(l_extendedprice) FROM lineitem GROUP BY l_quantity");

    for (auto _ : state) {
        ctx.executor->execute(*prepared, {});
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_CloudSQL_GroupBy)->Arg(10000)->Arg(100000);

// --- Benchmark: PostgreSQL JOIN ---
static void BM_PostgreSQL_Join(benchmark::State& state) {
    const int num_rows = state.range(0);
    PostgreSQLContext ctx;

    if (!ctx.conn) {
        state.SkipWithError("PostgreSQL not available");
        return;
    }

    ctx.create_tables();

    // Populate
    ctx.execute_sql("BEGIN");
    for (int i = 0; i < num_rows / 10; ++i) {
        ctx.execute_sql("INSERT INTO orders VALUES (" + std::to_string(i) + ", " +
                        std::to_string(i % 100) + ", '2024-01-01')");
    }
    for (int i = 0; i < num_rows; ++i) {
        ctx.execute_sql("INSERT INTO lineitem VALUES (" + std::to_string(i % (num_rows / 10)) +
                        ", " + std::to_string(i % 100) + ", " +
                        std::to_string(1 + (i % 10)) + ", 1000.0, 0.05, 0.02)");
    }
    ctx.execute_sql("COMMIT");

    for (auto _ : state) {
        ctx.execute_sql(
            "SELECT o.o_orderkey, SUM(l.l_extendedprice) FROM orders o JOIN lineitem l ON "
            "o.o_orderkey = l.l_orderkey GROUP BY o.o_orderkey");
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_PostgreSQL_Join)->Arg(10000)->Arg(50000);

// --- Benchmark: cloudSQL JOIN ---
static void BM_CloudSQL_Join(benchmark::State& state) {
    const int num_rows = state.range(0);
    CloudSQLContext ctx("./bench_pg_join_" + std::to_string(state.thread_index()));

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
                                        std::to_string(1 + (i % 10)) + ", 1000.0, 0.05, 0.02)"));
    }
    ctx.executor->execute("COMMIT");

    for (auto _ : state) {
        ctx.executor->execute(*ParseSQL(
            "SELECT o.o_orderkey, SUM(l.l_extendedprice) FROM orders o JOIN lineitem l ON "
            "o.o_orderkey = l.l_orderkey GROUP BY o.o_orderkey"));
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_CloudSQL_Join)->Arg(10000)->Arg(50000);

// --- Benchmark: PostgreSQL Complex WHERE ---
static void BM_PostgreSQL_ComplexWhere(benchmark::State& state) {
    const int num_rows = state.range(0);
    PostgreSQLContext ctx;

    if (!ctx.conn) {
        state.SkipWithError("PostgreSQL not available");
        return;
    }

    ctx.create_tables();

    // Populate
    ctx.execute_sql("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.execute_sql("INSERT INTO lineitem VALUES (" + std::to_string(i) + ", " +
                        std::to_string(i % 100) + ", " + std::to_string(1 + (i % 10)) +
                        ", 1000.0, 0.05, 0.02)");
    }
    ctx.execute_sql("COMMIT");

    for (auto _ : state) {
        ctx.execute_sql(
            "SELECT * FROM lineitem WHERE l_quantity > 5 AND l_discount < 0.06");
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_PostgreSQL_ComplexWhere)->Arg(10000)->Arg(100000);

// --- Benchmark: cloudSQL Complex WHERE ---
static void BM_CloudSQL_ComplexWhere(benchmark::State& state) {
    const int num_rows = state.range(0);
    CloudSQLContext ctx("./bench_pg_where_" + std::to_string(state.thread_index()));

    // Populate
    ctx.executor->execute("BEGIN");
    for (int i = 0; i < num_rows; ++i) {
        ctx.executor->execute(*ParseSQL("INSERT INTO lineitem VALUES (" + std::to_string(i) +
                                        ", " + std::to_string(i % 100) + ", " +
                                        std::to_string(1 + (i % 10)) + ", 1000.0, 0.05, 0.02)"));
    }
    ctx.executor->execute("COMMIT");

    for (auto _ : state) {
        ctx.executor->execute(
            *ParseSQL("SELECT * FROM lineitem WHERE l_quantity > 5 AND l_discount < 0.06"));
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_CloudSQL_ComplexWhere)->Arg(10000)->Arg(100000);

BENCHMARK_MAIN();
