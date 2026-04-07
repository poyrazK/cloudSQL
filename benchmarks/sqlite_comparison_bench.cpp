/**
 * @file sqlite_comparison_bench.cpp
 * @brief Performance comparison between cloudSQL and SQLite3
 */

#include <benchmark/benchmark.h>
#include <sqlite3.h>
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

        // Create table
        CreateTableStatement create_stmt;
        create_stmt.set_table_name("bench_table");
        create_stmt.add_column("id", "BIGINT");
        create_stmt.add_column("val", "DOUBLE");
        create_stmt.add_column("data", "TEXT");
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

// --- SQLite Setup ---
struct SQLiteContext {
    sqlite3* db;
    std::string test_db;

    SQLiteContext(const std::string& path) : test_db(path) {
        if (path == ":memory:") {
            sqlite3_open(":memory:", &db);
        } else {
            std::filesystem::remove(path);
            sqlite3_open(path.c_str(), &db);
        }
        
        // Fast settings
        sqlite3_exec(db, "PRAGMA journal_mode = OFF", nullptr, nullptr, nullptr);
        sqlite3_exec(db, "PRAGMA synchronous = OFF", nullptr, nullptr, nullptr);
        sqlite3_exec(db, "CREATE TABLE bench_table (id BIGINT, val DOUBLE, data TEXT)", nullptr, nullptr, nullptr);
    }

    ~SQLiteContext() {
        sqlite3_close(db);
        if (test_db != ":memory:") {
            std::filesystem::remove(test_db);
        }
    }
};

} // anonymous namespace

// --- Benchmark 1: cloudSQL Point Inserts ---
static void BM_CloudSQL_Insert(benchmark::State& state) {
    CloudSQLContext ctx("./bench_cloudsql_insert_" + std::to_string(state.thread_index()));
    
    for (auto _ : state) {
        std::string sql = "INSERT INTO bench_table VALUES (" + std::to_string(state.iterations()) + 
                          ", 3.14, 'some_payload_data');";
        ctx.executor->execute(sql);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_CloudSQL_Insert);

// --- Benchmark 2: SQLite Point Inserts ---
static void BM_SQLite_Insert(benchmark::State& state) {
    SQLiteContext ctx("./bench_sqlite_insert_" + std::to_string(state.thread_index()) + ".db");
    
    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(ctx.db, "INSERT INTO bench_table VALUES (?, ?, ?)", -1, &stmt, nullptr);

    for (auto _ : state) {
        sqlite3_bind_int64(stmt, 1, state.iterations());
        sqlite3_bind_double(stmt, 2, 3.14);
        sqlite3_bind_text(stmt, 3, "some_payload_data", -1, SQLITE_STATIC);
        
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    
    sqlite3_finalize(stmt);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SQLite_Insert);

// --- Benchmark 3: cloudSQL Sequential Scan ---
static void BM_CloudSQL_Scan(benchmark::State& state) {
    const int num_rows = state.range(0);
    CloudSQLContext ctx("./bench_cloudsql_scan_" + std::to_string(state.thread_index()));
    
    // Populate
    for (int i = 0; i < num_rows; ++i) {
        ctx.executor->execute(*ParseSQL(
            "INSERT INTO bench_table VALUES (" + std::to_string(i) + ", 1.1, 'data');"));
    }

    auto select_stmt = ParseSQL("SELECT * FROM bench_table");

    for (auto _ : state) {
        auto res = ctx.executor->execute(*select_stmt);
        benchmark::DoNotOptimize(res);
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_CloudSQL_Scan)->Arg(1000)->Arg(10000);

// --- Benchmark 4: SQLite Sequential Scan ---
static void BM_SQLite_Scan(benchmark::State& state) {
    const int num_rows = state.range(0);
    SQLiteContext ctx("./bench_sqlite_scan_" + std::to_string(state.thread_index()) + ".db");
    
    // Populate
    sqlite3_exec(ctx.db, "BEGIN TRANSACTION", nullptr, nullptr, nullptr);
    for (int i = 0; i < num_rows; ++i) {
        std::string sql = "INSERT INTO bench_table VALUES (" + std::to_string(i) + ", 1.1, 'data')";
        sqlite3_exec(ctx.db, sql.c_str(), nullptr, nullptr, nullptr);
    }
    sqlite3_exec(ctx.db, "COMMIT", nullptr, nullptr, nullptr);

    sqlite3_stmt* stmt;
    sqlite3_prepare_v2(ctx.db, "SELECT * FROM bench_table", -1, &stmt, nullptr);

    for (auto _ : state) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            benchmark::DoNotOptimize(stmt);
        }
        sqlite3_reset(stmt);
    }
    
    sqlite3_finalize(stmt);
    state.SetItemsProcessed(state.iterations() * num_rows);
}
BENCHMARK(BM_SQLite_Scan)->Arg(1000)->Arg(10000);
