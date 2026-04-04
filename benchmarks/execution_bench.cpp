#include <benchmark/benchmark.h>
#include <memory>
#include <vector>
#include <filesystem>
#include "storage/storage_manager.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"
#include "executor/operator.hpp"
#include "executor/query_executor.hpp"
#include "parser/expression.hpp"
#include "catalog/catalog.hpp"
#include "common/config.hpp"

using namespace cloudsql;
using namespace cloudsql::storage;
using namespace cloudsql::executor;
using namespace cloudsql::parser;

// Helper to create a table with N rows
static void SetupBenchTable(HeapTable& table, int num_rows) {
    for (int i = 0; i < num_rows; ++i) {
        std::vector<common::Value> values = {
            common::Value::make_int64(i),
            common::Value::make_text("Data_" + std::to_string(i))
        };
        table.insert(Tuple(values), 0);
    }
}

static void BM_ExecutionSeqScan(benchmark::State& state) {
    std::string test_dir = "./bench_exec_scan_" + std::to_string(state.range(0));
    std::filesystem::create_directories(test_dir);
    StorageManager disk_manager(test_dir);
    BufferPoolManager bpm(2000, disk_manager);
    
    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);
    schema.add_column("data", common::ValueType::TYPE_TEXT);
    
    // Setup table once per benchmark run
    auto table = std::make_unique<HeapTable>("scan_table", bpm, schema);
    table->create();
    SetupBenchTable(*table, state.range(0));

    for (auto _ : state) {
        auto scan_op = std::make_unique<SeqScanOperator>(std::move(table));
        scan_op->init();
        Tuple tuple;
        while (scan_op->next(tuple)) {
            benchmark::DoNotOptimize(tuple);
        }
        
        // Recover the table for the next iteration (it was moved into scan_op)
        table = std::make_unique<HeapTable>("scan_table", bpm, schema);
    }
    
    state.SetItemsProcessed(state.iterations() * state.range(0));
    std::filesystem::remove_all(test_dir);
}
BENCHMARK(BM_ExecutionSeqScan)->Arg(1000)->Arg(10000);

static void BM_ExecutionHashJoin(benchmark::State& state) {
    std::string test_dir = "./bench_exec_join_" + std::to_string(state.range(0));
    std::filesystem::create_directories(test_dir);
    StorageManager disk_manager(test_dir);
    BufferPoolManager bpm(4000, disk_manager);
    
    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);
    schema.add_column("data", common::ValueType::TYPE_TEXT);
    
    auto left_table = std::make_unique<HeapTable>("left_table", bpm, schema);
    left_table->create();
    SetupBenchTable(*left_table, state.range(0));
    
    auto right_table = std::make_unique<HeapTable>("right_table", bpm, schema);
    right_table->create();
    SetupBenchTable(*right_table, state.range(0));

    for (auto _ : state) {
        auto left_scan = std::make_unique<SeqScanOperator>(std::move(left_table));
        auto right_scan = std::make_unique<SeqScanOperator>(std::move(right_table));
        
        // Join on "id"
        auto left_key = std::make_unique<ColumnExpr>("id");
        auto right_key = std::make_unique<ColumnExpr>("id");
        
        auto join_op = std::make_unique<HashJoinOperator>(
            std::move(left_scan), std::move(right_scan), std::move(left_key), std::move(right_key));
        
        join_op->init();
        Tuple tuple;
        while (join_op->next(tuple)) {
            benchmark::DoNotOptimize(tuple);
        }
        
        // Recover tables for next iteration
        left_table = std::make_unique<HeapTable>("left_table", bpm, schema);
        right_table = std::make_unique<HeapTable>("right_table", bpm, schema);
    }
    
    state.SetItemsProcessed(state.iterations() * state.range(0));
    std::filesystem::remove_all(test_dir);
}
BENCHMARK(BM_ExecutionHashJoin)->Arg(100)->Arg(1000);

BENCHMARK_MAIN();
