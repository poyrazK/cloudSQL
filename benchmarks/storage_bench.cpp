#include <benchmark/benchmark.h>
#include <memory>
#include <vector>
#include <cstdio>
#include <filesystem>
#include "storage/storage_manager.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"
#include "catalog/catalog.hpp"
#include "common/config.hpp"

using namespace cloudsql;
using namespace cloudsql::storage;

static void BM_BufferPoolPageFetch(benchmark::State& state) {
    std::string test_dir = "./bench_data_" + std::to_string(state.range(0));
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    
    StorageManager disk_manager(test_dir);
    BufferPoolManager bpm(state.range(0), disk_manager);
    
    std::string file_name = "test_file.db";
    disk_manager.open_file(file_name);
    
    // Pre-allocate some pages
    std::vector<uint32_t> page_ids;
    for (int i = 0; i < 100; ++i) {
        uint32_t pid;
        bpm.new_page(file_name, &pid);
        bpm.unpin_page(file_name, pid, false);
        page_ids.push_back(pid);
    }

    for (auto _ : state) {
        for (uint32_t pid : page_ids) {
            auto page = bpm.fetch_page(file_name, pid);
            if (page) {
                bpm.unpin_page(file_name, pid, false);
            }
        }
    }
    
    state.SetItemsProcessed(state.iterations() * page_ids.size());
    std::filesystem::remove_all(test_dir);
}
BENCHMARK(BM_BufferPoolPageFetch)->Arg(10)->Arg(100)->Arg(1000);

static void BM_HeapTableInsert(benchmark::State& state) {
    std::string test_dir = "./bench_data_table";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    
    StorageManager disk_manager(test_dir);
    BufferPoolManager bpm(1000, disk_manager);
    
    executor::Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);
    schema.add_column("data", common::ValueType::TYPE_TEXT);
    
    HeapTable table("bench_table", bpm, schema);
    table.create();

    std::vector<common::Value> values = {
        common::Value::make_int64(42),
        common::Value::make_text("Benchmark test data string")
    };
    executor::Tuple tuple(values);

    for (auto _ : state) {
        table.insert(tuple, 0);
    }
    
    state.SetItemsProcessed(state.iterations());
    std::filesystem::remove_all(test_dir);
}
BENCHMARK(BM_HeapTableInsert);

BENCHMARK_MAIN();
