# Performance Analysis Report V1.0

## 1. Executive Summary
This report establishes the performance baseline for the `cloudSQL` distributed engine. We established a significant performance delta between Debug and Release builds, and identified a primary bottleneck in the data insertion path related to dynamic memory allocation.

## 2. Baseline Results (Release Build -O3)

| Component | Metric | Baseline Performance |
| :--- | :--- | :--- |
| **Storage** | Buffer Pool Page Fetch | **6.1 Million ops/sec** |
| **Storage** | Heap Table Insertion | **14.0k tuples/sec** |
| **Execution** | SeqScan (10k rows) | **41.8 Billion items/sec** |
| **Execution** | Hash Join (1k rows) | **1.3 Billion items/sec** |
| **Network** | RPC Round-Trip (64B) | **~12μs latency** |
| **Network** | RPC Throughput (16KB) | **2.16 GiB/s throughput** |

## 3. Profiling Findings (`HeapTable::insert`)

Using the macOS `sample` profiler on the `BM_HeapTableInsert` benchmark, we identified the following call graph distribution:

### The "Malloc" Bottleneck
*   **65% of CPU time** in the insertion path is spent in `malloc`, `free`, and `operator new`.
*   **42% of total insertion time** is attributed to `BufferPoolManager::unpin_page`.
*   Within `unpin_page`, the `LRUReplacer::unpin` method is triggering expensive internal allocations within `std::unordered_map` and `std::list`.

### Call Graph Insight:
```plaintext
BM_HeapTableInsert
  -> HeapTable::insert (1,859 samples)
  -> BufferPoolManager::unpin_page (789 samples)
    -> LRUReplacer::unpin (334 samples)
      -> std::__hash_table::emplace_unique_key_args
        -> operator new
          -> malloc_tiny
```

## 4. Conclusion & Recommendations
The system is currently **allocation-bound** for write-heavy workloads. While the execution engine (scans/joins) is highly optimized by the compiler, the storage layer's reliance on standard containers and frequent small allocations during unpinning and tuple creation is limiting throughput.

### Recommended Optimizations:
1.  **Tuple Arena/Pool**: Implement a fixed-size memory arena for `Tuple` and `Value` objects to eliminate `malloc` in the insertion hot-path.
2.  **Lock-Free / Pre-allocated LRU**: Refactor `LRUReplacer` to use a pre-allocated array-based structure (e.g., a CLOCK algorithm or a fixed-node linked list) to prevent allocations during `unpin`.
3.  **Batch Unpinning**: Reduce the frequency of `unpin_page` calls by holding pins for multiple operations where safe.
