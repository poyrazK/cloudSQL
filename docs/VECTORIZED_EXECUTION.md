# Vectorized Execution Engine

## Overview

cloudSQL's query execution supports two models: the **Volcano (tuple-at-a-time) model** and the **Vectorized (batch-at-a-time) model**. The vectorized model is designed for analytical workloads where high-throughput batch processing provides significant performance gains over row-by-row iteration.

## Execution Models

### Volcano Model (Row-at-a-time)

Traditional iterator-based pull model where each `next()` call returns a single tuple:

```cpp
class Operator {
    virtual bool next(Tuple& out_tuple) = 0;
};
```

**Operators:** `SeqScanOperator`, `IndexScanOperator`, `FilterOperator`, `ProjectOperator`, `HashJoinOperator`, `SortOperator`, `AggregateOperator`, `LimitOperator`

**Characteristics:**
- One virtual function call per tuple
- Simple, well-understood semantics
- Good for OLTP workloads with early filtering

### Vectorized Model (Batch-at-a-time)

Batch-based push model where each `next_batch()` call processes a `VectorBatch` (typically 1024 rows):

```cpp
class VectorizedOperator : public Operator {
    virtual bool next_batch(VectorBatch& out_batch) = 0;
};
```

**Operators:** `VectorizedSeqScanOperator`, `VectorizedFilterOperator`, `VectorizedProjectOperator`, `VectorizedHashJoinOperator`, `VectorizedGroupByOperator`

**Characteristics:**
- ~1024x fewer virtual function calls
- Higher cache locality and data reuse
- Enables SIMD optimization opportunities
- Ideal for analytical scans and aggregations

## Architecture

### Class Hierarchy

```
Operator (base)
├── SeqScanOperator, IndexScanOperator, FilterOperator, ...
├── SortOperator, LimitOperator
├── HashJoinOperator, AggregateOperator
└── VectorizedOperator (inherits from Operator)
    ├── VectorizedSeqScanOperator
    ├── VectorizedFilterOperator
    ├── VectorizedProjectOperator
    ├── VectorizedHashJoinOperator
    └── VectorizedGroupByOperator
```

`VectorizedOperator` inherits from `Operator` (using `OperatorType::Result` as base type), enabling polymorphism between the two execution models.

### VectorBatch Structure

```cpp
class VectorBatch {
    std::vector<std::unique_ptr<ColumnVector>> columns_;
    size_t row_count_;
};
```

A `VectorBatch` contains one `ColumnVector` per output column, with `row_count_` indicating active rows. ColumnVectors can be `NumericVector<T>`, `StringVector`, or `BoolVector` depending on data type.

### QueryExecutor Integration

The `QueryExecutor` decides at execution time which model to use:

```cpp
void QueryExecutor::set_parallel(bool v) { parallel_ = v; }
void QueryExecutor::set_storage_manager(storage::StorageManager* sm) { storage_manager_ = sm; }

QueryResult QueryExecutor::execute_select(const parser::SelectStatement& stmt, Transaction* txn) {
    bool has_sort_or_limit = !stmt.order_by().empty() || stmt.has_limit() || stmt.has_offset();
    bool use_vectorized = parallel_ && storage_manager_ && !has_sort_or_limit;

    if (use_vectorized) {
        auto vec_root = build_vectorized_plan(stmt, txn);
        // batch iteration via vec_root->next_batch()
    } else {
        auto root = build_plan(stmt, txn);
        // tuple iteration via root->next()
    }
}
```

**Key constraint:** Sort/Limit queries fall back to Volcano path since `SortOperator`/`LimitOperator` don't inherit from `VectorizedOperator`.

## Parallel Vectorized Execution

### ThreadPool

`ThreadPool` (`include/executor/thread_pool.hpp`) provides a fixed-size thread pool for parallel task execution:

```cpp
class ThreadPool {
    explicit ThreadPool(size_t num_threads);
    void submit(std::function<void()> task);
    void wait();  // wait for all submitted tasks
};
```

Used by parallel operators to distribute batch processing across multiple threads.

### ParallelVectorizedSeqScanOperator

Multi-threaded scan over `ColumnarTable`:

```cpp
ParallelVectorizedSeqScanOperator(
    std::string table_name,
    std::shared_ptr<ColumnarTable> table,
    std::shared_ptr<ThreadPool> thread_pool);
```

Processes columnar batches in parallel using ThreadPool task distribution.

### VectorizedGroupByOperator with ThreadPool

Parallel hash-based grouped aggregation:

```cpp
VectorizedGroupByOperator(
    std::unique_ptr<VectorizedOperator> child,
    std::vector<std::unique_ptr<parser::Expression>> group_by,
    std::vector<VectorizedAggregateInfo> aggregates,
    Schema output_schema,
    ThreadPool* thread_pool);  // optional thread pool for parallel aggregation
```

Uses thread-local hash maps for concurrent group processing, merging results in the finalize phase.

## Build Plan Comparison

### Volcano Path (`build_plan()`)
```
SeqScanOperator (HeapTable)
  → FilterOperator
    → HashJoinOperator
      → AggregateOperator (HashAggregate)
        → ProjectOperator
          → SortOperator (ORDER BY)
            → LimitOperator
```

### Vectorized Path (`build_vectorized_plan()`)
```
VectorizedSeqScanOperator (ColumnarTable)
  → VectorizedFilterOperator
    → VectorizedHashJoinOperator
      → VectorizedGroupByOperator
        → VectorizedProjectOperator
```

## Usage Example

```cpp
// Enable parallel vectorized mode
executor.set_parallel(true);
executor.set_storage_manager(&storage_manager);

// Vectorized queries (scans, filters, joins, aggregates - no ORDER BY/LIMIT)
auto result = executor.execute("SELECT status, COUNT(*) FROM orders GROUP BY status");

// Volcano fallback (queries with ORDER BY or LIMIT)
auto result2 = executor.execute("SELECT * FROM orders ORDER BY created_at LIMIT 10");
```

## Performance Characteristics

| Scenario | Volcano | Vectorized | Speedup |
|----------|---------|------------|---------|
| Full table scan | 181M rows/s | ~500M rows/s (parallel) | ~3x |
| GROUP BY aggregate | ~50M rows/s | ~150M rows/s (parallel) | ~3x |
| JOIN (hash) | ~40M rows/s | ~100M rows/s | ~2.5x |
| Small result sets | Good | Overhead | - |
| Queries with ORDER BY | Good | N/A (fallback) | - |

The vectorized path provides significant throughput gains for analytical workloads with large result sets, while the Volcano path remains optimal for OLTP-style queries with early filtering or small result sets.