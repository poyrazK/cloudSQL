# Performance Comparison: cloudSQL vs DuckDB

## 1. Overview

This report documents the head-to-head performance comparison between `cloudSQL` (local execution mode) and [DuckDB](https://duckdb.org/) v1.5.2, an embedded OLAP database with state-of-the-art vectorized execution. The goal is to validate cloudSQL's performance against the industry-standard in-memory analytical engine.

## 2. Test Environment

- **Hardware**: Apple M3 Pro
- **OS**: macOS 15.x (Darwin)
- **Build Type**: Release (`-O3`)
- **DuckDB**: v1.5.2 (installed via Homebrew)
- **Engine Configuration**:
  - `cloudSQL`: Local mode, 4096-page Buffer Pool, vectorized execution enabled
  - `DuckDB`: In-memory database, default configuration

## 3. Comparative Metrics

| Benchmark | Scale | cloudSQL | DuckDB | Winner |
|:----------|:------:|----------:|--------:|:-------|
| **Q1** GROUP BY aggregation | 10k rows | 161k rows/s | 61.8M rows/s | DuckDB 385x |
| **Q1** GROUP BY aggregation | 100k rows | 152k rows/s | 182M rows/s | DuckDB 1,196x |
| **Q6** Filter + aggregation | 10k rows | 770M rows/s | 79M rows/s | **cloudSQL 9.7x** |
| **Q6** Filter + aggregation | 100k rows | 7.3B rows/s | 474M rows/s | **cloudSQL 15.4x** |
| **Q3-like** Hash Join | 10k rows | 3.78M rows/s | 34.3M rows/s | DuckDB 9x |
| **Q3-like** Hash Join | 50k rows | 3.76M rows/s | 69.5M rows/s | DuckDB 18x |

## 4. Architectural Analysis

### Filter + Aggregation (cloudSQL wins 9.7x–15.4x)

cloudSQL significantly outperforms DuckDB on the filter+aggregate workload (Q6) after parallel hash aggregation optimization. Key improvements:

1. **Parallel hash aggregation**: Rows partitioned by `hash % num_threads_`, processed concurrently with per-thread `OpenAddressHashAgg`, merged at output phase
2. **Vectorized filter optimization**: `VectorizedFilterOperator` processes batches with tight inner loops and precomputed selection masks
3. **FNV-1a hash**: Fast 64-bit hashing for row partitioning with minimal overhead
4. **OpenAddressHashAgg**: Linear probing with 0.5 load factor provides excellent cache locality

### GROUP BY Aggregation (DuckDB wins 385x–1,196x)

DuckDB dominates GROUP BY workloads. This gap is expected and reflects a fundamental architectural difference:

1. **Columnar storage**: DuckDB stores data in Arrow columnar format, making aggregation on a single column extremely cache-efficient (read only that column)
2. **Hash aggregation maturity**: DuckDB's `HashAggregate` operator uses sophisticated grouping strategies (multi-level aggregation, pre-flighting)
3. **SIMD vectorization**: DuckDB leverages SIMD instructions for hashing and aggregation within batch processing
4. **cloudSQL row-oriented GROUP BY**: cloudSQL's current aggregation reads entire rows even when only one column is needed

**Action item**: Investigate using cloudSQL's ColumnarTable storage for analytical workloads where only a subset of columns is needed for aggregation.

### Hash Join (DuckDB wins 9x–18x)

DuckDB's hash join is significantly faster, likely due to:

1. **Vectorized probe**: DuckDB's `HashJoinProbe` processes batches without breaking for row-level iteration
2. **Build-side partitioning**: DuckDB uses probe-side partitioning to improve memory locality during probe
3. **cloudSQL's Volcano path**: The join benchmark may be exercising cloudSQL's row-oriented Volcano path (`HashJoinOperator`) rather than the vectorized `VectorizedHashJoinOperator`

## 5. Benchmark Methodology

The benchmark suite is located at `benchmarks/duckdb_comparison_bench.cpp` and follows the same pattern as `sqlite_comparison_bench.cpp`.

### Queries Tested

**Q1 (TPC-H inspired, GROUP BY aggregation)**
```sql
SELECT l_quantity, SUM(l_extendedprice), AVG(l_discount) FROM lineitem GROUP BY l_quantity
```

**Q6 (TPC-H inspired, filter + aggregation)**
```sql
SELECT SUM(l_extendedprice) FROM lineitem WHERE l_discount BETWEEN 0.04 AND 0.06 AND l_quantity < 25
```

**Q3-like (simplified multi-table join)**
```sql
SELECT o.o_orderkey, SUM(l.l_extendedprice)
FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY o.o_orderkey
```

### Schema

**lineitem** (6 columns, replicated from TPC-H)
| Column | Type |
|--------|------|
| l_orderkey | BIGINT |
| l_partkey | BIGINT |
| l_quantity | INT |
| l_extendedprice | DOUBLE |
| l_discount | DOUBLE |
| l_tax | DOUBLE |

**orders** (3 columns, for join tests)
| Column | Type |
|--------|------|
| o_orderkey | BIGINT |
| o_custkey | BIGINT |
| o_orderdate | TEXT |

## 6. Key Findings

| Finding | Implication |
|---------|-------------|
| cloudSQL's vectorized filter path is highly optimized | Good foundation for analytical workloads |
| GROUP BY aggregation needs significant work | Priority: optimize or offload to columnar storage |
| Join performance lags behind industry standard | Investigate vectorized join path and probe-side optimization |
| Filter+select outperforms DuckDB in simple cases | cloudSQL's row storage can win on point predicates |

## 7. Future Roadmap

1. **Columnar GROUP BY**: Add aggregation support to `ColumnarTable` and route GROUP BY queries through columnar storage
2. **SIMD aggregation**: Profile and vectorize hash-based grouping with AVX-512 on supported hardware
3. **Probe-side optimization**: Investigate partitioned hash join for better cache locality during probe
4. **Vectorized join by default**: Ensure joins exercise `VectorizedHashJoinOperator` rather than Volcano path
5. **TPC-H full suite**: Run the complete TPC-H SF=1 benchmark (22 queries) for comprehensive comparison

## 8. How to Run

```bash
# Configure with benchmarks enabled
cmake -B build -DBUILD_BENCHMARKS=ON -DBUILD_TESTS=OFF

# Build DuckDB comparison benchmark (requires DuckDB installed)
cmake --build build --target duckdb_comparison_bench

# Run benchmark
./build/duckdb_comparison_bench --benchmark_format=json > duckdb_results.json

# Compare results
jq '.benchmarks[] | {name, items_per_second}' duckdb_results.json
```