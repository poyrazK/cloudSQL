# PostgreSQL vs cloudSQL Benchmark

## Overview

This benchmark suite compares cloudSQL's vectorized SQL engine against PostgreSQL across multiple workload categories. The goal is to demonstrate cloudSQL's performance characteristics relative to the industry-standard open-source database.

## Benchmark Suite

### OLTP Workloads (Point Queries, Writes)

| Benchmark | Description | cloudSQL | PostgreSQL |
|-----------|-------------|----------|------------|
| `BM_CloudSQL_Insert` / `BM_PostgreSQL_Insert` | Bulk INSERT throughput | items/s | items/s |
| `BM_CloudSQL_Update` / `BM_PostgreSQL_Update` | Row UPDATE by key | items/s | items/s |
| `BM_CloudSQL_PointSelect` / `BM_PostgreSQL_PointSelect` | Primary key lookup | items/s | items/s |

### Analytical Workloads (Reads, Aggregation)

| Benchmark | Description | cloudSQL | PostgreSQL |
|-----------|-------------|----------|------------|
| `BM_CloudSQL_FullScan` / `BM_PostgreSQL_FullScan` | SELECT * FROM table | items/s | items/s |
| `BM_CloudSQL_GroupBy` / `BM_PostgreSQL_GroupBy` | GROUP BY aggregation | items/s | items/s |
| `BM_CloudSQL_Join` / `BM_PostgreSQL_Join` | Two-table JOIN | items/s | items/s |
| `BM_CloudSQL_ComplexWhere` / `BM_PostgreSQL_ComplexWhere` | Multi-condition filter | items/s | items/s |

## Schema

Both systems use identical TPC-H inspired schemas (no indexes for fair comparison):

```sql
CREATE TABLE lineitem (
    l_orderkey BIGINT,
    l_partkey BIGINT,
    l_quantity INT,
    l_extendedprice DOUBLE,
    l_discount DOUBLE,
    l_tax DOUBLE
);

CREATE TABLE orders (
    o_orderkey BIGINT,
    o_custkey BIGINT,
    o_orderdate TEXT
);
```

## Running the Benchmark

### Prerequisites

- PostgreSQL must be installed and running locally
- Environment variables (optional, defaults shown):
  - `PGHOST` (default: localhost)
  - `PGPORT` (default: 5432)
  - `PGDATABASE` (default: postgres)
  - `PGUSER` (default: postgres)

### Build

```bash
cmake -DBUILD_BENCHMARKS=ON -B build
cmake --build build --target postgresql_comparison_bench
```

### Run

```bash
./build/postgresql_comparison_bench --benchmark_format=json > pg_results.json
```

### Run specific benchmarks

```bash
# Full scan comparison
./build/postgresql_comparison_bench --benchmark_filter="FullScan"

# GROUP BY comparison
./build/postgresql_comparison_bench --benchmark_filter="GroupBy"

# All cloudSQL only
./build/postgresql_comparison_bench --benchmark_filter="CloudSQL"
```

## Expected Results

### Analytical Workloads (cloudSQL advantage)

cloudSQL's vectorized execution typically outperforms PostgreSQL on:
- **Full table scans**: Vectorized batch processing eliminates row-by-row overhead
- **GROUP BY aggregation**: Hash-based aggregation with OpenAddressHashAgg
- **JOIN operations**: Vectorized hash join with FNV-1a partitioning
- **Complex WHERE**: Early predicate evaluation reduces data movement

### OLTP Workloads (PostgreSQL advantage)

PostgreSQL typically outperforms cloudSQL on:
- **INSERT throughput**: WAL-based logging and MVCC for durability
- **UPDATE by key**: In-place updates with heap storage
- **Point SELECT**: B-tree index with minimal I/O

## Methodology Notes

### Fair Comparison Guidelines

1. **Same hardware**: Both systems run on identical hardware
2. **Same data**: Identical row counts and data distributions
3. **Same schema**: Matching column types and index definitions
4. **Warm vs cold**: Results should note whether data fits in memory
5. **Connection overhead**: Excluded from throughput measurements

### Limitations

- **No query optimization**: cloudSQL and PostgreSQL may choose different query plans
- **Index availability**: PostgreSQL indexes not replicated in cloudSQL
- **Storage engines**: PostgreSQL uses heap storage; cloudSQL uses columnar for analytics
- **Durability guarantees**: PostgreSQL's ACID compliance vs cloudSQL's eventual consistency

## Interpreting Results

### Throughput Ratio

```
ratio = cloudSQL_items_per_second / PostgreSQL_items_per_second
```

- `ratio > 1`: cloudSQL is faster
- `ratio < 1`: PostgreSQL is faster
- `ratio ≈ 1`: Equivalent performance

### When cloudSQL Wins

cloudSQL shows the largest advantages on:
1. Analytical scans over large datasets
2. Aggregation-heavy workloads
3. Complex expressions evaluated in vectorized fashion

### When PostgreSQL Wins

PostgreSQL shows advantages on:
1. Single-row lookups by indexed key
2. Write-heavy workloads with durability requirements
3. Workloads that benefit from sophisticated cost-based optimization

## Example Output

```json
{
  "benchmarks": [
    {
      "name": "BM_CloudSQL_FullScan/100000",
      "items_per_second": 2680000
    },
    {
      "name": "BM_PostgreSQL_FullScan/100000",
      "items_per_second": 890000
    }
  ]
}
```

## References

- cloudSQL: [GitHub Repository](../README.md)
- PostgreSQL: https://www.postgresql.org/
- TPC-H: http://www.tpc.org/tpch/
