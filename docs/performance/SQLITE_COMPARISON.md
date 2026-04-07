# Performance Comparison: cloudSQL vs SQLite3

## 1. Overview
This report documents the head-to-head performance comparison between the `cloudSQL` distributed engine (local execution mode) and the embedded SQLite3 database (C API). The goal is to establish an industry-standard baseline for raw storage and execution efficiency.

## 2. Test Environment
*   **Hardware**: Apple M3 Pro
*   **OS**: macOS 15.3.1 (Darwin)
*   **Build Type**: Release (`-O3`)
*   **Engine Configuration**:
    *   `cloudSQL`: Local mode, 4096-page Buffer Pool, Zero-Copy Binary Format.
    *   `SQLite3`: `PRAGMA synchronous = OFF`, `PRAGMA journal_mode = OFF` (Optimized for raw speed).

## 3. Comparative Metrics

| Benchmark | cloudSQL | SQLite3 | Performance Gap |
| :--- | :--- | :--- | :--- |
| **Point Inserts (10k)** | 16.1k rows/s | **114.1k rows/s** | 7.1x |
| **Sequential Scan (10k)** | 3.1M items/s | **20.1M items/s** | 6.5x |

## 4. Architectural Analysis

### Point Inserts
The 7.1x gap in insertion speed is attributed to:
1.  **Statement Parsing Overhead**: Our benchmark currently re-parses SQL strings for every `INSERT` in `cloudSQL`, whereas SQLite uses a prepared statement (`sqlite3_prepare_v2`).
2.  **Object Allocations**: `cloudSQL` allocates multiple `std::unique_ptr` objects (Statements, Expressions, Tuples) per row. SQLite uses a specialized register-based virtual machine with minimal allocations.
3.  **Storage Engine Maturity**: SQLite's B-Tree implementation is highly optimized for write-ahead logging and paged I/O compared to our current Heap Table.

### Sequential Scans
The 6.5x gap in scan speed is attributed to:
1.  **Volcano Model Overhead**: `cloudSQL` uses a tuple-at-a-time iterator model with virtual function calls for `next()`.
2.  **Value Type Overhead**: Our `common::Value` class uses `std::variant`, which introduces a small overhead for every column access compared to SQLite's raw buffer indexing.

## 5. Optimization Roadmap
To achieve parity with SQLite, the following optimizations are prioritized:
1.  **Prepared Statement Cache**: Eliminate SQL parsing overhead for recurring queries.
2.  **Tuple Memory Arena**: Implement a thread-local bump allocator to reduce `malloc` overhead during execution.
3.  **Vectorized Execution**: Move from tuple-at-a-time to batch-at-a-time (e.g., 1024 rows) to improve cache locality and enable SIMD.
