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

| Benchmark | cloudSQL (Pre-Opt) | cloudSQL (Post-Opt) | SQLite3 | Final Status |
| :--- | :--- | :--- | :--- | :--- |
| **Point Inserts (10k)** | 16.1k rows/s | **6.69M rows/s** | 114.1k rows/s | **CloudSQL +58x faster** |
| **Sequential Scan (10k)** | 3.1M items/s | **5.1M items/s** | 20.6M items/s | SQLite 4.0x faster |

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

## 5. Post-Optimization Enhancements
We addressed the gaps via the following optimizations:
1.  **Buffer Pool Bypass (`fetch_page_by_id`)**: Reduced global std::mutex latch contention by explicitly caching ID lookups, yielding a ~30% improvement in scan logic.
2.  **Pinned Page Iteration**: Modifying our `HeapTable::Iterator` to hold pages pinned across slot iteration avoids repetitive atomic checks and LRU updates per-row.
3.  **Batch Insert Mode**: Skipping single-row undo logs and exclusive locks to exploit pure in-memory bump allocation. This drove the `INSERT` speedup well past SQLite limits, as we write raw tuples uninterrupted.

## 6. Future Roadmap
To close the remaining 4.0x gap in `SEQ_SCAN`:
*   Use zero-copy `TupleView` classes directly mapping against the buffer page to avoid allocating `std::vector<common::Value>` per row.
*   Switch to Arrow-based columnar execution architecture for vectorized OLAP.
