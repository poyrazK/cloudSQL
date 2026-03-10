# cloudSQL

A lightweight, distributed SQL database engine. Designed for cloud environments with a focus on simplicity, type safety, and PostgreSQL compatibility. cloudSQL bridges the gap between single-node databases and complex distributed systems by providing horizontal scaling with a familiar interface.

## Key Features

- **Modern C++ Architecture**: High-performance, object-oriented codebase using C++17.
- **Distributed Consensus (Raft)**: Global metadata and catalog consistency powered by a custom Raft implementation.
- **Horizontal Sharding**: Hash-based data partitioning across multiple Data Nodes.
- **Distributed Query Optimization**: 
  - **Shard Pruning**: Intelligent routing to avoid cluster-wide broadcasts.
  - **Aggregation Merging**: Global coordination for `COUNT`, `SUM`, and other aggregates.
  - **Broadcast & Shuffle Joins**: Optimized cross-shard joins for small-to-large and large-to-large table scenarios.
- **Data Replication & HA**: Fully redundant data storage with multi-group Raft and automatic leader failover.
- **Analytics Performance**: 
  - **Columnar Storage**: Binary-per-column persistence for efficient analytical scanning.
  - **Vectorized Execution**: Batch-at-a-time processing model for high-throughput query execution.
- **Multi-Node Transactions**: ACID guarantees across the cluster via Two-Phase Commit (2PC) and connection-aware execution state supporting `BEGIN`, `COMMIT`, and `ROLLBACK`.
- **Advanced Execution Engine**: 
  - **Full Outer Join Support**: Specialized `HashJoinOperator` implementing `LEFT`, `RIGHT`, and `FULL` outer join semantics with automatic null-padding.
  - **B+ Tree Indexing**: Persistent indexing for high-speed point lookups and optimized query planning.
- **Type-Safe Value System**: Robust handling of SQL data types using `std::variant`.
- **Volcano & Vectorized Engine**: Flexible execution models supporting traditional row-based and high-performance columnar processing.
- **PostgreSQL Wire Protocol**: Handshake and simple query protocol implementation for tool compatibility.

## Project Structure

- `include/`: Header files defining the core engine and distributed API.
- `src/`: implementations modules.
  - `catalog/`: Metadata and schema management.
  - `distributed/`: Raft consensus, shard management, and distributed execution.
  - `executor/`: Volcano operators and local query coordination.
  - `network/`: PostgreSQL server and internal cluster RPC.
  - `parser/`: Lexical analysis and SQL parsing.
  - `storage/`: Paged storage, heap files, and B+ tree indexes.
- `docs/`: Technical documentation and [Phase-by-Phase Roadmap](./docs/phases/README.md).
- `tests/`: Comprehensive test suite including simulation-based Raft tests and distributed scenarios.

## Building and Running

### Prerequisites

- CMake (>= 3.16)
- C++17 compatible compiler (Clang or GCC)

### Build Instructions

```bash
mkdir build
cd build
cmake ..
make -j$(nproc) # Or ../tests/run_test.sh for automated multi-OS build
```

### Running Tests

```bash
# Run the integrated test suite (Unit + E2E + Logic)
./tests/run_test.sh

# Or run individual binaries
./build/sqlEngine_tests
./build/distributed_tests
```

### Starting the Cluster

Start a Coordinator:
```bash
./build/sqlEngine --mode coordinator --port 5432 --cluster-port 6432 --data ./coord_data
```

Start a Data Node:
```bash
./build/sqlEngine --mode data --cluster-port 6433 --data ./data_node_1
```

## Core Components

### 1. Raft Consensus
Ensures that all Coordinator nodes share an identical view of the database schema and shard mappings. DDL operations are replicated and committed via the Raft log before being applied to the local catalog.

### 2. Distributed Executor
Orchestrates query fragments across the cluster. It performs plan splitting, dispatches sub-queries to relevant Data Nodes, and merges partial results (e.g., summing partial counts) before returning the final set to the client.

### 3. Storage Layer
Data is persisted in fixed-size pages (default 4KB) using a slot-based layout. The `StorageManager` coordinates access, while the `BufferPoolManager` provides an LRU-K caching layer to minimize disk I/O.

## License

MIT
