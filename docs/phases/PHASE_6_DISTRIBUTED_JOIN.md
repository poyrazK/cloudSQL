# Phase 6: Distributed Multi-Shard Joins (Shuffle Join)

## Overview
Phase 6 focused on implementing high-performance data redistribution (Shuffle) to enable complex JOIN operations across multiple shards without requiring a full broadcast of tables.

## Key Components

### 1. Context-Aware Shuffle Infrastructure (`common/cluster_manager.hpp`)
Introduced isolated staging areas for inter-node data movement.
- **Shuffle Buffering**: Thread-safe memory regions in `ClusterManager` to store incoming data fragments.
- **Isolation**: Each shuffle context is uniquely identified, allowing multiple concurrent join operations without data corruption.

### 2. Shuffle RPC Protocol (`network/rpc_message.hpp`)
Developed a dedicated binary protocol for efficient data redistribution.
- **ShuffleFragment**: Metadata describing the fragment being pushed (target context, source node, schema).
- **PushData**: High-speed binary payload containing the actual tuple data for the shuffle phase.

### 3. Two-Phase Join Orchestration (`distributed/distributed_executor.cpp`)
Implemented the control logic for distributed shuffle joins.
- **Phase 1 (Redistribute)**: Coordinates all data nodes to re-hash and push their local data to the appropriate target nodes based on the join key.
- **Phase 2 (Local Join)**: Triggers local `HashJoin` operations on each node using the redistributed data stored in shuffle buffers.

### 4. BufferScanOperator Integration (`executor/operator.hpp`)
Seamlessly integrated shuffle buffers into the Volcano execution model.
- **Vectorized Buffering**: Optimized the `BufferScanOperator` to handle large volumes of redistributed data with minimal overhead.

## Lessons Learned
- Shuffle joins significantly reduce network traffic compared to broadcast joins for large-to-large table joins.
- Fine-grained locking in the shuffle buffers is critical for maintaining high throughput during the redistribution phase.

## Status: 100% Test Pass
Verified the end-to-end shuffle join flow, including multi-node data movement and final result merging, through automated integration tests.
