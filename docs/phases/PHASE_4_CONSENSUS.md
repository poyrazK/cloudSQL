# Phase 4: Distributed State (Raft)

## Overview
Phase 4 transformed cloudSQL from a single-node engine into a distributed system by implementing the Raft consensus protocol for metadata consistency.

## Key Components

### 1. Raft Core (`distributed/raft_node.cpp`)
Implemented the Raft consensus algorithm from scratch.
- **Leader Election**: Automated transition between Follower, Candidate, and Leader states based on heartbeats.
- **Log Replication**: Ensures all coordinator nodes have an identical sequence of catalog operations.
- **Persistence**: Raft log is persisted to disk to survive node restarts.

### 2. Catalog-Raft Integration
Linked the Raft log to catalog state transitions.
- **Replicated DDL**: `CREATE TABLE` and `DROP TABLE` are proposed to Raft; they are only applied to the local catalog after being committed to the majority of the cluster.
- **Consistency**: Guaranteed that all coordinators see the same schema at the same logical time.

### 3. Cluster Membership (`common/cluster_manager.hpp`)
Managed the dynamic topology of the cluster.
- **Node Discovery**: Automated registration of Data Nodes and Coordinators.
- **Role Awareness**: Distinguishes between nodes that participate in consensus (Coordinators) and those that store shards (Data Nodes).

## Lessons Learned
- Raft heartbeats must be fine-tuned to avoid unnecessary re-elections in high-latency cloud environments.
- Coupling the Catalog directly to Raft state machine application ensures strict serializability for schema changes.
