# cloudSQL Coverage Report

Generated: 2026-05-08
Test Suite: 38 test targets, all passing (BUILD_COVERAGE=ON, -fprofile-arcs -ftest-coverage -O0)

## Summary (Line Coverage Only)

| Module | Lines Hit / Total | Line % |
|--------|-------------------|--------|
| **catalog** | 211 / 282 | 74.8% |
| **common** | 219 / 271 | 80.8% |
| **distributed** | 742 / 956 | 77.6% |
| **executor** | 1228 / 1548 | 79.3% |
| **network** | 391 / 450 | 86.9% |
| **parser** | 1146 / 1274 | 90.0% |
| **recovery** | 340 / 355 | 95.8% |
| **storage** | 1624 / 1911 | 84.9% |
| **transaction** | 292 / 300 | 97.3% |

**Overall: 6193 / 7347 lines (84.3%)**

## Detailed File Coverage

### catalog/

| File | Lines Hit/Total | Line % | Branch Taken/Total | Branch % |
|------|-----------------|--------|--------------------|----------|
| catalog.cpp | 211/282 | 74.8% | 105/217 | 48.4% |

### common/

| File | Lines Hit/Total | Line % | Branch Taken/Total | Branch % |
|------|-----------------|--------|--------------------|----------|
| config.cpp | 125/125 | 100.0% | 169/265 | 63.8% |
| bloom_filter.cpp | 109/146 | 74.7% | 45/80 | 56.3% |

### distributed/

| File | Lines Hit/Total | Line % | Branch Taken/Total | Branch % |
|------|-----------------|--------|--------------------|----------|
| distributed_executor.cpp | 516/724 | 71.3% | 545/1260 | 43.3% |
| raft_group.cpp | 257/278 | 92.5% | 147/228 | 64.5% |
| raft_manager.cpp | 50/51 | 98.0% | 41/72 | 56.9% |

### executor/

| File | Lines Hit/Total | Line % | Branch Taken/Total | Branch % |
|------|-----------------|--------|--------------------|----------|
| operator.cpp | 654/737 | 88.9% | 448/721 | 62.1% |
| query_executor.cpp | 627/859 | 73.0% | 700/1679 | 41.7% |

### network/

| File | Lines Hit/Total | Line % | Branch Taken/Total | Branch % |
|------|-----------------|--------|--------------------|----------|
| server.cpp | 248/296 | 83.8% | 149/298 | 50.0% |
| rpc_client.cpp | 63/70 | 90.0% | 42/64 | 65.6% |
| rpc_server.cpp | 80/84 | 95.2% | 43/59 | 72.9% |

### parser/

| File | Lines Hit/Total | Line % | Branch Taken/Total | Branch % |
|------|-----------------|--------|--------------------|----------|
| expression.cpp | 258/312 | 82.7% | 265/463 | 57.3% |
| lexer.cpp | 211/219 | 96.4% | 181/294 | 61.6% |
| parser.cpp | 529/611 | 86.6% | 676/1174 | 57.6% |
| statement.cpp | 124/132 | 93.9% | 127/225 | 56.4% |

### recovery/

| File | Lines Hit/Total | Line % | Branch Taken/Total | Branch % |
|------|-----------------|--------|--------------------|----------|
| log_manager.cpp | 63/70 | 90.0% | 28/50 | 56.0% |
| log_record.cpp | 256/266 | 96.2% | 133/173 | 76.9% |
| recovery_manager.cpp | 23/23 | 100.0% | 0/24 | 0.0% |

### storage/

| File | Lines Hit/Total | Line % | Branch Taken/Total | Branch % |
|------|-----------------|--------|--------------------|----------|
| btree_index.cpp | 132/145 | 91.0% | 84/150 | 56.0% |
| buffer_pool_manager.cpp | 175/187 | 93.5% | 122/227 | 53.7% |
| columnar_table.cpp | 124/135 | 91.9% | 152/308 | 49.4% |
| heap_table.cpp | 528/595 | 88.7% | 289/476 | 60.7% |
| lru_replacer.cpp | 44/46 | 95.7% | 24/40 | 60.0% |
| storage_manager.cpp | 106/120 | 88.3% | 51/86 | 59.3% |

### transaction/

| File | Lines Hit/Total | Line % | Branch Taken/Total | Branch % |
|------|-----------------|--------|--------------------|----------|
| lock_manager.cpp | 80/82 | 97.6% | 78/116 | 67.2% |
| transaction_manager.cpp | 212/218 | 97.2% | 227/386 | 58.8% |

## Coverage Gaps

### Lowest Line Coverage

| File | Line % | Issue |
|------|--------|-------|
| distributed_executor.cpp | 71.0% | Shard routing and broadcast paths |
| query_executor.cpp | 73.0% | Distributed execution paths |

### Lowest Branch Coverage

| File | Branch % | Issue |
|------|----------|-------|
| query_executor.cpp | 41.7% | Executor dispatch branches |
| distributed_executor.cpp | 43.1% | Distributed coordination branches |
| lexer.cpp | 61.6% | Lexer token recognition branches |
| catalog.cpp | 48.4% | Catalog metadata paths |

## Recommendations for Next Tests

1. **query_executor.cpp** — 73.0% line / 41.7% branch coverage. Add tests for:
   - Distributed execution paths
   - More executor dispatch branches

2. **catalog.cpp** — 74.8% line / 48.4% branch coverage. Add tests for:
   - Catalog metadata paths
   - Index creation edge cases

3. **distributed_executor.cpp** — 71.3% line / 43.3% branch coverage (improved). Add tests for:
   - Insert RPC reply success=false error path
   - 2PC coordination failure branches
