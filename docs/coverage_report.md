# cloudSQL Coverage Report

Generated: 2026-04-30
Test Suite: 37 test targets, all passing

## Summary

| Module | Line Coverage | Branch Coverage |
|--------|--------------|-----------------|
| **catalog/** | 83.7% / 90.9% | 94.2% / 75.0% |
| **common/** | 0.0% - 100.0% | 12.9% - 100.0% |
| **distributed/** | 0.0% - 100.0% | 0.0% - 100.0% |
| **executor/** | 12.2% - 100.0% | 0.0% - 100.0% |
| **network/** | 0.0% - 100.0% | 0.0% - 100.0% |
| **parser/** | 29.2% - 100.0% | 0.0% - 100.0% |
| **recovery/** | 0.0% - 100.0% | 0.0% - 100.0% |
| **storage/** | 0.0% - 100.0% | 0.0% - 100.0% |
| **transaction/** | 0.0% - 100.0% | 37.5% - 100.0% |

## Detailed File Coverage

### catalog/
| File | Lines | Line % | Branches | Branch % |
|------|-------|--------|----------|----------|
| catalog.hpp | 33 | 90.9% | 8 | 75.0% |
| catalog.cpp | 209 | 83.7% | 242 | 94.2% |

### common/
| File | Lines | Line % | Branches | Branch % |
|------|-------|--------|----------|----------|
| arena_allocator.hpp | 85 | 97.7% | 36 | 94.4% |
| bloom_filter.hpp | 3 | 100.0% | 2 | 100.0% |
| bloom_filter.cpp | 2 | 100.0% | 62 | 12.9% |
| **cluster_manager.hpp** | 15 | **100.0%** | 12 | **100.0%** |
| config.hpp | 9 | 44.4% | 2 | 100.0% |
| config.cpp | 2 | 0.0% | 2 | 100.0% |
| fault_injection.hpp | 43 | 90.7% | 50 | 100.0% |
| value.hpp | 12 | 91.7% | 4 | 100.0% |

### distributed/
| File | Lines | Line % | Branches | Branch % |
|------|-------|--------|----------|----------|
| distributed_executor.cpp | 724 | 71.0% | 1260 | 72.1% |
| raft_group.hpp | 70 | 90.0% | 236 | 100.0% |
| raft_group.cpp | 11 | 72.7% | 24 | 41.7% |
| raft_manager.hpp | 15 | 60.0% | 6 | 100.0% |
| raft_manager.cpp | 2 | 100.0% | 2 | 0.0% |
| raft_types.hpp | 11 | 0.0% | 2 | 100.0% |
| shard_manager.hpp | 6 | 100.0% | 2 | 100.0% |

### executor/
| File | Lines | Line % | Branches | Branch % |
|------|-------|--------|----------|----------|
| operator.hpp | 43 | 83.7% | 122 | 100.0% |
| operator.cpp | 737 | 88.5% | 845 | 89.3% |
| query_executor.cpp | 41 | 12.2% | 12 | 0.0% |
| query_executor.hpp | 2 | 100.0% | 2 | 0.0% |
| types.hpp | 137 | 85.4% | 112 | 50.9% |
| vectorized_operator.hpp | 150 | 44.0% | 30 | 40.0% |

### network/
| File | Lines | Line % | Branches | Branch % |
|------|-------|--------|----------|----------|
| rpc_client.hpp | 34 | 85.3% | 44 | 100.0% |
| rpc_client.cpp | 5 | 100.0% | 2 | 0.0% |
| rpc_message.hpp | 336 | 99.4% | 240 | 57.9% |
| rpc_server.hpp | 23 | 73.9% | 52 | 100.0% |
| rpc_server.cpp | 1 | 0.0% | 4 | 100.0% |
| server.hpp | 28 | 100.0% | 30 | 100.0% |
| server.cpp | 296 | 60.0% | 298 | 40.0% |

### parser/
| File | Lines | Line % | Branches | Branch % |
|------|-------|--------|----------|----------|
| expression.hpp | 25 | 32.0% | 68 | 11.8% |
| expression.cpp | 31 | 74.2% | 80 | 77.5% |
| lexer.hpp | 24 | 29.2% | 74 | 13.5% |
| lexer.cpp | 4 | 100.0% | 30 | 53.3% |
| parser.hpp | 4 | 100.0% | 2 | 0.0% |
| parser.cpp | 41 | 97.6% | 148 | 100.0% |
| statement.hpp | 1 | 100.0% | 4 | 100.0% |
| statement.cpp | 13 | 23.1% | 4 | 0.0% |
| token.hpp | 1 | 100.0% | 2 | 100.0% |

### recovery/
| File | Lines | Line % | Branches | Branch % |
|------|-------|--------|----------|----------|
| log_manager.hpp | 24 | 29.2% | 58 | 20.7% |
| log_manager.cpp | 8 | 37.5% | 38 | 5.3% |
| log_record.hpp | 3 | 100.0% | 2 | 100.0% |
| log_record.cpp | 80 | 5.0% | 22 | 0.0% |
| recovery_manager.hpp | 17 | 35.3% | 12 | 33.3% |
| recovery_manager.cpp | 4 | 0.0% | 2 | 0.0% |

### storage/
| File | Lines | Line % | Branches | Branch % |
|------|-------|--------|----------|----------|
| btree_index.hpp | 2 | 100.0% | 2 | 100.0% |
| btree_index.cpp | 25 | 4.0% | 2 | 0.0% |
| buffer_pool_manager.hpp | 1 | 100.0% | 2 | 100.0% |
| buffer_pool_manager.cpp | 32 | 100.0% | 22 | 100.0% |
| columnar_table.hpp | 54 | 100.0% | 14 | 100.0% |
| columnar_table.cpp | 26 | 73.1% | 28 | 92.9% |
| heap_table.hpp | 3 | 100.0% | 2 | 100.0% |
| heap_table.cpp | 142 | 18.3% | 38 | 26.3% |
| lru_replacer.hpp | 12 | 83.3% | 2 | 100.0% |
| lru_replacer.cpp | 1 | 100.0% | 2 | 100.0% |
| page.hpp | 195 | 82.6% | 126 | 96.8% |
| storage_manager.hpp | 8 | 0.0% | 2 | 100.0% |
| storage_manager.cpp | 32 | 96.9% | 32 | 56.2% |

### transaction/
| File | Lines | Line % | Branches | Branch % |
|------|-------|--------|----------|----------|
| lock_manager.hpp | 28 | 57.1% | 8 | 50.0% |
| lock_manager.cpp | 86 | 62.8% | 16 | 37.5% |
| transaction.hpp | 1 | 0.0% | 16 | 87.5% |
| transaction_manager.hpp | 1 | 100.0% | 33 | 87.9% |
| transaction_manager.cpp | 68 | 83.8% | 24 | 75.0% |

## Coverage Gaps (Lines < 50%)

### Critical Gaps (< 20% line coverage)
| File | Line % | Issue |
|------|--------|-------|
| storage/btree_index.cpp | 4.0% | Minimal tests |
| recovery/log_record.cpp | 5.0% | Minimal tests |
| executor/query_executor.cpp | 12.2% | Minimal tests |
| storage/heap_table.cpp | 18.3% | Needs more tests |

### Moderate Gaps (20-50% line coverage)
| File | Line % | Issue |
|------|--------|-------|
| parser/statement.cpp | 23.1% | Partial coverage |
| network/rpc_message.hpp | 29.2% | Partial coverage |
| parser/lexer.hpp | 29.2% | Partial coverage |
| recovery/log_manager.hpp | 29.2% | Partial coverage |
| parser/expression.hpp | 32.0% | Partial coverage |
| recovery/recovery_manager.hpp | 35.3% | Partial coverage |
| recovery/log_manager.cpp | 37.5% | Partial coverage |
| network/server.cpp | 55.7% | Partial coverage |
| transaction/lock_manager.hpp | 57.1% | Partial coverage |

## Branch Coverage Highlights

### Best Branch Coverage (100% lines hit)
- common/cluster_manager.hpp: 100% lines, 100% branches
- common/bloom_filter.hpp: 100% lines, 100% branches
- common/fault_injection.hpp: 90.7% lines, 100% branches
- distributed/shard_manager.hpp: 100% lines, 100% branches
- storage/buffer_pool_manager.cpp: 100% lines, 100% branches

### Lowest Branch Coverage
- network/rpc_message.hpp: 29.2% lines, 15.0% branches
- recovery/log_manager.cpp: 37.5% lines, 5.3% branches
- storage/heap_table.cpp: 18.3% lines, 26.3% branches
- parser/expression.hpp: 32.0% lines, 11.8% branches

## Recommendations for Next Tests

1. **shard_manager.hpp** - Already 100% coverage from existing distributed_executor_tests
2. **config.hpp** - 44.4% lines, needs dedicated config_tests.cpp
3. **arena_allocator.hpp** - 97.7% lines, only 3% missing - could add corner cases
4. **rpc_message.hpp** - 29.2% lines, 15.0% branches - needs serialization tests
5. **heap_table.cpp** - 18.3% lines - needs more tests (but may be covered by logic tests)
