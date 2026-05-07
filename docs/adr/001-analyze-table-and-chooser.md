# ADR 001: Cost-Based Optimizer Phase 1 — ANALYZE TABLE and Volcano/Vectorized Chooser

## Status
Accepted

## Date
2026-05-05

## Context

PR #75 introduced Phase 1 of a Cost-Based Optimizer (CBO) for cloudSQL. Before this change, the Volcano/Vectorized execution chooser was flag-based, controlled by `parallel_ && storage_manager_ && !has_sort_or_limit`. This meant vectorized execution was engaged whenever those flags were set, regardless of actual table size or query cost.

The problem: small tables (a few rows) incur per-batch overhead from vectorized operators without benefiting from batch processing. Large analytical scans (>10k rows) benefit significantly from vectorized batch execution.

## Decision

Implement a row-count-based chooser that selects the Vectorized path when estimated scan rows exceed a heuristic threshold (`kVectorizedRowThreshold = 10000`), and falls back to Volcano for smaller tables.

### Components Added

**1. `ANALYZE TABLE` SQL Command**
- Single-pass table scan collects per-column statistics: min/max for int/float, string length range, number of distinct values (NDV), null count
- Stats stored in catalog on `ColumnInfo` (extended with `has_stats`, `null_count`, `min_int`, `max_int`, `min_double`, `max_double`, `min_str_len`, `max_str_len`, `ndv`)
- Text columns: NDV collected via `unordered_set<std::string>` with 64-char prefix truncation to limit memory

**2. `RowEstimator` Utility (`optimizer/row_estimator.cpp`)**
- `estimate_scan_rows(TableInfo)` — returns `table.num_rows` if stats available
- `estimate_filter_rows(TableInfo, col_name, predicate_value)` — NDV-based selectivity for equality; range-based for integer comparisons
- `estimate_join_rows(left, right, key_col)` — cardinality estimate `|A| * |B| / max(NDV_A, NDV_B)`

**3. Cost-Based Chooser (`execute_select()`)**
- Guards with `from_expr->type() == ExprType::Column` so JOINs, subqueries, and aliased tables fall through to Volcano (which already handles them correctly)
- Threshold comment explains the 10k heuristic: "Vectorized operators outperform Volcano-style tuple-at-a-time above ~10k rows for analytical scan workloads"

## Consequences

### Positive
- Small tables avoid vectorized batch overhead; Volcano path is more efficient
- Large analytical scans automatically use optimized Vectorized path after `ANALYZE TABLE`
- Statistics infrastructure in place for Phase 2 (filter selectivity, join ordering)
- Single-pass ANALYZE adds no second table scan

### Negative
- ANALYZE TABLE requires a full table scan; periodic re-analysis needed for fresh stats on dynamic tables
- NDV prefix truncation (64 chars) underestimates NDV for text columns with long shared prefixes
- No locking during ANALYZE scan; concurrent writes can skew stats
- Chooser threshold is a static heuristic; workload-tuned values may differ

### Neutral
- Volcano path remains for sort/limit queries, JOINs, subqueries, and table aliases — unchanged
- ANALYZE is a manual command (no auto-analyze); users must invoke it after bulk loads

## Alternatives Considered

### Alternative 1: HyperLogLog for NDV
**Why rejected:** HyperLogLog would improve NDV accuracy for text columns but adds significant implementation complexity (more state, merge semantics). Acceptable for Phase 2.

### Alternative 2: Auto-analyze on first query after bulk load
**Why rejected:** Implicit analyze adds unpredictable latency to queries and complicates transaction semantics. Manual ANALYZE gives users control; automated re-analysis can be a future enhancement.

### Alternative 3: Dynamic threshold based on benchmark calibration
**Why rejected:** Without production workload profiles, a static 10k heuristic is sufficient. Threshold can be exposed as a session/cluster config in Phase 2.

---

## Phase 2 Extensions (PR #79)

### Filter Selectivity in Chooser

PR #79 extended the cost-based chooser in `execute_select()` to use filter selectivity when estimating rows for `WHERE col OP constant` predicates. Instead of using the full table scan estimate for all queries, the chooser now:

1. Starts with `estimate_scan_rows()` as baseline
2. If WHERE clause is a simple binary expression (Column-Constant or Constant-Column), calls `estimate_filter_rows(table, col_name, pred_val)`
3. Uses the filter estimate if the column has statistics; otherwise falls back to scan estimate
4. Compares against `kVectorizedRowThreshold` to decide Volcano vs Vectorized

**Selectivity estimation strategies** (`estimate_filter_rows()` in `row_estimator.cpp`):
- **Equality** (`WHERE col = value`): uses `1/NDV` selectivity
- **Integer range** (`WHERE id > 10000`): uses `(max - value) / (max - min)` when value is within [min, max]
- **String length** (`WHERE name LIKE 'prefix%'`): uses `1/(max_str_len - min_str_len + 1)` selectivity

### Join Reordering in Vectorized Path

PR #79 also added join reordering to `build_vectorized_plan()`. For each INNER JOIN:

1. Determine `exec_join_type` (Inner/Left/Right/Full)
2. **Only for Inner joins** (outer join semantics must be preserved — see ADR 002):
   - Extract left and right key column names as strings
   - Compute `est_forward = estimate_join_rows(current_chain, right_table, right_key)`
   - Compute `est_reverse = estimate_join_rows(right_table, current_chain, left_key)`
   - If `est_reverse < est_forward && est_reverse > 0`, swap `left_key` and `right_key` so the smaller table feeds the build-side hash table
3. Update `current_est_rows` for the join output (used by subsequent joins in the chain)

**Key constraint:** Swapping key expressions does not change the operator tree topology (left child remains build, right child remains probe). It only redirects which table's key is hashed on the build side. Physical reordering (swapping build/probe children) is a Phase 3 item.

**Row estimate tracking:** `current_est_rows` is initialized from `estimate_scan_rows(base_table)` and updated after each join in the chain, allowing subsequent joins to benefit from accurate intermediate sizes.

### ADR 002: Outer Join Safety

The inner-join-only gating for join reordering is documented in [ADR 002](./002-join-reordering-inner-only.md). Outer joins (LEFT/RIGHT/FULL) skip reordering entirely to preserve which side is the outer (null-producing) side.
