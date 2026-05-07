# ADR 002: Join Reordering — Inner Join Only Constraint

## Status
Accepted

## Date
2026-05-07

## Context

PR #79 extended the Cost-Based Optimizer (Phase 2) with join reordering in `build_vectorized_plan()`. For each INNER JOIN, the optimizer estimates both orderings (A⋈B and B⋈A) using `RowEstimator::estimate_join_rows()` and swaps key expressions when the reverse order produces fewer rows — reducing the build-side hash table size.

However, the same reordering logic was originally written to apply to all join types including LEFT, RIGHT, and FULL outer joins. For outer joins, swapping key expressions can change which side is the outer (built) side, breaking query semantics — the side that must be preserved for null-outer tuples.

## Decision

Wrap the join reordering block with a type check:

```cpp
if (exec_join_type == executor::JoinType::Inner) {
    // ... estimate both orderings, swap keys if reverse is smaller ...
}
```

Outer joins (LEFT/RIGHT/FULL) skip reordering entirely. The `exec_join_type` variable is determined before the reordering block so the gating is available.

Additionally, the `current_est_rows` update (which tracks join output cardinality for subsequent joins in a chain) is also scoped inside the inner-join block, since outer join output size doesn't follow the same selectivity model.

## Consequences

### Positive
- LEFT/RIGHT/FULL joins preserve outer side semantics — null-outer tuples are produced from the correct side
- INNER joins benefit from smaller build-side hash tables when the right table is smaller
- `current_est_rows` for outer join chains is not polluted with potentially misleading inner-join estimates

### Negative
- A LEFT JOIN where the right (inner) table is much smaller won't get key-swapped to reduce build size — the larger left side remains the build side
- Future work could include physical operator tree reordering for outer joins (not just key swapping), but that requires more significant changes to `build_vectorized_plan()`

### Neutral
- Volcano path (`build_plan()`) does not have join reordering — this constraint applies only to the Vectorized path
- The inner join gating is a one-line change but is well-documented

## Alternatives Considered

### Alternative 1: Reorder operator tree for outer joins (swap build/probe children)
**Why rejected:** `VectorizedHashJoinOperator` always uses `current_root` as build and `right_scan` as probe. Swapping children would require reconstructing the scan operators, which is architecturally complex for Phase 2. The simpler key-swap approach is sufficient for inner joins.

### Alternative 2: Apply reordering to LEFT/RIGHT but not FULL
**Why rejected:** LEFT and RIGHT outer joins also have semantics that depend on preserving which side is the outer side. FULL outer joins are the clearest case (both sides can produce null-outer tuples), but LEFT/RIGHT have the same issue. Gate on all outer join types consistently.

### Alternative 3: Key swap for outer joins with outer-side detection
**Why rejected:** Detecting which side is the "outer" side of an outer join requires semantic analysis of the query — which side produces nulls when the other side has no match. This is non-trivial and better left to a future Phase 3 physical optimizer.