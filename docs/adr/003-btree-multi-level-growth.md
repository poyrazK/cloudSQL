# ADR 003: B+ Tree Multi-Level Growth

## Status
Accepted

## Date
2026-05-05

## Context

The cloudSQL storage engine needed a durable on-disk B+ tree index capable of multi-level growth. Early phases implemented slot array format (Phase 1) and find_leaf() traversal (Phase 2), but inserts into a full leaf would fail silently or corrupt tree structure.

The problem: a B+ tree must handle arbitrary depth growth through a cascade of splits — leaf splits propagate to parent internal nodes, which may themselves split, recursively up to a new root.

## Decision

Implement a five-phase approach to multi-level B+ tree growth:

### Phase 1: Slot Array Format
- **Entries grow backward** from PAGE_SIZE end
- **Slots grow forward** from after NodeHeader
- Slot array: `SlotEntry { uint16_t offset, uint16_t length }` — 4 bytes each
- Binary entry format enables O(1) slot access without deserializing all entries

### Phase 2: find_leaf() with Binary Search
- Traverse from root to leaf by binary-searching internal node slots
- `compare_separator()` compares key against separator at slot position
- Returns leaf page number directly; no iteration needed

### Phase 3: Leaf Split (split_leaf)
- Split at midpoint: upper half entries copied to new right leaf
- Right leaf's `next_leaf` pointer chain maintained for range scans
- `pending_separator_` stores the separator key for parent insertion
- Returns new right page number so caller can wire up parent link

### Phase 4: Parent Propagation (insert_into_parent / split_internal)
- **Separator promotion**: entry at split_point is **promoted** to parent, not copied to children
- Left node: slots [0, split_point), children [0, split_point+1)
- Right node: slots [split_point+1, num_keys), children [split_point+1, num_keys+1)
- Child at split_point+1 becomes leftmost child of right node after split
- `update_child_parent()` updates parent_page pointers on all affected children
- Split cascade: if parent is also full, recurse with promoted separator

### Phase 5: Root Split Handling
- Root split detected when `parent_page == 0` (root has no parent)
- `create_new_root()` allocates new root as internal node with 1 separator
- Both split children updated to point to new root
- `root_page_` updated to new root page number

### Entry Format
- **Leaf entry**: `type(1) + key_len(4) + key_data(N) + page_num(4) + slot_num(2)` = 11+N bytes
- **Internal entry**: `type(1) + key_len(4) + key_data(N) + child_page_num(4)` = 9+N bytes
- `NodeHeader`: 12 bytes — type + num_keys + parent_page + next_leaf

### Slot Access
- `get_slot(buffer, slot_idx, out)`: returns SlotEntry at slot_idx
- `put_slot(buffer, slot_idx, entry)`: writes SlotEntry at slot_idx
- `get_data_start_offset(num_keys)`: returns start of entry data area (grows backward)
- `compute_entry_size(key)`: computes serialized entry size for a key

## Consequences

### Positive
- Multi-level tree growth handled correctly through split cascade
- Root split case properly distinguished from non-root splits
- Range scans remain correct via next_leaf chain maintained on split
- Slot array format enables binary search without full entry deserialization

### Negative
- Split cascade may cause multiple page writes per insert in worst case
- Internal node entries do not store slot_num (unlike leaf entries which store page_num + slot_num for RIDs)
- No balancing/redistribution between siblings — always splits at midpoint

### Neutral
- Depth grows only when root (and only root) splits — tree depth increments slowly
- All children of split internal nodes get correct parent pointers via update_child_parent()

## Alternatives Considered

### Alternative 1: Always split at first available slot, redistribute later
**Why rejected:** Redistribution adds complexity and requires additional writes. Midpoint split is deterministic and provides good balance.

### Alternative 2: Store full entries in internal nodes (not just separators)
**Why rejected:** Internal nodes store separator keys only — actual data lives in leaf nodes. This keeps internal nodes lean and maximizes branching factor.

### Alternative 3: Top-down splitting (split during descent)
**Why rejected:** Top-down splitting requires holding locks on multiple pages during traversal. Bottom-up (split on insert) defers splits and only touches affected pages.

## Implementation Phases

| Phase | Feature | Status |
|-------|---------|--------|
| 1 | Slot array format | Done |
| 2 | find_leaf() traversal | Done |
| 3 | split_leaf() | Done |
| 4 | insert_into_parent() / split_internal() | Done |
| 5 | Root split handling | Done |

## Test Results
- 29/29 BTreeIndexTests pass
- 1 pre-existing failure: BTreeIndexNextLeafTests.ScanIterator_NextLeaf (page format mismatch — raw test predates slot array)