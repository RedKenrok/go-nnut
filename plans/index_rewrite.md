# Index Rewrite

## Objective
Rewrite the index implementation in the library to use a B-tree structure for improved performance, durability, and usability. This addresses current limitations where indexes require WAL replay and lack efficient length queries.

## Background
Indexes in nnut are currently implemented by storing key-value pairs in the underlying bbolt database, with composite keys (value + separator + key) for sorting. While this enables range queries, it has significant drawbacks that impact reliability and query efficiency.

## Current State Analysis
- Indexes are stored as sorted key-value pairs in bbolt buckets.
- WAL replay is required to make indexes usable after crashes, leading to potential staleness.
- Index lengths (e.g., number of keys per value or total entries) are not readily available without full scans.
- Mutations require querying existing data to determine old values for index updates.

## Detailed Implementation Plan

### 1. Design the B-tree Index Structure
- **B-tree Definition**: Implement a B-tree with configurable branching factor (e.g., 32-128) for disk efficiency. Each node contains:
  - Keys: Sorted list of indexed values (strings).
  - Values: Corresponding lists of record keys ([]string).
  - Children: Pointers to child nodes.
- **Struct**: Create a `BTreeIndex` struct with root node, branching factor, and methods (insert, delete, search).
- **In-memory Representation**: Use Go slices for node data; serialize for persistence.

### 2. Persistence Mechanism
- **Serialization**: Use msgpack to serialize nodes. Store the B-tree as database entries (e.g., `index_root` for root, `index_node_<id>` for nodes).
- **Lifecycle**: Deserialize on database open; serialize on flush.
- **Safety**: Ensure atomic updates during persistence to prevent corruption.

### 3. Core Operations
- **Insert/Add Key**: Traverse to the appropriate node. Append key to existing value list or create new entry, splitting nodes as needed for balance.
- **Delete/Remove Key**: Traverse to node, remove key from list. If list empties, remove entry and rebalance (merge/redistribute).
- **Search**: Traverse tree to find value node and return key list.
- **Mutations**: Query main data for old value, update old/new nodes accordingly.

### 4. Integration with WAL and Buffering
- **Buffering**: Index operations are buffered alongside data operations for WAL consistency.
- **Flush Process**: Apply B-tree changes during flush and persist the tree.
- **Concurrency**: Use existing mutexes to protect tree operations.

### 5. Query and Optimization Support
- **Range Queries**: Implement tree traversal for value ranges.
- **Counts**: Per-value lengths from node lists; total via summation (with optional caching).
- **Performance**: Leverage B-tree's O(log n) complexity for large indexes.

### 6. Testing and Validation
- **Unit Tests**: Cover insert/delete/search with various tree states.
- **Integration Tests**: Test with WAL, buffering, and concurrent access.
- **Edge Cases**: Empty trees, node splits/merges, large datasets.
- **Benchmarks**: Compare performance against current implementation.

### 7. Risks and Mitigations
- **Complexity**: Thorough testing to ensure correctness.
- **Performance**: Profile and optimize serialization/traversal.

## Success Criteria
- B-tree indexes provide O(log n) operations for mutations and queries.
- Indexes are immediately usable without WAL replay.
- Length queries are efficient without full scans.
- Comprehensive tests pass, including concurrency and recovery scenarios.
- No performance regressions; improved query times for large indexes.
