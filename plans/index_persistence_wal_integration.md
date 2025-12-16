# Index Persistence WAL Integration Plan

## Objective
Integrate B-tree index persistence with the WAL system to ensure atomicity between data and index operations, eliminating inconsistencies while minimizing serialization overhead through "dirty flags" instead of full B-tree serialization.

## Background
Currently, data operations are buffered through WAL and committed in batches via `DB.Flush()`, while index updates are applied immediately to in-memory B-trees and persisted separately via `Store.Flush()`. This creates potential inconsistencies where indexes may be persisted while data remains buffered, or vice versa.

The goal is to ensure data and index changes are committed atomically while avoiding the performance cost of serializing large B-tree structures in the WAL.

## Current State Analysis
- **Data Operations**: Buffered in WAL, committed atomically in `DB.Flush()`
- **Index Operations**: Updated in-memory immediately, persisted via separate `Store.Flush()` calls
- **Inconsistency Risk**: Indexes may be persisted independently of data commits
- **Performance**: Index persistence happens immediately, but `Store.Flush()` is rarely called in production

## Detailed Implementation Plan

### 1. Modify WAL Operation Structure
Replace the `IsPut` boolean with an operation type enum. Index operations in WAL are lightweight markers:

```go
type OperationType int

const (
    OpPut OperationType = iota
    OpDelete
    OpIndexDirty  // Lightweight marker indicating index needs rebuild on crash. Index is still commited during write.
)

type operation struct {
    Bucket []byte
    Key    string      // For data ops: record key; for index ops: index name (e.g., "users:email")
    Value  []byte      // For data ops: record value; unused for index ops
    Type   OperationType
    Epoch  uint64
}
```

### 2. Update Put/Delete Operations
Modify store operations to buffer full index data while using lightweight WAL markers:

**In `Put()` method:**
- Update in-memory B-tree as before
- Set `btree.dirty = true`
- **Buffer full index data**: Serialize the updated B-tree and store in `operationsBuffer` with `bufferKey(btreeBucketName, indexName)`
- **WAL marker only**: Create lightweight index operation with `Type = OpIndexDirty`, `Key = indexName` (no Value)
- Buffer data operation (`Type = OpPut`) and index marker together

**In `Delete()` method:**
- Remove from in-memory B-tree
- Set `btree.dirty = true`
- Buffer full serialized index data in operations buffer
- Create lightweight WAL marker with `Type = OpIndexDirty`
- Buffer data operation (`Type = OpDelete`) and index marker together

### 3. Modify DB.Flush()
Update the flush process to persist full index data from buffer while WAL contains only markers:

```go
func (db *DB) Flush() {
    // ... existing data flushing logic ...

    // Collect index names that need persistence from OpIndexDirty operations
    dirtyIndexes := make(map[string]bool)
    for _, op := range operations {
        if op.Type == OpIndexDirty {
            dirtyIndexes[op.Key] = true
        }
    }

    // Persist full index data from buffer (not from WAL operations)
    for indexName := range dirtyIndexes {
        bufferKey := bufferKey(btreeBucketName, indexName)
        if op, exists := db.operationsBuffer[bufferKey]; exists && op.Type == OpIndexDirty {
            // op.Value would contain the full serialized data in a real implementation
            // For now, this is a placeholder for persisting the buffered index data
            b, err := tx.CreateBucketIfNotExists([]byte(btreeBucketName))
            if err != nil {
                return err
            }
            // Persist the full index data (serialized B-tree from buffer)
            err = b.Put([]byte(indexName), op.Value)
            if err != nil {
                return err
            }
        }
    }
}
```

### 4. WAL Replay Integration
Modify `replayWAL()` to rebuild indexes marked as dirty:

- During replay, collect all `OpIndexDirty` operations
- After replaying data operations, rebuild affected indexes from scratch using the replayed data
- Rebuilding is acceptable since it only happens on crash recovery and WAL markers are lightweight

### 5. Index Loading Logic
Update `NewStore()` index loading:

- Load persisted indexes as before (from btree bucket)
- If WAL replay marked indexes as dirty, trigger rebuild from data
- Rebuild process: Scan all records in the bucket, extract index values, reconstruct B-tree

### 6. Remove Store.Flush()
Since indexes are now handled through WAL:
- Remove the `Store.Flush()` method
- Remove manual `Store.Flush()` calls from tests
- Update benchmarks to use `DB.Flush()` instead if `DB.Close()` is not already called.

### 7. Index Persistence Strategy
**Hybrid Approach:**
- Full index data buffered in `operationsBuffer` for fast access during normal operation
- WAL contains only lightweight `OpIndexDirty` markers to minimize serialization overhead
- `DB.Flush()` persists complete index data from buffer to disk

**Crash Recovery:**
- WAL replay identifies dirty indexes via markers
- Indexes are rebuilt from replayed data (not restored from WAL)
- Balances WAL size efficiency with exact recovery

### 8. Testing and Validation
- **Unit Tests**: Verify WAL entries include index dirty flags
- **Integration Tests**: Test crash recovery rebuilds indexes correctly
- **Performance Tests**: Ensure rebuild time is acceptable for various data sizes
- **Concurrency Tests**: Verify thread safety with index operations

### 9. Documentation Updates
- Update README with new atomic behavior
- Document that indexes are rebuilt on crash recovery
- Update API docs for changed behavior

## Benefits
- **Atomicity**: Data and index changes committed together via WAL
- **Minimal WAL Overhead**: Only lightweight markers in WAL, full data in buffer
- **Efficient Lookups**: O(1) buffer access for latest index state
- **Cleaner Architecture**: Enum-based operation types instead of boolean flags
- **Consistency**: No separate index persistence paths
- **Balanced Recovery**: Rebuild on crash vs. WAL size trade-off

## Risks and Mitigations
- **Rebuild Performance**: Index rebuilding on crash recovery may be slow for large datasets
  - *Mitigation*: Optimize rebuild process, consider incremental rebuilds
- **Memory Usage**: Buffered index data increases memory usage
  - *Mitigation*: Efficient serialization, periodic cleanup of old buffer entries
- **Complexity**: WAL logic becomes more complex with operation types
  - *Mitigation*: Thorough testing, clear separation of concerns

## Success Criteria
- Data and index operations are atomic
- WAL contains only lightweight index markers
- Crash recovery successfully rebuilds indexes
- Performance impact is acceptable
- All existing functionality preserved

## Implementation Status
✅ **Phase 1**: Extended operation struct with OperationType enum and updated WAL serialization to omit index data from WAL
✅ **Phase 2**: Updated Put/Delete methods to buffer full index data in operationsBuffer and create lightweight OpIndexDirty markers for WAL
✅ **Phase 3**: Modified DB.Flush() to persist buffered index data atomically with data operations
✅ **Phase 4**: Updated WAL replay to detect OpIndexDirty markers and trigger index rebuild on crash recovery
✅ **Phase 5**: Removed Store.Flush() method and updated test calls to use DB.Flush()
✅ **Phase 6**: Completed performance testing - benchmarks show acceptable performance impact
