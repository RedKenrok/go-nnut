# B-Tree Persistence Enhancement

## Objective
Enhance the persistence strategy for B-tree indexes to provide fast startup times and reliable recovery without rebuilding indexes from data.

## Background
Currently, B-tree indexes are rebuilt from stored data on every store creation, which can be slow for large datasets. While serialization exists, it's not actively maintained during operations. A proper persistence strategy should maintain B-trees on disk and provide fast loading.

## Current State Analysis
- B-trees are rebuilt on every store initialization
- Persistence methods exist but aren't called during operations
- No incremental updates or dirty tracking
- Startup time scales with data size

## Detailed Implementation Plan

### 1. Implement Dirty Tracking
- **Dirty Flags**: Add modification flags to BTreeIndex
- **Change Detection**: Mark tree as dirty on insert/delete operations
- **Batch Updates**: Track multiple changes before persistence

### 2. Add Persistence Triggers
- **On Flush**: Persist dirty B-trees during WAL flush operations
- **Periodic Persistence**: Background goroutine for regular saves
- **On Close**: Ensure all B-trees are persisted before shutdown
- **Conditional Persistence**: Only save when changes detected

### 3. Optimize Serialization
- **Incremental Updates**: Save only changed nodes instead of full tree
- **Compression**: Add optional compression for large trees
- **Versioning**: Include format versions for backward compatibility
- **Checksums**: Add integrity checks for persisted data

### 4. Enhance Loading Strategy
- **Lazy Loading**: Load B-trees only when first accessed
- **Partial Loading**: Load tree structure without full data initially
- **Fallback Rebuild**: Automatically rebuild if persisted data is corrupted
- **Memory Mapping**: Consider memory-mapped files for large indexes

### 5. Add Persistence Management
- **Persistence Manager**: Central component for coordinating saves
- **Queue System**: Async persistence to avoid blocking operations
- **Error Handling**: Graceful degradation if persistence fails
- **Monitoring**: Metrics for persistence performance and reliability

## Testing and Validation
- **Persistence Tests**: Verify B-trees survive restarts
- **Corruption Tests**: Test recovery from corrupted persisted data
- **Performance Tests**: Measure startup time improvements
- **Concurrency Tests**: Ensure persistence doesn't block operations

## Risks and Mitigations
- **Data Loss**: Implement atomic writes and backups
- **Performance**: Profile persistence overhead
- **Complexity**: Thorough testing of async persistence logic

## Success Criteria
- B-trees persist reliably across restarts
- Startup time significantly reduced for large datasets
- No performance impact on normal operations
- Automatic recovery from persistence failures
