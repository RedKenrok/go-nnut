# B-Tree Advanced Features

## Objective
Add advanced indexing features to the B-tree implementation to support complex query patterns and improved usability.

## Background
The current B-tree implementation provides basic indexing capabilities. Advanced features can enable more sophisticated queries, better performance for specific patterns, and enhanced functionality for complex applications.

## Current State Analysis
- Basic equality and range queries supported on indexed string fields (e.g., Name, Email) via single-field B-trees
- Efficient sorting and counting via B-tree traversal
- Full scans required for non-indexed fields (e.g., Age) and complex multi-condition queries (intersections of separate indexes)
- No prefix matching, composite indexes, or query planning
- Indexes are currently unnamed and tied directly to fields (one B-tree per `nnut:"index"` tag)
- Limited introspection: basic counts (keys, unique values) but no depth, fill factor, or selectivity metrics
- Modular persistence and bulk operations already implemented

## Prioritized Recommendations
Based on codebase analysis, features are prioritized by impact on common query patterns (e.g., autocomplete, multi-field queries) and current bottlenecks (scans for non-indexed fields).

1. **Prefix Search (High Priority)**: Essential for autocomplete and partial string queries. Addresses lack of efficient prefix support in current range queries.
2. **Composite Indexes (High Priority)**: Critical for multi-field queries without intersections/scans. Enables efficient AND operations on multiple indexed fields.
3. **Index Statistics and Introspection (Medium Priority)**: Improves query optimization with metrics for cost-based planning.
4. **Query Planning and Optimization (Medium Priority)**: Automates index selection and query rewriting for complex conditions.

## Detailed Implementation Plan

### 1. Prefix Search
- **Prefix Matching**: Find all keys starting with a given prefix using binary search to start position
- **Efficient Traversal**: Extend BTreeIterator for prefix bounds (e.g., min=prefix, max=prefix+"\uffff")
- **Wildcard Support**: Basic * suffix patterns
- **Auto-completion**: Support for prefix-based suggestions in query APIs
- **Integration**: Add PrefixSearch method to BTree, integrate with Store[T] for indexed fields

### 2. Composite Indexes
- **Named Indexes**: Support named indexes via `nnut:"index:name1,name2,..."` tags, where each name defines an index (single-field or composite)
- **Multi-field Keys**: Composite indexes use compound keys (e.g., delimited strings like "Name|Email") for efficient multi-field queries
- **Custom Comparators**: Pluggable comparison functions for composite keys to handle field-by-field sorting
- **Index Projections**: Partial field indexing for selective queries using prefix matching on composites
- **Query Optimization**: Choose optimal composite index for multi-field queries based on coverage and selectivity
- **Integration**: Update Store[T] to parse named indexes from tags, creating single-field B-trees for each name and composite B-trees for multi-field names (e.g., `nnut:"index:email,email-and-name"` creates "email" and "email-and-name" indexes)

### 3. Index Statistics and Introspection
- **Tree Metrics**: Depth, node count, fill factor, selectivity (unique values / total keys) for each named index
- **Query Statistics**: Track access patterns, hit rates, and performance per index
- **Index Health**: Monitor balance, efficiency, and fragmentation
- **Visualization**: Tree structure inspection tools for debugging
- **Integration**: Cache stats in Store[T] and use in query planner for cost-based decisions

### 4. Query Planning and Optimization
- **Index Selection**: Automatic choice of best named index (single or composite) for queries based on coverage, selectivity, and cost
- **Query Rewriting**: Optimize complex queries by combining conditions into composite key lookups or prefix searches
- **Cost Estimation**: Estimate query cost using index statistics (e.g., selectivity, depth) and intersection overhead
- **Adaptive Indexing**: Track query patterns and suggest/auto-create named composites for frequent multi-field queries
- **Integration**: Implement planner in store_query.go to evaluate named index combinations, preferring composites that cover multiple conditions

## Testing and Validation
- **Feature Tests**: Comprehensive testing for each advanced feature
- **Performance Benchmarks**: Measure impact of advanced features
- **Integration Tests**: Test with complex query scenarios
- **Compatibility Tests**: Ensure backward compatibility

## Risks and Mitigations
- **Complexity**: Modular design to isolate advanced features; implement incrementally by priority
- **Performance**: Optional features that don't impact basic operations; benchmark all changes
- **Maintenance**: Clear separation of core vs advanced functionality; ensure backward compatibility

## Success Criteria
- High-priority features (prefix search, named composite indexes) implemented and tested
- Efficient support for autocomplete, multi-field queries, and complex conditions via named indexes
- Modular architecture allowing feature selection without impacting basic operations
- No performance regression on existing equality/range queries
- Comprehensive documentation, examples, and benchmarks for new features, including tag syntax for named indexes
