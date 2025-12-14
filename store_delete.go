package nnut

import (
	"context"

	"go.etcd.io/bbolt"
)

// Delete removes a single record by its key.
func (s *Store[T]) Delete(ctx context.Context, key string) error {
	// Check primary key index first for fast rejection
	if s.btreeIndexes[primaryKeyIndexName].Search(key) == nil {
		return nil
	}

	s.database.Logger().Debugf("Deleting record with key %s from bucket %s", key, s.bucket)
	// Retrieve existing value to update indexes correctly
	var oldIndexValues map[string]string
	oldValue, err := s.Get(ctx, key)
	if err == nil {
		oldIndexValues = s.extractIndexValues(oldValue)
	} else {
		oldIndexValues = make(map[string]string)
	}

	// Update primary key index
	s.btreeIndexes[primaryKeyIndexName].Delete(key, key)

	// Update B-tree indexes
	for name := range s.indexFields {
		oldValue := oldIndexValues[name]
		if oldValue != "" {
			s.btreeIndexes[name].Delete(oldValue, key)
		}
	}

	operation := operation{
		Bucket: s.bucket,
		Key:    key,
		Value:  nil,
		IsPut:  false,
	}

	return s.database.writeOperation(ctx, operation)
}

// DeleteBatch removes multiple records by their keys.
// More efficient than calling Delete multiple times.
func (s *Store[T]) DeleteBatch(ctx context.Context, keys []string) error {
	s.database.Logger().Debugf("Deleting batch of %d records from bucket %s", len(keys), s.bucket)

	// Collect all index operations for batching
	indexDeletes := make(map[string][]BTreeItem)

	// Collect keys that exist in primary index
	var candidateKeys []string
	for _, key := range keys {
		if s.btreeIndexes[primaryKeyIndexName].Search(key) != nil {
			candidateKeys = append(candidateKeys, key)
		}
	}

	// Get old values for all candidate keys (handles buffer and DB)
	oldValuesMap, err := s.GetBatch(ctx, candidateKeys)
	if err != nil {
		return err
	}

	// Build operations and update indexes
	var operations []operation
	for _, key := range candidateKeys {
		if oldVal, exists := oldValuesMap[key]; exists {
			oldIndexValues := s.extractIndexValues(oldVal)

			// Update primary key index
			s.btreeIndexes[primaryKeyIndexName].Delete(key, key)

			// Collect B-tree index operations for batching
			for name := range s.indexFields {
				oldIdxVal := oldIndexValues[name]
				if oldIdxVal != "" {
					indexDeletes[name] = append(indexDeletes[name], BTreeItem{Key: oldIdxVal, Value: key})
				}
			}
		}

		operations = append(operations, operation{
			Bucket: s.bucket,
			Key:    key,
			Value:  nil,
			IsPut:  false,
		})
	}

	// Apply batched index operations
	for name, items := range indexDeletes {
		s.btreeIndexes[name].BulkDelete(items)
	}

	return s.database.writeOperations(ctx, operations)
}

// DeleteQuery deletes records matching the query conditions.
// Returns the number of records deleted.
// Supports the same query options as GetQuery for filtering and pagination.
func (s *Store[T]) DeleteQuery(ctx context.Context, query *Query) (int, error) {
	if err := s.validateQuery(query); err != nil {
		return 0, err
	}

	var keysToDelete []string
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	err := s.database.View(func(tx *bbolt.Tx) error {
		// Determine the maximum number of keys needed based on limit and offset
		maxKeys := 0
		if query.Limit > 0 {
			maxKeys = query.Offset + query.Limit
		}

		// Gather keys that potentially match the query conditions
		var candidateKeys []string
		if len(query.Conditions) > 0 {
			candidateKeys = s.getCandidateKeysTx(tx, query.Conditions, maxKeys)
		} else if query.Index != "" {
			// When no conditions but sorting is required, use the index directly
			candidateKeys = s.getKeysFromIndexTx(tx, query.Index, query.Sort, maxKeys)
		} else {
			// Fallback to scanning all keys when no optimizations apply
			candidateKeys = s.getAllKeysTx(tx, maxKeys)
		}

		// Skip offset and take only limit number of keys
		start := query.Offset
		if start > len(candidateKeys) {
			start = len(candidateKeys)
		}
		end := len(candidateKeys)
		if query.Limit > 0 && start+query.Limit < end {
			end = start + query.Limit
		}
		keysToDelete = candidateKeys[start:end]

		return nil
	})
	if err != nil {
		return 0, err
	}

	// Get old values for all keys to delete (handles buffer and DB)
	oldValuesMap, err := s.GetBatch(ctx, keysToDelete)
	if err != nil {
		return 0, err
	}

	// Collect all index operations for batching
	indexDeletes := make(map[string][]BTreeItem)

	// Build operations and update indexes
	var operations []operation
	for _, key := range keysToDelete {
		if oldVal, exists := oldValuesMap[key]; exists {
			oldIndexValues := s.extractIndexValues(oldVal)

			// Update primary key index
			s.btreeIndexes[primaryKeyIndexName].Delete(key, key)

			// Collect B-tree index operations for batching
			for name := range s.indexFields {
				oldIdxVal := oldIndexValues[name]
				if oldIdxVal != "" {
					indexDeletes[name] = append(indexDeletes[name], BTreeItem{Key: oldIdxVal, Value: key})
				}
			}
		}

		operations = append(operations, operation{
			Bucket: s.bucket,
			Key:    key,
			Value:  nil,
			IsPut:  false,
		})
	}

	// Apply batched index operations
	for name, items := range indexDeletes {
		s.btreeIndexes[name].BulkDelete(items)
	}

	err = s.database.writeOperations(ctx, operations)
	if err != nil {
		return 0, err
	}

	s.database.Flush()

	return len(keysToDelete), nil
}
