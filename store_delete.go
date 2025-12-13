package nnut

import (
	"bytes"
	"context"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

// Delete removes a single record by its key.
// Automatically updates indexes to remove references to the deleted record.
func (s *Store[T]) Delete(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
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
// Automatically updates indexes for all deleted records.
func (s *Store[T]) DeleteBatch(ctx context.Context, keys []string) error {
	s.database.Logger().Debugf("Deleting batch of %d records from bucket %s", len(keys), s.bucket)
	// Fetch current values to handle index updates in batch
	oldValues, err := s.GetBatch(ctx, keys)
	if err != nil {
		return WrappedError{Operation: "get_batch", Bucket: string(s.bucket), Err: err}
	}

	// Collect all index operations for batching
	indexDeletes := make(map[string][]BTreeItem)

	// Build operations for each key to be deleted
	var operations []operation
	for _, key := range keys {
		oldValue, exists := oldValues[key]
		var oldIndexValues map[string]string
		if exists {
			oldIndexValues = s.extractIndexValues(oldValue)
		} else {
			oldIndexValues = make(map[string]string)
		}

		// Collect B-tree index operations for batching
		for name := range s.indexFields {
			oldVal := oldIndexValues[name]
			if oldVal != "" {
				indexDeletes[name] = append(indexDeletes[name], BTreeItem{Key: oldVal, Value: key})
			}
		}

		operation := operation{
			Bucket: s.bucket,
			Key:    key,
			Value:  nil,
			IsPut:  false,
		}
		operations = append(operations, operation)
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

	var deletedCount int
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	// Gather keys that potentially match the query conditions using B-trees
	var candidateKeys []string
	if len(query.Conditions) > 0 {
		// Use B-tree based candidate selection
		candidateKeys = s.getCandidateKeys(query.Conditions, 0)
	} else if query.Index != "" {
		// When no conditions but sorting is required, use the index directly
		candidateKeys = s.getKeysFromIndex(query.Index, query.Sort, 0)
	} else {
		// Fallback to scanning all keys when no optimizations apply
		candidateKeys = s.getAllKeys(0)
	}

	// Apply offset and limit to candidate keys
	start := query.Offset
	if start > len(candidateKeys) {
		start = len(candidateKeys)
	}
	end := len(candidateKeys)
	if query.Limit > 0 && start+query.Limit < end {
		end = start + query.Limit
	}
	keysToDelete := candidateKeys[start:end]

	// Perform deletions in a transaction for immediate consistency
	err := s.database.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return BucketNotFoundError{Bucket: string(s.bucket)}
		}

		// Collect index operations for batching
		indexDeletes := make(map[string][]BTreeItem)

		for _, key := range keysToDelete {
			data := bucket.Get([]byte(key))
			if data == nil {
				continue
			}

			// Decode the record to update B-trees
			var item T
			decoder := msgpack.GetDecoder()
			decoder.Reset(bytes.NewReader(data))
			err := decoder.Decode(&item)
			msgpack.PutDecoder(decoder)
			if err != nil {
				s.database.Logger().Errorf("Failed to decode value for key %s in bucket %s during delete query: %v", key, s.bucket, err)
				continue
			}

			// Delete the record
			if err := bucket.Delete([]byte(key)); err != nil {
				continue
			}

			// Collect B-tree index operations for batching
			oldIndexValues := s.extractIndexValues(item)
			for indexName, value := range oldIndexValues {
				if value != "" {
					indexDeletes[indexName] = append(indexDeletes[indexName], BTreeItem{Key: value, Value: key})
				}
			}
			deletedCount++
		}

		// Apply batched index operations
		for indexName, items := range indexDeletes {
			s.btreeIndexes[indexName].BulkDelete(items)
		}

		return nil
	})
	if err != nil {
		return deletedCount, err
	}

	return deletedCount, nil
}
