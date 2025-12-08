package nnut

import (
	"bytes"
	"context"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
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

	// Set up index removals for each deleted item
	var indexOperations []indexOperation
	for name := range s.indexFields {
		oldValue := oldIndexValues[name]
		if oldValue != "" {
			indexOperations = append(indexOperations, indexOperation{
				IndexName: name,
				OldValue:  oldValue,
				NewValue:  "",
			})
		}
	}

	operation := operation{
		Bucket:          s.bucket,
		Key:             key,
		Value:           nil,
		IsPut:           false,
		IndexOperations: indexOperations,
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

		// Prepare index updates for deletion
		var indexOperations []indexOperation
		for name := range s.indexFields {
			oldValue := oldIndexValues[name]
			if oldValue != "" {
				indexOperations = append(indexOperations, indexOperation{
					IndexName: name,
					OldValue:  oldValue,
					NewValue:  "",
				})
			}
		}

		operation := operation{
			Bucket:          s.bucket,
			Key:             key,
			Value:           nil,
			IsPut:           false,
			IndexOperations: indexOperations,
		}
		operations = append(operations, operation)
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
	err := s.database.Update(func(tx *bbolt.Tx) error {
		// Gather keys that potentially match the query conditions
		var candidateKeys []string
		if len(query.Conditions) > 0 {
			candidateKeys = s.getCandidateKeysTx(tx, query.Conditions, 0)
		} else if query.Index != "" {
			// When no conditions but sorting is required, use the index directly
			candidateKeys = s.getKeysFromIndexTx(tx, query.Index, query.Sort, 0)
		} else {
			// Fallback to scanning all keys when no optimizations apply
			candidateKeys = s.getAllKeysTx(tx, 0)
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

		// Retrieve the actual data for the selected keys to get old index values
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return BucketNotFoundError{Bucket: string(s.bucket)}
		}
		decoder := msgpack.GetDecoder()
		defer msgpack.PutDecoder(decoder)

		for _, key := range keysToDelete {
			data := bucket.Get([]byte(key))
			if data == nil {
				continue
			}
			var item T
			decoder.Reset(bytes.NewReader(data))
			err := decoder.Decode(&item)
			if err != nil {
				s.database.Logger().Errorf("Failed to decode value for key %s in bucket %s during delete query: %v", key, s.bucket, err)
				continue
			}
			// Delete the record
			if err := bucket.Delete([]byte(key)); err != nil {
				continue
			}
			// Update indexes by removing old index entries
			oldIndexValues := s.extractIndexValues(item)
			for indexName, value := range oldIndexValues {
				indexBucketName := string(s.bucket) + "_index_" + indexName
				indexBucket := tx.Bucket([]byte(indexBucketName))
				if indexBucket != nil {
					indexKey := value + "\x00" + key
					indexBucket.Delete([]byte(indexKey))
				}
			}
			deletedCount++
		}
		return nil
	})
	if err != nil {
		return deletedCount, err
	}

	return deletedCount, nil
}
