package nnut

import (
	"bytes"
	"context"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

// Get retrieves a single record by its key.
// Returns the zero value of T and an error if the key is not found.
func (s *Store[T]) Get(ctx context.Context, key string) (T, error) {
	if err := validateKey(key); err != nil {
		var zero T
		return zero, err
	}

	s.database.Logger().Debugf("Getting record with key %s from bucket %s", key, s.bucket)
	var result T

	// Check primary key index first for fast rejection
	if s.indexes[primaryKeyIndexName].search(key) == nil {
		return result, KeyNotFoundError{Bucket: string(s.bucket), Key: key}
	}

	// Check buffer for pending changes first
	if operation, exists := s.database.getLatestBufferedOperation(s.bucket, key); exists {
		if operation.Type == OperationPut {
			// Apply buffered put operation
			decoder := msgpack.GetDecoder()
			defer msgpack.PutDecoder(decoder)
			decoder.Reset(bytes.NewReader(operation.Value))
			err := decoder.Decode(&result)
			if err != nil {
				s.database.Logger().Errorf("Failed to decode buffered value for key %s in bucket %s: %v", key, s.bucket, err)
				return result, WrappedError{Operation: "decode buffered", Bucket: string(s.bucket), Key: key, Err: err}
			}
			return result, nil
		} else {
			// Buffered delete operation
			return result, KeyNotFoundError{Bucket: string(s.bucket), Key: key}
		}
	}

	// No buffered changes, query database
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	default:
	}
	err := s.database.View(func(transaction *bbolt.Tx) error {
		bucket := transaction.Bucket(s.bucket)
		if bucket == nil {
			return BucketNotFoundError{Bucket: string(s.bucket)}
		}
		data := bucket.Get([]byte(key))
		if data == nil {
			return KeyNotFoundError{Bucket: string(s.bucket), Key: key}
		}
		decoder := msgpack.GetDecoder()
		defer msgpack.PutDecoder(decoder)
		decoder.Reset(bytes.NewReader(data))
		err := decoder.Decode(&result)
		if err != nil {
			s.database.Logger().Errorf("Failed to decode value for key %s in bucket %s: %v", key, s.bucket, err)
			return WrappedError{Operation: "decode", Bucket: string(s.bucket), Key: key, Err: err}
		}
		return nil
	})
	return result, err
}

// GetBatch retrieves multiple records by their keys.
// Returns a map of found records. Missing keys are not included in the map.
// If some keys fail to decode, returns a PartialBatchError.
func (s *Store[T]) GetBatch(ctx context.Context, keys []string) (map[string]T, error) {
	for _, key := range keys {
		if err := validateKey(key); err != nil {
			return nil, err
		}
	}

	s.database.Logger().Debugf("Getting batch of %d records from bucket %s", len(keys), s.bucket)
	results := make(map[string]T)
	failed := make(map[string]error)

	// Filter keys that exist in primary key index for fast rejection
	var existingKeys []string
	for _, key := range keys {
		if s.indexes[primaryKeyIndexName].search(key) != nil {
			existingKeys = append(existingKeys, key)
		}
	}

	// Check buffer for pending changes first
	bufferDecoder := msgpack.GetDecoder()
	defer msgpack.PutDecoder(bufferDecoder)
	for _, key := range existingKeys {
		if operation, exists := s.database.getLatestBufferedOperation(s.bucket, key); exists {
			if operation.Type == OperationPut {
				var item T
				bufferDecoder.Reset(bytes.NewReader(operation.Value))
				err := bufferDecoder.Decode(&item)
				if err != nil {
					s.database.Logger().Errorf("Failed to decode buffered value for key %s in bucket %s: %v", key, s.bucket, err)
					failed[key] = WrappedError{Operation: "decode buffered", Bucket: string(s.bucket), Key: key, Err: err}
					continue
				}
				results[key] = item
			}
			// For buffered deletes, don't add to results (treat as not found)
		} else {
			// No buffered change, will check DB below
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	err := s.database.View(func(transaction *bbolt.Tx) error {
		bucket := transaction.Bucket(s.bucket)
		if bucket == nil {
			// Missing bucket indicates no data exists - return empty results
			return nil
		}
		decoder := msgpack.GetDecoder()
		defer msgpack.PutDecoder(decoder)
		for _, key := range existingKeys {
			// Skip if already handled by buffer
			if _, alreadyHandled := results[key]; alreadyHandled {
				continue
			}
			if _, failedKey := failed[key]; failedKey {
				continue
			}

			data := bucket.Get([]byte(key))
			if data != nil {
				var item T
				decoder.Reset(bytes.NewReader(data))
				err := decoder.Decode(&item)
				if err != nil {
					s.database.Logger().Errorf("Failed to decode value for key %s in bucket %s: %v", key, s.bucket, err)
					// Collect decoding errors for individual items in batch
					failed[key] = WrappedError{Operation: "decode", Bucket: string(s.bucket), Key: key, Err: err}
					continue
				}
				results[key] = item
			} else {
				// Key not found - this is not an error, just missing data
				// Don't add to failed map for missing keys
			}
		}
		return nil
	})

	// Only return partial error if there were actual errors (not just missing keys)
	if err == nil && len(failed) > 0 {
		return results, PartialBatchError{SuccessfulCount: len(results), Failed: failed}
	}

	return results, err
}

// GetQuery retrieves records matching the given query conditions.
// Supports filtering, sorting, pagination, and indexing for efficient queries.
func (s *Store[T]) GetQuery(ctx context.Context, query *Query) ([]T, error) {
	if err := s.validateQuery(query); err != nil {
		return nil, err
	}

	// TODO: Turn into slice.
	var results map[string]T = make(map[string]T)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	err := s.database.View(func(transaction *bbolt.Tx) error {
		// Determine the maximum number of keys needed based on limit and offset
		maxKeys := 0
		if query.Limit > 0 {
			maxKeys = query.Offset + query.Limit
		}

		// Gather keys that potentially match the query conditions
		var candidateKeys []string
		if len(query.Conditions) > 0 {
			candidateKeys = s.getCandidateKeysTx(transaction, query.Conditions, maxKeys)
		} else if query.Index != "" {
			// When no conditions but sorting is required, use the index directly
			candidateKeys = s.getKeysFromIndexTx(transaction, query.Index, query.Sort, maxKeys)
		} else {
			// Fallback to scanning all keys when no optimizations apply
			candidateKeys = s.getAllKeysTx(transaction, maxKeys)
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
		keysToFetch := candidateKeys[start:end]

		// Retrieve the actual data for the selected keys
		bucket := transaction.Bucket(s.bucket)
		if bucket == nil {
			return BucketNotFoundError{Bucket: string(s.bucket)}
		}
		decoder := msgpack.GetDecoder()
		defer msgpack.PutDecoder(decoder)
		for _, key := range keysToFetch {
			data := bucket.Get([]byte(key))
			if data == nil {
				continue
			}
			var item T
			decoder.Reset(bytes.NewReader(data))
			err := decoder.Decode(&item)
			if err != nil {
				continue
			}
			results[key] = item
			// TODO: Look for puts in the buffer here.
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// TODO: Only insert new items from the buffer to the slice at the right location based on the sort.
	// Apply buffered operations
	bufferedOperations := s.database.getBufferedOperationsForBucket(s.bucket)
	for _, operation := range bufferedOperations {
		if operation.Type == OperationPut {
			var item T
			decoder := msgpack.GetDecoder()
			decoder.Reset(bytes.NewReader(operation.Value))
			err := decoder.Decode(&item)
			msgpack.PutDecoder(decoder)
			if err == nil {
				results[operation.Key] = item
			}
		} else if operation.Type == OperationDelete {
			delete(results, operation.Key)
		}
	}

	// Convert to slice
	var finalResults []T
	for _, item := range results {
		finalResults = append(finalResults, item)
	}

	// Apply sorting if the index wasn't used for ordering
	if query.Index != "" && len(query.Conditions) > 0 {
		s.sortResults(finalResults, query.Index, query.Sort)
	}

	return finalResults, nil
}
