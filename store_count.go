package nnut

import (
	"context"

	"go.etcd.io/bbolt"
)

// Count returns the total number of records in the store.
// Accounts for buffered operations that haven't been flushed yet.
func (s *Store[T]) Count(ctx context.Context) (int, error) {
	var count int
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	err := s.database.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			count = 0
			return nil
		}
		// Primary key index is always up to date with buffered operations
		count = s.btreeIndexes[primaryKeyIndexName].CountKeys()
		return nil
	})
	return count, err
}

// CountQuery returns the number of records matching the query conditions.
// More efficient than GetQuery when only the count is needed.
// Supports the same query options as GetQuery.
func (s *Store[T]) CountQuery(ctx context.Context, query *Query) (int, error) {
	if err := s.validateQuery(query); err != nil {
		return 0, err
	}

	var count int
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	err := s.database.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			count = 0
			return nil
		}

		// Collect candidate keys from conditions
		if len(query.Conditions) > 0 {
			candidateKeys := s.getCandidateKeysTx(tx, query.Conditions, 0)
			count = len(candidateKeys)

			// Apply buffered operations to get accurate count
			// Note: Put operations are already reflected in candidateKeys via index updates
			// We only need to handle Delete operations that remove keys from results
			bufferedOperations := s.database.getBufferedOperationsForBucket(s.bucket)
			for _, operation := range bufferedOperations {
				if !operation.IsPut {
					// For Delete operations, check if the key was in our candidates
					for _, candidateKey := range candidateKeys {
						if operation.Key == candidateKey {
							count-- // Existing key being deleted that matched conditions
							break
						}
					}
				}
			}
			return nil
		} else if query.Index != "" {
			// No conditions, but index, count from index
			// Note: B-tree indexes are updated immediately for buffered operations,
			// so CountKeys() already reflects buffered Puts
			count = s.btreeIndexes[query.Index].CountKeys()

			// Apply buffered operations to get accurate count
			// We only need to handle Delete operations that remove keys from the index
			bufferedOperations := s.database.getBufferedOperationsForBucket(s.bucket)
			for _, operation := range bufferedOperations {
				if !operation.IsPut {
					// For Delete operations, check if the key exists in the index
					if s.btreeIndexes[query.Index].Search(operation.Key) != nil {
						count-- // Existing key being deleted from index
					}
				}
			}
			return nil
		}

		// No conditions, no index, count all keys
		// Primary key index is always up to date with buffered operations
		count = s.btreeIndexes[primaryKeyIndexName].CountKeys()
		return nil
	})
	return count, err
}
