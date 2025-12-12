package nnut

import (
	"context"
	"strings"

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

		// Count only record keys, skip _btree_ metadata
		cursor := bucket.Cursor()
		for keyBytes, _ := cursor.First(); keyBytes != nil; keyBytes, _ = cursor.Next() {
			key := string(keyBytes)
			if !strings.HasPrefix(key, "_btree_") {
				count++
			}
		}

		// Adjust for buffered operations
		bufferedOperations := s.database.getBufferedOperationsForBucket(s.bucket)
		for _, operation := range bufferedOperations {
			// Skip B-tree operations
			if strings.HasPrefix(operation.Key, "_btree_") {
				continue
			}
			exists := bucket.Get([]byte(operation.Key)) != nil
			if operation.IsPut && !exists {
				count++ // New key being added
			} else if !operation.IsPut && exists {
				count-- // Existing key being deleted
			}
		}

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
		// Collect candidate keys from conditions
		var candidateKeys []string
		if len(query.Conditions) > 0 {
			candidateKeys = s.getCandidateKeysTx(tx, query.Conditions, 0)
		} else if query.Index != "" {
			// No conditions, but index, count from index
			count = s.countKeysFromIndexTx(tx, query.Index)
			return nil
		} else {
			// No conditions, no index, count all keys
			count = s.countAllKeysTx(tx)
			return nil
		}
		count = len(candidateKeys)
		return nil
	})
	return count, err
}
