package nnut

import "context"

// Has checks if a primary key exists in the store.
// Returns true if the key exists, false otherwise.
// This is more efficient than Get for existence checks.
func (s *Store[T]) Has(ctx context.Context, key string) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	// Check primary key index
	return s.btreeIndexes[primaryKeyIndexName].Search(key) != nil, nil
}
