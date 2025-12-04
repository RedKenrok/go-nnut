package nnut

import (
	"bytes"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

// Get retrieves a value by key
func (s *Store[T]) Get(key string) (T, error) {
	var result T
	err := s.database.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
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
			return WrappedError{Op: "decode", Bucket: string(s.bucket), Key: key, Err: err}
		}
		return nil
	})
	return result, err
}

// GetBatch retrieves multiple values by keys
func (s *Store[T]) GetBatch(keys []string) (map[string]T, error) {
	results := make(map[string]T)
	failed := make(map[string]error)

	err := s.database.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			// Missing bucket indicates no data exists - return empty results
			return nil
		}
		decoder := msgpack.GetDecoder()
		defer msgpack.PutDecoder(decoder)
		for _, key := range keys {
			data := bucket.Get([]byte(key))
			if data != nil {
				var item T
				decoder.Reset(bytes.NewReader(data))
				err := decoder.Decode(&item)
				if err != nil {
					// Collect decoding errors for individual items in batch
					failed[key] = WrappedError{Op: "decode", Bucket: string(s.bucket), Key: key, Err: err}
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
