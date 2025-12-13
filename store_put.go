package nnut

import (
	"bytes"
	"context"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

// Put stores a single record in the database.
// It automatically updates any indexes associated with the record.
// The record's key field must be set and valid.
func (s *Store[T]) Put(ctx context.Context, value T) error {
	// Retrieve the primary key via runtime type inspection
	valueReflection := reflect.ValueOf(value)
	key := valueReflection.Field(s.keyField).String()
	if err := validateKey(key); err != nil {
		return err
	}

	s.database.Logger().Debugf("Putting record with key %s in bucket %s", key, s.bucket)

	// Fetch existing record to handle index changes
	var oldIndexValues map[string]string
	oldValue, err := s.Get(ctx, key)
	if err == nil {
		oldIndexValues = s.extractIndexValues(oldValue)
	} else {
		oldIndexValues = make(map[string]string)
	}

	newIndexValues := s.extractIndexValues(value)

	// Update B-tree indexes
	for name := range s.indexFields {
		oldValue := oldIndexValues[name]
		newValue := newIndexValues[name]
		if oldValue != newValue {
			if oldValue != "" {
				s.btreeIndexes[name].Delete(oldValue, key)
			}
			if newValue != "" {
				s.btreeIndexes[name].Insert(newValue, key)
			}
		}
	}

	data, err := msgpack.Marshal(value)
	if err != nil {
		s.database.Logger().Errorf("Failed to marshal value for key %s in bucket %s: %v", key, s.bucket, err)
		return WrappedError{Operation: "marshal", Bucket: string(s.bucket), Key: key, Err: err}
	}

	operation := operation{
		Bucket: s.bucket,
		Key:    key,
		Value:  data,
		IsPut:  true,
	}

	return s.database.writeOperation(ctx, operation)
}

// PutBatch stores multiple records in a single batch operation.
// This is more efficient than calling Put multiple times.
// All records must have valid keys set.
func (s *Store[T]) PutBatch(ctx context.Context, values []T) error {
	s.database.Logger().Debugf("Putting batch of %d records in bucket %s", len(values), s.bucket)
	// Collect primary keys from all values
	keys := make([]string, len(values))
	keyToValue := make(map[string]T)
	for index, value := range values {
		valueReflection := reflect.ValueOf(value)
		key := valueReflection.Field(s.keyField).String()
		if err := validateKey(key); err != nil {
			return err
		}
		keys[index] = key
		keyToValue[key] = value
	}

	// Retrieve existing records for index updates
	oldValues, err := s.GetBatch(ctx, keys)
	if err != nil {
		return WrappedError{Operation: "get_batch", Bucket: string(s.bucket), Err: err}
	}

	// Build operations for each record
	var operations []operation
	for _, key := range keys {
		value := keyToValue[key]
		oldValue, exists := oldValues[key]
		var oldIndexValues map[string]string
		if exists {
			oldIndexValues = s.extractIndexValues(oldValue)
		} else {
			oldIndexValues = make(map[string]string)
		}

		newIndexValues := s.extractIndexValues(value)

		// Update B-tree indexes
		for name := range s.indexFields {
			oldValue := oldIndexValues[name]
			newValue := newIndexValues[name]
			if oldValue != newValue {
				if oldValue != "" {
					s.btreeIndexes[name].Delete(oldValue, key)
				}
				if newValue != "" {
					s.btreeIndexes[name].Insert(newValue, key)
				}
			}
		}

		buf := bufferPool.Get().(*bytes.Buffer)
		defer bufferPool.Put(buf)
		buf.Reset()
		encoder := msgpack.NewEncoder(buf)
		err = encoder.Encode(value)
		if err != nil {
			s.database.Logger().Errorf("Failed to encode value for key %s in bucket %s: %v", key, s.bucket, err)
			return WrappedError{Operation: "encode", Bucket: string(s.bucket), Key: key, Err: err}
		}
		data := buf.Bytes()

		operation := operation{
			Bucket: s.bucket,
			Key:    key,
			Value:  data,
			IsPut:  true,
		}
		operations = append(operations, operation)
	}

	// Add B-tree persistence operations to the batch for dirty indexes
	for fieldName, bt := range s.btreeIndexes {
		bt.mu.RLock()
		if !bt.dirty {
			bt.mu.RUnlock()
			continue
		}
		bt.mu.RUnlock()

		data, err := bt.Serialize()
		if err != nil {
			return err
		}
		btreeOp := operation{
			Bucket: s.bucket,
			Key:    "_btree_" + fieldName,
			Value:  data,
			IsPut:  true,
		}
		operations = append(operations, btreeOp)

		// Mark as clean
		bt.mu.Lock()
		bt.dirty = false
		bt.mu.Unlock()
	}

	return s.database.writeOperations(ctx, operations)
}
