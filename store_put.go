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

	// Update primary key index
	if oldValue, err := s.Get(ctx, key); err == nil {
		oldKey := reflect.ValueOf(oldValue).Field(s.keyField).String()
		if oldKey != key {
			s.indexes[primaryKeyIndexName].delete(oldKey, oldKey)
		}
	}
	s.indexes[primaryKeyIndexName].insert(key, key)

	// Update B-tree indexes
	for name := range s.indexFields {
		oldValue := oldIndexValues[name]
		newValue := newIndexValues[name]
		if oldValue != newValue {
			if oldValue != "" {
				s.indexes[name].delete(oldValue, key)
			}
			if newValue != "" {
				s.indexes[name].insert(newValue, key)
			}
		}
	}

	data, err := msgpack.Marshal(value)
	if err != nil {
		s.database.Logger().Errorf("Failed to marshal value for key %s in bucket %s: %v", key, s.bucket, err)
		return WrappedError{Operation: "marshal", Bucket: string(s.bucket), Key: key, Err: err}
	}

	// Collect modified indexes for buffering
	modifiedIndexes := []string{primaryKeyIndexName} // Primary key is always modified
	for name := range s.indexFields {
		oldValue := oldIndexValues[name]
		newValue := newIndexValues[name]
		if oldValue != newValue {
			modifiedIndexes = append(modifiedIndexes, name)
		}
	}

	// Create operations: data operation + index operations
	ops := make([]operation, 1+len(modifiedIndexes))
	ops[0] = operation{
		Bucket: s.bucket,
		Key:    key,
		Value:  data,
		Type:   OperationPut,
	}

	for i, indexName := range modifiedIndexes {
		ops[i+1] = operation{
			Bucket: []byte(btreeBucketName),
			Key:    buildBTreeKey(string(s.bucket)+":", indexName),
			Value:  nil, // Serialized on flush
			Type:   OperationIndex,
		}
	}

	return s.database.writeOperations(ctx, ops)
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

	// Collect all index operations for batching
	indexInserts := make(map[string][]bTreeItem)
	indexDeletes := make(map[string][]bTreeItem)

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

		// Update primary key index
		if oldValue, exists := oldValues[key]; exists {
			oldKey := reflect.ValueOf(oldValue).Field(s.keyField).String()
			if oldKey != key {
				s.indexes[primaryKeyIndexName].delete(oldKey, oldKey)
			}
		}
		s.indexes[primaryKeyIndexName].insert(key, key)

		// Collect B-tree index operations for batching
		for name := range s.indexFields {
			oldVal := oldIndexValues[name]
			newVal := newIndexValues[name]
			if oldVal != newVal {
				if oldVal != "" {
					indexDeletes[name] = append(indexDeletes[name], bTreeItem{Key: oldVal, Value: key})
				}
				if newVal != "" {
					indexInserts[name] = append(indexInserts[name], bTreeItem{Key: newVal, Value: key})
				}
			}
		}

		buf := bufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		encoder := msgpack.NewEncoder(buf)
		err = encoder.Encode(value)
		if err != nil {
			bufferPool.Put(buf)
			s.database.Logger().Errorf("Failed to encode value for key %s in bucket %s: %v", key, s.bucket, err)
			return WrappedError{Operation: "encode", Bucket: string(s.bucket), Key: key, Err: err}
		}
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())

		operation := operation{
			Bucket: s.bucket,
			Key:    key,
			Value:  data,
			Type:   OperationPut,
		}
		operations = append(operations, operation)
		bufferPool.Put(buf)
	}

	// Apply batched index operations
	for name, items := range indexDeletes {
		s.indexes[name].bulkDelete(items)
	}
	for name, items := range indexInserts {
		s.indexes[name].bulkInsert(items)
	}

	// Collect modified indexes for buffering
	modifiedIndexes := make(map[string]bool)
	modifiedIndexes[primaryKeyIndexName] = true // Primary key is always modified
	for name := range indexInserts {
		modifiedIndexes[name] = true
	}
	for name := range indexDeletes {
		modifiedIndexes[name] = true
	}

	// Create index operations
	for indexName := range modifiedIndexes {
		operations = append(operations, operation{
			Bucket: []byte(btreeBucketName),
			Key:    buildBTreeKey(string(s.bucket)+":", indexName),
			Value:  nil, // Serialized on flush
			Type:   OperationIndex,
		})
	}

	return s.database.writeOperations(ctx, operations)
}
