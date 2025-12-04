package nnut

import (
	"bytes"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

// Persist a single record with index updates
func (s *Store[T]) Put(value T) error {
	// Retrieve the primary key via runtime type inspection
	valueReflection := reflect.ValueOf(value)
	key := valueReflection.Field(s.keyField).String()
	if key == "" {
		return InvalidKeyError{Key: key}
	}

	// Fetch existing record to handle index changes
	var oldIndexValues map[string]string
	oldValue, err := s.Get(key)
	if err == nil {
		oldIndexValues = s.extractIndexValues(oldValue)
	} else {
		oldIndexValues = make(map[string]string)
	}

	newIndexValues := s.extractIndexValues(value)

	// Prepare index maintenance operations
	var indexOperations []indexOperation
	for name := range s.indexFields {
		oldValue := oldIndexValues[name]
		newValue := newIndexValues[name]
		if oldValue != newValue {
			indexOperations = append(indexOperations, indexOperation{
				IndexName: name,
				OldValue:  oldValue,
				NewValue:  newValue,
			})
		}
	}

	data, err := msgpack.Marshal(value)
	if err != nil {
		return WrappedError{Op: "marshal", Bucket: string(s.bucket), Key: key, Err: err}
	}

	operation := operation{
		Bucket:          s.bucket,
		Key:             key,
		Value:           data,
		IsPut:           true,
		IndexOperations: indexOperations,
	}

	// Log the operation for crash recovery
	s.database.walMutex.Lock()
	var walBuffer bytes.Buffer
	walEncoder := msgpack.NewEncoder(&walBuffer)
	err = walEncoder.Encode(operation)
	if err != nil {
		s.database.walMutex.Unlock()
		return WrappedError{Op: "encode WAL", Bucket: string(s.bucket), Key: key, Err: err}
	}
	_, err = s.database.walFile.Write(walBuffer.Bytes())
	s.database.walMutex.Unlock()
	if err != nil {
		return FileSystemError{Path: s.database.config.WALPath, Operation: "write", Err: err}
	}

	s.database.operationsBufferMutex.Lock()
	s.database.operationsBuffer = append(s.database.operationsBuffer, operation)
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALFlushSize
	s.database.operationsBufferMutex.Unlock()

	if shouldFlush {
		s.database.flushChannel <- struct{}{}
	}
	return nil
}

// Persist multiple records efficiently
func (s *Store[T]) PutBatch(values []T) error {
	// Collect primary keys from all values
	keys := make([]string, len(values))
	keyToValue := make(map[string]T)
	for index, value := range values {
		valueReflection := reflect.ValueOf(value)
		key := valueReflection.Field(s.keyField).String()
		if key == "" {
			return InvalidKeyError{Key: key}
		}
		keys[index] = key
		keyToValue[key] = value
	}

	// Retrieve existing records for index updates
	oldValues, err := s.GetBatch(keys)
	if err != nil {
		return WrappedError{Op: "get_batch", Bucket: string(s.bucket), Err: err}
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

		// Set up index modifications
		var indexOperations []indexOperation
		for name := range s.indexFields {
			oldValue := oldIndexValues[name]
			newValue := newIndexValues[name]
			if oldValue != newValue {
				indexOperations = append(indexOperations, indexOperation{
					IndexName: name,
					OldValue:  oldValue,
					NewValue:  newValue,
				})
			}
		}

		buf := bufferPool.Get().(*bytes.Buffer)
		defer bufferPool.Put(buf)
		buf.Reset()
		encoder := msgpack.NewEncoder(buf)
		err = encoder.Encode(value)
		if err != nil {
			return WrappedError{Op: "encode", Bucket: string(s.bucket), Key: key, Err: err}
		}
		data := buf.Bytes()

		operation := operation{
			Bucket:          s.bucket,
			Key:             key,
			Value:           data,
			IsPut:           true,
			IndexOperations: indexOperations,
		}
		operations = append(operations, operation)
	}

	// Write all operations to WAL atomically
	var walBuffer bytes.Buffer
	walEncoder := msgpack.NewEncoder(&walBuffer)
	for _, operation := range operations {
		err = walEncoder.Encode(operation)
		if err != nil {
			return WrappedError{Op: "encode WAL batch", Bucket: string(s.bucket), Err: err}
		}
	}
	s.database.walMutex.Lock()
	_, err = s.database.walFile.Write(walBuffer.Bytes())
	s.database.walMutex.Unlock()
	if err != nil {
		return FileSystemError{Path: s.database.config.WALPath, Operation: "write_batch", Err: err}
	}

	// Queue operations for eventual flush
	s.database.operationsBufferMutex.Lock()
	s.database.operationsBuffer = append(s.database.operationsBuffer, operations...)
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALFlushSize
	s.database.operationsBufferMutex.Unlock()

	if shouldFlush {
		s.database.flushChannel <- struct{}{}
	}

	return nil
}
