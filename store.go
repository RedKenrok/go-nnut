package nnut

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

const (
	MaxKeyLength        = 1024
	MaxBucketNameLength = 255
	btreeBucketName     = "__btree_indexes"
)

// Store represents a typed bucket for storing and retrieving values of type T.
// It provides type-safe operations with automatic indexing and serialization.
type Store[T any] struct {
	database     *DB
	bucket       []byte
	keyField     int               // index of the field tagged with nnut:"key"
	indexFields  map[string]int    // field name -> field index
	fieldMap     map[string]int    // field name -> field index
	btreeIndexes map[string]*BTree // field name -> B-tree index
}

// persistBTreeIndexes saves any dirty B-tree indexes to persistent storage
func (s *Store[T]) persistBTreeIndexes() error {
	return s.database.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(btreeBucketName))
		if err != nil {
			return err
		}

		for fieldName, btree := range s.btreeIndexes {
			btree.mu.RLock()
			if !btree.dirty {
				btree.mu.RUnlock()
				continue
			}
			btree.mu.RUnlock()

			data, err := btree.Serialize()
			if err != nil {
				return fmt.Errorf("failed to serialize B-tree for field %s: %w", fieldName, err)
			}

			key := string(s.bucket) + ":" + fieldName
			err = bucket.Put([]byte(key), data)
			if err != nil {
				return fmt.Errorf("failed to persist B-tree for field %s: %w", fieldName, err)
			}

			btree.mu.Lock()
			btree.dirty = false
			btree.mu.Unlock()
		}
		return nil
	})
}

// loadBTreeIndexes loads persisted B-tree indexes from storage
func (s *Store[T]) loadBTreeIndexes() error {
	return s.database.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			// No bucket yet
			return nil
		}

		for fieldName := range s.indexFields {
			key := "_btree_" + fieldName
			data := bucket.Get([]byte(key))
			if data == nil {
				// No persisted index for this field
				continue
			}

			btree, err := deserializeBTreeIndex(data)
			if err != nil {
				// Log error but continue - will rebuild from data
				s.database.Logger().Warningf("Failed to load persisted B-tree for field %s: %v", fieldName, err)
				continue
			}

			s.btreeIndexes[fieldName] = btree
		}
		return nil
	})
}

// PersistIndexes saves any dirty B-tree indexes to persistent storage
func (s *Store[T]) PersistIndexes() error {
	return s.persistBTreeIndexes()
}

// NewStore creates a new store for type T with the given bucket name.
// It analyzes the struct tags of T to set up key fields and indexes.
// The type T must have exactly one field tagged with `nnut:"key"` of type string.
// Fields tagged with `nnut:"index"` will be automatically indexed for efficient querying.
func NewStore[T any](database *DB, bucketName string) (*Store[T], error) {
	// Validate bucket name
	if bucketName == "" {
		return nil, BucketNameError{BucketName: bucketName, Reason: "cannot be empty"}
	}
	if len(bucketName) > MaxBucketNameLength {
		return nil, BucketNameError{BucketName: bucketName, Reason: "too long"}
	}
	for _, r := range bucketName {
		if r == '\x00' || r == '/' || r == '\\' {
			return nil, BucketNameError{BucketName: bucketName, Reason: "contains invalid character"}
		}
	}

	// Inspect struct fields at runtime to identify key and index fields for dynamic storage
	var zeroValue T
	typeOfStruct := reflect.TypeOf(zeroValue)
	if typeOfStruct.Kind() != reflect.Struct {
		return nil, InvalidTypeError{Type: typeOfStruct.String()}
	}
	keyFieldIndex := -1
	indexFields := make(map[string]int)
	fieldMap := make(map[string]int)
	for fieldIndex := 0; fieldIndex < typeOfStruct.NumField(); fieldIndex++ {
		field := typeOfStruct.Field(fieldIndex)
		fieldMap[field.Name] = fieldIndex
		tagValue := field.Tag.Get("nnut")
		if tagValue == "key" {
			if field.Type.Kind() != reflect.String {
				return nil, KeyFieldNotStringError{FieldName: field.Name}
			}
			keyFieldIndex = fieldIndex
		} else if tagValue == "index" {
			indexFields[field.Name] = fieldIndex
		}
	}
	if keyFieldIndex == -1 {
		return nil, KeyFieldNotFoundError{}
	}

	// Validate index fields are strings or comparable (int)
	for fieldName, fieldIndex := range indexFields {
		field := typeOfStruct.Field(fieldIndex)
		kind := field.Type.Kind()
		if kind != reflect.String && kind != reflect.Int {
			return nil, IndexFieldTypeError{FieldName: field.Name, Type: field.Type.String()}
		}
		_ = fieldName // avoid unused variable
	}

	btreeIndexes := make(map[string]*BTree)
	for fieldName := range indexFields {
		btreeIndexes[fieldName] = NewBTreeIndex(32) // default branching factor
	}

	store := &Store[T]{
		database:     database,
		bucket:       []byte(bucketName),
		keyField:     keyFieldIndex,
		indexFields:  indexFields,
		fieldMap:     fieldMap,
		btreeIndexes: btreeIndexes,
	}

	// Load persisted B-tree indexes
	if err := store.loadBTreeIndexes(); err != nil {
		return nil, fmt.Errorf("failed to load B-tree indexes: %w", err)
	}

	return store, nil
}

// Gather index field values to maintain secondary index consistency
func (s *Store[T]) extractIndexValues(value T) map[string]string {
	structValue := reflect.ValueOf(value)
	result := make(map[string]string)
	for fieldName, fieldIndex := range s.indexFields {
		fieldValue := structValue.Field(fieldIndex)
		if fieldValue.Kind() == reflect.String {
			result[fieldName] = fieldValue.String()
		}
	}
	return result
}

// populateBTreeIndexes loads or rebuilds the B-tree indexes
func (s *Store[T]) populateBTreeIndexes() error {
	return s.database.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return nil // no data yet
		}
		// Try to load persisted B-trees first
		for fieldName := range s.indexFields {
			data := b.Get([]byte("_btree_" + fieldName))
			if data != nil {
				bt, err := deserializeBTreeIndex(data)
				if err == nil {
					s.btreeIndexes[fieldName] = bt
					continue
				}
				// If deserialize fails, rebuild
			}
			// Rebuild from data
			s.btreeIndexes[fieldName] = NewBTreeIndex(32)
		}
		// Populate from data
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if bytes.HasPrefix(k, []byte("_btree_")) {
				continue // skip persisted B-trees
			}
			var value T
			err := msgpack.Unmarshal(v, &value)
			if err != nil {
				return err
			}
			indexValues := s.extractIndexValues(value)
			key := string(k)
			for fieldName, indexValue := range indexValues {
				if indexValue != "" {
					s.btreeIndexes[fieldName].Insert(indexValue, key)
				}
			}
		}
		return nil
	})
}

// Checks if a key is valid
func validateKey(key string) error {
	if key == "" {
		return InvalidKeyError{Key: key}
	}
	if len(key) > MaxKeyLength {
		return InvalidKeyError{Key: key}
	}
	return nil
}
