package nnut

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

const (
	MaxKeyLength        = 1024
	MaxBucketNameLength = 255
	btreeBucketName     = "__btree_indexes"
	primaryKeyIndexName = "__primary_key"
)

// keyBuilderPool provides reusable strings.Builder instances to reduce allocations
var keyBuilderPool = sync.Pool{
	New: func() interface{} {
		return &strings.Builder{}
	},
}

// buildBTreeKey constructs a B-tree key using the bucket prefix and field name
func buildBTreeKey(bucketPrefix, fieldName string) string {
	builder := keyBuilderPool.Get().(*strings.Builder)
	defer func() {
		builder.Reset()
		keyBuilderPool.Put(builder)
	}()

	builder.Grow(len(bucketPrefix) + len(fieldName))
	builder.WriteString(bucketPrefix)
	builder.WriteString(fieldName)
	return builder.String()
}

// Store represents a typed bucket for storing and retrieving values of type T.
// It provides type-safe operations with automatic indexing and serialization.
type Store[T any] struct {
	database     *DB
	bucket       []byte
	keyField     int               // index of the field tagged with nnut:"key"
	indexFields  map[string]int    // field name -> field index
	fieldMap     map[string]int    // field name -> field index
	btreeIndexes map[string]*BTree // field name -> B-tree index (includes primary key as "__primary_key")
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
	btreeIndexes[primaryKeyIndexName] = NewBTreeIndex(32) // primary key index

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

// loadBTreeIndexes loads persisted B-tree indexes from storage
func (s *Store[T]) loadBTreeIndexes() error {
	return s.database.View(func(tx *bolt.Tx) error {
		// Load from dedicated btree bucket
		bucket := tx.Bucket([]byte(btreeBucketName))
		// Pre-compute bucket prefix for key construction
		bucketPrefix := string(s.bucket) + ":"

		// Load primary key index
		var primaryData []byte
		if bucket != nil {
			primaryKey := buildBTreeKey(bucketPrefix, primaryKeyIndexName)
			primaryData = bucket.Get([]byte(primaryKey))
		}
		if primaryData == nil {
			// No persisted index, rebuild from database
			s.rebuildPrimaryKeyIndex(tx)
		} else {
			btree, err := deserializeBTree(primaryData)
			if err == nil {
				s.btreeIndexes[primaryKeyIndexName] = btree
			} else {
				s.database.Logger().Warningf("Failed to load persisted primary key B-tree: %v", err)
				// Rebuild index from database
				s.rebuildPrimaryKeyIndex(tx)
			}
		}

		// Load secondary key indexes
		for fieldName := range s.indexFields {
			var secondaryData []byte
			if bucket != nil {
				secondaryKey := buildBTreeKey(bucketPrefix, fieldName)
				secondaryData = bucket.Get([]byte(secondaryKey))
			}
			if secondaryData == nil {
				// No persisted index, rebuild from database
				s.rebuildSecondaryIndex(fieldName, tx)
			} else {
				btree, err := deserializeBTree(secondaryData)
				if err == nil {
					s.btreeIndexes[fieldName] = btree
				} else {
					// Log error but continue
					s.database.Logger().Warningf("Failed to load persisted B-tree for field %s: %v", fieldName, err)
					// Rebuild index from database
					s.rebuildSecondaryIndex(fieldName, tx)
				}
			}
		}
		return nil
	})
}

// Flush persists any pending B-tree index changes to disk
func (s *Store[T]) Flush() error {
	// TODO: This should be applied via the buffer and Write Ahead Log system!
	return s.database.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(btreeBucketName))
		if err != nil {
			return err
		}

		// Pre-compute bucket prefix for key construction
		bucketPrefix := string(s.bucket) + ":"

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

			key := buildBTreeKey(bucketPrefix, fieldName)
			err = bucket.Put([]byte(key), data)
			if err != nil {
				return fmt.Errorf("failed to persist B-tree for field %s: %w", fieldName, err)
			}

			btree.mu.Lock()
			btree.dirty = false
			btree.mu.Unlock()
		}

		// Persist primary key index
		primaryKeyIndex := s.btreeIndexes[primaryKeyIndexName]
		primaryKeyIndex.mu.RLock()
		if primaryKeyIndex.dirty {
			primaryKeyIndex.mu.RUnlock()

			data, err := primaryKeyIndex.Serialize()
			if err != nil {
				return fmt.Errorf("failed to serialize primary key B-tree: %w", err)
			}

			key := buildBTreeKey(bucketPrefix, primaryKeyIndexName)
			err = bucket.Put([]byte(key), data)
			if err != nil {
				return fmt.Errorf("failed to persist primary key B-tree: %w", err)
			}

			primaryKeyIndex.mu.Lock()
			primaryKeyIndex.dirty = false
			primaryKeyIndex.mu.Unlock()
		} else {
			primaryKeyIndex.mu.RUnlock()
		}
		return nil
	})
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

// rebuildPrimaryKeyIndex rebuilds the primary key index from the database bucket
func (s *Store[T]) rebuildPrimaryKeyIndex(tx *bolt.Tx) {
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		return
	}
	cursor := bucket.Cursor()
	for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
		key := string(k)
		s.btreeIndexes[primaryKeyIndexName].Insert(key, key)
	}
}

// rebuildSecondaryIndex rebuilds a secondary index for the given field from the database bucket
func (s *Store[T]) rebuildSecondaryIndex(fieldName string, tx *bolt.Tx) {
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		return
	}
	fieldIndex, exists := s.indexFields[fieldName]
	if !exists {
		return
	}
	cursor := bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if v == nil {
			continue
		}
		// Decode the record
		var item T
		decoder := msgpack.GetDecoder()
		decoder.Reset(bytes.NewReader(v))
		err := decoder.Decode(&item)
		msgpack.PutDecoder(decoder)
		if err != nil {
			continue
		}
		// Extract index value
		structValue := reflect.ValueOf(item)
		fieldValue := structValue.Field(fieldIndex)
		if fieldValue.Kind() == reflect.String {
			indexValue := fieldValue.String()
			if indexValue != "" {
				key := string(k)
				s.btreeIndexes[fieldName].Insert(indexValue, key)
			}
		}
	}
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
