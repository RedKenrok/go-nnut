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

	// Rebuild indexes if marked as dirty during WAL replay
	bucketPrefix := bucketName + ":"
	needsRebuild := false
	for indexKey := range database.indexesNeedRebuild {
		if strings.HasPrefix(indexKey, bucketPrefix) {
			needsRebuild = true
			break
		}
	}
	if needsRebuild {
		if err := store.rebuildIndexes(); err != nil {
			return nil, fmt.Errorf("failed to rebuild indexes: %w", err)
		}
		// Remove the rebuild flags for this store's indexes
		for indexKey := range database.indexesNeedRebuild {
			if strings.HasPrefix(indexKey, bucketPrefix) {
				delete(database.indexesNeedRebuild, indexKey)
			}
		}
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
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if v == nil {
			continue
		}
		key := string(k)
		s.btreeIndexes[primaryKeyIndexName].Insert(key, key)
	}
}

// rebuildIndexes rebuilds all B-tree indexes from the current database state
func (s *Store[T]) rebuildIndexes() error {
	return s.database.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return nil
		}

		// Clear existing indexes
		for name := range s.btreeIndexes {
			s.btreeIndexes[name] = NewBTreeIndex(32)
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
				continue // Skip corrupted records
			}

			key := string(k)

			// Rebuild primary key index
			s.btreeIndexes[primaryKeyIndexName].Insert(key, key)

			// Rebuild secondary indexes
			structValue := reflect.ValueOf(item)
			for fieldName, fieldIndex := range s.indexFields {
				fieldValue := structValue.Field(fieldIndex)
				if fieldValue.Kind() == reflect.String {
					indexValue := fieldValue.String()
					if indexValue != "" {
						s.btreeIndexes[fieldName].Insert(indexValue, key)
					}
				}
			}
		}
		return nil
	})
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
