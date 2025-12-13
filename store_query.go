package nnut

import (
	"bytes"
	"reflect"
	"sort"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

var mapPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]bool)
	},
}

type Operator int

const (
	Equals Operator = iota
	GreaterThan
	LessThan
	GreaterThanOrEqual
	LessThanOrEqual
)

type Sorting int

const (
	Unsorted Sorting = iota
	Ascending
	Descending
)

// Condition represents a filter condition for queries.
// Field is the name of the field to filter on.
// Value is the value to compare against.
// Operator specifies the comparison type (Equals, GreaterThan, etc.).
type Condition struct {
	Field    string
	Value    interface{}
	Operator Operator
}

// Query defines parameters for retrieving records from the store.
// Index specifies which field to use for sorting (must be an indexed field).
// Limit restricts the number of results (0 means no limit).
// Offset skips the first N results.
// Sort specifies ascending or descending order.
// Conditions is a list of filters to apply.
type Query struct {
	Index      string
	Limit      int
	Offset     int
	Sort       Sorting
	Conditions []Condition
}

type condWithSize struct {
	cond Condition
	size int
}

// validateQuery validates query parameters
func (s *Store[T]) validateQuery(query *Query) error {
	if query == nil {
		return InvalidQueryError{Field: "query", Value: nil, Reason: "cannot be nil"}
	}
	if query.Limit < 0 {
		return InvalidQueryError{Field: "Limit", Value: query.Limit, Reason: "cannot be negative"}
	}
	if query.Offset < 0 {
		return InvalidQueryError{Field: "Offset", Value: query.Offset, Reason: "cannot be negative"}
	}
	if query.Index != "" {
		if _, exists := s.indexFields[query.Index]; !exists {
			return InvalidQueryError{Field: "Index", Value: query.Index, Reason: "index field does not exist"}
		}
	}
	// Validate conditions
	for _, cond := range query.Conditions {
		if _, exists := s.fieldMap[cond.Field]; !exists {
			return InvalidQueryError{Field: "Condition.Field", Value: cond.Field, Reason: "field does not exist"}
		}
		// Check if value is comparable (string or int)
		if cond.Value != nil {
			switch cond.Value.(type) {
			case string, int:
				// ok
			default:
				return InvalidQueryError{Field: "Condition.Value", Value: cond.Value, Reason: "must be string or int"}
			}
		}
	}
	return nil
}

// getCandidateKeys returns keys that match all conditions
func (s *Store[T]) getCandidateKeys(conditions []Condition, maxKeys int) []string {
	var keys []string
	s.database.View(func(tx *bbolt.Tx) error {
		keys = s.getCandidateKeysTx(tx, conditions, maxKeys)
		return nil
	})
	return keys
}

// getKeysFromIndex returns all keys sorted by the index
func (s *Store[T]) getKeysFromIndex(index string, sorting Sorting, maxKeys int) []string {
	var keys []string
	s.database.View(func(tx *bbolt.Tx) error {
		keys = s.getKeysFromIndexTx(tx, index, sorting, maxKeys)
		return nil
	})
	return keys
}

// getAllKeys returns all keys in the store
func (s *Store[T]) getAllKeys(maxKeys int) []string {
	var keys []string
	s.database.View(func(tx *bbolt.Tx) error {
		keys = s.getAllKeysTx(tx, maxKeys)
		return nil
	})
	return keys
}

// getCandidateKeysTx returns keys that match all conditions using the provided tx
func (s *Store[T]) getCandidateKeysTx(tx *bbolt.Tx, conditions []Condition, maxKeys int) []string {
	if len(conditions) == 0 {
		return s.getAllKeysTx(tx, maxKeys)
	}

	// Partition conditions to leverage indexes where possible
	var indexedConditions []Condition
	var nonIndexedConditions []Condition
	for _, condition := range conditions {
		if _, ok := s.indexFields[condition.Field]; ok && condition.Value != nil {
			if _, isString := condition.Value.(string); isString {
				indexedConditions = append(indexedConditions, condition)
			} else {
				nonIndexedConditions = append(nonIndexedConditions, condition)
			}
		} else {
			nonIndexedConditions = append(nonIndexedConditions, condition)
		}
	}

	// Get key sets from indexed conditions, starting with the shortest
	var indexedKeys []string
	if len(indexedConditions) > 0 {
		var conditionSizes []condWithSize
		for _, condition := range indexedConditions {
			size := s.countKeysForConditionTx(tx, condition, maxKeys)
			conditionSizes = append(conditionSizes, condWithSize{condition, size})
		}
		// Sort by size ascending
		sort.Slice(conditionSizes, func(i, j int) bool {
			return conditionSizes[i].size < conditionSizes[j].size
		})
		// Primary is the smallest
		primaryCondition := conditionSizes[0].cond
		keysMax := 0
		if len(indexedConditions) == 1 && len(nonIndexedConditions) == 0 {
			keysMax = maxKeys
		}
		indexedKeys = s.getKeysForConditionTx(tx, primaryCondition, keysMax)
		// Intersect others into primary
		for index := 1; index < len(conditionSizes); index++ {
			otherConditionKeys := s.getKeysForConditionTx(tx, conditionSizes[index].cond, 0)
			indexedKeys = intersectSlices(indexedKeys, otherConditionKeys)
		}
	}

	// Get keys from non-indexed conditions via single scan
	var nonIndexedKeys []string
	if len(nonIndexedConditions) > 0 {
		// If we have indexed keys, scan only those; otherwise scan all
		var candidates []string
		if len(indexedConditions) > 0 {
			candidates = indexedKeys
		}
		nonIndexedKeys = s.scanForConditionsTx(tx, nonIndexedConditions, candidates, maxKeys)
	}

	// Intersect with non-indexed
	if len(nonIndexedConditions) == 0 {
		return indexedKeys
	}
	if len(indexedConditions) == 0 {
		return nonIndexedKeys
	}
	// Intersect the two
	indexedMap := make(map[string]bool, len(indexedKeys))
	for _, key := range indexedKeys {
		indexedMap[key] = true
	}
	var result []string
	for _, key := range nonIndexedKeys {
		if indexedMap[key] {
			result = append(result, key)
		}
	}
	return result
}

// getKeysForConditionTx returns keys that match the condition, sorted
func (s *Store[T]) getKeysForConditionTx(tx *bbolt.Tx, condition Condition, maxKeys int) []string {
	var keys []string
	_, indexed := s.indexFields[condition.Field]
	valueString, isString := condition.Value.(string)
	if !indexed || !isString {
		// This should not happen, as we separate indexed and non-indexed
		return keys
	}

	// Use B-tree index
	var min, max string
	var includeMin, includeMax bool
	switch condition.Operator {
	case Equals:
		btreeKeys := s.btreeIndexes[condition.Field].Search(valueString)
		for _, key := range btreeKeys {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			keys = append(keys, key)
		}
		return keys
	case GreaterThan:
		min = valueString
		includeMin = false
		max = ""
		includeMax = true
	case GreaterThanOrEqual:
		min = valueString
		includeMin = true
		max = ""
		includeMax = true
	case LessThan:
		min = ""
		includeMin = true
		max = valueString
		includeMax = false
	case LessThanOrEqual:
		min = ""
		includeMin = true
		max = valueString
		includeMax = true
	}

	btreeKeys := s.btreeIndexes[condition.Field].RangeSearch(min, max, includeMin, includeMax)
	for _, key := range btreeKeys {
		if maxKeys > 0 && len(keys) >= maxKeys {
			break
		}
		keys = append(keys, key)
	}
	return keys
}

// countKeysForConditionTx returns the count of keys matching the condition
func (s *Store[T]) countKeysForConditionTx(tx *bbolt.Tx, condition Condition, maxKeys int) int {
	var count int
	_, indexed := s.indexFields[condition.Field]
	valueString, isString := condition.Value.(string)
	if !indexed || !isString {
		return 0
	}

	indexBucketName := string(s.bucket) + "_index_" + condition.Field
	indexBucket := tx.Bucket([]byte(indexBucketName))
	if indexBucket == nil {
		return 0
	}
	cursor := indexBucket.Cursor()
	var keyBytes []byte
	switch condition.Operator {
	case Equals:
		prefix := valueString + "\x00"
		for keyBytes, _ = cursor.Seek([]byte(prefix)); keyBytes != nil && bytes.HasPrefix(keyBytes, []byte(prefix)); keyBytes, _ = cursor.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
		}
	case GreaterThan:
		keyBytes, _ = cursor.Seek([]byte(valueString + "\x00"))
		// Skip equals
		for keyBytes != nil && bytes.HasPrefix(keyBytes, []byte(valueString+"\x00")) {
			keyBytes, _ = cursor.Next()
		}
		// Now count greater
		for keyBytes != nil {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value <= valueString {
					break
				}
			}
			keyBytes, _ = cursor.Next()
		}
	case GreaterThanOrEqual:
		for keyBytes, _ = cursor.Seek([]byte(valueString + "\x00")); keyBytes != nil; keyBytes, _ = cursor.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value < valueString {
					break
				}
			}
		}
	case LessThan:
		for keyBytes, _ = cursor.First(); keyBytes != nil; keyBytes, _ = cursor.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value >= valueString {
					break
				}
			}
		}
	case LessThanOrEqual:
		for keyBytes, _ = cursor.First(); keyBytes != nil; keyBytes, _ = cursor.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value > valueString {
					break
				}
			}
		}
	}
	return count
}

// matchesCondition checks if the item matches the condition
func (s *Store[T]) matchesCondition(item T, condition Condition) bool {
	itemValue := reflect.ValueOf(item)
	if fieldIndex, ok := s.fieldMap[condition.Field]; ok {
		fieldValue := itemValue.Field(fieldIndex)
		switch condition.Operator {
		case Equals:
			return reflect.DeepEqual(fieldValue.Interface(), condition.Value)
		case GreaterThan:
			return compare(fieldValue.Interface(), condition.Value) > 0
		case LessThan:
			return compare(fieldValue.Interface(), condition.Value) < 0
		case GreaterThanOrEqual:
			return compare(fieldValue.Interface(), condition.Value) >= 0
		case LessThanOrEqual:
			return compare(fieldValue.Interface(), condition.Value) <= 0
		}
	}
	return false
}

// compare compares two values, assumes comparable types
func compare(a, b interface{}) int {
	switch va := a.(type) {
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int:
		if vb, ok := b.(int); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}
	return 0 // not comparable, treat as equal
}

// getAllKeysTx returns all keys in the bucket, sorted, up to maxKeys if >0
func (s *Store[T]) getAllKeysTx(tx *bbolt.Tx, maxKeys int) []string {
	var keys []string
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		// Bucket not found, return empty
		return keys
	}
	cursor := bucket.Cursor()
	for keyBytes, _ := cursor.First(); keyBytes != nil; keyBytes, _ = cursor.Next() {
		if maxKeys > 0 && len(keys) >= maxKeys {
			break
		}
		key := string(keyBytes)
		keys = append(keys, key)
	}
	return keys
}

// countAllKeysTx returns the count of all keys in the bucket
func (s *Store[T]) countAllKeysTx(tx *bbolt.Tx) int {
	count := 0
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		// Bucket not found, return 0
		return count
	}
	// TODO: This does not keep the WAL / Operations buffer into account.
	return bucket.Stats().KeyN
}

// getKeysFromIndexTx returns all keys sorted by the index
func (s *Store[T]) getKeysFromIndexTx(tx *bbolt.Tx, index string, sorting Sorting, maxKeys int) []string {
	keys := s.btreeIndexes[index].GetAllKeys()
	if sorting == Descending {
		// Reverse the slice
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
	}
	if maxKeys > 0 && len(keys) > maxKeys {
		keys = keys[:maxKeys]
	}
	return keys
}

// scanForConditionsTx scans records and returns keys matching all conditions
// If candidates is not nil, only scans those keys; otherwise scans all.
// Limits to maxKeys if >0.
func (s *Store[T]) scanForConditionsTx(tx *bbolt.Tx, conditions []Condition, candidates []string, maxKeys int) []string {
	var keys []string
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		// Bucket not found, return empty
		return keys
	}
	decoder := msgpack.GetDecoder()
	defer msgpack.PutDecoder(decoder)
	if candidates != nil {
		// Scan only candidate keys
		for _, key := range candidates {
			data := bucket.Get([]byte(key))
			if data == nil {
				continue
			}
			var item T
			decoder.Reset(bytes.NewReader(data))
			err := decoder.Decode(&item)
			if err != nil {
				// Log error but continue scanning
				continue
			}
			matches := true
			for _, condition := range conditions {
				if !s.matchesCondition(item, condition) {
					matches = false
					break
				}
			}
			if matches {
				keys = append(keys, key)
				if maxKeys > 0 && len(keys) >= maxKeys {
					break
				}
			}
		}
	} else {
		// Scan all records
		cursor := bucket.Cursor()
		for keyBytes, valueBytes := cursor.First(); keyBytes != nil; keyBytes, valueBytes = cursor.Next() {
			var item T
			decoder.Reset(bytes.NewReader(valueBytes))
			err := decoder.Decode(&item)
			if err != nil {
				// Log error but continue scanning
				continue
			}
			matches := true
			for _, condition := range conditions {
				if !s.matchesCondition(item, condition) {
					matches = false
					break
				}
			}
			if matches {
				keys = append(keys, string(keyBytes))
				if maxKeys > 0 && len(keys) >= maxKeys {
					break
				}
			}
		}
	}
	return keys
}

// intersectSlices intersects two key slices, returning keys in base that are also in other
func intersectSlices(base, other []string) []string {
	baseMap := make(map[string]bool, len(base))
	for _, k := range base {
		baseMap[k] = true
	}
	var result []string
	for _, k := range other {
		if baseMap[k] {
			result = append(result, k)
		}
	}
	return result
}

// sortResults sorts the results by the index field
func (s *Store[T]) sortResults(results []T, index string, sorting Sorting) {
	fieldIndex, ok := s.indexFields[index]
	if !ok {
		return
	}
	sort.Slice(results, func(i, j int) bool {
		valueA := reflect.ValueOf(results[i]).Field(fieldIndex)
		valueB := reflect.ValueOf(results[j]).Field(fieldIndex)
		comparison := compare(valueA.Interface(), valueB.Interface())
		if sorting == Descending {
			return comparison > 0
		}
		return comparison < 0
	})
}
