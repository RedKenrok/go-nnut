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

type Condition struct {
	Field    string
	Value    interface{}
	Operator Operator
}

type Query struct {
	Index  string
	Limit  int // Maximum number of results to return (0 = no limit)
	Offset int // Number of results to skip
	Sort   Sorting

	Conditions []Condition
}

type condWithSize struct {
	cond Condition
	size int
}

// Query queries for records matching the conditions
func (s *Store[T]) Query(query *Query) ([]T, error) {
	var results []T
	err := s.database.View(func(tx *bbolt.Tx) error {
		// Calculate max keys to collect
		maxKeys := 0
		if query.Limit > 0 {
			maxKeys = query.Offset + query.Limit
		}

		// Collect candidate keys from conditions
		var candidateKeys []string
		if len(query.Conditions) > 0 {
			candidateKeys = s.getCandidateKeysTx(tx, query.Conditions, maxKeys)
		} else if query.Index != "" {
			// No conditions, but index for sorting, iterate index
			candidateKeys = s.getKeysFromIndexTx(tx, query.Index, query.Sort, maxKeys)
		} else {
			// No conditions, no index, get all keys
			candidateKeys = s.getAllKeysTx(tx, maxKeys)
		}

		// Apply offset and limit to keys
		start := query.Offset
		if start > len(candidateKeys) {
			start = len(candidateKeys)
		}
		end := len(candidateKeys)
		if query.Limit > 0 && start+query.Limit < end {
			end = start + query.Limit
		}
		keysToFetch := candidateKeys[start:end]

		// Fetch records
		b := tx.Bucket(s.bucket)
		if b == nil {
			return nil
		}
		dec := msgpack.GetDecoder()
		defer msgpack.PutDecoder(dec)
		for _, key := range keysToFetch {
			data := b.Get([]byte(key))
			if data == nil {
				continue
			}
			var item T
			dec.Reset(bytes.NewReader(data))
			err := dec.Decode(&item)
			if err != nil {
				continue
			}
			results = append(results, item)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Sort if Index specified and not already sorted
	if query.Index != "" && len(query.Conditions) > 0 {
		s.sortResults(results, query.Index, query.Sort)
	}

	return results, nil
}

// QueryCount returns the number of records matching the query
func (s *Store[T]) QueryCount(query *Query) (int, error) {
	var count int
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

// getCandidateKeysTx returns keys that match all conditions using the provided tx
func (s *Store[T]) getCandidateKeysTx(tx *bbolt.Tx, conditions []Condition, maxKeys int) []string {
	if len(conditions) == 0 {
		return s.getAllKeysTx(tx, maxKeys)
	}

	// Separate indexed and non-indexed conditions
	var indexedConds []Condition
	var nonIndexedConds []Condition
	for _, cond := range conditions {
		if _, ok := s.indexFields[cond.Field]; ok && cond.Value != nil {
			if _, isString := cond.Value.(string); isString {
				indexedConds = append(indexedConds, cond)
			} else {
				nonIndexedConds = append(nonIndexedConds, cond)
			}
		} else {
			nonIndexedConds = append(nonIndexedConds, cond)
		}
	}

	// Get key sets from indexed conditions, starting with the shortest
	var indexedKeys []string
	if len(indexedConds) > 0 {
		var condSizes []condWithSize
		for _, cond := range indexedConds {
			size := s.countKeysForConditionTx(tx, cond, maxKeys)
			condSizes = append(condSizes, condWithSize{cond, size})
		}
		// Sort by size ascending
		sort.Slice(condSizes, func(i, j int) bool {
			return condSizes[i].size < condSizes[j].size
		})
		// Primary is the smallest
		primaryCond := condSizes[0].cond
		keysMax := 0
		if len(indexedConds) == 1 && len(nonIndexedConds) == 0 {
			keysMax = maxKeys
		}
		indexedKeys = s.getKeysForConditionTx(tx, primaryCond, keysMax)
		// Intersect others into primary
		for i := 1; i < len(condSizes); i++ {
			otherKeys := s.getKeysForConditionTx(tx, condSizes[i].cond, 0)
			indexedKeys = intersectSlices(indexedKeys, otherKeys)
		}
	}

	// Get keys from non-indexed conditions via single scan
	var nonIndexedKeys []string
	if len(nonIndexedConds) > 0 {
		// If we have indexed keys, scan only those; otherwise scan all
		var candidates []string
		if len(indexedConds) > 0 {
			candidates = indexedKeys
		}
		nonIndexedKeys = s.scanForConditionsTx(tx, nonIndexedConds, candidates, maxKeys)
	}

	// Intersect with non-indexed
	if len(nonIndexedConds) == 0 {
		return indexedKeys
	}
	if len(indexedConds) == 0 {
		return nonIndexedKeys
	}
	// Intersect the two
	indexedMap := make(map[string]bool, len(indexedKeys))
	for _, k := range indexedKeys {
		indexedMap[k] = true
	}
	var result []string
	for _, k := range nonIndexedKeys {
		if indexedMap[k] {
			result = append(result, k)
		}
	}
	return result
}

// getKeysForConditionTx returns keys that match the condition, sorted
func (s *Store[T]) getKeysForConditionTx(tx *bbolt.Tx, cond Condition, maxKeys int) []string {
	var keys []string
	_, indexed := s.indexFields[cond.Field]
	valStr, isString := cond.Value.(string)
	if !indexed || !isString {
		// This should not happen, as we separate indexed and non-indexed
		return keys
	}

	// Use index
	idxBucketName := string(s.bucket) + "_index_" + cond.Field
	idxB := tx.Bucket([]byte(idxBucketName))
	if idxB == nil {
		return keys
	}
	c := idxB.Cursor()
	var k []byte
	switch cond.Operator {
	case Equals:
		prefix := valStr + "\x00"
		for k, _ = c.Seek([]byte(prefix)); k != nil && bytes.HasPrefix(k, []byte(prefix)); k, _ = c.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				key := string(parts[1])
				keys = append(keys, key)
			}
		}
	case GreaterThan:
		k, _ = c.Seek([]byte(valStr + "\x00"))
		// Skip equals
		for k != nil && bytes.HasPrefix(k, []byte(valStr+"\x00")) {
			k, _ = c.Next()
		}
		// Now collect greater
		for k != nil {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val > valStr {
					key := string(parts[1])
					keys = append(keys, key)
				} else {
					break
				}
			}
			k, _ = c.Next()
		}
	case GreaterThanOrEqual:
		for k, _ = c.Seek([]byte(valStr + "\x00")); k != nil; k, _ = c.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val >= valStr {
					key := string(parts[1])
					keys = append(keys, key)
				} else {
					break
				}
			}
		}
	case LessThan:
		for k, _ = c.First(); k != nil; k, _ = c.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val < valStr {
					key := string(parts[1])
					keys = append(keys, key)
				} else {
					break
				}
			}
		}
	case LessThanOrEqual:
		for k, _ = c.First(); k != nil; k, _ = c.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val <= valStr {
					key := string(parts[1])
					keys = append(keys, key)
				} else {
					break
				}
			}
		}
	}
	sort.Strings(keys)
	return keys
}

// countKeysForConditionTx returns the count of keys matching the condition
func (s *Store[T]) countKeysForConditionTx(tx *bbolt.Tx, cond Condition, maxKeys int) int {
	var count int
	_, indexed := s.indexFields[cond.Field]
	valStr, isString := cond.Value.(string)
	if !indexed || !isString {
		return 0
	}

	idxBucketName := string(s.bucket) + "_index_" + cond.Field
	idxB := tx.Bucket([]byte(idxBucketName))
	if idxB == nil {
		return 0
	}
	c := idxB.Cursor()
	var k []byte
	switch cond.Operator {
	case Equals:
		prefix := valStr + "\x00"
		for k, _ = c.Seek([]byte(prefix)); k != nil && bytes.HasPrefix(k, []byte(prefix)); k, _ = c.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
		}
	case GreaterThan:
		k, _ = c.Seek([]byte(valStr + "\x00"))
		// Skip equals
		for k != nil && bytes.HasPrefix(k, []byte(valStr+"\x00")) {
			k, _ = c.Next()
		}
		// Now count greater
		for k != nil {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val <= valStr {
					break
				}
			}
			k, _ = c.Next()
		}
	case GreaterThanOrEqual:
		for k, _ = c.Seek([]byte(valStr + "\x00")); k != nil; k, _ = c.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val < valStr {
					break
				}
			}
		}
	case LessThan:
		for k, _ = c.First(); k != nil; k, _ = c.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val >= valStr {
					break
				}
			}
		}
	case LessThanOrEqual:
		for k, _ = c.First(); k != nil; k, _ = c.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val > valStr {
					break
				}
			}
		}
	}
	return count
}

// matchesCondition checks if the item matches the condition
func (s *Store[T]) matchesCondition(item T, cond Condition) bool {
	v := reflect.ValueOf(item)
	if idx, ok := s.fieldMap[cond.Field]; ok {
		fieldVal := v.Field(idx)
		switch cond.Operator {
		case Equals:
			return reflect.DeepEqual(fieldVal.Interface(), cond.Value)
		case GreaterThan:
			return compare(fieldVal.Interface(), cond.Value) > 0
		case LessThan:
			return compare(fieldVal.Interface(), cond.Value) < 0
		case GreaterThanOrEqual:
			return compare(fieldVal.Interface(), cond.Value) >= 0
		case LessThanOrEqual:
			return compare(fieldVal.Interface(), cond.Value) <= 0
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
	b := tx.Bucket(s.bucket)
	if b == nil {
		return keys
	}
	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if maxKeys > 0 && len(keys) >= maxKeys {
			break
		}
		keys = append(keys, string(k))
	}
	return keys
}

// countAllKeysTx returns the count of all keys in the bucket
func (s *Store[T]) countAllKeysTx(tx *bbolt.Tx) int {
	count := 0
	b := tx.Bucket(s.bucket)
	if b == nil {
		return count
	}
	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		count++
	}
	return count
}

// getKeysFromIndexTx returns all keys sorted by the index
func (s *Store[T]) getKeysFromIndexTx(tx *bbolt.Tx, index string, sorting Sorting, maxKeys int) []string {
	var keys []string
	idxBucketName := string(s.bucket) + "_index_" + index
	idxB := tx.Bucket([]byte(idxBucketName))
	if idxB == nil {
		return keys
	}
	c := idxB.Cursor()
	var k []byte
	if sorting == Descending {
		for k, _ = c.Last(); k != nil && (maxKeys == 0 || len(keys) < maxKeys); k, _ = c.Prev() {
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				key := string(parts[1])
				keys = append(keys, key)
			}
		}
	} else {
		for k, _ = c.First(); k != nil && (maxKeys == 0 || len(keys) < maxKeys); k, _ = c.Next() {
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				key := string(parts[1])
				keys = append(keys, key)
			}
		}
	}
	return keys
}

// countKeysFromIndexTx returns the count of keys in the index
func (s *Store[T]) countKeysFromIndexTx(tx *bbolt.Tx, index string) int {
	count := 0
	idxBucketName := string(s.bucket) + "_index_" + index
	idxB := tx.Bucket([]byte(idxBucketName))
	if idxB == nil {
		return count
	}
	c := idxB.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		count++
	}
	return count
}

// scanForConditionsTx scans records and returns keys matching all conditions
// If candidates is not nil, only scans those keys; otherwise scans all.
// Limits to maxKeys if >0.
func (s *Store[T]) scanForConditionsTx(tx *bbolt.Tx, conditions []Condition, candidates []string, maxKeys int) []string {
	var keys []string
	b := tx.Bucket(s.bucket)
	if b == nil {
		return keys
	}
	dec := msgpack.GetDecoder()
	defer msgpack.PutDecoder(dec)
	if candidates != nil {
		// Scan only candidate keys
		for _, key := range candidates {
			data := b.Get([]byte(key))
			if data == nil {
				continue
			}
			var item T
			dec.Reset(bytes.NewReader(data))
			err := dec.Decode(&item)
			if err != nil {
				continue
			}
			matches := true
			for _, cond := range conditions {
				if !s.matchesCondition(item, cond) {
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
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var item T
			dec.Reset(bytes.NewReader(v))
			err := dec.Decode(&item)
			if err != nil {
				continue
			}
			matches := true
			for _, cond := range conditions {
				if !s.matchesCondition(item, cond) {
					matches = false
					break
				}
			}
			if matches {
				keys = append(keys, string(k))
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
	fieldIdx, ok := s.indexFields[index]
	if !ok {
		return
	}
	sort.Slice(results, func(i, j int) bool {
		va := reflect.ValueOf(results[i]).Field(fieldIdx)
		vb := reflect.ValueOf(results[j]).Field(fieldIdx)
		cmp := compare(va.Interface(), vb.Interface())
		if sorting == Descending {
			return cmp > 0
		}
		return cmp < 0
	})
}
