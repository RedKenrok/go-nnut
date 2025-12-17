package nnut

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestBTreeIndex_InsertAndSearch(t *testing.T) {
	bt := newBTree(4)

	// Insert some values
	bt.insert("value1", "key1")
	bt.insert("value1", "key2")
	bt.insert("value2", "key3")

	// Search
	keys1 := bt.search("value1")
	if len(keys1) != 2 {
		t.Errorf("Expected 2 keys for value1, got %d", len(keys1))
	}
	if keys1[0] != "key1" || keys1[1] != "key2" {
		t.Errorf("Unexpected keys: %v", keys1)
	}

	keys2 := bt.search("value2")
	if len(keys2) != 1 || keys2[0] != "key3" {
		t.Errorf("Expected [key3], got %v", keys2)
	}

	keys3 := bt.search("value3")
	if len(keys3) != 0 {
		t.Errorf("Expected empty, got %v", keys3)
	}
}

func TestBTreeIndex_Delete(t *testing.T) {
	bt := newBTree(4)

	bt.insert("value1", "key1")
	bt.insert("value1", "key2")

	// Delete one
	bt.delete("value1", "key1")

	keys := bt.search("value1")
	if len(keys) != 1 || keys[0] != "key2" {
		t.Errorf("Expected [key2], got %v", keys)
	}

	// Delete the last
	bt.delete("value1", "key2")

	keys = bt.search("value1")
	if len(keys) != 0 {
		t.Errorf("Expected empty, got %v", keys)
	}
}

func TestBTreeIndex_Counts(t *testing.T) {
	bt := newBTree(4)

	bt.insert("value1", "key1")
	bt.insert("value1", "key2")
	bt.insert("value2", "key3")

	if bt.countUniqueValues() != 2 {
		t.Errorf("Expected 2 unique values, got %d", bt.countUniqueValues())
	}

	if bt.countKeys() != 3 {
		t.Errorf("Expected 3 keys, got %d", bt.countKeys())
	}
}

func TestBTreeIndex_Serialization(t *testing.T) {
	bt := newBTree(4)

	bt.insert("value1", "key1")
	bt.insert("value2", "key2")

	data, err := bt.serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	bt2, err := deserializeBTree(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if bt2.countKeys() != 2 {
		t.Errorf("Deserialized tree has wrong count")
	}

	keys := bt2.search("value1")
	if len(keys) != 1 || keys[0] != "key1" {
		t.Errorf("Deserialized search failed")
	}
}

func TestBTreeIndex_Rebalancing(t *testing.T) {
	bt := newBTree(4) // t=4, min keys=3

	// Insert many keys to force splits
	for i := 0; i < 20; i++ {
		bt.insert(fmt.Sprintf("value%d", i), fmt.Sprintf("key%d", i))
	}

	// Delete some to potentially cause underflow and rebalancing
	for i := 0; i < 10; i++ {
		bt.delete(fmt.Sprintf("value%d", i), fmt.Sprintf("key%d", i))
	}

	// Check remaining keys are still found
	for i := 10; i < 20; i++ {
		keys := bt.search(fmt.Sprintf("value%d", i))
		if len(keys) != 1 || keys[0] != fmt.Sprintf("key%d", i) {
			t.Errorf("Search failed for value%d: got %v", i, keys)
		}
	}

	// Check deleted keys are gone
	for i := 0; i < 10; i++ {
		keys := bt.search(fmt.Sprintf("value%d", i))
		if len(keys) != 0 {
			t.Errorf("Deleted value%d still found: %v", i, keys)
		}
	}

	// Verify total count
	if bt.countKeys() != 10 {
		t.Errorf("Expected 10 keys remaining, got %d", bt.countKeys())
	}
}

func TestBTreeIndex_RangeSearch(t *testing.T) {
	bt := newBTree(4)

	// Insert test data
	bt.insert("apple", "key1")
	bt.insert("banana", "key2")
	bt.insert("cherry", "key3")
	bt.insert("date", "key4")
	bt.insert("elderberry", "key5")

	// Test range search
	keys := bt.rangeSearch("banana", "date", true, true)
	expected := []string{"key2", "key3", "key4"}
	if len(keys) != len(expected) {
		t.Errorf("Expected %d keys, got %d: %v", len(expected), len(keys), keys)
	}
	for i, key := range expected {
		if i >= len(keys) || keys[i] != key {
			t.Errorf("Expected %s at position %d, got %s", key, i, keys[i])
		}
	}

	// Test greater than
	keys = bt.rangeSearch("cherry", "", false, true)
	expected = []string{"key4", "key5"}
	if len(keys) != len(expected) {
		t.Errorf("Greater than test failed: expected %v, got %v", expected, keys)
	}

	// Test less than or equal
	keys = bt.rangeSearch("", "banana", true, true)
	expected = []string{"key1", "key2"}
	if len(keys) != len(expected) {
		t.Errorf("Less than or equal test failed: expected %v, got %v", expected, keys)
	}
}

func TestBTreeIndex_BulkOperations(t *testing.T) {
	bt := newBTree(4)

	// Test bulk insert
	items := []bTreeItem{
		{"apple", "key1"},
		{"banana", "key2"},
		{"cherry", "key3"},
		{"date", "key4"},
		{"elderberry", "key5"},
	}
	bt.bulkInsert(items)

	// Verify insertions
	for _, item := range items {
		keys := bt.search(item.Key)
		if len(keys) != 1 || keys[0] != item.Value {
			t.Errorf("Bulk insert failed for %s: expected [%s], got %v", item.Key, item.Value, keys)
		}
	}

	// Test bulk search
	searchKeys := []string{"apple", "cherry", "elderberry", "nonexistent"}
	results := bt.bulkSearch(searchKeys)
	if len(results["apple"]) != 1 || results["apple"][0] != "key1" {
		t.Errorf("Bulk search failed for apple")
	}
	if len(results["nonexistent"]) != 0 {
		t.Errorf("Bulk search should return empty for nonexistent key")
	}

	// Test bulk delete
	deleteItems := []bTreeItem{
		{"banana", "key2"},
		{"date", "key4"},
	}
	bt.bulkDelete(deleteItems)

	// Verify deletions
	if len(bt.search("banana")) != 0 {
		t.Errorf("Bulk delete failed for banana")
	}
	if len(bt.search("date")) != 0 {
		t.Errorf("Bulk delete failed for date")
	}

	// Verify remaining items
	remaining := []string{"apple", "cherry", "elderberry"}
	for _, key := range remaining {
		if len(bt.search(key)) == 0 {
			t.Errorf("Remaining item %s was deleted", key)
		}
	}
}

func TestBTreeIndex_Persistence(t *testing.T) {
	// Create a test database
	dbPath := filepath.Join(t.TempDir(), "persistence_test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Define test type
	type TestUser struct {
		UUID  string `nnut:"key"`
		Name  string `nnut:"index"`
		Email string `nnut:"index"`
	}

	// Create store and add data
	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com"},
		{UUID: "2", Name: "Bob", Email: "bob@example.com"},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com"},
	}

	err = store.PutBatch(context.Background(), users)
	if err != nil {
		t.Fatalf("Failed to put batch: %v", err)
	}

	// Flush to ensure data is persisted
	db.Flush()

	// Verify indexes work
	query := &Query{
		Conditions: []Condition{
			{Field: "Name", Operator: Equals, Value: "Alice"},
		},
	}
	results, err := store.GetQuery(context.Background(), query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 1 || results[0].UUID != "1" {
		t.Errorf("Expected Alice, got %v", results)
	}

	// Close and reopen database
	db.Flush()
	db.Close()
	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer db.Close()

	// Create new store instance
	newStore, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create new store: %v", err)
	}

	// Verify persisted indexes work without rebuilding
	results, err = newStore.GetQuery(context.Background(), query)
	if err != nil {
		t.Fatalf("Query on reloaded store failed: %v", err)
	}
	if len(results) != 1 || results[0].UUID != "1" {
		t.Errorf("Expected Alice from persisted index, got %v", results)
	}

	emailQuery := &Query{
		Conditions: []Condition{
			{Field: "Email", Operator: Equals, Value: "bob@example.com"},
		},
	}
	results, err = newStore.GetQuery(context.Background(), emailQuery)
	if err != nil {
		t.Fatalf("Email query failed: %v", err)
	}
	if len(results) != 1 || results[0].UUID != "2" {
		t.Errorf("Expected Bob from persisted index, got %v", results)
	}
}

func TestBTreeIndex_EdgeCases(t *testing.T) {
	// Test empty tree
	bt := newBTree(4)
	if len(bt.search("nonexistent")) != 0 {
		t.Errorf("Empty tree search should return empty")
	}
	if bt.countKeys() != 0 {
		t.Errorf("Empty tree should have 0 keys")
	}

	// Test single node operations
	bt.insert("key1", "value1")
	if bt.countKeys() != 1 {
		t.Errorf("Expected 1 key")
	}
	results := bt.search("key1")
	if len(results) != 1 || results[0] != "value1" {
		t.Errorf("Single node search failed")
	}

	// Test duplicate inserts
	bt.insert("key1", "value2")
	results = bt.search("key1")
	if len(results) != 2 {
		t.Errorf("Expected 2 values for key1")
	}

	// Test delete to empty
	bt.delete("key1", "value1")
	results = bt.search("key1")
	if len(results) != 1 || results[0] != "value2" {
		t.Errorf("Delete failed")
	}
	bt.delete("key1", "value2")
	if bt.countKeys() != 0 {
		t.Errorf("Tree should be empty after deleting all")
	}
}

func TestBTreeIndex_BoundaryConditions(t *testing.T) {
	bt := newBTree(4)

	// Test min/max keys
	bt.insert("", "empty")
	bt.insert("z", "max")

	results := bt.search("")
	if len(results) != 1 || results[0] != "empty" {
		t.Errorf("Empty key search failed")
	}

	results = bt.search("z")
	if len(results) != 1 || results[0] != "max" {
		t.Errorf("Max key search failed")
	}

	// Test range with boundaries
	bt.insert("a", "a_val")
	bt.insert("m", "m_val")

	results = bt.rangeSearch("", "m", true, true)
	if len(results) != 3 {
		t.Errorf("Range search failed, got %v", results)
	}

	results = bt.rangeSearch("a", "", true, true)
	if len(results) != 3 {
		t.Errorf("Open-ended range failed")
	}
}

func TestBTreeIndex_ComplexOperations(t *testing.T) {
	bt := newBTree(4)

	// Insert in reverse order
	for i := 20; i >= 0; i-- {
		bt.insert(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
	}

	// Verify all inserted
	for i := 0; i <= 20; i++ {
		results := bt.search(fmt.Sprintf("key%d", i))
		if len(results) != 1 || results[0] != fmt.Sprintf("val%d", i) {
			t.Errorf("Reverse insert failed for key%d", i)
		}
	}

	// Delete every other
	for i := 0; i <= 20; i += 2 {
		bt.delete(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
	}

	// Verify remaining
	for i := 1; i <= 20; i += 2 {
		results := bt.search(fmt.Sprintf("key%d", i))
		if len(results) != 1 {
			t.Errorf("Remaining key%d should exist", i)
		}
	}
	for i := 0; i <= 20; i += 2 {
		results := bt.search(fmt.Sprintf("key%d", i))
		if len(results) != 0 {
			t.Errorf("Deleted key%d should not exist", i)
		}
	}
}

func TestBTreeIndex_IteratorEdgeCases(t *testing.T) {
	bt := newBTree(4)

	// Empty iterator
	it := newBTreeIterator(bt, "", "", true, true)
	if it.hasNext() {
		t.Errorf("Empty tree iterator should not have next")
	}

	// Single item
	bt.insert("key1", "val1")
	it = newBTreeIterator(bt, "key1", "key1", true, true)
	if !it.hasNext() {
		t.Errorf("Single item iterator should have next")
	}
	val := it.next()
	if val != "val1" {
		t.Errorf("Iterator returned wrong value: %s", val)
	}
	if it.hasNext() {
		t.Errorf("Iterator should be exhausted")
	}

	// Range with no matches
	it = newBTreeIterator(bt, "a", "b", true, true)
	if it.hasNext() {
		t.Errorf("No match range should not have next")
	}
}

func TestBTreeIndex_IteratorRangeQueries(t *testing.T) {
	bt := newBTree(4)

	// Insert test data
	keys := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for i, key := range keys {
		bt.insert(key, fmt.Sprintf("val%d", i))
	}

	// Test full range
	it := newBTreeIterator(bt, "", "", true, true)
	var results []string
	for it.hasNext() {
		results = append(results, it.next())
	}
	expected := []string{"val0", "val1", "val2", "val3", "val4", "val5", "val6"}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Full range iterator returned %v, expected %v", results, expected)
	}

	// Test partial range
	it = newBTreeIterator(bt, "banana", "fig", true, true)
	results = nil
	for it.hasNext() {
		results = append(results, it.next())
	}
	expected = []string{"val1", "val2", "val3", "val4", "val5"}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Partial range iterator returned %v, expected %v", results, expected)
	}

	// Test exclusive bounds
	it = newBTreeIterator(bt, "banana", "fig", false, false)
	results = nil
	for it.hasNext() {
		results = append(results, it.next())
	}
	expected = []string{"val2", "val3", "val4"}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Exclusive range iterator returned %v, expected %v", results, expected)
	}

	// Test min only
	it = newBTreeIterator(bt, "cherry", "", true, true)
	results = nil
	for it.hasNext() {
		results = append(results, it.next())
	}
	expected = []string{"val2", "val3", "val4", "val5", "val6"}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Min-only range iterator returned %v, expected %v", results, expected)
	}

	// Test max only
	it = newBTreeIterator(bt, "", "date", true, true)
	results = nil
	for it.hasNext() {
		results = append(results, it.next())
	}
	expected = []string{"val0", "val1", "val2", "val3"}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Max-only range iterator returned %v, expected %v", results, expected)
	}
}

// Fuzz test for B-tree operations
func FuzzBTreeOperations(f *testing.F) {
	f.Add([]byte("insert"), []byte("key1"), []byte("val1"))
	f.Add([]byte("delete"), []byte("key1"), []byte("val1"))
	f.Add([]byte("search"), []byte("key1"), []byte(""))

	f.Fuzz(func(t *testing.T, op []byte, key []byte, value []byte) {
		bt := newBTree(4)
		keyStr := string(key)
		valueStr := string(value)

		switch string(op) {
		case "insert":
			bt.insert(keyStr, valueStr)
			results := bt.search(keyStr)
			if len(results) == 0 {
				t.Errorf("Insert failed for key %s", keyStr)
			}
		case "delete":
			bt.insert(keyStr, valueStr)
			bt.delete(keyStr, valueStr)
			results := bt.search(keyStr)
			if len(results) != 0 {
				t.Errorf("Delete failed for key %s", keyStr)
			}
		case "search":
			results := bt.search(keyStr)
			// Search on empty tree should be fine
			if len(results) != 0 {
				t.Errorf("Unexpected results for empty tree search")
			}
		}
	})
}

// Fuzz test for serialization
func FuzzBTreeSerialization(f *testing.F) {
	f.Add([]byte("key1"), []byte("val1"))

	f.Fuzz(func(t *testing.T, key []byte, value []byte) {
		bt := newBTree(4)
		keyStr := string(key)
		valueStr := string(value)

		bt.insert(keyStr, valueStr)

		data, err := bt.serialize()
		if err != nil {
			t.Errorf("Serialization failed: %v", err)
		}

		bt2, err := deserializeBTree(data)
		if err != nil {
			t.Errorf("Deserialization failed: %v", err)
		}

		results := bt2.search(keyStr)
		if len(results) != 1 || results[0] != valueStr {
			t.Errorf("Deserialized tree search failed")
		}
	})
}

// Test B-tree invariants
func TestBTreeInvariants(t *testing.T) {
	bt := newBTree(4)

	// Insert some data
	keys := []string{"a", "b", "c", "d", "e", "f", "g"}
	for _, key := range keys {
		bt.insert(key, key+"_val")
	}

	// Check invariants
	if !checkBTreeInvariants(bt.Root, bt.BranchingFactor) {
		t.Errorf("B-tree invariants violated")
	}

	// Delete some
	bt.delete("c", "c_val")
	bt.delete("f", "f_val")

	// Check invariants again
	if !checkBTreeInvariants(bt.Root, bt.BranchingFactor) {
		t.Errorf("B-tree invariants violated after delete")
	}
}

// checkBTreeInvariants verifies B-tree properties
func checkBTreeInvariants(node *bTreeNode, t int) bool {
	if node == nil {
		return true
	}

	// Check node capacity
	if len(node.Keys) >= 2*t {
		return false
	}

	// Check leaf or internal
	if node.IsLeaf {
		if len(node.Children) != 0 {
			return false
		}
	} else {
		if len(node.Children) != len(node.Keys)+1 {
			return false
		}
		// Check children
		for _, child := range node.Children {
			if !checkBTreeInvariants(child, t) {
				return false
			}
		}
	}

	// Check key ordering
	for i := 1; i < len(node.Keys); i++ {
		if node.Keys[i] <= node.Keys[i-1] {
			return false
		}
	}

	return true
}

func TestBTreeConcurrencyStress(t *testing.T) {
	bt := newBTree(4)
	const numGoroutines = 10
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				value := fmt.Sprintf("val_%d_%d", id, j)

				bt.insert(key, value)

				// Occasional search
				if j%10 == 0 {
					results := bt.search(key)
					if len(results) == 0 {
						t.Errorf("Concurrent search failed")
					}
				}

				// Occasional delete
				if j%20 == 0 {
					bt.delete(key, value)
				}
			}
		}(i)
	}

	wg.Wait()

	// Final invariant check
	if !checkBTreeInvariants(bt.Root, bt.BranchingFactor) {
		t.Errorf("B-tree invariants violated after concurrent operations")
	}
}
