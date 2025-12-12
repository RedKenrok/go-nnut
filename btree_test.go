package nnut

import (
	"testing"
)

func TestBTreeIndex_InsertAndSearch(t *testing.T) {
	bt := NewBTreeIndex(4)

	// Insert some values
	bt.Insert("value1", "key1")
	bt.Insert("value1", "key2")
	bt.Insert("value2", "key3")

	// Search
	keys1 := bt.Search("value1")
	if len(keys1) != 2 {
		t.Errorf("Expected 2 keys for value1, got %d", len(keys1))
	}
	if keys1[0] != "key1" || keys1[1] != "key2" {
		t.Errorf("Unexpected keys: %v", keys1)
	}

	keys2 := bt.Search("value2")
	if len(keys2) != 1 || keys2[0] != "key3" {
		t.Errorf("Expected [key3], got %v", keys2)
	}

	keys3 := bt.Search("value3")
	if len(keys3) != 0 {
		t.Errorf("Expected empty, got %v", keys3)
	}
}

func TestBTreeIndex_Delete(t *testing.T) {
	bt := NewBTreeIndex(4)

	bt.Insert("value1", "key1")
	bt.Insert("value1", "key2")

	// Delete one
	bt.Delete("value1", "key1")

	keys := bt.Search("value1")
	if len(keys) != 1 || keys[0] != "key2" {
		t.Errorf("Expected [key2], got %v", keys)
	}

	// Delete the last
	bt.Delete("value1", "key2")

	keys = bt.Search("value1")
	if len(keys) != 0 {
		t.Errorf("Expected empty, got %v", keys)
	}
}

func TestBTreeIndex_Counts(t *testing.T) {
	bt := NewBTreeIndex(4)

	bt.Insert("value1", "key1")
	bt.Insert("value1", "key2")
	bt.Insert("value2", "key3")

	if bt.CountUniqueValues() != 2 {
		t.Errorf("Expected 2 unique values, got %d", bt.CountUniqueValues())
	}

	if bt.CountKeys() != 3 {
		t.Errorf("Expected 3 keys, got %d", bt.CountKeys())
	}
}

func TestBTreeIndex_Serialization(t *testing.T) {
	bt := NewBTreeIndex(4)

	bt.Insert("value1", "key1")
	bt.Insert("value2", "key2")

	data, err := bt.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	bt2, err := deserializeBTreeIndex(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if bt2.CountKeys() != 2 {
		t.Errorf("Deserialized tree has wrong count")
	}

	keys := bt2.Search("value1")
	if len(keys) != 1 || keys[0] != "key1" {
		t.Errorf("Deserialized search failed")
	}
}
