package nnut

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

// TestUser for testing
type TestUser struct {
	UUID  string `nnut:"key"`
	Name  string `nnut:"index"`
	Email string `nnut:"index"`
	Age   int    `nnut:"index"`
}

// ExampleOpen demonstrates opening a database with default configuration.
func ExampleOpen() {
	db, err := Open("mydata.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// Use db...
}

// ExampleOpenWithConfig demonstrates opening a database with custom configuration.
func ExampleOpenWithConfig() {
	config := &Config{
		FlushInterval:  10 * time.Minute,
		MaxBufferBytes: 5 * 1024 * 1024, // 5MB
	}
	db, err := OpenWithConfig("mydata.db", config)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// Use db...
}

func TestOpen(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")
}

func TestOperationTypeEnum(t *testing.T) {
	t.Parallel()
	// Test that OperationType constants are defined correctly
	if OpPut != 0 {
		t.Errorf("Expected OpPut = 0, got %d", OpPut)
	}
	if OpDelete != 1 {
		t.Errorf("Expected OpDelete = 1, got %d", OpDelete)
	}
	if OpIndexDirty != 2 {
		t.Errorf("Expected OpIndexDirty = 2, got %d", OpIndexDirty)
	}
}

func TestIndexBufferingAndPersistence(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Put a user to trigger index buffering
	user := TestUser{UUID: "1", Name: "John", Email: "john@example.com", Age: 30}
	err = store.Put(context.Background(), user)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Check that operationsBuffer contains index operations with data
	db.operationsBufferMutex.Lock()
	foundIndexOp := false
	for _, op := range db.operationsBuffer {
		if op.Type == OpIndexDirty && len(op.Value) > 0 {
			foundIndexOp = true
			break
		}
	}
	db.operationsBufferMutex.Unlock()

	if !foundIndexOp {
		t.Fatal("Expected to find index operation with data in operationsBuffer")
	}

	// Flush and verify index is persisted
	db.Flush()

	// Check that index data exists in DB
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("__btree_indexes"))
		if bucket == nil {
			t.Fatal("Expected __btree_indexes bucket to exist")
		}
		key := buildBTreeKey("users", "__primary_key")
		data := bucket.Get([]byte(key))
		if len(data) == 0 {
			t.Fatal("Expected index data to be persisted")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to check index persistence: %v", err)
	}
}

func TestWALIndexMarkerSerialization(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Put a user
	user := TestUser{UUID: "1", Name: "John", Email: "john@example.com", Age: 30}
	err = store.Put(context.Background(), user)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Read WAL and verify OpIndexDirty has no Value
	walFile, err := os.Open(db.config.WALPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer walFile.Close()

	decoder := msgpack.NewDecoder(walFile)
	for {
		var entry walEntry
		err := decoder.Decode(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Failed to decode WAL entry: %v", err)
		}

		if entry.Operation.Type == OpIndexDirty {
			if len(entry.Operation.Value) != 0 {
				t.Fatal("Expected OpIndexDirty in WAL to have empty Value")
			}
		}
	}
}

func TestIndexRebuildOnFlag(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Put some data first
	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	user := TestUser{UUID: "1", Name: "John", Email: "john@example.com", Age: 30}
	err = store.Put(context.Background(), user)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Flush to persist data
	db.Flush()

	// Manually set the rebuild flag (simulating crash recovery)
	db.indexesNeedRebuild["users:__primary_key"] = true

	// Create new store - should trigger rebuild
	store2, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Flag should be reset after rebuild
	if len(db.indexesNeedRebuild) > 0 {
		t.Fatal("Expected indexesNeedRebuild to be reset after store creation")
	}

	// Verify indexes are functional after rebuild
	retrieved, err := store2.Get(context.Background(), "1")
	if err != nil {
		t.Fatalf("Failed to get after rebuild: %v", err)
	}
	if retrieved != user {
		t.Fatalf("Data mismatch after rebuild: got %v, want %v", retrieved, user)
	}

	// Test that new operations work
	user2 := TestUser{UUID: "2", Name: "Jane", Email: "jane@example.com", Age: 25}
	err = store2.Put(context.Background(), user2)
	if err != nil {
		t.Fatalf("Failed to put after rebuild: %v", err)
	}

	retrieved2, err := store2.Get(context.Background(), "2")
	if err != nil {
		t.Fatalf("Failed to get new data after rebuild: %v", err)
	}
	if retrieved2 != user2 {
		t.Fatalf("New data mismatch after rebuild: got %v, want %v", retrieved2, user2)
	}
}
