package nnut

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewStore(t *testing.T) {
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
	if store == nil {
		t.Fatal("Store is nil")
	}
}

func TestPutAndGet(t *testing.T) {
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

	user := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(user)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	store.database.Flush()

	retrieved, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if retrieved.Name != user.Name || retrieved.Email != user.Email {
		t.Fatalf("Retrieved data mismatch: got %+v, want %+v", retrieved, user)
	}
}

func TestGetNonExistent(t *testing.T) {
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

	_, err = store.Get("nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent key")
	}
}

func TestDelete(t *testing.T) {
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

	user := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(user)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	db.Flush()

	// Check exists
	retrieved, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get before delete: %v", err)
	}
	if retrieved.Name != user.Name {
		t.Fatal("Data mismatch before delete")
	}

	// Delete
	err = store.Delete("key1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	db.Flush()

	// Check not exists
	_, err = store.Get("key1")
	if err == nil {
		t.Fatal("Expected error after delete")
	}
}

func TestBatchOperations(t *testing.T) {
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

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com"},
		{UUID: "2", Name: "Bob", Email: "bob@example.com"},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com"},
	}
	err = store.PutBatch(users)
	if err != nil {
		t.Fatalf("Failed to put batch: %v", err)
	}
	db.Flush()

	// Get batch
	results, err := store.GetBatch([]string{"1", "2", "4"})
	if err != nil {
		t.Fatalf("Failed to get batch: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	if results["1"].Name != "Alice" || results["2"].Name != "Bob" {
		t.Fatal("Wrong batch get results")
	}

	// Delete batch
	err = store.DeleteBatch([]string{"1", "3"})
	if err != nil {
		t.Fatalf("Failed to delete batch: %v", err)
	}
	db.Flush()

	// Check remaining
	results, err = store.GetBatch([]string{"1", "2", "3"})
	if err != nil {
		t.Fatalf("Failed to get batch after delete: %v", err)
	}
	if len(results) != 1 || results["2"].Name != "Bob" {
		t.Fatal("Wrong results after batch delete")
	}
}
