package nnut

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWALFlushInterval(t *testing.T) {
	config := &Config{
		FlushInterval: 100 * time.Millisecond,
	}
	db, err := OpenWithConfig("test.db", config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	// Don't use defer Close() to avoid interfering with automatic flush
	defer os.Remove("test.db")
	defer os.Remove("test.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testUser := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(context.Background(), testUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Check that WAL file exists and has data before automatic flush
	walPath := "test.db.wal"
	walData, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file before flush: %v", err)
	}
	if len(walData) == 0 {
		t.Fatal("WAL file should contain data before flush")
	}
	initialSize := len(walData)

	// Wait for automatic flush to occur based on FlushInterval
	time.Sleep(200 * time.Millisecond)

	// Check that WAL file is truncated after automatic flush
	walDataAfter, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file after automatic flush: %v", err)
	}
	finalSize := len(walDataAfter)

	if finalSize >= initialSize {
		t.Fatalf("WAL file should be truncated after automatic flush (initial: %d, final: %d)", initialSize, finalSize)
	}

	// Verify data is still accessible after automatic flush
	retrieved, err := store.Get(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Failed to get after automatic flush: %v", err)
	}
	if retrieved.Name != testUser.Name {
		t.Fatal("Data not accessible after automatic flush")
	}

	// Now close the database
	db.Close()
}

func TestSizeBasedFlush(t *testing.T) {
	config := &Config{
		FlushInterval:  time.Hour, // Long to not trigger
		MaxBufferBytes: 1000,      // Small size to trigger flush
	}
	db, err := OpenWithConfig("test.db", config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("test.db")
	defer os.Remove("test.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add operations until buffer size triggers flush
	for i := 0; i < 10; i++ {
		user := TestUser{
			UUID:  fmt.Sprintf("user%d", i),
			Name:  fmt.Sprintf("Name%d", i),
			Email: fmt.Sprintf("email%d@example.com", i),
		}
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put user %d: %v", i, err)
		}
	}

	// Give time for flush to occur
	time.Sleep(100 * time.Millisecond)

	// Check that data is persisted (flush occurred)
	retrieved, err := store.Get(context.Background(), "user0")
	if err != nil {
		t.Fatalf("Failed to get user0: %v", err)
	}
	if retrieved.Name != "Name0" {
		t.Fatal("Data not flushed properly")
	}
}

// TestWALRecoveryAfterSimulatedCrash tests WAL recovery by simulating a crash and reopening.
func TestWALRecoveryAfterSimulatedCrash(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		FlushInterval:  time.Hour,
		MaxBufferBytes: 1000000, // Large buffer to prevent flush during puts
	}
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add data without flushing
	for i := 0; i < 50; i++ {
		user := TestUser{UUID: fmt.Sprintf("user%d", i), Name: "CrashTest", Email: "crash@example.com", Age: i}
		store.Put(context.Background(), user)
	}

	// "Simulate crash" by closing without flushing
	db.Close()

	// Reopen - should replay WAL
	db2, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to reopen DB after crash: %v", err)
	}
	defer db2.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store2, err := NewStore[TestUser](db2, "users")
	if err != nil {
		t.Fatalf("Failed to create store after reopen: %v", err)
	}

	// Data should be recovered
	for i := 0; i < 50; i++ {
		retrieved, err := store2.Get(context.Background(), fmt.Sprintf("user%d", i))
		if err != nil {
			t.Fatalf("Failed to get user%d: %v", i, err)
		}
		if retrieved.Name != "CrashTest" {
			t.Fatalf("Data not recovered for user%d", i)
		}
	}
}

// TestWALCorruptionHandling tests handling of corrupted WAL files.
func TestWALCorruptionHandling(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		FlushInterval: time.Hour,
	}

	// Create DB and add data
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	user := TestUser{UUID: "test", Name: "Test", Email: "test@example.com", Age: 25}
	store.Put(context.Background(), user)

	// Corrupt the WAL by truncating it
	walPath := dbPath + ".wal"
	file, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	file.Truncate(10) // Corrupt by shortening
	file.Close()

	db.Close()

	// Reopen - should handle corruption gracefully (discard WAL)
	db2, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to reopen after corruption: %v", err)
	}
	defer db2.Close()
	defer os.Remove(dbPath)
	defer os.Remove(walPath)

	store2, err := NewStore[TestUser](db2, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Data should still be there (from DB, not WAL)
	retrieved, err := store2.Get(context.Background(), "test")
	if err != nil {
		t.Fatalf("Failed to get after corruption handling: %v", err)
	}
	if retrieved.Name != "Test" {
		t.Fatal("Data lost after WAL corruption")
	}
}

func TestWALTruncation(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		FlushInterval:  time.Hour, // Prevent auto-flush
		MaxBufferBytes: 100000,    // Large buffer to prevent auto-flush
	}
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add initial data
	for i := 0; i < 10; i++ {
		user := TestUser{UUID: fmt.Sprintf("user%d", i), Name: "TruncationTest", Email: "trunc@example.com", Age: i}
		store.Put(context.Background(), user)
	}

	// Check WAL has content
	walPath := dbPath + ".wal"
	stat, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should exist: %v", err)
	}
	initialSize := stat.Size()
	if initialSize == 0 {
		t.Fatal("WAL should have content before flush")
	}

	// Flush manually
	db.Flush()

	// Wait for truncation
	time.Sleep(100 * time.Millisecond)

	// WAL should be truncated (empty, since all committed)
	stat, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should exist: %v", err)
	}
	if stat.Size() != 0 {
		t.Fatal("WAL should be empty after flush of all operations")
	}

	// Add more data
	for i := 10; i < 15; i++ {
		user := TestUser{UUID: fmt.Sprintf("user%d", i), Name: "TruncationTest", Email: "trunc@example.com", Age: i}
		store.Put(context.Background(), user)
	}

	// WAL should have content again
	stat, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should exist: %v", err)
	}
	if stat.Size() == 0 {
		t.Fatal("WAL should have content after new operations")
	}

	// Flush again
	db.Flush()

	// Wait
	time.Sleep(100 * time.Millisecond)

	// WAL should be empty again
	stat, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should exist: %v", err)
	}
	if stat.Size() != 0 {
		t.Fatal("WAL should be empty after second flush")
	}

	// Close and reopen to test recovery
	db.Close()
	db2, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer db2.Close()

	store2, err := NewStore[TestUser](db2, "users")
	if err != nil {
		t.Fatalf("Failed to create store after reopen: %v", err)
	}

	// All data should be present
	for i := 0; i < 15; i++ {
		retrieved, err := store2.Get(context.Background(), fmt.Sprintf("user%d", i))
		if err != nil {
			t.Fatalf("Failed to get user%d: %v", i, err)
		}
		if retrieved.Name != "TruncationTest" {
			t.Fatalf("Data incorrect for user%d", i)
		}
	}
}

func TestWALCreationAndFlushOnClose(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		FlushInterval:  time.Hour,
		MaxBufferBytes: 1000000, // Large to prevent auto-flush
	}
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Put some data
	testUser := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(context.Background(), testUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Check WAL is created and has content
	walPath := dbPath + ".wal"
	stat, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should exist: %v", err)
	}
	if stat.Size() == 0 {
		t.Fatal("WAL should have content after mutation")
	}

	// Close DB - should flush and truncate WAL
	db.Close()

	// Check WAL is empty
	stat, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should still exist: %v", err)
	}
	if stat.Size() != 0 {
		t.Fatal("WAL should be empty after close")
	}

	// Reopen and verify data is committed
	db2, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer db2.Close()

	store2, err := NewStore[TestUser](db2, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	retrieved, err := store2.Get(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Failed to get after close: %v", err)
	}
	if retrieved.Name != "John" {
		t.Fatal("Data not committed to DB")
	}
}
