package nnut

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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
