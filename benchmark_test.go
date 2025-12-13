package nnut

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"
)

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

const userCount = 10000

// TestSetupBenchmarkDB creates a template database for benchmarks
func TestSetupBenchmarkDB(t *testing.T) {
	os.Remove("benchmark_template.db")
	os.Remove("benchmark_template.db.wal")
	db, err := Open("benchmark_template.db")
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Create diverse test data for realistic benchmarking
	commonNames := []string{"John", "Jane", "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Peter", "Quinn", "Ryan"}
	for index := 0; index < userCount; index++ {
		key := fmt.Sprintf("user_%d", index)
		name := commonNames[index%len(commonNames)]
		email := fmt.Sprintf("%s%d@example.com", name, index)
		age := rand.Intn(63) + 18 // Ages 18-80
		testUser := TestUser{UUID: key, Name: name, Email: email, Age: age}
		err := store.Put(context.Background(), testUser)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()
	os.Remove("benchmark_template.db.wal") // Remove WAL after flush to avoid replay issues
	// Leave the DB file behind for benchmarks to copy
}

// BenchmarkHighLoadConcurrent simulates high concurrent load with mixed operations
func BenchmarkHighLoadConcurrent(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}

	db, err := Open("benchmark.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("benchmark.db")
	defer os.Remove("benchmark.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// High load: concurrent Puts, Gets, and Queries
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		index := 0
		for pb.Next() {
			key := fmt.Sprintf("load_%d_%d", index%10, index)
			index++

			// Mix of operations
			switch index % 3 {
			case 0: // Put
				user := TestUser{UUID: key, Name: "New", Email: "new@example.com", Age: 29}
				store.Put(context.Background(), user)
			case 1: // Get
				store.Get(context.Background(), fmt.Sprintf("user_%d", index%userCount))
			case 2: // Query
				store.GetQuery(context.Background(), &Query{
					Conditions: []Condition{{Field: "Name", Value: "Alice"}},
					Limit:      10,
				})
			}
		}
	})
	db.Flush()
}

// BenchmarkWALTruncation measures the performance of WAL truncation after flush
func BenchmarkWALTruncation(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}

	config := &Config{
		FlushInterval:  time.Hour, // Prevent auto-flush
		MaxBufferBytes: 100000,    // Large buffer
	}
	db, err := OpenWithConfig("benchmark.db", config)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("benchmark.db")
	defer os.Remove("benchmark.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// Add some operations to buffer
	for i := 0; i < 1000; i++ {
		user := TestUser{UUID: fmt.Sprintf("trunc_%d", i), Name: "Trunc", Email: "trunc@example.com", Age: i}
		store.Put(context.Background(), user)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Flush and truncate
		db.Flush()
	}
}
