package nnut

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func BenchmarkGet(b *testing.B) {
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

	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		key := fmt.Sprintf("user_%d", iteration%userCount)
		_, err := store.Get(context.Background(), key)
		if err != nil {
			b.Fatalf("Failed to get: %v", err)
		}
	}
}

func BenchmarkGetBatch(b *testing.B) {
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

	// Standardize batch size for fair performance comparison
	batchSize := 100
	b.ResetTimer()
	for batchIndex := 0; batchIndex < b.N/batchSize; batchIndex++ {
		var keys []string
		for keyIndex := 0; keyIndex < batchSize; keyIndex++ {
			keys = append(keys, fmt.Sprintf("user_%d", (batchIndex*batchSize+keyIndex)%userCount))
		}
		_, err := store.GetBatch(context.Background(), keys)
		if err != nil {
			b.Fatalf("Failed to get batch: %v", err)
		}
	}
}
