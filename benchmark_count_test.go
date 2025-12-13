package nnut

import (
	"context"
	"os"
	"testing"
)

func BenchmarkCount(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}
	if _, err := os.Stat("benchmark_template.db.wal"); err == nil {
		copyFile("benchmark_template.db.wal", "benchmark.db.wal")
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
	for i := 0; i < b.N; i++ {
		_, err := store.Count(context.Background())
		if err != nil {
			b.Fatalf("Failed to count: %v", err)
		}
	}
}
