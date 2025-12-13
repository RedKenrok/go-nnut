package nnut

import (
	"context"
	"os"
	"testing"
)

func BenchmarkDeleteQuery(b *testing.B) {
	// Copy template database for each benchmark run
	for i := 0; i < b.N; i++ {
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

		store, err := NewStore[TestUser](db, "users")
		if err != nil {
			b.Fatalf("Failed to create store: %v", err)
		}

		// Delete a small subset to avoid depleting the database
		_, err = store.DeleteQuery(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice", Operator: Equals},
			},
			Limit: 1, // Only delete one to keep DB populated
		})
		if err != nil {
			b.Fatalf("Failed to delete query: %v", err)
		}

		db.Close()
		os.Remove("benchmark.db")
		os.Remove("benchmark.db.wal")
	}
}
