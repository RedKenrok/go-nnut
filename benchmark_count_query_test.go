package nnut

import (
	"context"
	"os"
	"testing"
)

func BenchmarkCountQuery(b *testing.B) {
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
	// Count users with a specific name
	for i := 0; i < b.N; i++ {
		_, err := store.CountQuery(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
			},
		})
		if err != nil {
			b.Fatalf("Failed to query count: %v", err)
		}
	}
}

func BenchmarkCountQueryIndex(b *testing.B) {
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
	// Count all users using index
	for i := 0; i < b.N; i++ {
		_, err := store.CountQuery(context.Background(), &Query{
			Index: "Name",
		})
		if err != nil {
			b.Fatalf("Failed to query count index: %v", err)
		}
	}
}

func BenchmarkCountQueryNoConditions(b *testing.B) {
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
		_, err := store.CountQuery(context.Background(), &Query{})
		if err != nil {
			b.Fatalf("Failed to query count no conditions: %v", err)
		}
	}
}

func BenchmarkCountQueryNonIndexed(b *testing.B) {
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
		_, err := store.CountQuery(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Age", Value: 25, Operator: GreaterThan},
			},
		})
		if err != nil {
			b.Fatalf("Failed to query count non-indexed: %v", err)
		}
	}
}
