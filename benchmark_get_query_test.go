package nnut

import (
	"context"
	"os"
	"testing"
)

func BenchmarkGetQuery(b *testing.B) {
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

	// Test typical query patterns
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_, err := store.GetQuery(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
			},
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query: %v", err)
		}
	}
}

func BenchmarkGetQueryMultipleConditions(b *testing.B) {
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
		_, err := store.GetQuery(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
				{Field: "Age", Value: 30, Operator: GreaterThan},
			},
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query: %v", err)
		}
	}
}

func BenchmarkGetQuerySorting(b *testing.B) {
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
	// Query with sorting by name
	for i := 0; i < b.N; i++ {
		_, err := store.GetQuery(context.Background(), &Query{
			Index: "Name",
			Sort:  Descending,
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query: %v", err)
		}
	}
}

func BenchmarkGetQueryLimitOffset(b *testing.B) {
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
	// Query with limit and offset
	for i := 0; i < b.N; i++ {
		_, err := store.GetQuery(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
			},
			Offset: 50,
			Limit:  50,
		})
		if err != nil {
			b.Fatalf("Failed to query: %v", err)
		}
	}
}

func BenchmarkGetQueryNoConditions(b *testing.B) {
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
		_, err := store.GetQuery(context.Background(), &Query{
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query no conditions: %v", err)
		}
	}
}

func BenchmarkGetQueryNonIndexedField(b *testing.B) {
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
		_, err := store.GetQuery(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Age", Value: 25, Operator: GreaterThan},
			},
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query non-indexed: %v", err)
		}
	}
}

func BenchmarkGetQueryComplexOperators(b *testing.B) {
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
		_, err := store.GetQuery(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "A", Operator: GreaterThanOrEqual},
				{Field: "Age", Value: 30, Operator: LessThan},
			},
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query complex operators: %v", err)
		}
	}
}

func BenchmarkGetQueryLargeLimit(b *testing.B) {
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
		_, err := store.GetQuery(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
			},
			Limit: 10000,
		})
		if err != nil {
			b.Fatalf("Failed to query large limit: %v", err)
		}
	}
}

func BenchmarkGetQueryOffsetOnly(b *testing.B) {
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
		_, err := store.GetQuery(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
			},
			Offset: 50,
		})
		if err != nil {
			b.Fatalf("Failed to query offset only: %v", err)
		}
	}
}

func BenchmarkGetQuerySortingAscending(b *testing.B) {
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
		_, err := store.GetQuery(context.Background(), &Query{
			Index: "Name",
			Sort:  Ascending,
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query sorting ascending: %v", err)
		}
	}
}
