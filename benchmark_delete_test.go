package nnut

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func BenchmarkDelete(b *testing.B) {
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")

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

	// Put b.N records
	testUsers := make([]TestUser, b.N)
	for i := 0; i < b.N; i++ {
		testUsers[i] = TestUser{
			UUID:  fmt.Sprintf("user_%d", i),
			Name:  fmt.Sprintf("name_%d", i),
			Email: fmt.Sprintf("email_%d@example.com", i),
		}
	}
	err = store.PutBatch(context.Background(), testUsers)
	if err != nil {
		b.Fatalf("Failed to put batch: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user_%d", i)
		err := store.Delete(context.Background(), key)
		if err != nil {
			b.Fatalf("Failed to delete: %v", err)
		}
	}
}

func BenchmarkDeleteBatch(b *testing.B) {
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")

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

	// Put b.N records
	testUsers := make([]TestUser, b.N)
	for i := 0; i < b.N; i++ {
		testUsers[i] = TestUser{
			UUID:  fmt.Sprintf("user_%d", i),
			Name:  fmt.Sprintf("name_%d", i),
			Email: fmt.Sprintf("email_%d@example.com", i),
		}
	}
	err = store.PutBatch(context.Background(), testUsers)
	if err != nil {
		b.Fatalf("Failed to put batch: %v", err)
	}

	// Using a batch size allows us to compare directly with BenchmarkDelete
	batchSize := 100
	b.ResetTimer()
	for i := 0; i < b.N/batchSize; i++ {
		var keys []string
		for j := 0; j < batchSize; j++ {
			keys = append(keys, fmt.Sprintf("user_%d", i*batchSize+j))
		}
		err := store.DeleteBatch(context.Background(), keys)
		if err != nil {
			b.Fatalf("Failed to delete batch: %v", err)
		}
	}
}
