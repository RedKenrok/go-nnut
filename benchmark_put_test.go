package nnut

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func BenchmarkPut(b *testing.B) {
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user_%d", i)
		user := TestUser{UUID: key, Name: "John", Email: "john@example.com", Age: 30}
		err := store.Put(context.Background(), user)
		if err != nil {
			b.Fatalf("Failed to put: %v", err)
		}
	}
}

func BenchmarkPutBatch(b *testing.B) {
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

	// Using a batch size allows us to compare directly with BenchmarkPut
	batchSize := 100
	b.ResetTimer()
	for i := 0; i < b.N/batchSize; i++ {
		var users []TestUser
		for j := 0; j < batchSize; j++ {
			key := fmt.Sprintf("user_%d", (i*batchSize)+j)
			user := TestUser{UUID: key, Name: "John", Email: "john@example.com", Age: 30}
			users = append(users, user)
		}
		err := store.PutBatch(context.Background(), users)
		if err != nil {
			b.Fatalf("Failed to put batch: %v", err)
		}
	}
}
