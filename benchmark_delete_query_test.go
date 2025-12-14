package nnut

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
)

func BenchmarkDeleteQuery(b *testing.B) {
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

	batchSize := 100

	// Put b.N records
	testUsers := make([]TestUser, b.N)
	for i := 0; i < b.N; i++ {
		testUsers[i] = TestUser{
			UUID:  fmt.Sprintf("user_%d", i),
			Name:  fmt.Sprintf("name_%d", i),
			Email: fmt.Sprintf("email_%d@example.com", i),
			Age:   i%int(math.Ceil(float64(b.N)/float64(batchSize))),
		}
	}
	err = store.PutBatch(context.Background(), testUsers)
	if err != nil {
		b.Fatalf("Failed to put batch: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.DeleteQuery(context.Background(), &Query{
      Conditions: []Condition{
        {Field: "Age", Value: i%int(math.Ceil(float64(b.N)/float64(batchSize)))},
      },
		})
		if err != nil {
			b.Fatalf("Failed to delete query: %v", err)
		}
	}
}
