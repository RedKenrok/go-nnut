package nnut

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestQuery(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Alice", Email: "alice2@example.com", Age: 35},
	}
	for _, user := range testUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Test filtering by indexed field
	retrievedResults, err := store.GetQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(retrievedResults) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(retrievedResults))
	}
	for _, result := range retrievedResults {
		if result.Name != "Alice" {
			t.Fatal("Wrong result")
		}
	}
}

func TestQueryMultipleConditions(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Alice", Email: "alice2@example.com", Age: 35},
		{UUID: "4", Name: "Charlie", Email: "charlie@example.com", Age: 40},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Test combining indexed and non-indexed conditions
	results, err := store.GetQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
			{Field: "Age", Value: 30, Operator: GreaterThan},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].UUID != "3" {
		t.Fatal("Wrong result")
	}
}

func TestQuerySorting(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Charlie", Email: "charlie@example.com", Age: 40},
		{UUID: "2", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "3", Name: "Bob", Email: "bob@example.com", Age: 25},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Test ordering results using index
	results, err := store.GetQuery(context.Background(), &Query{
		Index: "Name",
		Sort:  Ascending,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
	if results[0].Name != "Alice" || results[1].Name != "Bob" || results[2].Name != "Charlie" {
		t.Fatal("Wrong sort order")
	}

	// Query sorted by name descending
	results, err = store.GetQuery(context.Background(), &Query{
		Index: "Name",
		Sort:  Descending,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if results[0].Name != "Charlie" || results[1].Name != "Bob" || results[2].Name != "Alice" {
		t.Fatal("Wrong sort order")
	}
}

func TestQueryLimitOffset(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 40},
		{UUID: "4", Name: "David", Email: "david@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Test pagination with limit
	results, err := store.GetQuery(context.Background(), &Query{
		Index: "Name",
		Sort:  Ascending,
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	if results[0].Name != "Alice" || results[1].Name != "Bob" {
		t.Fatal("Wrong results")
	}

	// Query with offset
	results, err = store.GetQuery(context.Background(), &Query{
		Index:  "Name",
		Sort:   Ascending,
		Offset: 1,
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	if results[0].Name != "Bob" || results[1].Name != "Charlie" {
		t.Fatal("Wrong results")
	}
}

func TestQueryOperators(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 40},
		{UUID: "4", Name: "David", Email: "david@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Test GreaterThan on Age
	retrievedResults, err := store.GetQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Age", Value: 30, Operator: GreaterThan},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(retrievedResults) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(retrievedResults))
	}
	// Should be Charlie (40) and David (35)
	retrievedAges := []int{retrievedResults[0].Age, retrievedResults[1].Age}
	if !((retrievedAges[0] == 35 && retrievedAges[1] == 40) || (retrievedAges[0] == 40 && retrievedAges[1] == 35)) {
		t.Fatal("Wrong results for GreaterThan")
	}

	// Test LessThanOrEqual on Name
	retrievedResults, err = store.GetQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Bob", Operator: LessThanOrEqual},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(retrievedResults) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(retrievedResults))
	}
	// Alice and Bob
}

func TestQueryCount(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Alice", Email: "alice2@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Count with condition
	count, err := store.CountQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query count: %v", err)
	}
	if count != 2 {
		t.Fatalf("Expected count 2, got %d", count)
	}
}

func TestQueryNoConditionsLimit(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 40},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query no conditions, no limit
	results, err := store.GetQuery(context.Background(), &Query{})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Query no conditions with limit
	results, err = store.GetQuery(context.Background(), &Query{
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
}

func TestQueryNonIndexedWithLimit(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 40},
		{UUID: "4", Name: "David", Email: "david@example.com", Age: 35},
		{UUID: "5", Name: "Eve", Email: "eve@example.com", Age: 28},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query non-indexed field with limit
	results, err := store.GetQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Age", Value: 25, Operator: GreaterThan},
		},
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	// Should return 2 out of 4 matching (30,35,40,28 >25)
}

func TestQueryComplexWithLimit(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 40},
		{UUID: "4", Name: "David", Email: "david@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query with mixed conditions and limit
	results, err := store.GetQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "A", Operator: GreaterThanOrEqual}, // Alice, Bob, Charlie, David
			{Field: "Age", Value: 30, Operator: LessThan},             // Bob (25)
		},
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].UUID != "2" {
		t.Fatal("Wrong result")
	}
}

func TestQueryLargeLimit(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add 10 users
	users := make([]TestUser, 10)
	for i := 0; i < 10; i++ {
		users[i] = TestUser{
			UUID:  fmt.Sprintf("%d", i+1),
			Name:  fmt.Sprintf("User%d", i+1),
			Email: fmt.Sprintf("user%d@example.com", i+1),
			Age:   20 + i,
		}
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query with large limit
	results, err := store.GetQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "User", Operator: GreaterThanOrEqual},
		},
		Limit: 100, // More than total
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 10 {
		t.Fatalf("Expected 10 results, got %d", len(results))
	}
}

func TestQueryOffsetNoLimit(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 40},
		{UUID: "4", Name: "David", Email: "david@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query with offset but no limit
	results, err := store.GetQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
		Offset: 1,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	// Since only Alice matches, offset 1 should return 0
	if len(results) != 0 {
		t.Fatalf("Expected 0 results, got %d", len(results))
	}
}

func TestQueryCountAll(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Count no conditions
	count, err := store.CountQuery(context.Background(), &Query{})
	if err != nil {
		t.Fatalf("Failed to query count: %v", err)
	}
	if count != 2 {
		t.Fatalf("Expected count 2, got %d", count)
	}
}

func TestQueryCountNonIndexed(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 40},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Count non-indexed
	count, err := store.CountQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Age", Value: 25, Operator: GreaterThan},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query count: %v", err)
	}
	if count != 2 { // 30 and 40
		t.Fatalf("Expected count 2, got %d", count)
	}
}

func TestCount(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Alice", Email: "alice2@example.com", Age: 35},
		{UUID: "4", Name: "Charlie", Email: "charlie@example.com", Age: 30},
	}
	for _, user := range testUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Test count for total items (should be 4)
	count, err := store.Count(context.Background())
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 4 {
		t.Fatalf("Expected count 4, got %d", count)
	}
}

func TestCountEmpty(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// No data added, store should be empty
	count, err := store.Count(context.Background())
	if err != nil {
		t.Fatalf("Failed to count empty store: %v", err)
	}
	if count != 0 {
		t.Fatalf("Expected count 0 for empty store, got %d", count)
	}
}

func TestCountWithBufferedOperations(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add some initial data and flush
	initialUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
	}
	for _, user := range initialUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Count should be 2
	count, err := store.Count(context.Background())
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 2 {
		t.Fatalf("Expected count 2, got %d", count)
	}

	// Add more users without flushing (buffered)
	bufferedUsers := []TestUser{
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 35}, // new
		{UUID: "4", Name: "David", Email: "david@example.com", Age: 40},     // new
	}
	for _, user := range bufferedUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}

	// Count should now include buffered operations (4 total)
	count, err = store.Count(context.Background())
	if err != nil {
		t.Fatalf("Failed to count with buffered ops: %v", err)
	}
	if count != 4 {
		t.Fatalf("Expected count 4 with buffered operations, got %d", count)
	}

	// Delete one existing user (buffered)
	err = store.Delete(context.Background(), "1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Count should be 3 (2 flushed + 2 buffered puts - 1 buffered delete)
	count, err = store.Count(context.Background())
	if err != nil {
		t.Fatalf("Failed to count after buffered delete: %v", err)
	}
	if count != 3 {
		t.Fatalf("Expected count 3 after buffered delete, got %d", count)
	}
}

func TestCountIdenticalValues(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add users with identical names
	testUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice1@example.com", Age: 30},
		{UUID: "2", Name: "Alice", Email: "alice2@example.com", Age: 25},
		{UUID: "3", Name: "Alice", Email: "alice3@example.com", Age: 35},
	}
	for _, user := range testUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Should return 3 total items
	count, err := store.Count(context.Background())
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 3 {
		t.Fatalf("Expected count 3 for total items, got %d", count)
	}
}

func TestCountContextCancellation(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
	}
	for _, user := range testUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = store.Count(ctx)
	if err != context.Canceled {
		t.Fatalf("Expected context.Canceled, got %v", err)
	}
}

func TestDeleteQuery(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Alice", Email: "alice2@example.com", Age: 35},
		{UUID: "4", Name: "Charlie", Email: "charlie@example.com", Age: 30},
	}
	for _, user := range testUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Delete users with name "Alice"
	deletedCount, err := store.DeleteQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to delete query: %v", err)
	}
	if deletedCount != 2 {
		t.Fatalf("Expected 2 deletions, got %d", deletedCount)
	}

	// Verify deletions by querying remaining users
	results, err := store.GetQuery(context.Background(), &Query{})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 remaining users, got %d", len(results))
	}

	// Verify total count is updated
	count, err := store.Count(context.Background())
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 2 { // Bob and Charlie
		t.Fatalf("Expected count 2, got %d", count)
	}
}

func TestDeleteQueryLimitOffset(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Alice", Email: "alice2@example.com", Age: 25},
		{UUID: "3", Name: "Bob", Email: "bob@example.com", Age: 35},
		{UUID: "4", Name: "Charlie", Email: "charlie@example.com", Age: 40},
	}
	for _, user := range testUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Delete with limit 1, offset 0 (should delete first Alice)
	deletedCount, err := store.DeleteQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("Failed to delete query with limit: %v", err)
	}
	if deletedCount != 1 {
		t.Fatalf("Expected 1 deletion, got %d", deletedCount)
	}

	// Verify remaining Alice still exists
	results, err := store.GetQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 Alice remaining, got %d", len(results))
	}
}

func TestDeleteQueryNoConditions(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
	}
	for _, user := range testUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Delete all records (no conditions)
	deletedCount, err := store.DeleteQuery(context.Background(), &Query{})
	if err != nil {
		t.Fatalf("Failed to delete all: %v", err)
	}
	if deletedCount != 2 {
		t.Fatalf("Expected 2 deletions, got %d", deletedCount)
	}

	// Verify no records remain
	results, err := store.GetQuery(context.Background(), &Query{})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("Expected 0 records remaining, got %d", len(results))
	}

	// Verify store is empty
	count, err := store.Count(context.Background())
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 0 {
		t.Fatalf("Expected count 0, got %d", count)
	}
}

func TestDeleteQueryNonIndexedConditions(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 35},
	}
	for _, user := range testUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Delete users with Age > 30 (non-indexed condition)
	deletedCount, err := store.DeleteQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Age", Value: 30, Operator: GreaterThan},
		},
	})
	if err != nil {
		t.Fatalf("Failed to delete with non-indexed condition: %v", err)
	}
	if deletedCount != 1 { // Charlie with Age 35
		t.Fatalf("Expected 1 deletion, got %d", deletedCount)
	}

	// Verify remaining users
	results, err := store.GetQuery(context.Background(), &Query{})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 users remaining, got %d", len(results))
	}
}

func TestDeleteQueryMultipleConditions(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Alice", Email: "alice2@example.com", Age: 25},
		{UUID: "3", Name: "Bob", Email: "bob@example.com", Age: 30},
	}
	for _, user := range testUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Delete users with Name="Alice" AND Age=30 (should delete only first Alice)
	deletedCount, err := store.DeleteQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
			{Field: "Age", Value: 30, Operator: Equals},
		},
	})
	if err != nil {
		t.Fatalf("Failed to delete with multiple conditions: %v", err)
	}
	if deletedCount != 1 {
		t.Fatalf("Expected 1 deletion, got %d", deletedCount)
	}

	// Verify remaining users: Alice(25), Bob(30)
	results, err := store.GetQuery(context.Background(), &Query{})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 users remaining, got %d", len(results))
	}
}

func TestDeleteQueryContextCancellation(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
	}
	for _, user := range testUsers {
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = store.DeleteQuery(ctx, &Query{})
	if err != context.Canceled {
		t.Fatalf("Expected context.Canceled, got %v", err)
	}
}

// FuzzQueryConditions fuzzes query conditions to find edge cases and crashes.
// Run with: go test -fuzz=FuzzQueryConditions -fuzztime=30s
func FuzzQueryConditions(f *testing.F) {
	// Seed with some initial inputs
	f.Add("Name", "Alice")
	f.Add("Age", "25")
	f.Add("Email", "test@example.com")

	f.Fuzz(func(t *testing.T, field string, valueStr string) {
		dbPath := filepath.Join(t.TempDir(), "fuzz.db")
		db, err := Open(dbPath)
		if err != nil {
			return // Skip if DB open fails
		}
		defer db.Close()
		defer os.Remove(dbPath)
		defer os.Remove(dbPath + ".wal")

		store, err := NewStore[TestUser](db, "users")
		if err != nil {
			return
		}

		// Add some test data
		user := TestUser{UUID: "test1", Name: "Alice", Email: "alice@example.com", Age: 30}
		store.Put(context.Background(), user)
		db.Flush()

		// Fuzz the condition with random operator
		operator := Operator(len(field) % 6) // Simple way to vary operator
		condition := Condition{Field: field, Value: valueStr, Operator: operator}

		// This should not panic or crash
		_, _ = store.GetQuery(context.Background(), &Query{Conditions: []Condition{condition}})
		_, _ = store.CountQuery(context.Background(), &Query{Conditions: []Condition{condition}})
		_, _ = store.Count(context.Background())                                                             // Test Count method
		_, _ = store.DeleteQuery(context.Background(), &Query{Conditions: []Condition{condition}, Limit: 1}) // Test DeleteQuery with limit to avoid deleting everything
	})
}

func TestCountQueryWithBufferedOperations(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Put initial data and flush
	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Test CountQuery with conditions and buffered operations
	// Count Alice users (should be 1)
	count, err := store.CountQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to count query: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected count 1 for Alice, got %d", count)
	}

	// Add a new Alice user (buffered operation)
	newAlice := TestUser{UUID: "4", Name: "Alice", Email: "alice2@example.com", Age: 40}
	err = store.Put(context.Background(), newAlice)
	if err != nil {
		t.Fatalf("Failed to put new Alice: %v", err)
	}

	// Count should now include the buffered operation
	count, err = store.CountQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to count query with buffered op: %v", err)
	}
	if count != 2 {
		t.Fatalf("Expected count 2 for Alice (including buffered), got %d", count)
	}

	// Delete an existing Alice (buffered operation)
	err = store.Delete(context.Background(), "1") // Delete the original Alice
	if err != nil {
		t.Fatalf("Failed to delete Alice: %v", err)
	}

	// Count should reflect the deletion
	count, err = store.CountQuery(context.Background(), &Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to count query after delete: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected count 1 for Alice (after delete), got %d", count)
	}
}

func TestCountQueryIndexWithBufferedOperations(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Put initial data and flush
	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(context.Background(), u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Test CountQuery with index and buffered operations
	// Count all users by Name index
	count, err := store.CountQuery(context.Background(), &Query{
		Index: "Name",
	})
	if err != nil {
		t.Fatalf("Failed to count query by index: %v", err)
	}
	if count != 3 {
		t.Fatalf("Expected count 3 by Name index, got %d", count)
	}

	// Add a new user (buffered operation)
	newUser := TestUser{UUID: "4", Name: "David", Email: "david@example.com", Age: 28}
	err = store.Put(context.Background(), newUser)
	if err != nil {
		t.Fatalf("Failed to put new user: %v", err)
	}

	// Count should include the buffered operation
	count, err = store.CountQuery(context.Background(), &Query{
		Index: "Name",
	})
	if err != nil {
		t.Fatalf("Failed to count query by index with buffered op: %v", err)
	}
	if count != 4 {
		t.Fatalf("Expected count 4 by Name index (including buffered), got %d", count)
	}

	// Delete an existing user (buffered operation)
	err = store.Delete(context.Background(), "2") // Delete Bob
	if err != nil {
		t.Fatalf("Failed to delete user: %v", err)
	}

	// Count should reflect the deletion
	count, err = store.CountQuery(context.Background(), &Query{
		Index: "Name",
	})
	if err != nil {
		t.Fatalf("Failed to count query by index after delete: %v", err)
	}
	if count != 3 {
		t.Fatalf("Expected count 3 by Name index (after delete), got %d", count)
	}
}
