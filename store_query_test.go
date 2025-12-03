package nnut

import (
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

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Alice", Email: "alice2@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query by name
	results, err := store.Query(&Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Name != "Alice" {
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query with multiple conditions: Name == "Alice" AND Age > 30
	results, err := store.Query(&Query{
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query sorted by name ascending
	results, err := store.Query(&Query{
		Index: "name",
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
	results, err = store.Query(&Query{
		Index: "name",
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query with limit
	results, err := store.Query(&Query{
		Index: "name",
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
	results, err = store.Query(&Query{
		Index:  "name",
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Test GreaterThan on Age
	results, err := store.Query(&Query{
		Conditions: []Condition{
			{Field: "Age", Value: 30, Operator: GreaterThan},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	// Should be Charlie (40) and David (35)
	ages := []int{results[0].Age, results[1].Age}
	if !((ages[0] == 35 && ages[1] == 40) || (ages[0] == 40 && ages[1] == 35)) {
		t.Fatal("Wrong results for GreaterThan")
	}

	// Test LessThanOrEqual on Name
	results, err = store.Query(&Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Bob", Operator: LessThanOrEqual},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Count with condition
	count, err := store.QueryCount(&Query{
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query no conditions, no limit
	results, err := store.Query(&Query{})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Query no conditions with limit
	results, err = store.Query(&Query{
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query non-indexed field with limit
	results, err := store.Query(&Query{
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query with mixed conditions and limit
	results, err := store.Query(&Query{
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query with large limit
	results, err := store.Query(&Query{
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query with offset but no limit
	results, err := store.Query(&Query{
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Count no conditions
	count, err := store.QueryCount(&Query{})
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
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Count non-indexed
	count, err := store.QueryCount(&Query{
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
