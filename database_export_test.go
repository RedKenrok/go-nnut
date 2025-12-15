package nnut

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestExport(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	exportPath := filepath.Join(t.TempDir(), t.Name()+"_export.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")
	defer os.Remove(exportPath)

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Put some data
	user := TestUser{UUID: "1", Name: "John", Email: "john@example.com", Age: 30}
	err = store.Put(context.Background(), user)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Export
	err = db.Export(exportPath)
	if err != nil {
		t.Fatalf("Failed to export: %v", err)
	}

	// Check if files exist
	if _, err := os.Stat(exportPath); os.IsNotExist(err) {
		t.Fatal("Export DB file not created")
	}

	// Open the exported DB to verify
	exportedDB, err := Open(exportPath)
	if err != nil {
		t.Fatalf("Failed to open exported DB: %v", err)
	}
	defer exportedDB.Close()

	exportedStore, err := NewStore[TestUser](exportedDB, "users")
	if err != nil {
		t.Fatalf("Failed to create exported store: %v", err)
	}

	// Check data
	retrieved, err := exportedStore.Get(context.Background(), "1")
	if err != nil {
		t.Fatalf("Failed to get from exported DB: %v", err)
	}
	if retrieved != user {
		t.Fatalf("Data mismatch: got %v, want %v", retrieved, user)
	}
}

func TestExportDestinationExists(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	exportPath := filepath.Join(t.TempDir(), t.Name()+"_export.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create file at export path
	err = os.WriteFile(exportPath, []byte("dummy"), 0644)
	if err != nil {
		t.Fatalf("Failed to create dummy file: %v", err)
	}
	defer os.Remove(exportPath)

	// Export should fail
	err = db.Export(exportPath)
	if err == nil {
		t.Fatal("Expected error when exporting to existing file")
	}
}
