package nnut

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

// testLogger implements the Logger interface for testing
type testLogger struct {
	buffer *bytes.Buffer
}

func newTestLogger() *testLogger {
	return &testLogger{buffer: &bytes.Buffer{}}
}

func (l *testLogger) Debug(v ...interface{}) {
	l.buffer.WriteString("DEBUG: " + fmt.Sprint(v...) + "\n")
}

func (l *testLogger) Debugf(format string, v ...interface{}) {
	l.buffer.WriteString("DEBUG: " + fmt.Sprintf(format, v...) + "\n")
}

func (l *testLogger) Error(v ...interface{}) {
	l.buffer.WriteString("ERROR: " + fmt.Sprint(v...) + "\n")
}

func (l *testLogger) Errorf(format string, v ...interface{}) {
	l.buffer.WriteString("ERROR: " + fmt.Sprintf(format, v...) + "\n")
}

func (l *testLogger) Info(v ...interface{}) {
	l.buffer.WriteString("INFO: " + fmt.Sprint(v...) + "\n")
}

func (l *testLogger) Infof(format string, v ...interface{}) {
	l.buffer.WriteString("INFO: " + fmt.Sprintf(format, v...) + "\n")
}

func (l *testLogger) Warning(v ...interface{}) {
	l.buffer.WriteString("WARN: " + fmt.Sprint(v...) + "\n")
}

func (l *testLogger) Warningf(format string, v ...interface{}) {
	l.buffer.WriteString("WARN: " + fmt.Sprintf(format, v...) + "\n")
}

func (l *testLogger) Fatal(v ...interface{}) {
	l.buffer.WriteString("FATAL: " + fmt.Sprint(v...) + "\n")
}

func (l *testLogger) Fatalf(format string, v ...interface{}) {
	l.buffer.WriteString("FATAL: " + fmt.Sprintf(format, v...) + "\n")
}

func (l *testLogger) Panic(v ...interface{}) {
	l.buffer.WriteString("PANIC: " + fmt.Sprint(v...) + "\n")
}

func (l *testLogger) Panicf(format string, v ...interface{}) {
	l.buffer.WriteString("PANIC: " + fmt.Sprintf(format, v...) + "\n")
}

func (l *testLogger) String() string {
	return l.buffer.String()
}

func TestLogging(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")

	// Create a test logger to capture log output
	logger := newTestLogger()

	config := &Config{
		Logger:         logger,
		FlushInterval:  time.Minute * 15,
		MaxBufferBytes: 10 * 1024 * 1024,
	}

	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB with logging: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Check that opening logs were generated
	logOutput := logger.String()
	if !strings.Contains(logOutput, "Opening nnut database") {
		t.Errorf("Expected opening log message, got: %s", logOutput)
	}

	// Create a store and perform operations to generate more logs
	userStore, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	user := TestUser{
		UUID:  "test-uuid",
		Name:  "Test User",
		Email: "test@example.com",
		Age:   30,
	}

	// Put operation should generate debug logs
	err = userStore.Put(t.Context(), user)
	if err != nil {
		t.Fatalf("Failed to put user: %v", err)
	}

	// Get operation should generate debug logs
	_, err = userStore.Get(t.Context(), "test-uuid")
	if err != nil {
		t.Fatalf("Failed to get user: %v", err)
	}

	// Check for debug log messages
	logOutput = logger.String()
	if !strings.Contains(logOutput, "Putting record") {
		t.Errorf("Expected put log message, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "Getting record") {
		t.Errorf("Expected get log message, got: %s", logOutput)
	}
}

func TestBatchOperationsLogging(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")

	// Create a test logger to capture log output
	logger := newTestLogger()

	config := &Config{
		Logger:         logger,
		FlushInterval:  time.Minute * 15,
		MaxBufferBytes: 10 * 1024 * 1024,
	}

	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB with logging: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create a store and perform batch operations
	userStore, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	users := []TestUser{
		{UUID: "user1", Name: "User One", Email: "user1@example.com", Age: 25},
		{UUID: "user2", Name: "User Two", Email: "user2@example.com", Age: 30},
		{UUID: "user3", Name: "User Three", Email: "user3@example.com", Age: 35},
	}

	// PutBatch operation should generate debug logs
	err = userStore.PutBatch(t.Context(), users)
	if err != nil {
		t.Fatalf("Failed to put batch: %v", err)
	}

	// GetBatch operation should generate debug logs
	keys := []string{"user1", "user2", "user3"}
	_, err = userStore.GetBatch(t.Context(), keys)
	if err != nil {
		t.Fatalf("Failed to get batch: %v", err)
	}

	// DeleteBatch operation should generate debug logs
	err = userStore.DeleteBatch(t.Context(), keys)
	if err != nil {
		t.Fatalf("Failed to delete batch: %v", err)
	}

	// Check for batch operation log messages
	logOutput := logger.String()
	if !strings.Contains(logOutput, "Putting batch of 3 records") {
		t.Errorf("Expected put batch log message, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "Getting batch of 3 records") {
		t.Errorf("Expected get batch log message, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "Deleting batch of 3 records") {
		t.Errorf("Expected delete batch log message, got: %s", logOutput)
	}
}

func TestFlushLogging(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")

	// Create a test logger to capture log output
	logger := newTestLogger()

	config := &Config{
		Logger:         logger,
		FlushInterval:  time.Minute * 15,
		MaxBufferBytes: 1024, // Small buffer to trigger flush
	}

	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB with logging: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create a store and add many records to trigger flush
	userStore, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add multiple users to trigger buffer flush
	for i := 0; i < 10; i++ {
		user := TestUser{
			UUID:  fmt.Sprintf("user%d", i),
			Name:  fmt.Sprintf("User %d", i),
			Email: fmt.Sprintf("user%d@example.com", i),
			Age:   20 + i,
		}
		err = userStore.Put(t.Context(), user)
		if err != nil {
			t.Fatalf("Failed to put user %d: %v", i, err)
		}
	}

	// Force flush
	db.Flush()

	// Check for flush log messages
	logOutput := logger.String()
	if !strings.Contains(logOutput, "Flushing") {
		t.Errorf("Expected flush log message, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "Successfully flushed") {
		t.Errorf("Expected successful flush log message, got: %s", logOutput)
	}
}

func TestLoggerLevels(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")

	// Create a test logger to capture log output
	logger := newTestLogger()

	config := &Config{
		Logger:         logger,
		FlushInterval:  time.Minute * 15,
		MaxBufferBytes: 10 * 1024 * 1024,
	}

	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB with logging: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create a store
	userStore, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Test error logging by trying to get a non-existent key
	_, err = userStore.Get(t.Context(), "nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent key")
	}

	// Check that different log levels are captured
	logOutput := logger.String()
	if !strings.Contains(logOutput, "DEBUG:") {
		t.Errorf("Expected debug log messages, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "INFO:") {
		t.Errorf("Expected info log messages, got: %s", logOutput)
	}
}

func TestBoltOptions(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")

	config := &Config{
		FlushInterval:  time.Minute * 15,
		MaxBufferBytes: 10 * 1024 * 1024,
		BoltOptions: &bbolt.Options{
			Timeout:   time.Second * 5,
			NoSync:    true,
			MmapFlags: 0,
		},
	}

	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB with bolt options: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Verify that the database was opened successfully with the options
	// The NoSync option should be reflected in the underlying bbolt DB
	if !db.DB.NoSync {
		t.Errorf("Expected NoSync to be true, but it was false")
	}
}
