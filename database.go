package nnut

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

// Config holds configuration options for the database.
type Config struct {
	// FlushInterval specifies how often the WAL is flushed to disk.
	// Default is 15 minutes.
	FlushInterval time.Duration

	// WALPath is the file path for the Write-Ahead Log.
	// If empty, defaults to dbPath + ".wal".
	WALPath string

	// MaxBufferBytes is the maximum size of the in-memory buffer before forcing a flush.
	// Default is 10MB.
	MaxBufferBytes int

	// FlushChannelSize is the size of the flush channel buffer.
	// Default is 10.
	FlushChannelSize int

	// Logger is the logger used for both nnut and underlying bbolt operations.
	// If nil, bbolt's default discard logger is used.
	// This enables integration with bbolt's logging system for consistent logging across the bbolt ecosystem.
	Logger bbolt.Logger

	// BoltOptions contains bbolt-specific options that are passed to the underlying bbolt database.
	// This allows full configuration control over bbolt's behavior including timeouts, sync options, etc.
	BoltOptions *bbolt.Options
}

// DB represents a database instance with WAL support.
// It wraps bbolt.DB and adds Write-Ahead Logging for improved durability.
type DB struct {
	*bbolt.DB
	config *Config
	logger *bbolt.Logger

	walFile               *os.File
	walMutex              sync.Mutex
	operationsBuffer      map[string]operation
	operationsBufferMutex sync.Mutex
	bytesInBuffer         uint64
	currentEpoch          uint64
	currentEpochMutex     sync.Mutex

	indexes      map[string]*bTree // indexKey -> BTree for serialization on flush
	indexesMutex sync.RWMutex

	indexesNeedRebuild map[string]bool // indexKey -> needs rebuild (set during WAL replay)

	flushChannel   chan struct{}
	closeChannel   chan struct{}
	closeWaitGroup sync.WaitGroup
}

type OperationType int

const (
	OperationPut OperationType = iota
	OperationDelete
	OperationIndex
)

type operation struct {
	Bucket []byte
	Key    string
	Value  []byte
	Type   OperationType
	Epoch  uint64
}

type walEntry struct {
	Operation operation
	Checksum  uint32
}

var (
	discardLogger = &bbolt.DefaultLogger{Logger: log.New(io.Discard, "", 0)}
)

// Open opens a database at the given path with default configuration.
// It creates a WAL file at path + ".wal" and uses sensible defaults for buffering.
func Open(path string) (*DB, error) {
	config := &Config{
		FlushInterval:    time.Minute * 15,
		WALPath:          path + ".wal",
		MaxBufferBytes:   10 * 1024 * 1024, // 10MB
		FlushChannelSize: 10,
	}
	return OpenWithConfig(path, config)
}

// validateConfig validates the configuration parameters
func validateConfig(config *Config) error {
	if config == nil {
		return InvalidConfigError{Field: "config", Value: nil, Reason: "cannot be nil"}
	}
	if config.FlushInterval <= 0 {
		return InvalidConfigError{Field: "FlushInterval", Value: config.FlushInterval, Reason: "must be positive"}
	}
	if config.WALPath == "" {
		return InvalidConfigError{Field: "WALPath", Value: config.WALPath, Reason: "cannot be empty"}
	}
	if config.MaxBufferBytes <= 0 {
		return InvalidConfigError{Field: "MaxBufferBytes", Value: config.MaxBufferBytes, Reason: "must be positive"}
	}
	if config.FlushChannelSize < 0 {
		return InvalidConfigError{Field: "FlushChannelSize", Value: config.FlushChannelSize, Reason: "cannot be negative"}
	}
	return nil
}

// OpenWithConfig opens a database at the given path with custom configuration.
// The config parameter allows customization of WAL behavior, buffer sizes, and flush intervals.
func OpenWithConfig(path string, config *Config) (*DB, error) {
	if config != nil && config.WALPath == "" {
		config.WALPath = path + ".wal"
	}
	if config != nil && config.MaxBufferBytes == 0 {
		config.MaxBufferBytes = 10 * 1024 * 1024 // 10MB
	}
	if config != nil && config.FlushChannelSize == 0 {
		config.FlushChannelSize = 10
	}

	if err := validateConfig(config); err != nil {
		return nil, err
	}

	// Create bbolt options from config
	var bboltOptions *bbolt.Options
	if config != nil && config.BoltOptions != nil {
		bboltOptions = config.BoltOptions
	} else {
		bboltOptions = &bbolt.Options{}
	}

	// Set logger for bbolt if provided
	var logger bbolt.Logger
	if config == nil || config.Logger == nil {
		logger = discardLogger
	} else {
		logger = config.Logger
	}
	if bboltOptions.Logger == nil {
		bboltOptions.Logger = logger
	}

	// Log database opening
	logger.Info("Opening nnut database at path: %s", path)

	database, err := bbolt.Open(path, 0600, bboltOptions)
	if err != nil {
		if config != nil {
			logger.Errorf("Failed to open nnut database at path %s: %v", path, err)
		}
		return nil, FileSystemError{Path: path, Operation: "open", Err: err}
	}
	databaseInstance := &DB{
		DB:                 database,
		config:             config,
		logger:             &logger,
		operationsBuffer:   make(map[string]operation),
		currentEpoch:       1,
		indexes:            make(map[string]*bTree),
		indexesNeedRebuild: make(map[string]bool),
		flushChannel:       make(chan struct{}, config.FlushChannelSize),
		closeChannel:       make(chan struct{}),
	}

	// Recover uncommitted operations from previous session to ensure data consistency
	databaseInstance.Logger().Info("Replaying WAL from path: %s", config.WALPath)
	err = databaseInstance.replayWAL()
	if err != nil {
		databaseInstance.Logger().Errorf("Failed to replay WAL from path %s: %v", config.WALPath, err)
		database.Close()
		return nil, err
	}
	databaseInstance.Logger().Info("Successfully replayed WAL from path: %s", config.WALPath)

	// Prepare WAL file for logging new operations to enable crash recovery
	databaseInstance.walFile, err = os.OpenFile(config.WALPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		database.Close()
		return nil, FileSystemError{Path: config.WALPath, Operation: "create", Err: err}
	}

	databaseInstance.closeWaitGroup.Add(1)
	go databaseInstance.flushWAL()
	return databaseInstance, nil
}

// getLatestBufferedOperation checks the buffer for pending changes to a key
func (db *DB) getLatestBufferedOperation(bucket []byte, key string) (operation, bool) {
	db.operationsBufferMutex.Lock()
	defer db.operationsBufferMutex.Unlock()
	op, exists := db.operationsBuffer[bufferKey(bucket, key)]
	return op, exists
}

// getBufferedOperationsForBucket returns all buffered operations for a specific bucket
func (db *DB) getBufferedOperationsForBucket(bucket []byte) []operation {
	db.operationsBufferMutex.Lock()
	defer db.operationsBufferMutex.Unlock()
	var operations []operation
	for _, operation := range db.operationsBuffer {
		if bytes.Equal(operation.Bucket, bucket) {
			operations = append(operations, operation)
		}
	}
	return operations
}

func (db *DB) Logger() bbolt.Logger {
	if db == nil || db.logger == nil {
		return discardLogger
	}
	return *db.logger
}

func (db *DB) replayWAL() error {
	file, err := os.Open(db.config.WALPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No WAL, ok
			return nil
		}
		return FileSystemError{Path: db.config.WALPath, Operation: "open", Err: err}
	}
	defer file.Close()

	decoder := msgpack.GetDecoder()
	defer msgpack.PutDecoder(decoder)
	decoder.Reset(file)
	operationIndex := 0
	for {
		var entry walEntry
		err := decoder.Decode(&entry)
		if err != nil {
			if err == io.EOF {
				break
			}
			// Corrupted WAL file cannot be trusted, discard to avoid applying invalid operations
			os.Remove(db.config.WALPath)
			// Don't return error, as DB is consistent without WAL
			break
		}

		// Verify checksum
		var operationBuffer bytes.Buffer
		operationEncoder := msgpack.NewEncoder(&operationBuffer)
		err = operationEncoder.Encode(entry.Operation)
		if err != nil {
			db.Logger().Errorf("Error re-encoding operation for checksum: %v", err)
			os.Remove(db.config.WALPath)
			break
		}
		encodedOp := operationBuffer.Bytes()
		computedChecksum := crc32.ChecksumIEEE(encodedOp)
		if computedChecksum != entry.Checksum {
			db.Logger().Errorf("WAL checksum mismatch at operation %d", operationIndex)
			os.Remove(db.config.WALPath)
			break
		}

		operation := entry.Operation

		// Mark indexes as needing rebuild if dirty marker found
		if operation.Type == OperationIndex {
			db.indexesNeedRebuild[operation.Key] = true
		}

		// Reapply data operations to restore database state
		if operation.Type == OperationPut || operation.Type == OperationDelete {
			err = db.Update(func(transaction *bbolt.Tx) error {
				bucket, err := transaction.CreateBucketIfNotExists(operation.Bucket)
				if err != nil {
					return WALReplayError{WALPath: db.config.WALPath, OperationIndex: operationIndex, Err: err}
				}
				if operation.Type == OperationPut {
					err = bucket.Put([]byte(operation.Key), operation.Value)
					if err != nil {
						return WALReplayError{WALPath: db.config.WALPath, OperationIndex: operationIndex, Err: err}
					}
				} else if operation.Type == OperationDelete {
					err = bucket.Delete([]byte(operation.Key))
					if err != nil {
						return WALReplayError{WALPath: db.config.WALPath, OperationIndex: operationIndex, Err: err}
					}
				}

				return nil
			})
			if err != nil {
				return WrappedError{Operation: "replay_wal", Err: err}
			}
		}
		operationIndex++
	}

	// WAL is no longer needed after successful replay
	os.Remove(db.config.WALPath)

	return nil
}

func (db *DB) flushWAL() {
	defer db.closeWaitGroup.Done()
	ticker := time.NewTicker(db.config.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			db.Flush()
		case <-db.flushChannel:
			db.Flush()
			ticker.Reset(db.config.FlushInterval)
		case <-db.closeChannel:
			return
		}
	}
}

// Flush forces an immediate flush of the WAL buffer to disk.
// This ensures all pending operations are persisted to the database.
func (db *DB) Flush() {
	db.operationsBufferMutex.Lock()
	db.currentEpochMutex.Lock()
	operations := make([]operation, 0, len(db.operationsBuffer))
	for _, operation := range db.operationsBuffer {
		operation.Epoch = db.currentEpoch
		operations = append(operations, operation)
	}
	db.operationsBuffer = make(map[string]operation)
	db.bytesInBuffer = 0
	db.operationsBufferMutex.Unlock()
	db.currentEpochMutex.Unlock()

	if len(operations) == 0 {
		return
	}

	db.Logger().Infof("Flushing %d operations to database", len(operations))

	err := db.Update(func(transaction *bbolt.Tx) error {
		for _, operation := range operations {
			bucket, err := transaction.CreateBucketIfNotExists(operation.Bucket)
			if err != nil {
				return err
			}
			if operation.Type == OperationPut {
				err = bucket.Put([]byte(operation.Key), operation.Value)
				if err != nil {
					return err
				}
			} else if operation.Type == OperationDelete {
				err = bucket.Delete([]byte(operation.Key))
				if err != nil {
					return err
				}
			} else if operation.Type == OperationIndex {
				// Serialize the current BTree
				db.indexesMutex.RLock()
				btree, exists := db.indexes[operation.Key]
				db.indexesMutex.RUnlock()
				if exists {
					data, err := btree.serialize()
					if err != nil {
						return err
					}
					err = bucket.Put([]byte(operation.Key), data)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		db.Logger().Errorf("Flush error: %v", FlushError{OperationCount: len(operations), Err: err})
		return
	}

	db.Logger().Infof("Successfully flushed %d operations to database", len(operations))

	// Truncate WAL after successful flush
	db.currentEpochMutex.Lock()
	committedEpoch := db.currentEpoch
	db.currentEpoch++
	db.currentEpochMutex.Unlock()
	db.truncateWAL(committedEpoch)
}

func (db *DB) truncateWAL(committedEpoch uint64) {
	db.walMutex.Lock()
	defer db.walMutex.Unlock()

	// Close WAL file
	if err := db.walFile.Close(); err != nil {
		db.Logger().Errorf("Error closing WAL for truncation: %v", err)
		return
	}

	// Read entire WAL
	data, err := os.ReadFile(db.config.WALPath)
	if err != nil {
		db.Logger().Errorf("Error reading WAL for truncation: %v", err)
		// Reopen WAL
		db.walFile, err = os.OpenFile(db.config.WALPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			db.Logger().Errorf("Error reopening WAL: %v", err)
		}
		return
	}

	// Decode and filter operations
	var remainingOperations []operation
	decoder := msgpack.GetDecoder()
	defer msgpack.PutDecoder(decoder)
	decoder.Reset(bytes.NewReader(data))
	for {
		var entry walEntry
		err := decoder.Decode(&entry)
		if err != nil {
			if err == io.EOF {
				break
			}
			db.Logger().Errorf("Error decoding WAL entry: %v", err)
			// On decode error, keep all remaining data
			break
		}
		// Verify checksum
		var operationBuffer bytes.Buffer
		operationEncoder := msgpack.NewEncoder(&operationBuffer)
		err = operationEncoder.Encode(entry.Operation)
		if err != nil {
			db.Logger().Errorf("Error re-encoding operation for checksum: %v", err)
			continue
		}
		encodedOperation := operationBuffer.Bytes()
		computedChecksum := crc32.ChecksumIEEE(encodedOperation)
		if computedChecksum != entry.Checksum {
			db.Logger().Errorf("WAL checksum mismatch during truncation")
			continue
		}
		if entry.Operation.Epoch > committedEpoch {
			remainingOperations = append(remainingOperations, entry.Operation)
		}
	}

	// Encode remaining operations
	var buffer bytes.Buffer
	encoder := msgpack.NewEncoder(&buffer)
	for _, operation := range remainingOperations {
		if err := encoder.Encode(operation); err != nil {
			db.Logger().Errorf("Error encoding remaining operation: %v", err)
			// On error, don't truncate
			db.walFile, err = os.OpenFile(db.config.WALPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				db.Logger().Errorf("Error reopening WAL: %v", err)
			}
			return
		}
	}

	// Write back to WAL
	err = os.WriteFile(db.config.WALPath, buffer.Bytes(), 0644)
	if err != nil {
		db.Logger().Errorf("Error writing truncated WAL: %v", err)
		// Reopen
		db.walFile, err = os.OpenFile(db.config.WALPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			db.Logger().Errorf("Error reopening WAL: %v", err)
		}
		return
	}

	// Reopen WAL for append
	db.walFile, err = os.OpenFile(db.config.WALPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		db.Logger().Errorf("Error reopening WAL after truncation: %v", err)
	}
}

// Close flushes any data from the WAL to the database and closes the database
func (db *DB) Close() error {
	db.Flush()

	// Close channels to stop background goroutines (only if not already closed)
	select {
	case <-db.closeChannel:
		// Already closed
	default:
		close(db.closeChannel)
	}
	db.closeWaitGroup.Wait()

	// Close the underlying bolt database
	return db.DB.Close()
}

// Export creates a backup of the database to the specified destination path.
// It flushes pending operations and backs up the DB file.
// The destination will be a valid nnut database that can be opened with Open or OpenWithConfig.
func (db *DB) Export(destinationPath string) error {
	// Validate destination path
	if _, err := os.Stat(destinationPath); err == nil {
		return FileSystemError{Path: destinationPath, Operation: "export", Err: os.ErrExist}
	}

	// Flush all pending operations to ensure DB is up-to-date
	db.Flush()

	// Create destination file for DB backup
	destinationFile, err := os.Create(destinationPath)
	if err != nil {
		return FileSystemError{Path: destinationPath, Operation: "create", Err: err}
	}
	defer destinationFile.Close()

	// Backup the DB using bbolt's transaction WriteTo for consistency
	err = db.DB.View(func(transaction *bbolt.Tx) error {
		_, err := transaction.WriteTo(destinationFile)
		return err
	})
	if err != nil {
		os.Remove(destinationPath) // Clean up on failure
		return WrappedError{Operation: "backup_db", Err: err}
	}

	return nil
}

// bufferKey generates a unique key for the operations buffer
func bufferKey(bucket []byte, key string) string {
	return string(bucket) + "\x00" + key
}

// writeOperations adds multiple operations to WAL and buffer atomically
func (db *DB) writeOperations(ctx context.Context, operations []operation) error {
	if len(operations) == 0 {
		return nil
	}

	db.currentEpochMutex.Lock()
	currentEpoch := db.currentEpoch
	db.currentEpochMutex.Unlock()
	for i := range operations {
		operations[i].Epoch = currentEpoch
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Add to buffer with deduplication (preserve full data)
	db.operationsBufferMutex.Lock()
	bufferBytes := uint64(0)
	for _, operation := range operations {
		key := bufferKey(operation.Bucket, operation.Key)
		db.operationsBuffer[key] = operation
		bufferBytes += uint64(len(operation.Value))
	}
	db.bytesInBuffer += bufferBytes
	shouldFlush := db.bytesInBuffer >= uint64(db.config.MaxBufferBytes)
	db.operationsBufferMutex.Unlock()

	if shouldFlush {
		select {
		case db.flushChannel <- struct{}{}:
		default:
		}
	}

	// Encode all entries for WAL
	var walBuffer bytes.Buffer
	walEncoder := msgpack.NewEncoder(&walBuffer)
	for _, operation := range operations {
		// For WAL efficiency, omit large index data from serialization
		walOperation := operation
		if walOperation.Type == OperationIndex {
			walOperation.Value = nil // Index data is in buffer, not WAL
		}

		// Encode operation
		var operationBuffer bytes.Buffer
		operationEncoder := msgpack.NewEncoder(&operationBuffer)
		err := operationEncoder.Encode(walOperation)
		if err != nil {
			return WrappedError{Operation: "encode operation batch", Err: err}
		}
		encodedOperator := operationBuffer.Bytes()

		// Compute checksum
		checksum := crc32.ChecksumIEEE(encodedOperator)

		// Create WAL entry
		entry := walEntry{Operation: walOperation, Checksum: checksum}

		// Encode entry
		err = walEncoder.Encode(entry)
		if err != nil {
			return WrappedError{Operation: "encode WAL entry batch", Err: err}
		}
	}
	walBytes := walBuffer.Bytes()

	// Write batch to WAL file
	db.walMutex.Lock()
	_, err := db.walFile.Write(walBytes)
	db.walMutex.Unlock()
	if err != nil {
		return FileSystemError{Path: db.config.WALPath, Operation: "write_batch", Err: err}
	}

	return nil
}
