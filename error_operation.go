package nnut

import "fmt"

// WALReplayError indicates an error during WAL replay.
type WALReplayError struct {
	WALPath        string
	OperationIndex int
	Err            error
}

func (e WALReplayError) Error() string {
	return fmt.Sprintf("WAL replay failed at operation %d in %s: %v", e.OperationIndex, e.WALPath, e.Err)
}

func (e WALReplayError) Unwrap() error {
	return e.Err
}

// FlushError indicates an error during flush operation.
type FlushError struct {
	OperationCount int
	Err            error
}

func (e FlushError) Error() string {
	return fmt.Sprintf("flush failed for %d operations: %v", e.OperationCount, e.Err)
}

func (e FlushError) Unwrap() error {
	return e.Err
}

// PartialBatchError contains results and errors for batch operations.
type PartialBatchError struct {
	SuccessfulCount int              // number of successful operations
	Failed          map[string]error // key -> error for failed operations
}

func (e PartialBatchError) Error() string {
	return fmt.Sprintf("batch operation partially failed: %d successful, %d failed", e.SuccessfulCount, len(e.Failed))
}

// IndexError indicates an error with index operations.
type IndexError struct {
	IndexName string
	Operation string
	Bucket    string
	Key       string
	Err       error
}

func (e IndexError) Error() string {
	return fmt.Sprintf("index '%s' %s failed for bucket '%s', key '%s': %v", e.IndexName, e.Operation, e.Bucket, e.Key, e.Err)
}

func (e IndexError) Unwrap() error {
	return e.Err
}

// FileSystemError indicates file system related errors.
type FileSystemError struct {
	Path      string
	Operation string
	Err       error
}

func (e FileSystemError) Error() string {
	return fmt.Sprintf("filesystem operation '%s' failed for path '%s': %v", e.Operation, e.Path, e.Err)
}

func (e FileSystemError) Unwrap() error {
	return e.Err
}

// ConcurrentAccessError indicates concurrent access issues.
type ConcurrentAccessError struct {
	Resource string
	Op       string
	Err      error
}

func (e ConcurrentAccessError) Error() string {
	return fmt.Sprintf("concurrent access error on %s during %s: %v", e.Resource, e.Op, e.Err)
}

func (e ConcurrentAccessError) Unwrap() error {
	return e.Err
}
