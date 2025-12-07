package nnut

import (
	"fmt"
)

// InvalidTypeError indicates that the type does not meet the requirements (e.g., not a struct).
type InvalidTypeError struct {
	Type string
}

func (e InvalidTypeError) Error() string {
	return fmt.Sprintf("invalid type: %s", e.Type)
}

// KeyFieldNotFoundError indicates that no field is tagged with nnut:"key".
type KeyFieldNotFoundError struct{}

func (e KeyFieldNotFoundError) Error() string {
	return "no field tagged with nnut:\"key\""
}

// KeyFieldNotStringError indicates that the key field is not a string.
type KeyFieldNotStringError struct {
	FieldName string
}

func (e KeyFieldNotStringError) Error() string {
	return fmt.Sprintf("key field '%s' must be of type string", e.FieldName)
}

// InvalidKeyError indicates that the provided key is invalid (e.g., empty or too long).
type InvalidKeyError struct {
	Key string
}

func (e InvalidKeyError) Error() string {
	return fmt.Sprintf("invalid key: '%s'", e.Key)
}

// InvalidFieldTypeError indicates a field has an invalid type.
type InvalidFieldTypeError struct {
	FieldName string
	Expected  string
	Actual    string
}

func (e InvalidFieldTypeError) Error() string {
	return fmt.Sprintf("field '%s' has invalid type '%s', expected '%s'", e.FieldName, e.Actual, e.Expected)
}

// IndexFieldTypeError indicates an index field is not a string.
type IndexFieldTypeError struct {
	FieldName string
	Type      string
}

func (e IndexFieldTypeError) Error() string {
	return fmt.Sprintf("index field '%s' must be string, got '%s'", e.FieldName, e.Type)
}

// BucketNameError indicates an invalid bucket name.
type BucketNameError struct {
	BucketName string
	Reason     string
}

func (e BucketNameError) Error() string {
	return fmt.Sprintf("invalid bucket name '%s': %s", e.BucketName, e.Reason)
}

// InvalidConfigError indicates invalid configuration parameters.
type InvalidConfigError struct {
	Field  string
	Value  interface{}
	Reason string
}

func (e InvalidConfigError) Error() string {
	return fmt.Sprintf("invalid config %s=%v: %s", e.Field, e.Value, e.Reason)
}

// BucketNotFoundError indicates that the specified bucket does not exist.
type BucketNotFoundError struct {
	Bucket string
}

func (e BucketNotFoundError) Error() string {
	return fmt.Sprintf("bucket '%s' not found", e.Bucket)
}

// KeyNotFoundError indicates that a requested key was not found in the store.
type KeyNotFoundError struct {
	Bucket string
	Key    string
}

func (e KeyNotFoundError) Error() string {
	return fmt.Sprintf("key '%s' not found in bucket '%s'", e.Key, e.Bucket)
}

// WrappedError wraps an underlying error with additional context.
type WrappedError struct {
	Operation string
	Bucket    string
	Key       string
	Err       error
}

func (e WrappedError) Error() string {
	if e.Key != "" {
		return fmt.Sprintf("%s failed for bucket '%s', key '%s': %v", e.Operation, e.Bucket, e.Key, e.Err)
	}
	return fmt.Sprintf("%s failed for bucket '%s': %v", e.Operation, e.Bucket, e.Err)
}

func (e WrappedError) Unwrap() error {
	return e.Err
}

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
// It indicates that some operations in a batch succeeded while others failed.
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

// InvalidQueryError indicates invalid query parameters.
type InvalidQueryError struct {
	Field  string
	Value  interface{}
	Reason string
}

func (e InvalidQueryError) Error() string {
	return fmt.Sprintf("invalid query %s=%v: %s", e.Field, e.Value, e.Reason)
}
