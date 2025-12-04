package nnut

import "fmt"

// InvalidTypeError indicates that the type does not meet the requirements (e.g., not a struct).
type InvalidTypeError struct {
	Type string
}

func (e InvalidTypeError) Error() string {
	return fmt.Sprintf("invalid type: %s", e.Type)
}

// KeyNotFoundError indicates that a requested key was not found in the store.
//
// Example:
//
//	_, err := store.Get("nonexistent-key")
//	if _, ok := err.(KeyNotFoundError); ok {
//	    // Handle missing key
//	}
type KeyNotFoundError struct {
	Bucket string
	Key    string
}

func (e KeyNotFoundError) Error() string {
	return fmt.Sprintf("key '%s' not found in bucket '%s'", e.Key, e.Bucket)
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

// InvalidKeyError indicates that the provided key is invalid (e.g., empty).
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
