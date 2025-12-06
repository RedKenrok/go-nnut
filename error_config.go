package nnut

import "fmt"

// InvalidConfigError indicates invalid configuration parameters.
//
// Example:
//
//	config := &Config{MaxBufferBytes: -1}
//	err := validateConfig(config)
//	if invalid, ok := err.(InvalidConfigError); ok {
//	    fmt.Printf("Config error: %s", invalid.Field) // Output: MaxBufferBytes
//	}
type InvalidConfigError struct {
	Field  string
	Value  interface{}
	Reason string
}

func (e InvalidConfigError) Error() string {
	return fmt.Sprintf("invalid config %s=%v: %s", e.Field, e.Value, e.Reason)
}
