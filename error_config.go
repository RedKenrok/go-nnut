package nnut

import "fmt"

// InvalidConfigError indicates invalid configuration parameters.
//
// Example:
//
//	config := &Config{WALFlushSize: -1}
//	err := validateConfig(config)
//	if invalid, ok := err.(InvalidConfigError); ok {
//	    fmt.Printf("Config error: %s", invalid.Field) // Output: WALFlushSize
//	}
type InvalidConfigError struct {
	Field  string
	Value  interface{}
	Reason string
}

func (e InvalidConfigError) Error() string {
	return fmt.Sprintf("invalid config %s=%v: %s", e.Field, e.Value, e.Reason)
}
