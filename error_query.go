package nnut

import "fmt"

// InvalidQueryError indicates invalid query parameters.
type InvalidQueryError struct {
	Field  string
	Value  interface{}
	Reason string
}

func (e InvalidQueryError) Error() string {
	return fmt.Sprintf("invalid query %s=%v: %s", e.Field, e.Value, e.Reason)
}
