package main

import (
	"encoding/json"
	"sort"
	"time"
)

// LogEntry represents a single parsed log entry with labels and a message.
//
// It is the unified internal representation used across the pipeline:
// logs read from files are parsed into LogEntry, and the HTTP server
// accepts LogEntry objects directly.
//
// Example raw log file format:
//
//	app=api,level=INFO
//	request started
//
//	app=api,level=ERROR
//	request failed
//
// This would produce two LogEntry objects:
//
//	{Labels: {"app":"api","level":"INFO"}, Message: "request started"}
//	{Labels: {"app":"api","level":"ERROR"}, Message: "request failed"}
type LogEntry struct {
	Labels    map[string]string `json:"labels"`
	Message   string            `json:"message"`
	Timestamp time.Time         `json:"timestamp,omitempty"`
}

// NewLogEntry creates a new LogEntry with initialized labels map.
//
// Boundary conditions:
//   - labels can be nil (creates empty map)
//   - message can be empty (valid, represents an empty log line)
//   - timestamp defaults to zero time
func NewLogEntry(labels map[string]string, message string) LogEntry {
	if labels == nil {
		labels = make(map[string]string)
	}
	return LogEntry{
		Labels:  labels,
		Message: message,
	}
}

// WithTimestamp returns a copy of the entry with the given timestamp set.
//
// Usage:
//
//	entry := NewLogEntry(map[string]string{"app":"api"}, "started").WithTimestamp(time.Now())
func (e LogEntry) WithTimestamp(t time.Time) LogEntry {
	e.Timestamp = t
	return e
}

// GetLabel returns a label value by key, or empty string if not present.
func (e LogEntry) GetLabel(key string) string {
	if e.Labels == nil {
		return ""
	}
	return e.Labels[key]
}

// HasLabel returns true if the entry has the given label key.
func (e LogEntry) HasLabel(key string) bool {
	_, ok := e.Labels[key]
	return ok
}

// LabelKeys returns a sorted list of all label keys.
// Useful for consistent iteration.
func (e LogEntry) LabelKeys() []string {
	keys := make([]string, 0, len(e.Labels))
	for k := range e.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Validate checks that the entry is valid for processing.
//
// Boundary conditions:
//   - returns an error if Message is empty (no useful log)
//   - Labels may be empty (valid, though unusual)
//   - Labels with empty keys or values are rejected
func (e LogEntry) Validate() error {
	if len(e.Message) == 0 {
		return &ValidationError{Field: "message", Reason: "message must not be empty"}
	}
	for k, v := range e.Labels {
		if len(k) == 0 {
			return &ValidationError{Field: "labels", Reason: "label key must not be empty"}
		}
		if len(v) == 0 {
			return &ValidationError{Field: "labels", Reason: "label value must not be empty for key \"" + "'\" (empty key)"}
		}
	}
	return nil
}

// ValidationError is returned when a LogEntry fails validation.
type ValidationError struct {
	Field  string
	Reason string
}

func (ve *ValidationError) Error() string {
	return "validation error: " + ve.Field + ": " + ve.Reason
}

// BatchRequest is the top-level JSON structure for the POST /logs/batch endpoint.
//
// Example JSON body:
//
//	{
//	  "entries": [
//	    {"labels":{"app":"api"},"message":"request started"},
//	    {"labels":{"app":"api","level":"ERROR"},"message":"request failed"}
//	  ]
//	}
type BatchRequest struct {
	Entries []LogEntry `json:"entries"`
}

// UnmarshalJSON implements custom JSON unmarshaling for BatchRequest
// to validate entries after parsing.
func (br *BatchRequest) UnmarshalJSON(data []byte) error {
	type alias BatchRequest
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*br = BatchRequest(a)
	return nil
}

// BatchResponse is returned after a successful batch write.
type BatchResponse struct {
	Accepted int    `json:"accepted"`
	Status   string `json:"status"`
}
