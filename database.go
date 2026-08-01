package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

// DatabaseWriter defines the interface for writing batches of log entries
// to a backend storage system.
//
// Implementations include:
//   - HTTPWriter: sends entries via HTTP POST to a remote server
//   - FileWriter: writes entries to a local file for backup/debugging
//   - MultiWriter: fans out writes to multiple DatabaseWriter backends
//   - NoopWriter: discards all entries (useful for testing)
//
// Usage example:
//
//	db := NewMultiWriter(
//	    NewHTTPWriter("http://127.0.0.1:9999/logs/batch", 10*time.Second),
//	    NewFileWriter("output/backup.log"),
//	)
//	err := db.WriteBatch(ctx, entries)
//
// Boundary conditions:
//   - WriteBatch with nil or empty entries: must not panic, should be a no-op
//   - Concurrent calls to WriteBatch: implementations must document thread safety
//   - Context cancellation: implementations should respect context deadlines
//   - Large batches: implementations should handle batches of any size
//     (consumers may set their own limits)
type DatabaseWriter interface {
	// WriteBatch writes a batch of log entries to the backend.
	// The context can be used for cancellation and timeouts.
	// Returns an error if the write fails.
	WriteBatch(ctx context.Context, entries []LogEntry) error

	// Close releases any resources held by the writer.
	// After Close, WriteBatch should return an error.
	Close() error
}

// ============================================================================
// NoopWriter - discards all entries (useful for testing and benchmarks)
// ============================================================================

// NoopWriter is a DatabaseWriter that discards all entries.
// It is safe for concurrent use and always succeeds.
//
// Usage:
//
//	db := &NoopWriter{}
//	db.WriteBatch(ctx, entries) // always returns nil
type NoopWriter struct{}

func (n *NoopWriter) WriteBatch(_ context.Context, _ []LogEntry) error {
	return nil
}

func (n *NoopWriter) Close() error {
	return nil
}

// ============================================================================
// HTTPWriter - sends entries via HTTP POST
// ============================================================================

// HTTPWriter implements DatabaseWriter by sending batches as JSON
// to a remote HTTP endpoint.
//
// It uses the same JSON format as the server endpoint:
//
//	POST /logs/batch
//	Content-Type: application/json
//	{"entries":[{"labels":{...},"message":"..."},...]}
//
// Features:
//   - Configurable timeout per request
//   - Retries on transient errors (5xx, network errors) up to maxRetries
//   - Thread-safe: uses an internal mutex and a shared http.Client
//
// Boundary conditions:
//   - Empty entries slice: skips the HTTP request (no-op)
//   - Network timeout: returns error after retries exhausted
//   - Non-2xx response (4xx): returns error without retry (client error)
//   - Non-2xx response (5xx): retries up to maxRetries with exponential backoff
//   - Response body >= 10KB: truncated in error messages to prevent log spam
//
// Example:
//
//	writer := NewHTTPWriter("http://127.0.0.1:9999/logs/batch", 5*time.Second)
//	defer writer.Close()
//	err := writer.WriteBatch(ctx, entries)
type HTTPWriter struct {
	url        string
	client     *http.Client
	timeout    time.Duration
	maxRetries int
	mu         sync.Mutex
	closed     bool
}

// NewHTTPWriter creates an HTTPWriter with the given endpoint URL and timeout.
// maxRetries defaults to 3.
func NewHTTPWriter(url string, timeout time.Duration) *HTTPWriter {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &HTTPWriter{
		url:     url,
		timeout: timeout,
		client: &http.Client{
			Timeout: timeout,
		},
		maxRetries: 3,
	}
}

func (h *HTTPWriter) WriteBatch(ctx context.Context, entries []LogEntry) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return fmt.Errorf("HTTPWriter: writer is closed")
	}

	if len(entries) == 0 {
		return nil
	}

	body, err := json.Marshal(BatchRequest{Entries: entries})
	if err != nil {
		return fmt.Errorf("HTTPWriter: marshal batch: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= h.maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 100ms, 200ms, 400ms
			backoff := time.Duration(100*(1<<(attempt-1))) * time.Millisecond
			log.Printf("[HTTPWriter] retry attempt %d/%d after %v", attempt, h.maxRetries, backoff)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("HTTPWriter: create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := h.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("HTTPWriter: post request: %w", err)
			continue
		}

		// Read and discard body to allow connection reuse
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 10*1024))
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Printf("[HTTPWriter] wrote %d entries to %s (status %d)", len(entries), h.url, resp.StatusCode)
			return nil
		}

		// Client errors (4xx) are not retried
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return fmt.Errorf("HTTPWriter: server rejected batch (status %d): %s", resp.StatusCode, string(respBody))
		}

		// Server errors (5xx) are retried
		lastErr = fmt.Errorf("HTTPWriter: server error (status %d): %s", resp.StatusCode, string(respBody))
	}

	return fmt.Errorf("HTTPWriter: failed after %d retries: %w", h.maxRetries+1, lastErr)
}

func (h *HTTPWriter) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed = true
		h.client.CloseIdleConnections()
	}
	return nil
}

// ============================================================================
// FileWriter - writes entries to a local file as JSON lines
// ============================================================================

// FileWriter implements DatabaseWriter by appending JSON-encoded entries
// to a local file. Each line contains one LogEntry as JSON.
//
// Useful for:
//   - Local backup/audit trail
//   - Debugging and development
//   - Offline mode when the database is unreachable
//
// Boundary conditions:
//   - File does not exist: created automatically
//   - Empty entries slice: no-op (no empty lines written)
//   - Directory does not exist: returns error on first WriteBatch
//   - Concurrent writes: protected by internal mutex
//   - File permissions: created with 0644
//
// Example:
//
//	writer, err := NewFileWriter("logs/backup.jsonl")
//	if err != nil { ... }
//	defer writer.Close()
//	writer.WriteBatch(ctx, entries)
type FileWriter struct {
	path   string
	file   *os.File
	mu     sync.Mutex
	closed bool
}

// NewFileWriter creates or opens the file at the given path.
func NewFileWriter(path string) (*FileWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("FileWriter: open %s: %w", path, err)
	}
	return &FileWriter{path: path, file: file}, nil
}

func (f *FileWriter) WriteBatch(_ context.Context, entries []LogEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return fmt.Errorf("FileWriter: writer is closed")
	}

	if len(entries) == 0 {
		return nil
	}

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("FileWriter: marshal entry: %w", err)
		}
		if _, err := f.file.Write(append(data, '\n')); err != nil {
			return fmt.Errorf("FileWriter: write entry: %w", err)
		}
	}

	if err := f.file.Sync(); err != nil {
		return fmt.Errorf("FileWriter: fsync: %w", err)
	}

	log.Printf("[FileWriter] wrote %d entries to %s", len(entries), f.path)
	return nil
}

func (f *FileWriter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		return f.file.Close()
	}
	return nil
}

// ============================================================================
// MultiWriter - fans out writes to multiple DatabaseWriter backends
// ============================================================================

// MultiWriter implements DatabaseWriter by delegating to multiple writers.
// If any writer fails, the error is logged but writing continues to
// remaining writers. The first error encountered is returned.
//
// Boundary conditions:
//   - Nil or empty writers slice: acts as NoopWriter
//   - One writer fails: others continue, first error returned
//   - Close: all writers are closed, errors are collected
//
// Example:
//
//	db := NewMultiWriter(
//	    NewHTTPWriter("http://db.example.com/logs", 5*time.Second),
//	    NewFileWriter("local_backup.log"),
//	)
type MultiWriter struct {
	writers []DatabaseWriter
}

// NewMultiWriter creates a MultiWriter that fans out to all given writers.
func NewMultiWriter(writers ...DatabaseWriter) *MultiWriter {
	return &MultiWriter{writers: writers}
}

func (m *MultiWriter) WriteBatch(ctx context.Context, entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	var firstErr error
	for _, w := range m.writers {
		if err := w.WriteBatch(ctx, entries); err != nil {
			log.Printf("[MultiWriter] writer failed: %v", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (m *MultiWriter) Close() error {
	var firstErr error
	for _, w := range m.writers {
		if err := w.Close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
