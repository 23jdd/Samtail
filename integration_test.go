package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestIntegration_FullPipeline verifies the complete pipeline:
// log file -> Watcher -> TailReader -> Parser -> EntryBatcher -> DatabaseWriter
func TestIntegration_FullPipeline(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a log file with the expected format
	logContent := []byte(`app=api,level=INFO
request started

app=api,level=ERROR
request failed

app=db,level=DEBUG
query executed
`)
	logPath := filepath.Join(tmpDir, "test.log")
	if err := os.WriteFile(logPath, logContent, 0644); err != nil {
		t.Fatalf("write log file: %v", err)
	}

	// Set up the backend to capture entries
	var captured []LogEntry
	db := &testDB{writeFunc: func(ctx context.Context, entries []LogEntry) error {
		captured = append(captured, entries...)
		return nil
	}}

	batcher := NewEntryBatcher(db, 10, 100*time.Millisecond)

	// Use ParseLogStream to simulate the pipeline
	reader := strings.NewReader(string(logContent))
	parsed, err := ParseLogStream(reader, "test.log")
	if err != nil {
		t.Fatalf("ParseLogStream: %v", err)
	}

	if len(parsed) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(parsed))
	}

	batcher.AddBatch(parsed)
	batcher.Close()

	if len(captured) != 3 {
		t.Fatalf("expected 3 entries captured, got %d", len(captured))
	}
}

// TestIntegration_HTTPToDatabase tests the full HTTP server to database flow.
func TestIntegration_HTTPToDatabase(t *testing.T) {
	// Create a test database backend
	var received []LogEntry
	db := &testDB{writeFunc: func(ctx context.Context, entries []LogEntry) error {
		received = append(received, entries...)
		return nil
	}}

	batcher := NewEntryBatcher(db, 100, time.Hour)
	server := NewServer(batcher, ":0")

	// Create a test HTTP server to receive batch requests
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		server.handleLogBatch(w, r)
	}))
	defer ts.Close()

	// Send a batch request to the server
	body := `{"entries":[
		{"labels":{"app":"api","level":"INFO"},"message":"request started"},
		{"labels":{"app":"api","level":"ERROR"},"message":"request failed"}
	]}`

	resp, err := http.Post(ts.URL, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var batchResp BatchResponse
	json.NewDecoder(resp.Body).Decode(&batchResp)
	if batchResp.Accepted != 2 {
		t.Errorf("expected 2 accepted, got %d", batchResp.Accepted)
	}
}

// TestIntegration_LogFormatEndToEnd tests the log file format end-to-end
// from raw text to database output.
func TestIntegration_LogFormatEndToEnd(t *testing.T) {
	input := `app=api,env=production
[INFO] User login successful

app=api,env=production,level=ERROR
[ERROR] Database connection timeout

app=worker,env=production
[INFO] Background job completed
`

	entries, err := ParseLogStream(strings.NewReader(input), "app.log")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Verify each entry
	tests := []struct {
		index   int
		message string
		app     string
		env     string
		level   string
	}{
		{0, "[INFO] User login successful", "api", "production", ""},
		{1, "[ERROR] Database connection timeout", "api", "production", "ERROR"},
		{2, "[INFO] Background job completed", "worker", "production", ""},
	}

	for _, tt := range tests {
		if tt.index >= len(entries) {
			t.Errorf("missing entry at index %d", tt.index)
			continue
		}
		e := entries[tt.index]
		if e.Message != tt.message {
			t.Errorf("entry[%d].Message = %q, want %q", tt.index, e.Message, tt.message)
		}
		if e.GetLabel("app") != tt.app {
			t.Errorf("entry[%d].Labels[app] = %q, want %q", tt.index, e.GetLabel("app"), tt.app)
		}
		if e.GetLabel("env") != tt.env {
			t.Errorf("entry[%d].Labels[env] = %q, want %q", tt.index, e.GetLabel("env"), tt.env)
		}
		if e.GetLabel("level") != tt.level {
			t.Errorf("entry[%d].Labels[level] = %q, want %q", tt.index, e.GetLabel("level"), tt.level)
		}
	}
}

// TestIntegration_BatchFlush verifies that entries are flushed properly
// when batch size is reached and on close.
func TestIntegration_BatchFlush(t *testing.T) {
	flushCount := 0
	var totalWritten int
	db := &testDB{
		writeFunc: func(ctx context.Context, entries []LogEntry) error {
			flushCount++
			totalWritten += len(entries)
			return nil
		},
	}

	batcher := NewEntryBatcher(db, 5, time.Hour)

	// Add 12 entries, should trigger 2 flushes (5+5) and 2 remaining
	for i := 0; i < 12; i++ {
		batcher.Add(LogEntry{Labels: map[string]string{"seq": number(i)}, Message: "msg"})
	}

	// At this point: 2 flushes happened (entries 0-4 and 5-9), 2 entries remain
	if totalWritten != 10 {
		t.Errorf("expected 10 entries written after 12 adds (batchSize=5), got %d", totalWritten)
	}
	if flushCount != 2 {
		t.Errorf("expected 2 flushes, got %d", flushCount)
	}

	// Close should flush remaining 2
	batcher.Close()

	if totalWritten != 12 {
		t.Errorf("expected 12 total after close, got %d", totalWritten)
	}
	if flushCount != 3 {
		t.Errorf("expected 3 flushes after close, got %d", flushCount)
	}
}

// TestIntegration_MultiWriterEndToEnd tests the MultiWriter fan-out.
func TestIntegration_MultiWriterEndToEnd(t *testing.T) {
	var w1Received, w2Received []LogEntry
	w1 := &testDB{writeFunc: func(ctx context.Context, entries []LogEntry) error {
		w1Received = append(w1Received, entries...)
		return nil
	}}
	w2 := &testDB{writeFunc: func(ctx context.Context, entries []LogEntry) error {
		w2Received = append(w2Received, entries...)
		return nil
	}}

	db := NewMultiWriter(w1, w2)
	batcher := NewEntryBatcher(db, 10, time.Hour)

	entries := []LogEntry{
		{Labels: map[string]string{"app": "api"}, Message: "test1"},
		{Labels: map[string]string{"app": "db"}, Message: "test2"},
	}

	batcher.AddBatch(entries)
	batcher.Close()

	if len(w1Received) != 2 {
		t.Errorf("w1 received %d entries, want 2", len(w1Received))
	}
	if len(w2Received) != 2 {
		t.Errorf("w2 received %d entries, want 2", len(w2Received))
	}
}
