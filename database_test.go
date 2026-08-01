package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNoopWriter(t *testing.T) {
	tests := []struct {
		name    string
		entries []LogEntry
	}{
		{"nil entries", nil},
		{"empty entries", []LogEntry{}},
		{"single entry", []LogEntry{{Labels: map[string]string{"app": "api"}, Message: "test"}}},
		{"multiple entries", []LogEntry{
			{Labels: map[string]string{"app": "api"}, Message: "msg1"},
			{Labels: map[string]string{"app": "db"}, Message: "msg2"},
		}},
	}

	writer := &NoopWriter{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := writer.WriteBatch(context.Background(), tt.entries); err != nil {
				t.Errorf("NoopWriter.WriteBatch should never return error, got: %v", err)
			}
		})
	}

	if err := writer.Close(); err != nil {
		t.Errorf("NoopWriter.Close should never return error, got: %v", err)
	}
}

func TestFileWriter(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_backup.jsonl")

	writer, err := NewFileWriter(filePath)
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}
	defer writer.Close()

	// Test: write empty batch (should be no-op)
	if err := writer.WriteBatch(context.Background(), nil); err != nil {
		t.Errorf("empty batch: %v", err)
	}
	if err := writer.WriteBatch(context.Background(), []LogEntry{}); err != nil {
		t.Errorf("empty slice: %v", err)
	}

	// Test: write entries
	entries := []LogEntry{
		{Labels: map[string]string{"app": "api"}, Message: "started", Timestamp: testTime},
		{Labels: map[string]string{"app": "db", "level": "ERROR"}, Message: "error", Timestamp: testTime},
	}
	if err := writer.WriteBatch(context.Background(), entries); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	// Verify file exists and has content
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("file is empty after write")
	}

	// Test: writer after close should error
	writer.Close()
	if err := writer.WriteBatch(context.Background(), entries); err == nil {
		t.Error("expected error writing to closed FileWriter")
	}
}

func TestFileWriter_CreatesDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	// Path in a non-existent subdirectory
	filePath := filepath.Join(tmpDir, "nonexistent", "backup.jsonl")

	_, err := NewFileWriter(filePath)
	if err == nil {
		t.Error("expected error when directory does not exist")
	}
}

func TestMultiWriter(t *testing.T) {
	// Test empty writers
	t.Run("empty writers", func(t *testing.T) {
		mw := NewMultiWriter()
		if err := mw.WriteBatch(context.Background(), []LogEntry{{Message: "test"}}); err != nil {
			t.Errorf("empty MultiWriter should not error: %v", err)
		}
		if err := mw.Close(); err != nil {
			t.Errorf("empty MultiWriter Close: %v", err)
		}
	})

	// Test multiple writers
	t.Run("multiple writers", func(t *testing.T) {
		tmpDir := t.TempDir()
		f1 := filepath.Join(tmpDir, "f1.jsonl")
		f2 := filepath.Join(tmpDir, "f2.jsonl")

		w1, _ := NewFileWriter(f1)
		w2, _ := NewFileWriter(f2)
		defer w1.Close()
		defer w2.Close()

		mw := NewMultiWriter(w1, w2)
		err := mw.WriteBatch(context.Background(), []LogEntry{{Message: "shared"}})
		if err != nil {
			t.Fatalf("MultiWriter.WriteBatch: %v", err)
		}

		// Both files should have content
		for _, path := range []string{f1, f2} {
			info, err := os.Stat(path)
			if err != nil {
				t.Errorf("stat %s: %v", path, err)
				continue
			}
			if info.Size() == 0 {
				t.Errorf("%s is empty", path)
			}
		}
	})

	// Test one writer fails, other continues
	t.Run("partial failure", func(t *testing.T) {
		tmpDir := t.TempDir()
		f1 := filepath.Join(tmpDir, "good.jsonl")
		w1, _ := NewFileWriter(f1)
		w1.Close() // close it so writes fail

		f2 := filepath.Join(tmpDir, "also_good.jsonl")
		w2, _ := NewFileWriter(f2)
		defer w2.Close()

		mw := NewMultiWriter(w1, w2)
		err := mw.WriteBatch(context.Background(), []LogEntry{{Message: "test"}})
		// Should return an error (from w1), but w2 should still have data
		if err == nil {
			t.Error("expected error from closed writer")
		}

		info, _ := os.Stat(f2)
		if info.Size() == 0 {
			t.Error("w2 should have data despite w1 failure")
		}
	})
}

func TestFileWriter_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "concurrent.jsonl")
	writer, err := NewFileWriter(filePath)
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}
	defer writer.Close()

	const goroutines = 10
	const entriesPerRoutine = 100
	errChan := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			for j := 0; j < entriesPerRoutine; j++ {
				errChan <- writer.WriteBatch(context.Background(), []LogEntry{
					{Labels: map[string]string{"goroutine": fmt.Sprintf("%d", id), "seq": fmt.Sprintf("%d", j)}, Message: "msg"},
				})
			}
		}(i)
	}

	for i := 0; i < goroutines*entriesPerRoutine; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("concurrent write error: %v", err)
		}
	}
}

func TestMultiWriter_CloseAll(t *testing.T) {
	w1 := &NoopWriter{}
	w2 := &NoopWriter{}
	mw := NewMultiWriter(w1, w2)

	if err := mw.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// HTTPWriter tests use httptest; see httpdb_test.go
func TestHTTPWriter_NewHTTPWriterDefaultTimeout(t *testing.T) {
	w := NewHTTPWriter("http://localhost/logs", 0)
	if w.timeout != 10*time.Second {
		t.Errorf("expected default timeout 10s, got %v", w.timeout)
	}
}

func TestHTTPWriter_EmptyEntries(t *testing.T) {
	w := NewHTTPWriter("http://localhost/logs", time.Second)
	if err := w.WriteBatch(context.Background(), nil); err != nil {
		t.Errorf("nil entries should not error: %v", err)
	}
	if err := w.WriteBatch(context.Background(), []LogEntry{}); err != nil {
		t.Errorf("empty entries should not error: %v", err)
	}
	w.Close()
	if err := w.WriteBatch(context.Background(), []LogEntry{{Message: "test"}}); err == nil {
		t.Error("expected error writing to closed HTTPWriter")
	}
}
