package main

// This file contains example usage of the samtail log collection system.
// These examples demonstrate common patterns for collecting, batching,
// and sending log entries to a database backend.

import (
	"context"
	"strings"
	"time"
)

// Example_parseLogFile demonstrates how to parse a log file in the
// label=value format into structured LogEntry objects.
//
// Expected log format:
//
//	app=api,level=INFO
//	request started
//
//	app=api,level=ERROR
//	request failed
func Example_parseLogFile() {
	input := `app=api,level=INFO
request started

app=api,level=ERROR
request failed
`
	entries, err := ParseLogStream(strings.NewReader(input), "api.log")
	if err != nil {
		panic(err)
	}
	// entries contains 2 LogEntry objects:
	// {Labels: {"app":"api", "level":"INFO"}, Message: "request started"}
	// {Labels: {"app":"api", "level":"ERROR"}, Message: "request failed"}
	_ = entries
}

// Example_batchWriter demonstrates using EntryBatcher to buffer entries
// and flush them to a DatabaseWriter when the batch size is reached.
//
// Boundary conditions handled:
//   - If batchSize is 0, defaults to 1 (flush after each entry)
//   - If flushInterval is 0, defaults to 5 seconds
//   - If db is nil, entries are silently discarded
func Example_batchWriter() {
	db := &NoopWriter{} // Replace with real implementation
	batcher := NewEntryBatcher(db, 1000, 2*time.Second)

	// Add entries from anywhere (parser, HTTP server, etc.)
	entry := NewLogEntry(map[string]string{"app": "api"}, "request started")
	batcher.Add(entry)

	// When done, close to flush remaining entries
	_ = batcher.Close()
}

// Example_httpWriter demonstrates sending log entries to a SamKv database
// via HTTPWriter.
//
// SamKv responds with HTTP 201 Created and an auto-assigned sequence number:
//
//	{"sequence": 42}
//
// The sequence is logged for observability.
func Example_httpWriter() {
	writer := NewHTTPWriter("http://127.0.0.1:6379/logs/batch", 10*time.Second)
	defer writer.Close()

	ctx := context.Background()
	entries := []LogEntry{
		{Labels: map[string]string{"app": "api"}, Message: "request started"},
		{Labels: map[string]string{"app": "api", "level": "ERROR"}, Message: "request failed"},
	}
	_ = writer.WriteBatch(ctx, entries)
}

// Example_multiWriter demonstrates sending logs to multiple backends
// simultaneously (e.g. remote database + local file backup).
//
// If one writer fails, the error is logged but other writers continue.
func Example_multiWriter() {
	// Local backup for audit trail
	backupWriter, _ := NewFileWriter("output/backup.jsonl")
	defer backupWriter.Close()

	// Remote database
	httpWriter := NewHTTPWriter("http://db.example.com/logs/batch", 10*time.Second)
	defer httpWriter.Close()

	// Fan-out to both
	db := NewMultiWriter(backupWriter, httpWriter)

	ctx := context.Background()
	entries := []LogEntry{
		{Labels: map[string]string{"app": "api"}, Message: "started"},
	}
	_ = db.WriteBatch(ctx, entries)
}

// Example_fullPipeline demonstrates the complete pipeline:
//
//	log file → TailReader → Parser → EntryBatcher → DatabaseWriter
//
// The parseLabelLine function is exposed for testing and custom use.
func Example_fullPipeline() {
	// Create channels
	lineChan := make(chan LogLine, 100)
	entryChan := make(chan LogEntry, 100)

	// Create components
	parser := NewParser(lineChan, entryChan)
	db := &NoopWriter{}
	batcher := NewEntryBatcher(db, 10, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start pipeline
	go parser.Run(ctx)
	go batcher.Run(ctx)

	// Feed raw lines (simulating TailReader output)
	lineChan <- LogLine{FilePath: "app.log", Content: "app=api,level=INFO", Timestamp: time.Now()}
	lineChan <- LogLine{FilePath: "app.log", Content: "request started", Timestamp: time.Now()}

	// Read parsed entries from entryChan
	select {
	case entry := <-entryChan:
		// entry.Labels = {"app":"api","level":"INFO"}
		// entry.Message = "request started"
		batcher.Add(entry)
	case <-time.After(100 * time.Millisecond):
		// No entry received
	}

	cancel()
	batcher.Close()
}

// Example_entryValidation demonstrates the validation boundary conditions
// for LogEntry objects.
func Example_entryValidation() {
	// Valid entry
	valid := LogEntry{Labels: map[string]string{"app": "api"}, Message: "started"}
	if err := valid.Validate(); err != nil {
		// err is nil
		_ = err
	}

	// Empty message is invalid
	invalid := LogEntry{Labels: map[string]string{"app": "api"}, Message: ""}
	if err := invalid.Validate(); err != nil {
		// err.Error() == "validation error: message: message must not be empty"
		_ = err
	}

	// Empty label key is invalid
	invalidKey := LogEntry{Labels: map[string]string{"": "value"}, Message: "msg"}
	if err := invalidKey.Validate(); err != nil {
		// err.Error() == "validation error: labels: label key must not be empty"
		_ = err
	}
}
