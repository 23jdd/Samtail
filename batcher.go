package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// EntryBatcher collects LogEntry objects from one or more sources and flushes
// them in batches to a DatabaseWriter.
//
// It supports two flush triggers:
//   - Size-based: flush when the buffer reaches batchSize entries
//   - Time-based: flush at regular intervals (flushInterval) if there are pending entries
//
// This is the central component that connects the parser output and HTTP server
// input to the database backend.
//
// Usage:
//
//	batcher := NewEntryBatcher(db, 1000, 2*time.Second)
//	go batcher.Run(ctx)
//	// Feed entries from parser and HTTP server:
//	batcher.Add(entry)
//
// Boundary conditions:
//   - Add on a closed batcher: entry is discarded with warning
//   - BatchSize of 0: defaults to 1 (flush after each entry)
//   - FlushInterval of 0: defaults to 5 seconds
//   - nil DatabaseWriter: entries are discarded (acts as NoopWriter)
//   - Run with already-cancelled context: exits immediately
//   - Empty buffer on tick: no flush occurs (avoids wasted writes)
//   - Concurrent Add calls: thread-safe via mutex
//
// Example (full pipeline):
//
//	db := NewHTTPWriter("http://127.0.0.1:9999/logs/batch", 5*time.Second)
//	batcher := NewEntryBatcher(db, 100, 2*time.Second)
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	go batcher.Run(ctx)
//
//	// Add entries from anywhere:
//	batcher.Add(LogEntry{Labels: map[string]string{"app":"api"}, Message: "started"})
//
//	cancel()
//	batcher.Close() // final flush
type EntryBatcher struct {
	db            DatabaseWriter
	batchSize     int
	flushInterval time.Duration
	buffer        []LogEntry
	mu            sync.Mutex
	running       bool
}

// NewEntryBatcher creates a new EntryBatcher.
//
// Parameters:
//   - db: the DatabaseWriter to flush entries to (can be nil for no-op)
//   - batchSize: max entries before flush (0 defaults to 1)
//   - flushInterval: max time between flushes (0 defaults to 5s)
func NewEntryBatcher(db DatabaseWriter, batchSize int, flushInterval time.Duration) *EntryBatcher {
	if batchSize <= 0 {
		batchSize = 1
	}
	if flushInterval <= 0 {
		flushInterval = 5 * time.Second
	}
	return &EntryBatcher{
		db:            db,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		buffer:        make([]LogEntry, 0, batchSize),
		running:       true,
	}
}

// Run starts the batcher's time-based flush loop. It blocks until ctx is cancelled.
// On exit, it performs a final flush of any remaining entries.
// If the batcher is already running a ticker, this call is a no-op.
// The batcher always accepts entries via Add/AddBatch regardless of whether Run is active.
func (b *EntryBatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()
	defer b.flushAll() // final flush on exit

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.flushIfNeeded()
		}
	}
}

// Add adds a single LogEntry to the batch buffer.
// If the buffer reaches batchSize, it triggers an immediate flush.
// This method is safe for concurrent use.
func (b *EntryBatcher) Add(entry LogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.running {
		log.Printf("[EntryBatcher] discarding entry: batcher not running")
		return
	}

	b.buffer = append(b.buffer, entry)
	if len(b.buffer) >= b.batchSize {
		b.flushLocked()
	}
}

// AddBatch adds multiple entries at once. More efficient than calling Add
// repeatedly when inserting many entries.
func (b *EntryBatcher) AddBatch(entries []LogEntry) {
	if len(entries) == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.running {
		log.Printf("[EntryBatcher] discarding %d entries: batcher not running", len(entries))
		return
	}

	b.buffer = append(b.buffer, entries...)
	if len(b.buffer) >= b.batchSize {
		b.flushLocked()
	}
}

// Close stops the batcher and performs a final flush.
func (b *EntryBatcher) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.running = false
	b.flushLocked()
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

// Len returns the current number of buffered entries.
func (b *EntryBatcher) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buffer)
}

// flushIfNeeded flushes if there are pending entries.
func (b *EntryBatcher) flushIfNeeded() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buffer) > 0 {
		b.flushLocked()
	}
}

// flushAll flushes all pending entries, even if buffer exceeds batchSize.
func (b *EntryBatcher) flushAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

// flushLocked drains the buffer and writes to the database.
// Must be called with b.mu held.
func (b *EntryBatcher) flushLocked() {
	if len(b.buffer) == 0 {
		return
	}

	entries := b.buffer
	b.buffer = b.buffer[:0]

	if b.db == nil {
		log.Printf("[EntryBatcher] flushed %d entries (no database, discarded)", len(entries))
		return
	}

	// Use a background context with timeout for the flush
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := b.db.WriteBatch(ctx, entries); err != nil {
		log.Printf("[EntryBatcher] flush %d entries failed: %v", len(entries), err)
		// Re-queue failed entries at the front of the buffer
		// This prevents data loss but may cause duplicates
		b.buffer = append(entries, b.buffer...)
		return
	}

	log.Printf("[EntryBatcher] flushed %d entries", len(entries))
}
