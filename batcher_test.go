package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestEntryBatcher_SizeFlush(t *testing.T) {
	var written []LogEntry
	var mu sync.Mutex
	db := &testDB{writeFunc: func(ctx context.Context, entries []LogEntry) error {
		mu.Lock()
		written = append(written, entries...)
		mu.Unlock()
		return nil
	}}

	batcher := NewEntryBatcher(db, 2, 10*time.Second) // flush every 2 entries

	for i := 0; i < 5; i++ {
		batcher.Add(LogEntry{Message: number(i)})
	}

	// Force final flush
	batcher.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(written) != 5 {
		t.Errorf("expected 5 entries written, got %d", len(written))
	}
}

func TestEntryBatcher_TimeFlush(t *testing.T) {
	var written []LogEntry
	var mu sync.Mutex
	db := &testDB{writeFunc: func(ctx context.Context, entries []LogEntry) error {
		mu.Lock()
		written = append(written, entries...)
		mu.Unlock()
		return nil
	}}

	batcher := NewEntryBatcher(db, 100, 50*time.Millisecond) // flush every 50ms

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go batcher.Run(ctx)

	batcher.Add(LogEntry{Message: "entry1"})

	// Wait for time-based flush
	time.Sleep(100 * time.Millisecond)

	cancel()
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(written) < 1 {
		t.Error("expected at least 1 entry flushed by timer")
	}
}

func TestEntryBatcher_AddBatch(t *testing.T) {
	var batchCount int
	db := &testDB{writeFunc: func(ctx context.Context, entries []LogEntry) error {
		batchCount++
		return nil
	}}

	batcher := NewEntryBatcher(db, 10, time.Hour)
	defer batcher.Close()

	// Add 5 entries in one batch - should not trigger flush (batchSize=10)
	batcher.AddBatch([]LogEntry{
		{Message: "1"}, {Message: "2"}, {Message: "3"}, {Message: "4"}, {Message: "5"},
	})

	if batchCount != 0 {
		t.Error("should not flush before reaching batchSize")
	}

	// Add 6 more - now 11 total, should trigger flush
	batcher.AddBatch([]LogEntry{
		{Message: "6"}, {Message: "7"}, {Message: "8"}, {Message: "9"}, {Message: "10"}, {Message: "11"},
	})

	if batchCount != 1 {
		t.Errorf("expected 1 flush after reaching batchSize, got %d", batchCount)
	}
}

func TestEntryBatcher_Len(t *testing.T) {
	db := &testDB{}
	batcher := NewEntryBatcher(db, 100, time.Hour)
	defer batcher.Close()

	if batcher.Len() != 0 {
		t.Errorf("initial len = %d, want 0", batcher.Len())
	}

	batcher.Add(LogEntry{Message: "test"})
	if batcher.Len() != 1 {
		t.Errorf("len = %d, want 1", batcher.Len())
	}
}

func TestEntryBatcher_CloseFlushesRemaining(t *testing.T) {
	var written int
	db := &testDB{writeFunc: func(ctx context.Context, entries []LogEntry) error {
		written += len(entries)
		return nil
	}}

	batcher := NewEntryBatcher(db, 100, time.Hour)

	batcher.Add(LogEntry{Message: "1"})
	batcher.Add(LogEntry{Message: "2"})
	batcher.Add(LogEntry{Message: "3"})

	batcher.Close()

	if written != 3 {
		t.Errorf("expected 3 entries flushed on close, got %d", written)
	}
}

func TestEntryBatcher_RequeueOnFailure(t *testing.T) {
	callCount := 0
	db := &testDB{writeFunc: func(ctx context.Context, entries []LogEntry) error {
		callCount++
		if callCount == 1 {
			return errTest
		}
		return nil
	}}

	batcher := NewEntryBatcher(db, 2, time.Hour)

	batcher.Add(LogEntry{Message: "1"})
	batcher.Add(LogEntry{Message: "2"}) // triggers flush, fails

	// Entries should be re-queued
	if batcher.Len() != 2 {
		t.Errorf("expected entries re-queued after failure, len = %d, want 2", batcher.Len())
	}

	// Close should retry and succeed
	batcher.Close()

	if callCount < 2 {
		t.Errorf("expected at least 2 flush attempts, got %d", callCount)
	}
}

func TestEntryBatcher_AddAfterClose(t *testing.T) {
	db := &testDB{}
	batcher := NewEntryBatcher(db, 10, time.Hour)
	batcher.Close()

	// Should not panic, just logs warning
	batcher.Add(LogEntry{Message: "test"})
	if batcher.Len() != 0 {
		t.Error("should not buffer entries after close")
	}
}

func TestEntryBatcher_ConcurrentAdd(t *testing.T) {
	var written int
	var mu sync.Mutex
	db := &testDB{writeFunc: func(ctx context.Context, entries []LogEntry) error {
		mu.Lock()
		written += len(entries)
		mu.Unlock()
		return nil
	}}

	batcher := NewEntryBatcher(db, 50, time.Hour)

	const goroutines = 20
	const entriesPerRoutine = 25
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < entriesPerRoutine; j++ {
				batcher.Add(LogEntry{Labels: map[string]string{"g": number(id)}, Message: number(j)})
			}
		}(i)
	}

	wg.Wait()
	batcher.Close()

	mu.Lock()
	defer mu.Unlock()
	expected := goroutines * entriesPerRoutine
	if written != expected {
		t.Errorf("expected %d entries, got %d", expected, written)
	}
}

func TestEntryBatcher_ZeroConfigDefaults(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 0, 0)
	if batcher.batchSize != 1 {
		t.Errorf("expected batchSize=1 for zero config, got %d", batcher.batchSize)
	}
	if batcher.flushInterval != 5*time.Second {
		t.Errorf("expected flushInterval=5s for zero config, got %v", batcher.flushInterval)
	}
}

func TestEntryBatcher_NilDB(t *testing.T) {
	batcher := NewEntryBatcher(nil, 10, time.Hour)
	defer batcher.Close()

	// Should not panic or error with nil DB
	batcher.Add(LogEntry{Message: "test1"})
	batcher.Add(LogEntry{Message: "test2"})
	// These won't be flushed since batchSize=10, but Close() will flush
	batcher.Close()
	// No panic means success
}
