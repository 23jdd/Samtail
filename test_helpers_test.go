package main

import (
	"context"
	"fmt"
	"time"
)

var testTime = time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

var errTest = fmt.Errorf("test error")

type testDB struct {
	writeFunc func(ctx context.Context, entries []LogEntry) error
	closeFunc func() error
}

func (t *testDB) WriteBatch(ctx context.Context, entries []LogEntry) error {
	if t.writeFunc != nil {
		return t.writeFunc(ctx, entries)
	}
	return nil
}

func (t *testDB) Close() error {
	if t.closeFunc != nil {
		return t.closeFunc()
	}
	return nil
}

func number(n int) string {
	return fmt.Sprintf("%d", n)
}
