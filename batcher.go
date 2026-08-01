package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// EntryBatcher 批量缓冲 LogEntry，达到 batchSize 或 flushInterval 时刷新到 DatabaseWriter。
// 写入失败时条目自动重回缓冲区，防止数据丢失。
type EntryBatcher struct {
	db            DatabaseWriter
	batchSize     int
	flushInterval time.Duration
	buffer        []LogEntry
	mu            sync.Mutex
	running       bool
}

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

// Run 启动定时刷新循环，阻塞直到 ctx 取消，退出前最终刷新。
func (b *EntryBatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()
	defer b.flushAll()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.flushIfNeeded()
		}
	}
}

// Add 添加一条 entry，达到 batchSize 时立即刷新。并发安全。
func (b *EntryBatcher) Add(entry LogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.running {
		log.Printf("[EntryBatcher] 丢弃 entry：batcher 未运行")
		return
	}

	b.buffer = append(b.buffer, entry)
	if len(b.buffer) >= b.batchSize {
		b.flushLocked()
	}
}

// AddBatch 批量添加，比多次 Add 更高效。
func (b *EntryBatcher) AddBatch(entries []LogEntry) {
	if len(entries) == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.running {
		log.Printf("[EntryBatcher] 丢弃 %d 条 entries：batcher 未运行", len(entries))
		return
	}

	b.buffer = append(b.buffer, entries...)
	if len(b.buffer) >= b.batchSize {
		b.flushLocked()
	}
}

// Close 停止 batcher，执行最终刷新并关闭后端 writer。
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

// Len 返回当前缓冲的条目数。
func (b *EntryBatcher) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buffer)
}

func (b *EntryBatcher) flushIfNeeded() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buffer) > 0 {
		b.flushLocked()
	}
}

func (b *EntryBatcher) flushAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

// flushLocked 排空缓冲区写入数据库。调用前需持有 b.mu。
// 写入失败时条目重回缓冲区头部。
func (b *EntryBatcher) flushLocked() {
	if len(b.buffer) == 0 {
		return
	}

	entries := b.buffer
	b.buffer = b.buffer[:0]

	if b.db == nil {
		log.Printf("[EntryBatcher] 已刷新 %d 条（无数据库，已丢弃）", len(entries))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := b.db.WriteBatch(ctx, entries); err != nil {
		log.Printf("[EntryBatcher] 刷新 %d 条失败: %v", len(entries), err)
		b.buffer = append(entries, b.buffer...)
		return
	}

	log.Printf("[EntryBatcher] 已刷新 %d 条", len(entries))
}
