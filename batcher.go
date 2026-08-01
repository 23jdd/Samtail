package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// EntryBatcher 批量缓冲 LogEntry，达到 batchSize 或 flushInterval 时刷新到 DatabaseWriter。
//
// 两种刷新策略（满足任一即触发）：
//   - 按数量：缓冲区达到 batchSize 条时立即刷新
//   - 按时间：每隔 flushInterval 检查并刷新非空缓冲区
//
// 可靠性保证：
//   - 写入失败时条目自动重回缓冲区头部，防止数据丢失（可能产生重复）
//   - 关闭时（Close）执行最终刷新
//   - 并发安全的 Add/AddBatch
type EntryBatcher struct {
	db            DatabaseWriter
	batchSize     int
	flushInterval time.Duration
	buffer        []LogEntry // 未刷新条目缓冲区
	mu            sync.Mutex
	running       bool // false 时拒绝新条目（已关闭）
}

// NewEntryBatcher 创建 EntryBatcher。
// batchSize <= 0 默认为 1，flushInterval <= 0 默认为 5s。
// db 为 nil 时条目被静默丢弃。
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
		running:       true, // 创建即可接受条目
	}
}

// Run 启动定时刷新循环。阻塞直到 ctx 取消，退出前执行最终刷新。
// 无论 Run 是否在运行，Add/AddBatch 都可以正常接受条目。
func (b *EntryBatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()
	defer b.flushAll() // 退出前最终刷新

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.flushIfNeeded()
		}
	}
}

// Add 添加一条 entry。达到 batchSize 时立即触发刷新。并发安全。
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

// AddBatch 批量添加 entries，比多次 Add 更高效。
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

// Close 停止 batcher，拒绝新条目，执行最终刷新并关闭后端 writer。
func (b *EntryBatcher) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.running = false
	b.flushLocked() // 排空剩余条目
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

// Len 返回当前缓冲区中待刷新的条目数。
func (b *EntryBatcher) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buffer)
}

// flushIfNeeded 缓冲区非空时执行刷新（用于定时触发）。
func (b *EntryBatcher) flushIfNeeded() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buffer) > 0 {
		b.flushLocked()
	}
}

// flushAll 无条件刷新（用于关闭和 Run 退出时）。
func (b *EntryBatcher) flushAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

// flushLocked 排空缓冲区写入数据库。调用前必须持有 b.mu。
// 写入失败时条目重回缓冲区头部，防止数据丢失。
func (b *EntryBatcher) flushLocked() {
	if len(b.buffer) == 0 {
		return
	}

	entries := b.buffer
	b.buffer = b.buffer[:0] // 清空缓冲区

	if b.db == nil {
		log.Printf("[EntryBatcher] 已刷新 %d 条（无数据库，已丢弃）", len(entries))
		return
	}

	// 使用独立的 context，超时 30s，避免刷新被外部取消影响
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := b.db.WriteBatch(ctx, entries); err != nil {
		log.Printf("[EntryBatcher] 刷新 %d 条失败: %v", len(entries), err)
		// 失败条目重回缓冲区头部，下次刷新重试
		b.buffer = append(entries, b.buffer...)
		return
	}

	log.Printf("[EntryBatcher] 已刷新 %d 条", len(entries))
}
