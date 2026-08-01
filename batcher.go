package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// EntryBatcher 从多个来源收集 LogEntry 对象，并批量刷新到 DatabaseWriter。
//
// 支持两种刷新触发条件：
//   - 按数量：缓冲区达到 batchSize 时刷新
//   - 按时间：到达 flushInterval 间隔时刷新（如果有待定条目）
//
// 这是连接解析器输出到数据库后端的核心组件。
//
// 用法：
//
//	batcher := NewEntryBatcher(db, 1000, 2*time.Second)
//	go batcher.Run(ctx)
//	// 从解析器投喂 entries：
//	batcher.Add(entry)
//
// 边界条件：
//   - 在已关闭的 batcher 上调用 Add：条目被丢弃并记录警告
//   - BatchSize 为 0：默认为 1（每条即刷新）
//   - FlushInterval 为 0：默认为 5 秒
//   - nil DatabaseWriter：条目被丢弃（行为同 NoopWriter）
//   - 在已取消的 context 上调用 Run：立即退出
//   - 定时器触发时缓冲区为空：不执行刷新（避免无效写入）
//   - 并发的 Add 调用：通过互斥锁保证线程安全
//
// 示例（完整管道）：
//
//	db := NewHTTPWriter("http://127.0.0.1:6379/logs/batch", 5*time.Second)
//	batcher := NewEntryBatcher(db, 100, 2*time.Second)
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	go batcher.Run(ctx)
//
//	// 从任意来源添加 entry：
//	batcher.Add(LogEntry{Labels: map[string]string{"app":"api"}, Message: "started"})
//
//	cancel()
//	batcher.Close() // 最终刷新
type EntryBatcher struct {
	db            DatabaseWriter
	batchSize     int
	flushInterval time.Duration
	buffer        []LogEntry
	mu            sync.Mutex
	running       bool
}

// NewEntryBatcher 创建一个新的 EntryBatcher。
//
// 参数：
//   - db: 刷新条目目标 DatabaseWriter（可为 nil，此时为空操作）
//   - batchSize: 触发刷新的最大条目数（0 默认为 1）
//   - flushInterval: 两次刷新的最大时间间隔（0 默认为 5s）
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

// Run 启动 batcher 的定时刷新循环，阻塞直到 ctx 被取消。
// 退出时会执行一次剩余的最终刷新。
// batcher 始终可以通过 Add/AddBatch 接受条目，无论 Run 是否在运行。
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

// Add 向批次缓冲区中添加一条 LogEntry。
// 如果缓冲区达到 batchSize，触发立即刷新。
// 本方法并发安全。
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

// AddBatch 一次性添加多条 entries，比多次调用 Add 更高效。
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

// Close 停止 batcher 并执行最终刷新。
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

// Len 返回当前缓冲区中的条目数。
func (b *EntryBatcher) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buffer)
}

// flushIfNeeded 在有待定条目时执行刷新。
func (b *EntryBatcher) flushIfNeeded() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buffer) > 0 {
		b.flushLocked()
	}
}

// flushAll 刷新所有待定条目，即使缓冲区超过 batchSize。
func (b *EntryBatcher) flushAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

// flushLocked 排空缓冲区并写入数据库。
// 调用前必须持有 b.mu 锁。
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

	// 使用带超时的后台 context 执行刷新
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := b.db.WriteBatch(ctx, entries); err != nil {
		log.Printf("[EntryBatcher] 刷新 %d 条失败: %v", len(entries), err)
		// 将失败条目重新放回缓冲区头部
		// 防止数据丢失，但可能导致重复
		b.buffer = append(entries, b.buffer...)
		return
	}

	log.Printf("[EntryBatcher] 已刷新 %d 条", len(entries))
}
