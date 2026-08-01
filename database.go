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

// DatabaseWriter 定义了将批量日志条目写入后端存储系统的接口。
//
// 实现包括：
//   - HTTPWriter: 通过 HTTP POST 将条目发送到 SamKv 服务器
//   - FileWriter: 将条目写入本地文件用于备份/调试
//   - MultiWriter: 将写入扇出到多个 DatabaseWriter 后端
//   - NoopWriter: 丢弃所有条目（用于测试）
//
// 用法示例：
//
//	db := NewMultiWriter(
//	    NewHTTPWriter("http://127.0.0.1:6379/logs/batch", 10*time.Second),
//	    NewFileWriter("output/backup.log"),
//	)
//	err := db.WriteBatch(ctx, entries)
//
// 边界条件：
//   - WriteBatch 传入 nil 或空 entries：不得 panic，应为空操作
//   - WriteBatch 并发调用：实现必须记录线程安全性
//   - Context 取消：实现应遵守 context 截止时间
//   - 大批次：实现应能处理任意大小的批次（消费者可自行设置上限）
type DatabaseWriter interface {
	// WriteBatch 将批量日志条目写入后端。
	// ctx 可用于取消和超时控制。
	// 写入失败时返回错误。
	WriteBatch(ctx context.Context, entries []LogEntry) error

	// Close 释放 writer 持有的所有资源。
	// Close 后 WriteBatch 应返回错误。
	Close() error
}

// ============================================================================
// NoopWriter - 丢弃所有条目（用于测试和基准测试）
// ============================================================================

// NoopWriter 是一个丢弃所有条目的 DatabaseWriter。
// 并发安全，始终成功。
//
// 用法：
//
//	db := &NoopWriter{}
//	db.WriteBatch(ctx, entries) // 始终返回 nil
type NoopWriter struct{}

func (n *NoopWriter) WriteBatch(_ context.Context, _ []LogEntry) error {
	return nil
}

func (n *NoopWriter) Close() error {
	return nil
}

// ============================================================================
// HTTPWriter - 通过 HTTP POST 发送条目到 SamKv
// ============================================================================

// HTTPWriter 通过发送 JSON 批次到 SamKv 数据库端点来实现 DatabaseWriter。
//
// 使用 POST /logs/batch 格式：
//
//	POST /logs/batch
//	Content-Type: application/json
//	{"entries":[{"labels":{...},"message":"..."},...]}
//
// SamKv 返回 HTTP 201 Created 和自动分配的序列号数组：
//
//	[1, 2, 3]
//
// 数组中的每个元素对应批次中相应 entry 的序列号。
//
// 特性：
//   - 每个请求可配置超时时间
//   - 对 5xx 和网络错误最多重试 maxRetries 次
//   - 线程安全：使用内部互斥锁和共享 http.Client
//
// 边界条件：
//   - 空 entries 切片：跳过 HTTP 请求（空操作）
//   - 网络超时：全部重试耗尽后返回错误
//   - 非 201 响应（4xx）：返回错误不重试（客户端错误）
//   - 非 201 响应（5xx）：以指数退避重试最多 maxRetries 次
//   - 响应体 >= 10KB：在错误消息中截断，防止日志刷屏
//   - 响应体不是合法 JSON 数组：序列号被忽略，不返回错误
//
// 示例：
//
//	writer := NewHTTPWriter("http://127.0.0.1:6379/logs/batch", 5*time.Second)
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

// NewHTTPWriter 用指定的端点 URL 和超时时间创建 HTTPWriter。
// maxRetries 默认为 3。
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
			// 指数退避：100ms, 200ms, 400ms
			backoff := time.Duration(100*(1<<(attempt-1))) * time.Millisecond
			log.Printf("[HTTPWriter] 重试 %d/%d，等待 %v", attempt, h.maxRetries, backoff)

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

		// 读取响应体以获取序列号或错误信息
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 10*1024))
		resp.Body.Close()

		if resp.StatusCode == http.StatusCreated {
			var seqs BatchResponse
			if err := json.Unmarshal(respBody, &seqs); err != nil {
				log.Printf("[HTTPWriter] 解码序列号失败: %v (body: %s)", err, string(respBody))
			} else if len(seqs) > 0 {
				log.Printf("[HTTPWriter] 已写入 %d 条到 %s (201, seq=%d..%d)", len(entries), h.url, seqs[0], seqs[len(seqs)-1])
			} else {
				log.Printf("[HTTPWriter] 已写入 %d 条到 %s (201)", len(entries), h.url)
			}
			return nil
		}

		// 客户端错误（4xx）不重试
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return fmt.Errorf("HTTPWriter: server rejected batch (status %d): %s", resp.StatusCode, string(respBody))
		}

		// 服务端错误（5xx）重试
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
// FileWriter - 将条目以 JSON 行格式写入本地文件
// ============================================================================

// FileWriter 通过将 JSON 编码的条目追加到本地文件来实现 DatabaseWriter。
// 每行包含一条 JSON 格式的 LogEntry。
//
// 适用场景：
//   - 本地备份/审计跟踪
//   - 调试和开发
//   - 数据库不可达时的离线模式
//
// 边界条件：
//   - 文件不存在：自动创建
//   - 空 entries 切片：空操作（不写入空行）
//   - 目录不存在：首次 WriteBatch 时返回错误
//   - 并发写入：由内部互斥锁保护
//   - 文件权限：创建时使用 0644
//
// 示例：
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

// NewFileWriter 在指定路径创建或打开文件。
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

	log.Printf("[FileWriter] 已写入 %d 条到 %s", len(entries), f.path)
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
// MultiWriter - 将写入扇出到多个 DatabaseWriter 后端
// ============================================================================

// MultiWriter 通过将写入委托给多个 writer 来实现 DatabaseWriter。
// 如果任一 writer 失败，错误会被记录日志，但写入继续到其余 writer。
// 返回遇到的第一个错误。
//
// 边界条件：
//   - nil 或空 writers 切片：行为同 NoopWriter
//   - 一个 writer 失败：其他继续，返回第一个错误
//   - Close：关闭所有 writer，收集所有错误
//
// 示例：
//
//	db := NewMultiWriter(
//	    NewHTTPWriter("http://db.example.com/logs", 5*time.Second),
//	    NewFileWriter("local_backup.log"),
//	)
type MultiWriter struct {
	writers []DatabaseWriter
}

// NewMultiWriter 创建一个扇出到所有给定 writer 的 MultiWriter。
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
			log.Printf("[MultiWriter] writer 失败: %v", err)
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
