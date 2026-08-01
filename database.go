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

// DatabaseWriter 批量写入日志条目的后端接口。
// 实现：HTTPWriter（发送到 SamKv）、FileWriter（本地备份）、MultiWriter（多后端扇出）、NoopWriter（测试用）。
type DatabaseWriter interface {
	// WriteBatch 写入一批条目。ctx 控制超时和取消。空 entries 应为空操作。
	WriteBatch(ctx context.Context, entries []LogEntry) error
	// Close 释放资源。关闭后 WriteBatch 应返回错误。
	Close() error
}

// --- NoopWriter --------------------------------------------------------------

// NoopWriter 丢弃所有条目，用于测试和开发。并发安全，始终成功。
type NoopWriter struct{}

func (n *NoopWriter) WriteBatch(_ context.Context, _ []LogEntry) error { return nil }
func (n *NoopWriter) Close() error                                     { return nil }

// --- HTTPWriter --------------------------------------------------------------

// HTTPWriter 通过 POST /logs/batch 发送 JSON 批次到 SamKv。
//
// 请求格式：{"entries": [{"labels": {...}, "message": "..."}, ...]}
// 成功响应：201 Created，body 为序列号数组 [seq1, seq2, ...]，每个对应一条 entry。
//
// 可靠性：
//   - 5xx / 网络错误：指数退避重试（100ms→200ms→400ms），最多 3 次
//   - 4xx 客户端错误：不重试，直接返回错误
//   - 空批次：跳过 HTTP 请求
type HTTPWriter struct {
	url        string
	client     *http.Client
	timeout    time.Duration
	maxRetries int
	mu         sync.Mutex
	closed     bool
}

// NewHTTPWriter 创建 HTTPWriter。timeout <= 0 时默认 10s，maxRetries 默认 3。
func NewHTTPWriter(url string, timeout time.Duration) *HTTPWriter {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &HTTPWriter{
		url:     url,
		timeout: timeout,
		client:  &http.Client{Timeout: timeout},
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
			backoff := time.Duration(100*(1<<(attempt-1))) * time.Millisecond
			log.Printf("[HTTPWriter] 重试 %d/%d，等待 %v", attempt, h.maxRetries, backoff)
			select {
			case <-ctx.Done():
				return ctx.Err() // 上下文取消时立即退出
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
			continue // 网络错误，重试
		}

		// 读取响应体，限制 10KB 防日志刷屏
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

		// 4xx：客户端错误，不重试
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return fmt.Errorf("HTTPWriter: server rejected batch (status %d): %s", resp.StatusCode, string(respBody))
		}
		// 5xx：服务端错误，重试
		lastErr = fmt.Errorf("HTTPWriter: server error (status %d): %s", resp.StatusCode, string(respBody))
	}

	return fmt.Errorf("HTTPWriter: failed after %d retries: %w", h.maxRetries+1, lastErr)
}

// Close 关闭 HTTPWriter，释放空闲连接。关闭后 WriteBatch 返回错误。
func (h *HTTPWriter) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed = true
		h.client.CloseIdleConnections()
	}
	return nil
}

// --- FileWriter --------------------------------------------------------------

// FileWriter 将 LogEntry 以 JSONL 格式（每行一条 JSON）追加写入本地文件。
// 每次写入后 fsync 保证持久化，用于备份和审计。
//
// 边界条件：
//   - 文件不存在：自动创建（O_CREATE），权限 0644
//   - 目录不存在：返回错误
//   - 空批次：不写入，不做 fsync
//   - 并发写入：互斥锁保护
type FileWriter struct {
	path   string
	file   *os.File
	mu     sync.Mutex
	closed bool
}

// NewFileWriter 打开或创建指定路径的 JSONL 文件。
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

	// 逐条序列化并写入
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("FileWriter: marshal entry: %w", err)
		}
		if _, err := f.file.Write(append(data, '\n')); err != nil {
			return fmt.Errorf("FileWriter: write entry: %w", err)
		}
	}

	// 强制刷盘，保证断电不丢
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

// --- MultiWriter -------------------------------------------------------------

// MultiWriter 将写入扇出到多个 DatabaseWriter 后端。
// 任一后端失败会记录日志，但不影响其余后端继续写入。
// 返回遇到的第一个错误（如果有）。
type MultiWriter struct {
	writers []DatabaseWriter
}

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
