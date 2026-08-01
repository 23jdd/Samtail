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
type DatabaseWriter interface {
	WriteBatch(ctx context.Context, entries []LogEntry) error
	Close() error
}

// NoopWriter 丢弃所有条目，用于测试。
type NoopWriter struct{}

func (n *NoopWriter) WriteBatch(_ context.Context, _ []LogEntry) error { return nil }
func (n *NoopWriter) Close() error                                     { return nil }

// HTTPWriter 通过 POST /logs/batch 发送 JSON 到 SamKv。
// SamKv 返回 201 Created 和序列号数组 [seq1, seq2, ...]。
// 5xx/网络错误重试最多 3 次（指数退避），4xx 不重试。
type HTTPWriter struct {
	url        string
	client     *http.Client
	timeout    time.Duration
	maxRetries int
	mu         sync.Mutex
	closed     bool
}

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

		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return fmt.Errorf("HTTPWriter: server rejected batch (status %d): %s", resp.StatusCode, string(respBody))
		}
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

// FileWriter 将 JSON 编码的条目每行一条追加写入本地文件，用于备份。
type FileWriter struct {
	path   string
	file   *os.File
	mu     sync.Mutex
	closed bool
}

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

// MultiWriter 将写入扇出到多个后端，任一失败不影响其余，返回第一个错误。
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
