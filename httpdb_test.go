package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPWriter_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type: application/json, got %s", r.Header.Get("Content-Type"))
		}

		var req BatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if len(req.Entries) != 2 {
			t.Errorf("expected 2 entries, got %d", len(req.Entries))
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(BatchResponse{Accepted: len(req.Entries), Status: "ok"})
	}))
	defer server.Close()

	writer := NewHTTPWriter(server.URL, 5*time.Second)
	defer writer.Close()

	entries := []LogEntry{
		{Labels: map[string]string{"app": "api"}, Message: "started"},
		{Labels: map[string]string{"app": "db", "level": "ERROR"}, Message: "failed"},
	}

	ctx := context.Background()
	if err := writer.WriteBatch(ctx, entries); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
}

func TestHTTPWriter_ServerError_WithRetry(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	writer := NewHTTPWriter(server.URL, 5*time.Second)
	writer.maxRetries = 3
	defer writer.Close()

	// Should succeed after retries
	err := writer.WriteBatch(context.Background(), []LogEntry{{Message: "test"}})
	if err != nil {
		t.Errorf("expected success after retries, got: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts (2 failures + 1 success), got %d", attempts)
	}
}

func TestHTTPWriter_ClientError_NoRetry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	writer := NewHTTPWriter(server.URL, 5*time.Second)
	writer.maxRetries = 3
	defer writer.Close()

	err := writer.WriteBatch(context.Background(), []LogEntry{{Message: "test"}})
	if err == nil {
		t.Error("expected error for 4xx response")
	}
}

func TestHTTPWriter_ContextCancel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	writer := NewHTTPWriter(server.URL, 5*time.Second)
	writer.maxRetries = 5 // set high so retries hit context cancellation
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := writer.WriteBatch(ctx, []LogEntry{{Message: "test"}})
	if err == nil {
		t.Error("expected context deadline error")
	}
}

func TestHTTPWriter_Closed(t *testing.T) {
	writer := NewHTTPWriter("http://localhost:9999", time.Second)
	writer.Close()

	err := writer.WriteBatch(context.Background(), []LogEntry{{Message: "test"}})
	if err == nil {
		t.Error("expected error writing to closed writer")
	}
}

func TestHTTPWriter_EmptyBatch_NoRequest(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer server.Close()

	writer := NewHTTPWriter(server.URL, time.Second)
	defer writer.Close()

	// Empty batch should not send HTTP request
	if err := writer.WriteBatch(context.Background(), nil); err != nil {
		t.Error(err)
	}
	if err := writer.WriteBatch(context.Background(), []LogEntry{}); err != nil {
		t.Error(err)
	}
	if called {
		t.Error("HTTP endpoint should not be called for empty batches")
	}
}
