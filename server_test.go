package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestServer_HandleLogBatch_Success(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, ":0")

	body := `{"entries":[{"labels":{"app":"api"},"message":"request started"},{"labels":{"app":"api","level":"ERROR"},"message":"request failed"}]}`
	req := httptest.NewRequest(http.MethodPost, "/logs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.handleLogBatch(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp BatchResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Accepted != 2 {
		t.Errorf("expected Accepted=2, got %d", resp.Accepted)
	}
	if resp.Status != "ok" {
		t.Errorf("expected status='ok', got %q", resp.Status)
	}
}

func TestServer_HandleLogBatch_EmptyEntries(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, ":0")

	body := `{"entries":[]}`
	req := httptest.NewRequest(http.MethodPost, "/logs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.handleLogBatch(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestServer_HandleLogBatch_WrongMethod(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, ":0")

	req := httptest.NewRequest(http.MethodGet, "/logs/batch", nil)
	w := httptest.NewRecorder()

	srv.handleLogBatch(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestServer_HandleLogBatch_WrongContentType(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, ":0")

	body := `{"entries":[{"labels":{"app":"api"},"message":"test"}]}`
	req := httptest.NewRequest(http.MethodPost, "/logs/batch", strings.NewReader(body))
	// No Content-Type header
	w := httptest.NewRecorder()

	srv.handleLogBatch(w, req)

	if w.Code != http.StatusUnsupportedMediaType {
		t.Errorf("expected 415, got %d", w.Code)
	}
}

func TestServer_HandleLogBatch_InvalidJSON(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, ":0")

	body := `not json at all`
	req := httptest.NewRequest(http.MethodPost, "/logs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.handleLogBatch(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestServer_HandleLogBatch_InvalidEntries(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, ":0")

	// Entries with empty message are invalid
	body := `{"entries":[
		{"labels":{"app":"api"},"message":""},
		{"labels":{"app":"db"},"message":"valid message"}
	]}`
	req := httptest.NewRequest(http.MethodPost, "/logs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.handleLogBatch(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp BatchResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Accepted != 1 {
		t.Errorf("expected 1 valid entry accepted, got %d", resp.Accepted)
	}
}

func TestServer_HandleLogBatch_BodyTooLarge(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, ":0")

	// Create a body larger than 1MB
	largeMessage := strings.Repeat("x", 1<<20+1)
	body := `{"entries":[{"labels":{"app":"api"},"message":"` + largeMessage + `"}]}`
	req := httptest.NewRequest(http.MethodPost, "/logs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.handleLogBatch(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413, got %d", w.Code)
	}
}

func TestServer_HealthEndpoint(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, ":0")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	srv.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", resp["status"])
	}
}

func TestServer_HealthEndpoint_WrongMethod(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, ":0")

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()

	srv.handleHealth(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestServer_MetricsEndpoint(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, ":0")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	srv.handleMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestServer_RunAndShutdown(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, "127.0.0.1:0")

	// Start the server
	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Run()
	}()

	// Give it time to start
	time.Sleep(50 * time.Millisecond)

	// Try to make a health request
	// Since we don't know the exact port (used :0), we can't easily test.
	// Just verify it doesn't panic on shutdown.

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(ctx)

	select {
	case <-errChan:
		// Server exited cleanly
	case <-time.After(time.Second):
		t.Error("server did not shut down within 1 second")
	}
}

func TestServer_ServeMux_Routing(t *testing.T) {
	batcher := NewEntryBatcher(&NoopWriter{}, 100, time.Hour)
	srv := NewServer(batcher, "127.0.0.1:0")

	// Test that all registered endpoints return appropriate responses
	tests := []struct {
		method string
		path   string
		want   int
	}{
		{http.MethodGet, "/health", http.StatusOK},
		{http.MethodGet, "/metrics", http.StatusOK},
		{http.MethodPost, "/logs/batch", http.StatusUnsupportedMediaType}, // no Content-Type
		{http.MethodGet, "/nonexistent", http.StatusOK},                  // 404 actually, but mux handling
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			var handler http.HandlerFunc
			switch tt.path {
			case "/health":
				handler = srv.handleHealth
			case "/metrics":
				handler = srv.handleMetrics
			case "/logs/batch":
				handler = srv.handleLogBatch
			default:
				// Unknown paths - tested via mux
				mux := http.NewServeMux()
				mux.HandleFunc("/logs/batch", srv.handleLogBatch)
				mux.HandleFunc("/health", srv.handleHealth)
				mux.HandleFunc("/metrics", srv.handleMetrics)
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(tt.method, tt.path, nil)
				mux.ServeHTTP(rec, req)
				// Unregistered paths should return 404
				if rec.Code != http.StatusNotFound {
					t.Errorf("expected 404 for %s %s, got %d", tt.method, tt.path, rec.Code)
				}
				return
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			handler(rec, req)

			if rec.Code != tt.want {
				t.Errorf("%s %s: expected %d, got %d", tt.method, tt.path, tt.want, rec.Code)
			}
		})
	}
}

func TestServer_Middleware_Logging(t *testing.T) {
	handler := withLogging(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestServer_Middleware_Recovery(t *testing.T) {
	handler := withRecovery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	// Should not panic
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 after panic recovery, got %d", w.Code)
	}
}

func TestBatchRequest_JSONUnmarshal(t *testing.T) {
	body := `{
		"entries": [
			{"labels":{"app":"api","level":"INFO"},"message":"request started"},
			{"labels":{"app":"db"},"message":"query executed","timestamp":"2024-01-15T10:30:00Z"}
		]
	}`

	var req BatchRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(req.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(req.Entries))
	}

	if req.Entries[0].Labels["app"] != "api" {
		t.Errorf("entry[0].labels.app = %q", req.Entries[0].Labels["app"])
	}
	if req.Entries[0].Message != "request started" {
		t.Errorf("entry[0].message = %q", req.Entries[0].Message)
	}
	if req.Entries[1].Labels["app"] != "db" {
		t.Errorf("entry[1].labels.app = %q", req.Entries[1].Labels["app"])
	}
	if req.Entries[1].Message != "query executed" {
		t.Errorf("entry[1].message = %q", req.Entries[1].Message)
	}
}
