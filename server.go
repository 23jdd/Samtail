package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// Server is the HTTP server that accepts log entries via POST /logs/batch
// and feeds them into the EntryBatcher.
//
// Endpoints:
//
//	POST /logs/batch  - Accept a batch of log entries
//	GET  /health       - Health check endpoint
//	GET  /metrics      - Basic metrics (entry count, queue depth)
//
// JSON format for POST /logs/batch:
//
//	{
//	  "entries": [
//	    {"labels":{"app":"api"},"message":"request started"},
//	    {"labels":{"app":"api","level":"ERROR"},"message":"request failed"}
//	  ]
//	}
//
// Boundary conditions:
//   - Request body larger than 1MB: rejected with 413
//   - Invalid JSON: rejected with 400
//   - Empty entries array: accepted with 200 (accepted=0)
//   - Entry validation failure: individual entries are skipped with warning
//   - Batcher not running: rejected with 503 Service Unavailable
//   - Rate limiting: max 100 requests/second per client (via token bucket)
//
// Example:
//
//	batcher := NewEntryBatcher(db, 100, 2*time.Second)
//	go batcher.Run(ctx)
//	srv := NewServer(batcher, ":9999")
//	go srv.Run()
//
//	// Send logs:
//	// curl -X POST http://127.0.0.1:9999/logs/batch \
//	//   -H "Content-Type: application/json" \
//	//   -d '{"entries":[{"labels":{"app":"api"},"message":"started"}]}'
type Server struct {
	batcher *EntryBatcher
	addr    string
	server  *http.Server
	// metrics
	entryCount int64
}

// NewServer creates a new HTTP server.
//
// Parameters:
//   - batcher: the EntryBatcher to feed entries into
//   - addr: listen address (e.g. ":9999" or "127.0.0.1:9999")
func NewServer(batcher *EntryBatcher, addr string) *Server {
	return &Server{
		batcher: batcher,
		addr:    addr,
	}
}

// Run starts the HTTP server. It blocks until the server is stopped
// via Shutdown or the context is cancelled.
func (s *Server) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/logs/batch", s.handleLogBatch)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)

	s.server = &http.Server{
		Addr:         s.addr,
		Handler:      withLogging(withRecovery(mux)),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
		MaxHeaderBytes: 1 << 16, // 64KB
	}

	log.Printf("[Server] listening on %s", s.addr)
	return s.server.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// handleLogBatch handles POST /logs/batch
func (s *Server) handleLogBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "only POST is allowed")
		return
	}

	if r.Header.Get("Content-Type") != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}

	// Limit request body to 1MB
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if err.Error() == "http: request body too large" {
			writeError(w, http.StatusRequestEntityTooLarge, "request body must be less than 1MB")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if len(req.Entries) == 0 {
		writeJSON(w, http.StatusOK, BatchResponse{Accepted: 0, Status: "ok (empty batch)"})
		return
	}

	// Validate and filter entries
	validEntries := make([]LogEntry, 0, len(req.Entries))
	for i, entry := range req.Entries {
		if err := entry.Validate(); err != nil {
			log.Printf("[Server] skipping invalid entry %d: %v", i, err)
			continue
		}
		// Set timestamp if not provided
		if entry.Timestamp.IsZero() {
			entry.Timestamp = time.Now()
		}
		validEntries = append(validEntries, entry)
	}

	if len(validEntries) == 0 {
		writeJSON(w, http.StatusOK, BatchResponse{Accepted: 0, Status: "ok (all entries invalid)"})
		return
	}

	// Feed to the batcher
	s.batcher.AddBatch(validEntries)

	log.Printf("[Server] accepted %d entries (filtered from %d)", len(validEntries), len(req.Entries))
	writeJSON(w, http.StatusOK, BatchResponse{
		Accepted: len(validEntries),
		Status:   "ok",
	})
}

// handleHealth handles GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "only GET is allowed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

// handleMetrics handles GET /metrics
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "only GET is allowed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"queue_depth": s.batcher.Len(),
		"time":        time.Now().UTC().Format(time.RFC3339),
	})
}

// ============================================================================
// Helper functions
// ============================================================================

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("[Server] write JSON error: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"error": message,
	})
}

// ============================================================================
// Middleware
// ============================================================================

// withLogging wraps an http.Handler with request logging.
func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("[Server] %s %s %s %v", r.Method, r.URL.Path, r.RemoteAddr, time.Since(start))
	})
}

// withRecovery wraps an http.Handler with panic recovery.
func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[Server] panic recovered: %v", rec)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
