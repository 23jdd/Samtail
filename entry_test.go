package main

import (
	"encoding/json"
	"testing"
)

func TestNewLogEntry(t *testing.T) {
	tests := []struct {
		name    string
		labels  map[string]string
		message string
		want    map[string]string
	}{
		{
			name:    "normal entry with labels",
			labels:  map[string]string{"app": "api", "level": "INFO"},
			message: "request started",
			want:    map[string]string{"app": "api", "level": "INFO"},
		},
		{
			name:    "nil labels initialized to empty map",
			labels:  nil,
			message: "message without labels",
			want:    map[string]string{},
		},
		{
			name:    "empty message is valid",
			labels:  map[string]string{"app": "test"},
			message: "",
			want:    map[string]string{"app": "test"},
		},
		{
			name:    "empty labels and empty message",
			labels:  map[string]string{},
			message: "",
			want:    map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := NewLogEntry(tt.labels, tt.message)
			if entry.Message != tt.message {
				t.Errorf("Message = %q, want %q", entry.Message, tt.message)
			}
			if len(entry.Labels) != len(tt.want) {
				t.Errorf("Labels count = %d, want %d", len(entry.Labels), len(tt.want))
			}
			for k, v := range tt.want {
				if got := entry.Labels[k]; got != v {
					t.Errorf("Label[%q] = %q, want %q", k, got, v)
				}
			}
		})
	}
}

func TestLogEntry_WithTimestamp(t *testing.T) {
	entry := NewLogEntry(map[string]string{"app": "api"}, "started")
	if !entry.Timestamp.IsZero() {
		t.Error("expected zero timestamp on new entry")
	}

	ts := entry.WithTimestamp(testTime)
	if ts.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp after WithTimestamp")
	}
	if ts.Timestamp != testTime {
		t.Errorf("timestamp = %v, want %v", ts.Timestamp, testTime)
	}

	// Original entry should be unchanged
	if !entry.Timestamp.IsZero() {
		t.Error("original entry timestamp should remain zero")
	}
}

func TestLogEntry_GetLabel(t *testing.T) {
	entry := NewLogEntry(map[string]string{"app": "api", "level": "ERROR"}, "msg")

	tests := []struct {
		key  string
		want string
	}{
		{"app", "api"},
		{"level", "ERROR"},
		{"missing", ""},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if got := entry.GetLabel(tt.key); got != tt.want {
				t.Errorf("GetLabel(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}

	// Nil labels should return empty string, not panic
	nilEntry := NewLogEntry(nil, "msg")
	if got := nilEntry.GetLabel("any"); got != "" {
		t.Errorf("GetLabel on nil labels = %q, want empty", got)
	}
}

func TestLogEntry_HasLabel(t *testing.T) {
	entry := NewLogEntry(map[string]string{"app": "api"}, "msg")

	if !entry.HasLabel("app") {
		t.Error("expected HasLabel(\"app\") = true")
	}
	if entry.HasLabel("missing") {
		t.Error("expected HasLabel(\"missing\") = false")
	}

	nilEntry := NewLogEntry(nil, "msg")
	if nilEntry.HasLabel("any") {
		t.Error("expected HasLabel on nil labels = false")
	}
}

func TestLogEntry_LabelKeys(t *testing.T) {
	entry := NewLogEntry(map[string]string{"z": "1", "a": "2", "m": "3"}, "msg")
	keys := entry.LabelKeys()
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	// Keys should be sorted
	if keys[0] != "a" || keys[1] != "m" || keys[2] != "z" {
		t.Errorf("keys not sorted: %v", keys)
	}

	// Empty labels should return empty slice, not nil
	emptyEntry := NewLogEntry(map[string]string{}, "msg")
	if keys := emptyEntry.LabelKeys(); keys == nil || len(keys) != 0 {
		t.Errorf("expected empty slice for empty labels, got %v", keys)
	}
}

func TestLogEntry_JSONRoundtrip(t *testing.T) {
	entry := LogEntry{
		Labels:  map[string]string{"app": "api", "level": "INFO"},
		Message: "request started",
	}

	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded LogEntry
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Message != entry.Message {
		t.Errorf("message = %q, want %q", decoded.Message, entry.Message)
	}
	if decoded.Labels["app"] != entry.Labels["app"] {
		t.Errorf("label app = %q, want %q", decoded.Labels["app"], entry.Labels["app"])
	}
	if decoded.Labels["level"] != entry.Labels["level"] {
		t.Errorf("label level = %q, want %q", decoded.Labels["level"], entry.Labels["level"])
	}
}

func TestLogEntry_Validate(t *testing.T) {
	tests := []struct {
		name    string
		entry   LogEntry
		wantErr bool
	}{
		{
			name:    "valid entry",
			entry:   LogEntry{Labels: map[string]string{"app": "api"}, Message: "started"},
			wantErr: false,
		},
		{
			name:    "empty message",
			entry:   LogEntry{Labels: map[string]string{"app": "api"}, Message: ""},
			wantErr: true,
		},
		{
			name:    "empty labels is valid",
			entry:   LogEntry{Labels: map[string]string{}, Message: "started"},
			wantErr: false,
		},
		{
			name:    "empty label key is invalid",
			entry:   LogEntry{Labels: map[string]string{"": "value"}, Message: "started"},
			wantErr: true,
		},
		{
			name:    "empty label value is invalid",
			entry:   LogEntry{Labels: map[string]string{"key": ""}, Message: "started"},
			wantErr: true,
		},
		{
			name:    "message with only whitespace is still valid (non-empty)",
			entry:   LogEntry{Labels: map[string]string{"app": "api"}, Message: "   "},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.entry.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestBatchRequest_JSON(t *testing.T) {
	body := `{"entries":[{"labels":{"app":"api"},"message":"request started"}]}`
	var req BatchRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(req.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(req.Entries))
	}
	if req.Entries[0].Message != "request started" {
		t.Errorf("message = %q", req.Entries[0].Message)
	}

	// Empty entries
	emptyBody := `{"entries":[]}`
	var emptyReq BatchRequest
	if err := json.Unmarshal([]byte(emptyBody), &emptyReq); err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}
	if len(emptyReq.Entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(emptyReq.Entries))
	}
}

func TestValidationError_Error(t *testing.T) {
	ve := &ValidationError{Field: "message", Reason: "must not be empty"}
	if got := ve.Error(); got != "validation error: message: must not be empty" {
		t.Errorf("Error() = %q, want %q", got, "validation error: message: must not be empty")
	}
}
