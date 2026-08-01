package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestParseLabelLine(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  map[string]string
	}{
		{
			name:  "single label",
			input: "app=api",
			want:  map[string]string{"app": "api"},
		},
		{
			name:  "multiple labels comma-separated",
			input: "app=api,level=INFO,env=prod",
			want:  map[string]string{"app": "api", "level": "INFO", "env": "prod"},
		},
		{
			name:  "labels with whitespace around comma",
			input: "app=api , level=INFO , env=prod",
			want:  map[string]string{"app": "api", "level": "INFO", "env": "prod"},
		},
		{
			name:  "labels with whitespace around equals",
			input: "app = api, level = INFO",
			want:  map[string]string{"app": "api", "level": "INFO"},
		},
		{
			name:  "label value with spaces",
			input: "msg=hello world,code=200",
			want:  map[string]string{"msg": "hello world", "code": "200"},
		},
		{
			name:  "empty input returns nil",
			input: "",
			want:  nil,
		},
		{
			name:  "whitespace-only input returns nil",
			input: "   ",
			want:  nil,
		},
		{
			name:  "line without equals sign returns nil",
			input: "this is a message line",
			want:  nil,
		},
		{
			name:  "line with only commas returns nil",
			input: ",,,",
			want:  nil,
		},
		{
			name:  "empty key (value without key) is skipped",
			input: "=value",
			want:  nil,
		},
		{
			name:  "empty value (key without value) is kept",
			input: "key=",
			want:  map[string]string{"key": ""},
		},
		{
			name:  "duplicate keys: last wins",
			input: "app=old,app=new",
			want:  map[string]string{"app": "new"},
		},
		{
			name:  "numeric values",
			input: "count=42,ratio=1.5",
			want:  map[string]string{"count": "42", "ratio": "1.5"},
		},
		{
			name:  "special characters in values",
			input: "path=/usr/local/bin,url=http://example.com",
			want:  map[string]string{"path": "/usr/local/bin", "url": "http://example.com"},
		},
		{
			name:  "trailing comma",
			input: "app=api,",
			want:  map[string]string{"app": "api"},
		},
		{
			name:  "leading comma",
			input: ",app=api",
			want:  map[string]string{"app": "api"},
		},
		{
			name:  "nested equals in value (first = is separator)",
			input: "query=a=b=c",
			want:  map[string]string{"query": "a=b=c"},
		},
		{
			name:  "mixed valid and invalid pairs",
			input: "app=api,,level=INFO,=bad,env=prod",
			want:  map[string]string{"app": "api", "level": "INFO", "env": "prod"},
		},
		{
			name:  "key with space is rejected (avoid timestamp m=+0.001 misparse)",
			input: "2026-08-02 00:17:11 +0800 CST m=+0.001615501",
			want:  nil,
		},
		{
			name:  "space in key among valid pairs returns nil",
			input: "app=api,bad key=val",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseLabelLine(tt.input)
			if len(got) != len(tt.want) {
				t.Errorf("parseLabelLine(%q) = %v (%d entries), want %v (%d entries)",
					tt.input, got, len(got), tt.want, len(tt.want))
				return
			}
			for k, wantV := range tt.want {
				if gotV, ok := got[k]; !ok {
					t.Errorf("parseLabelLine(%q): missing key %q", tt.input, k)
				} else if gotV != wantV {
					t.Errorf("parseLabelLine(%q): key %q = %q, want %q", tt.input, k, gotV, wantV)
				}
			}
		})
	}
}

func TestParseLogStream(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int // number of entries
		check   func(*testing.T, []LogEntry)
	}{
		{
			name:  "single entry",
			input: "app=api\nrequest started\n",
			want:  1,
			check: func(t *testing.T, entries []LogEntry) {
				if entries[0].Message != "request started" {
					t.Errorf("message = %q", entries[0].Message)
				}
				if entries[0].Labels["app"] != "api" {
					t.Errorf("label app = %q", entries[0].Labels["app"])
				}
			},
		},
		{
			name:  "multiple entries separated by blank line",
			input: "app=api\nrequest started\n\napp=db\nquery failed\n",
			want:  2,
		},
		{
			name:  "adjacent entries (no blank line separator still works)",
			input: "app=api\nstarted\napp=db\nerror\n",
			want:  2,
		},
		{
			name:  "empty input",
			input: "",
			want:  0,
		},
		{
			name:  "whitespace only input",
			input: "\n\n\n",
			want:  0,
		},
		{
			name:  "message line without labels is skipped",
			input: "this is just a message\nwith multiple lines\n",
			want:  0,
		},
		{
			name:  "labels without message at end are discarded",
			input: "app=api\nstarted\n\napp=db\n",
			want:  1,
			check: func(t *testing.T, entries []LogEntry) {
				if entries[0].Message != "started" {
					t.Errorf("expected 'started', got %q", entries[0].Message)
				}
			},
		},
		{
			name: "labels without message then new labels (orphan discarded)",
			input: `app=api
app=db
query error
`,
			want: 1,
			check: func(t *testing.T, entries []LogEntry) {
				if entries[0].Labels["app"] != "db" {
					t.Errorf("expected app=db label, got %v", entries[0].Labels)
				}
				if entries[0].Message != "query error" {
					t.Errorf("expected 'query error', got %q", entries[0].Message)
				}
			},
		},
		{
			name: "multiple blank lines between entries",
			input: `app=api
started


app=db
error
`,
			want: 2,
		},
		{
			name: "labels with multiple keys",
			input: `app=api,level=INFO,env=prod
request processed successfully
`,
			want: 1,
			check: func(t *testing.T, entries []LogEntry) {
				if len(entries[0].Labels) != 3 {
					t.Errorf("expected 3 labels, got %d: %v", len(entries[0].Labels), entries[0].Labels)
				}
			},
		},
		{
			name: "message with special characters",
			input: `app=api
[2024-01-15] GET /api/users - 200 OK
`,
			want: 1,
			check: func(t *testing.T, entries []LogEntry) {
				expected := "[2024-01-15] GET /api/users - 200 OK"
				if entries[0].Message != expected {
					t.Errorf("message = %q, want %q", entries[0].Message, expected)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			entries, err := ParseLogStream(reader, "test.log")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(entries) != tt.want {
				t.Errorf("got %d entries, want %d: %v", len(entries), tt.want, entries)
			}
			if tt.check != nil {
				tt.check(t, entries)
			}
		})
	}
}

func TestParser_WithChannel(t *testing.T) {
	lineChan := make(chan LogLine, 10)
	entryChan := make(chan LogEntry, 10)

	parser := NewParser(lineChan, entryChan)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go parser.Run(ctx)

	testLines := []LogLine{
		{FilePath: "test.log", Content: "app=api", Timestamp: testTime},
		{FilePath: "test.log", Content: "request started", Timestamp: testTime},
		{FilePath: "test.log", Content: "", Timestamp: testTime},
		{FilePath: "test.log", Content: "app=db,level=ERROR", Timestamp: testTime},
		{FilePath: "test.log", Content: "connection failed", Timestamp: testTime},
	}

	for _, line := range testLines {
		lineChan <- line
	}

	// Collect entries with a timeout
	var entries []LogEntry
	timeout := time.After(500 * time.Millisecond)
	for len(entries) < 2 {
		select {
		case entry := <-entryChan:
			entries = append(entries, entry)
		case <-timeout:
			t.Fatalf("timed out waiting for entries, got %d", len(entries))
		}
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	if entries[0].Message != "request started" {
		t.Errorf("entry[0].Message = %q, want 'request started'", entries[0].Message)
	}
	if entries[0].Labels["app"] != "api" {
		t.Errorf("entry[0].Labels[app] = %q, want 'api'", entries[0].Labels["app"])
	}
	if entries[1].Message != "connection failed" {
		t.Errorf("entry[1].Message = %q, want 'connection failed'", entries[1].Message)
	}
	if entries[1].Labels["level"] != "ERROR" {
		t.Errorf("entry[1].Labels[level] = %q, want 'ERROR'", entries[1].Labels["level"])
	}
}

func TestParser_OrphanLabels(t *testing.T) {
	lineChan := make(chan LogLine, 10)
	entryChan := make(chan LogEntry, 10)

	parser := NewParser(lineChan, entryChan)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go parser.Run(ctx)

	// Send two consecutive label lines - first should be orphaned
	lineChan <- LogLine{FilePath: "test.log", Content: "app=old", Timestamp: testTime}
	lineChan <- LogLine{FilePath: "test.log", Content: "app=new,level=INFO", Timestamp: testTime}
	lineChan <- LogLine{FilePath: "test.log", Content: "valid message", Timestamp: testTime}

	var entries []LogEntry
	timeout := time.After(500 * time.Millisecond)
	for len(entries) < 1 {
		select {
		case entry := <-entryChan:
			entries = append(entries, entry)
		case <-timeout:
			t.Fatalf("timed out waiting for entries, got %d", len(entries))
		}
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Labels["app"] != "new" {
		t.Errorf("expected app=new, got app=%q", entries[0].Labels["app"])
	}
	if entries[0].Labels["level"] != "INFO" {
		t.Errorf("label level missing or wrong: %v", entries[0].Labels)
	}
}

func TestParser_GracefulShutdown(t *testing.T) {
	lineChan := make(chan LogLine, 10)
	entryChan := make(chan LogEntry, 10)

	parser := NewParser(lineChan, entryChan)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		parser.Run(ctx)
		close(done)
	}()

	// Send a label without a message
	lineChan <- LogLine{FilePath: "test.log", Content: "app=api", Timestamp: testTime}

	// Cancel context - the orphaned label should be silently discarded
	cancel()

	select {
	case <-done:
		// Parser exited cleanly
	case <-time.After(time.Second):
		t.Fatal("parser did not exit within 1 second after cancellation")
	}
}
