package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

// Parser reads raw LogLine input and assembles them into LogEntry objects
// according to the label=value,\nmessage\n\n format.
//
// Log format specification:
//
//	label1=value1,label2=value2
//	message content
//
//	label3=value3
//	another message
//
// Rules:
//   - A label line contains one or more comma-separated key=value pairs.
//   - A message line immediately follows the label line.
//   - Entries are separated by one or more blank lines.
//   - Label keys and values are trimmed of whitespace.
//   - Lines that do not match the format are skipped with a log warning.
//   - An entry is emitted as soon as a complete label+message pair is parsed.
//
// Boundary conditions:
//   - Empty input stream: no entries emitted, no error.
//   - Label line without message: entry is held until next non-blank line
//     becomes the message (or is discarded on stream end).
//   - Message without label: line is skipped with warning.
//   - Label line with invalid key=value: that pair is skipped, valid pairs kept.
//   - Label keys with duplicate names: last value wins.
//   - Very long lines (> bufio.MaxScanTokenSize): handled by bufio.Scanner.
type Parser struct {
	lineChan <-chan LogLine
	// entryChan sends parsed LogEntry objects downstream
	entryChan chan<- LogEntry

	// pending holds the current set of labels being accumulated.
	// When a non-blank, non-label line follows, it becomes the message
	// for the pending labels and an entry is emitted.
	pendingLabels map[string]string
	pendingSource string
	mu            sync.Mutex
}

// NewParser creates a Parser that reads from lineChan and writes to entryChan.
//
// Parameters:
//   - lineChan: input channel of raw LogLine objects (typically from TailReader)
//   - entryChan: output channel for parsed LogEntry objects (typically to BatchWriter)
//
// The entryChan should be buffered to avoid blocking the parser on slow consumers.
func NewParser(lineChan <-chan LogLine, entryChan chan<- LogEntry) *Parser {
	return &Parser{
		lineChan:      lineChan,
		entryChan:     entryChan,
		pendingLabels: make(map[string]string),
	}
}

// Run starts the parse loop. It blocks until ctx is cancelled or lineChan is closed.
// When the input stream ends, any pending labels without a message are discarded.
func (p *Parser) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-p.lineChan:
			if !ok {
				return
			}
			p.processLine(line)
		}
	}
}

// processLine handles a single line of input, assembling entries.
func (p *Parser) processLine(line LogLine) {
	content := strings.TrimSpace(line.Content)

	// Blank line: flushes any pending labels without a message?
	// No - blank lines are just separators. If we have pending labels
	// with no message when we hit a blank line, we continue waiting
	// for a message line. Only when we get a non-label, non-blank line
	// after labels do we emit an entry.
	if len(content) == 0 {
		return
	}

	// Try to parse as labels first
	labels := parseLabelLine(content)
	if len(labels) > 0 {
		// Flush any previously pending entry (labels without message)
		// This handles the case where two label lines appear consecutively:
		// the first one is discarded as incomplete.
		if len(p.pendingLabels) > 0 {
			log.Printf("[Parser] discarding orphan labels from %s: %v", p.pendingSource, p.pendingLabels)
		}

		p.pendingLabels = labels
		p.pendingSource = line.FilePath
		return
	}

	// Not a label line. If we have pending labels, this is the message.
	if len(p.pendingLabels) > 0 {
		entry := LogEntry{
			Labels:    p.pendingLabels,
			Message:   content,
			Timestamp: line.Timestamp,
		}
		p.pendingLabels = nil

		select {
		case p.entryChan <- entry:
		default:
			log.Printf("[Parser] entryChan full, discarding entry: %s", content)
		}
		return
	}

	// Message line with no preceding labels: skip with warning
	log.Printf("[Parser] skipping line without labels: %s", content)
}

// parseLabelLine parses a line like "key1=val1,key2=val2" into a map.
// Returns nil if the line does not look like a label line (no = signs).
//
// Boundary conditions:
//   - Empty input: returns nil
//   - Line with no '=' characters: returns nil
//   - Key without value ("key="): value is empty string
//   - Value without key ("=val"): pair is skipped
//   - Extra whitespace around commas and equals: trimmed
//   - Duplicate keys: last occurrence wins
//   - Escape sequences: commas in values are NOT supported (use \% comma
//     or similar encoding; currently literal commas terminate the pair)
func parseLabelLine(line string) map[string]string {
	if len(strings.TrimSpace(line)) == 0 {
		return nil
	}

	// A label line must contain at least one '='
	if !strings.Contains(line, "=") {
		return nil
	}

	labels := make(map[string]string)
	pairs := strings.Split(line, ",")

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if len(pair) == 0 {
			continue
		}

		idx := strings.Index(pair, "=")
		if idx < 0 {
			// No equals sign in this pair, skip it
			continue
		}

		key := strings.TrimSpace(pair[:idx])
		if len(key) == 0 {
			// Empty key, skip
			continue
		}

		value := strings.TrimSpace(pair[idx+1:])
		labels[key] = value
	}

	if len(labels) == 0 {
		return nil
	}

	return labels
}

// ParseLogStream reads an entire log stream and returns all parsed entries.
// This is a convenience function for testing and batch processing.
//
// Usage:
//
//	reader := strings.NewReader("app=api\nstarted\n\napp=db\nerror\n")
//	entries, err := ParseLogStream(reader, "test.log")
//
// Boundary conditions:
//   - Empty reader: returns nil entries, nil error
//   - Incomplete final entry (labels without message): labels are discarded
//   - Max line length: limited by bufio.Scanner default (64KB)
func ParseLogStream(r io.Reader, source string) ([]LogEntry, error) {
	var entries []LogEntry
	var pendingLabels map[string]string

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		content := strings.TrimSpace(scanner.Text())
		if len(content) == 0 {
			continue
		}

		labels := parseLabelLine(content)
		if len(labels) > 0 {
			if pendingLabels != nil {
				// Discard orphan labels
				pendingLabels = nil
			}
			pendingLabels = labels
			continue
		}

		if pendingLabels != nil {
			entries = append(entries, LogEntry{
				Labels:    pendingLabels,
				Message:   content,
				Timestamp: time.Now(),
			})
			pendingLabels = nil
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}
