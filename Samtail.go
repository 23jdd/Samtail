package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	MetaFile      = "meta.json"
	TemplateFile  = "meta_template_*.json"
	DefaultDir    = "logs"
	DefaultOutDir = "./output"
	DefaultDBURL  = "http://127.0.0.1:6379/logs/batch"
)

// LogEvent fsnotify 转化来的内部事件
type LogEvent struct {
	Op       string // "create", "write", "rename", "remove"
	FilePath string
}

// LogLine 单条日志行
type LogLine struct {
	FilePath  string
	Content   string
	Timestamp time.Time
}

// FileState 文件读取偏移量，持久化到 meta.json 防重启丢失
type FileState struct {
	Offset int64
	Path   string
}

// Watcher 监控目录，检测 .log/.txt 文件的创建、写入、重命名、删除
type Watcher struct {
	watcher   *fsnotify.Watcher
	eventChan chan LogEvent
	targetDir string
}

func NewWatcher(dir string, eventChan chan LogEvent) (*Watcher, error) {
	absPath, _ := filepath.Abs(dir)
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		log.Fatalf("路径不存在: %s", absPath)
	}
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := fw.Add(dir); err != nil {
		return nil, err
	}
	return &Watcher{
		watcher:   fw,
		eventChan: eventChan,
		targetDir: dir,
	}, nil
}

func (w *Watcher) Run(ctx context.Context) {
	defer close(w.eventChan)
	defer w.watcher.Close()

	w.scanExistingFiles()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			if !isLogFile(event.Name) {
				continue
			}
			var op string
			switch {
			case event.Op&fsnotify.Create == fsnotify.Create:
				op = "create"
			case event.Op&fsnotify.Write == fsnotify.Write:
				op = "write"
			case event.Op&fsnotify.Rename == fsnotify.Rename:
				op = "rename"
			case event.Op&fsnotify.Remove == fsnotify.Remove:
				op = "remove"
			default:
				continue
			}
			select {
			case w.eventChan <- LogEvent{Op: op, FilePath: event.Name}:
			default:
				log.Printf("[Watcher] 事件队列已满，丢弃: %s", event.Name)
			}
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("[Watcher] 错误: %v", err)
		}
	}
}

func (w *Watcher) scanExistingFiles() {
	entries, err := os.ReadDir(w.targetDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !isLogFile(entry.Name()) {
			continue
		}
		w.eventChan <- LogEvent{Op: "create", FilePath: filepath.Join(w.targetDir, entry.Name())}
	}
}

func isLogFile(name string) bool {
	return strings.HasSuffix(name, ".log") || strings.HasSuffix(name, ".txt")
}

// TailReader 按 inode 跟踪文件读取偏移量，支持断点续读和轮转检测
type TailReader struct {
	eventChan chan LogEvent
	lineChan  chan LogLine
	states    map[string]*FileState
	mu        sync.RWMutex
}

func NewTailReader(eventChan chan LogEvent, lineChan chan LogLine) *TailReader {
	return &TailReader{
		eventChan: eventChan,
		lineChan:  lineChan,
		states:    make(map[string]*FileState),
	}
}

func (t *TailReader) ReLoad() error {
	r, err := os.Open(MetaFile)
	if err != nil {
		return err
	}
	defer r.Close()
	err = json.NewDecoder(r).Decode(&t.states)
	return err
}

func (t *TailReader) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-t.eventChan:
			if !ok {
				return
			}
			t.handleEvent(evt)
		}
	}
}

func (t *TailReader) handleEvent(evt LogEvent) {
	switch evt.Op {
	case "create", "write":
		t.readFile(evt.FilePath)
	case "rename", "remove":
		t.closeFile(evt.FilePath)
	}
}

func (t *TailReader) readFile(path string) {
	log.Println("read", path)
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	id, err := GetLstat(path)
	if err != nil {
		return
	}

	t.mu.Lock()
	state, exists := t.states[id]
	if !exists {
		state = &FileState{Offset: 0, Path: path}
		t.states[id] = state
	} else if state.Path != path {
		state.Path = path
	}
	offset := state.Offset
	t.mu.Unlock()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return
	}

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			content := strings.TrimRight(line, "\r\n")
			select {
			case t.lineChan <- LogLine{
				FilePath:  path,
				Content:   content,
				Timestamp: time.Now(),
			}:
			default:
				log.Printf("[TailReader] lineChan 已满，丢弃一行")
			}
			offset += int64(len(line))
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("[TailReader] 读文件错误: %v", err)
			}
			break
		}
	}

	t.mu.Lock()
	if s, ok := t.states[id]; ok {
		s.Offset = offset
	}
	t.mu.Unlock()
}

func (t *TailReader) closeFile(path string) {
	// 保守策略：保留 state，防止轮转时丢失
}

// Checkpoit 每 500ms 将读取状态原子写入 meta.json
func (t *TailReader) Checkpoit(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if err := t.SafeWriteFile(); err != nil {
				log.Println(err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// SafeWriteFile 通过写临时文件 + 原子重命名实现安全写入
func (t *TailReader) SafeWriteFile() error {
	tmpFile, err := os.CreateTemp(".", TemplateFile)
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmpFile.Name()

	defer func() {
		tmpFile.Close()
		os.Remove(tmpPath)
	}()

	t.mu.RLock()
	if err := json.NewEncoder(tmpFile).Encode(t.states); err != nil {
		t.mu.RUnlock()
		return fmt.Errorf("write temp: %w", err)
	}
	t.mu.RUnlock()

	if err = tmpFile.Sync(); err != nil {
		return fmt.Errorf("fsync temp: %w", err)
	}
	if err = tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}
	if err = os.Rename(tmpPath, MetaFile); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

func main() {
	targetDir := envOrDefault("SAMTAIL_DIR", DefaultDir)
	outputDir := envOrDefault("SAMTAIL_OUTPUT", DefaultOutDir)
	dbURL := envOrDefault("SAMTAIL_DB_URL", DefaultDBURL)
	batchSize := envIntOrDefault("SAMTAIL_BATCH_SIZE", 1000)
	flushSecs := envIntOrDefault("SAMTAIL_FLUSH_SECS", 2)

	log.Printf("samtail starting: dir=%s output=%s db=%s batch=%d flush=%ds",
		targetDir, outputDir, dbURL, batchSize, flushSecs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventChan := make(chan LogEvent, 256)
	lineChan := make(chan LogLine, 5000)
	entryChan := make(chan LogEntry, 5000)

	dbWriter := buildDatabaseWriter(dbURL, outputDir)
	defer dbWriter.Close()

	batcher := NewEntryBatcher(dbWriter, batchSize, time.Duration(flushSecs)*time.Second)

	watcher, err := NewWatcher(targetDir, eventChan)
	if err != nil {
		log.Fatalf("create watcher: %v", err)
	}

	reader := NewTailReader(eventChan, lineChan)
	if err := reader.ReLoad(); err != nil {
		log.Printf("load metadata: %v (starting fresh)", err)
	}

	parser := NewParser(lineChan, entryChan)

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		log.Println("shutting down...")
		cancel()
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() { defer wg.Done(); watcher.Run(ctx) }()

	wg.Add(1)
	go func() { defer wg.Done(); reader.Run(ctx) }()

	wg.Add(1)
	go func() { defer wg.Done(); reader.Checkpoit(ctx) }()

	wg.Add(1)
	go func() { defer wg.Done(); parser.Run(ctx) }()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case entry, ok := <-entryChan:
				if !ok {
					return
				}
				batcher.Add(entry)
			}
		}
	}()

	wg.Add(1)
	go func() { defer wg.Done(); batcher.Run(ctx) }()

	wg.Wait()

	log.Println("final flush...")
	batcher.Close()
	log.Println("samtail stopped")
}

func buildDatabaseWriter(dbURL, outputDir string) DatabaseWriter {
	var writers []DatabaseWriter

	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			log.Printf("create output dir: %v", err)
		} else {
			backupPath := filepath.Join(outputDir, fmt.Sprintf("backup_%s.jsonl", time.Now().Format("20060102_150405")))
			if fw, err := NewFileWriter(backupPath); err != nil {
				log.Printf("create file writer: %v", err)
			} else {
				writers = append(writers, fw)
				log.Printf("local backup: %s", backupPath)
			}
		}
	}

	if dbURL != "" {
		writers = append(writers, NewHTTPWriter(dbURL, 10*time.Second))
		log.Printf("remote database: %s", dbURL)
	}

	if len(writers) == 0 {
		log.Println("no database configured, using noop writer")
		return &NoopWriter{}
	}
	if len(writers) == 1 {
		return writers[0]
	}
	return NewMultiWriter(writers...)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return def
}
