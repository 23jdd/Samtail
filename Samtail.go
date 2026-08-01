package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	MetaFile     = "meta.json"
	TemplateFile = "meta_template_*.json"
)

// LogEvent 从 fsnotify 转化来的内部事件
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

// FileState 跟踪每个文件的读取状态（持久化可防重启丢失）
type FileState struct {
	Offset int64  // 当前读取偏移量
	Path   string // 当前路径（可能因轮转改变）
}

// Watcher
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

	// 启动时扫描现有日志文件，发送 create 事件
	w.scanExistingFiles()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			// 只关注日志文件
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
			// 非阻塞发送，队列满则丢弃（背压保护）
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

// scanExistingFiles 扫描目标目录所有日志文件
func (w *Watcher) scanExistingFiles() {
	entries, err := os.ReadDir(w.targetDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !isLogFile(entry.Name()) {
			continue
		}
		path := filepath.Join(w.targetDir, entry.Name())
		w.eventChan <- LogEvent{Op: "create", FilePath: path}
	}
}

func isLogFile(name string) bool {
	return strings.HasSuffix(name, ".log") || strings.HasSuffix(name, ".txt")
}

// TailReader：文件读取 + Offset 跟踪
type TailReader struct {
	eventChan chan LogEvent
	lineChan  chan LogLine
	states    map[string]*FileState // ID -> state
	mu        sync.RWMutex
}

func NewTailReader(eventChan chan LogEvent, lineChan chan LogLine) *TailReader {
	return &TailReader{
		eventChan: eventChan,
		lineChan:  lineChan,
		states:    make(map[string]*FileState),
	}
}

func (t *TailReader) Run(ctx context.Context) {
	go t.Checkpoit()
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

// 核心读取逻辑：按 inode 跟踪，支持追加读取和轮转检测
func (t *TailReader) readFile(path string) {
	log.Println("read", path)
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	// 获取文件 inode
	id, err := GetLstat(path)
	if err != nil {
		return
	}
	t.mu.Lock()
	state, exists := t.states[id]
	if !exists {
		// 新文件：从开头或末尾读取？生产环境通常从新文件开头读
		state = &FileState{Offset: 0, Path: path}
		t.states[id] = state
	} else if state.Path != path {
		// 同一个 inode 但路径变了？说明被重命名了，更新路径
		state.Path = path
	}
	offset := state.Offset
	t.mu.Unlock()

	// 跳到上次读取位置
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return
	}

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			// 去掉换行符
			content := strings.TrimRight(line, "\r\n")
			// 非阻塞发送
			select {
			case t.lineChan <- LogLine{
				FilePath:  path,
				Content:   content,
				Timestamp: time.Now(),
			}:
			default:
				log.Printf("[TailReader] lineChan 已满，丢弃一行")
			}
			// 更新偏移量（包含换行符长度）
			offset += int64(len(line))
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("[TailReader] 读文件错误: %v", err)
			}
			break
		}
	}

	// 写回 offset
	t.mu.Lock()
	if s, ok := t.states[id]; ok {
		s.Offset = offset
	}
	t.mu.Unlock()
}

func (t *TailReader) closeFile(path string) {
	// 可选：清理已删除/重命名文件的 state（保守策略可保留，防止轮转时丢失）
}
func (t *TailReader) Checkpoit() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		<-ticker.C
		err := t.SafeWriteFile()
		if err != nil {
			log.Println(err)
		}
	}
}

// SafeWriteFile 原子写入文件：要么写入完整新内容，要么保持旧内容不变
func (t *TailReader) SafeWriteFile() error {
	tmpFile, err := os.CreateTemp(".", TemplateFile)
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmpFile.Name()

	// 确保临时文件关闭和清理（失败时删除）
	defer func() {
		tmpFile.Close()
		os.Remove(tmpPath)
	}()

	//写入数据
	t.mu.RLock()
	if err := json.NewEncoder(tmpFile).Encode(t.states); err != nil {
		t.mu.RUnlock()
		return fmt.Errorf("write temp: %w", err)
	}
	t.mu.RUnlock()

	// 强制刷盘
	if err = tmpFile.Sync(); err != nil {
		return fmt.Errorf("fsync temp: %w", err)
	}

	// 关闭文件描述符
	if err = tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}

	// 原子重命名：覆盖已存在的目标，或新建
	// 这一步是原子的，外部只会看到"旧内容"或"完整新内容"
	if err = os.Rename(tmpPath, MetaFile); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}

// BatchWriter：批量缓冲存储
type BatchWriter struct {
	lineChan   chan LogLine
	outputDir  string
	batchSize  int           // 每批行数
	flushIntv  time.Duration // 定时刷盘间隔
	buffer     []LogLine
	mu         sync.Mutex
	outputFile *os.File
	writer     *bufio.Writer
}

func NewBatchWriter(lineChan chan LogLine, outDir string) *BatchWriter {
	return &BatchWriter{
		lineChan:  lineChan,
		outputDir: outDir,
		batchSize: 1000,            // 每 1000 行刷一次
		flushIntv: 2 * time.Second, // 或每 2 秒刷一次
		buffer:    make([]LogLine, 0, 1000),
	}
}

func (b *BatchWriter) Run(ctx context.Context) {
	// 初始化输出文件
	if err := os.MkdirAll(b.outputDir, 0755); err != nil {
		log.Fatalf("创建输出目录失败: %v", err)
	}
	outPath := filepath.Join(b.outputDir, fmt.Sprintf("merged_%s.log", time.Now().Format("20060102_150405")))
	file, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("打开输出文件失败: %v", err)
	}
	b.outputFile = file
	b.writer = bufio.NewWriterSize(file, 256*1024) // 256KB 用户态缓冲

	// 定时刷盘
	ticker := time.NewTicker(b.flushIntv)
	defer ticker.Stop()
	defer b.flush() // 退出前强制刷盘

	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-b.lineChan:
			if !ok {
				return
			}
			b.mu.Lock()
			b.buffer = append(b.buffer, line)
			shouldFlush := len(b.buffer) >= b.batchSize
			b.mu.Unlock()

			if shouldFlush {
				b.flush()
			}
		case <-ticker.C:
			b.flush()
		}
	}
}

func (b *BatchWriter) flush() {
	b.mu.Lock()
	if len(b.buffer) == 0 {
		b.mu.Unlock()
		return
	}
	lines := b.buffer
	b.buffer = b.buffer[:0]
	b.mu.Unlock()

	// 批量写入（用户态缓冲）
	for _, line := range lines {
		// 格式: [时间] [源文件] 内容
		fmt.Fprintf(b.writer, "[%s] [%s] %s\n",
			line.Timestamp.Format("2006-01-02T15:04:05.000"),
			filepath.Base(line.FilePath),
			line.Content,
		)
	}

	// 刷入内核缓冲
	if err := b.writer.Flush(); err != nil {
		log.Printf("[BatchWriter] Flush 失败: %v", err)
	}

	// 根据可靠性需求决定是否 fsync
	// if err := b.outputFile.Sync(); err != nil {
	//     log.Printf("[BatchWriter] Sync 失败: %v", err)
	// }

	log.Printf("[BatchWriter] 刷盘 %d 行", len(lines))
}

// ==================== 4. 主程序：组装流水线 ====================

func main() {
	targetDir := "logs"     // 监控目录
	outputDir := "./output" // 存储目录

	// 创建带取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建 Channel（带缓冲，解耦生产消费速率）
	eventChan := make(chan LogEvent, 256) // Watcher -> TailReader
	lineChan := make(chan LogLine, 5000)  // TailReader -> BatchWriter

	// 初始化组件
	watcher, err := NewWatcher(targetDir, eventChan)
	if err != nil {
		log.Fatalf("创建 Watcher 失败: %v", err)
	}

	reader := NewTailReader(eventChan, lineChan)
	writer := NewBatchWriter(lineChan, outputDir)

	// 优雅关闭：捕获信号
	go func() {
		//TODO 用 signal.Notify 捕获 SIGINT/SIGTERM
		time.Sleep(30 * time.Second) // 演示：运行 30 秒后退出
		log.Println("开始优雅关闭...")
		cancel()
	}()

	var wg sync.WaitGroup

	// Stage 1: Watcher（单 goroutine，fsnotify 非线程安全）
	wg.Add(1)
	go func() {
		defer wg.Done()
		watcher.Run(ctx)
	}()

	// Stage 2: TailReader（单 goroutine，文件 offset 需顺序维护）
	wg.Add(1)
	go func() {
		defer wg.Done()
		reader.Run(ctx)
	}()

	// Stage 3: BatchWriter（单 goroutine，磁盘顺序写）
	wg.Add(1)
	go func() {
		defer wg.Done()
		writer.Run(ctx)
	}()

	// 等待所有组件退出
	wg.Wait()
	log.Println("监控程序已退出")
}
