package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
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
	Inode  uint64 // 通过 inode 识别文件，不受重命名影响
	Offset int64  // 当前读取偏移量
	Path   string // 当前路径（可能因轮转改变）
}

// Watcher
type Watcher struct {
	watcher   *fsnotify.Watcher
	eventChan chan LogEvent
	targetDir string
}

func NewWatcher(dir string) (*Watcher, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := fw.Add(dir); err != nil {
		return nil, err
	}
	return &Watcher{
		watcher:   fw,
		eventChan: make(chan LogEvent, 256), // 缓冲避免事件丢失
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

// TailReader：文件读取 + Offset 跟踪 =
type TailReader struct {
	eventChan chan LogEvent
	lineChan  chan LogLine
	states    map[uint64]*FileState // inode -> state
	mu        sync.RWMutex
}

func NewTailReader(eventChan chan LogEvent, lineChan chan LogLine) *TailReader {
	return &TailReader{
		eventChan: eventChan,
		lineChan:  lineChan,
		states:    make(map[uint64]*FileState),
	}
}

func (t *TailReader) Run(ctx context.Context) {
	// 定期扫描已打开文件的写入（兜底，防止 fsnotify 漏事件）
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-t.eventChan:
			if !ok {
				return
			}
			t.handleEvent(evt)
		case <-ticker.C:
			t.pollAllFiles()
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
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	// 获取文件 inode
	info, err := file.Stat()
	if err != nil {
		return
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return // 非 Unix 系统需适配
	}
	inode := stat.Ino

	t.mu.Lock()
	state, exists := t.states[inode]
	if !exists {
		// 新文件：从开头或末尾读取？生产环境通常从新文件开头读
		state = &FileState{Inode: inode, Offset: 0, Path: path}
		t.states[inode] = state
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
	if s, ok := t.states[inode]; ok {
		s.Offset = offset
	}
	t.mu.Unlock()
}

// 定期轮询：处理 fsnotify 可能遗漏的持续写入
func (t *TailReader) pollAllFiles() {
	t.mu.RLock()
	paths := make([]string, 0, len(t.states))
	for _, state := range t.states {
		paths = append(paths, state.Path)
	}
	t.mu.RUnlock()

	for _, path := range paths {
		t.readFile(path)
	}
}

func (t *TailReader) closeFile(path string) {
	// 可选：清理已删除/重命名文件的 state（保守策略可保留，防止轮转时丢失）
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
	targetDir := "./logs"    // 监控目录
	outputDir := "./output"  // 存储目录

	// 创建带取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建 Channel（带缓冲，解耦生产消费速率）
	eventChan := make(chan LogEvent, 256)  // Watcher -> TailReader
	lineChan := make(chan LogLine, 5000)   // TailReader -> BatchWriter

	// 初始化组件
	watcher, err := NewWatcher(targetDir)
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