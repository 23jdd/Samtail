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

// Parser 将 LogLine 流按 "标签行 + 消息行" 格式解析为 LogEntry。
//
// 格式规范：
//   - 标签行：逗号分隔的 key=value 对，如 "app=api,level=INFO"
//   - 消息行：紧跟在标签行之后的一行文本
//   - 条目间用空行分隔
//   - 连续两条标签行时前者丢弃（孤儿标签）
//   - 无前置标签的消息行被跳过并记录警告
type Parser struct {
	lineChan  <-chan LogLine  // 输入：来自 TailReader 的原始行
	entryChan chan<- LogEntry // 输出：解析后的 LogEntry，流向 EntryBatcher

	// 待定标签：收到标签行后暂存，等待下一非标签非空行作为消息
	pendingLabels map[string]string
	pendingSource string // 标签来源文件路径，用于日志
	mu            sync.Mutex
}

// NewParser 创建解析器。entryChan 应为带缓冲 channel，避免慢消费者阻塞。
func NewParser(lineChan <-chan LogLine, entryChan chan<- LogEntry) *Parser {
	return &Parser{
		lineChan:      lineChan,
		entryChan:     entryChan,
		pendingLabels: make(map[string]string),
	}
}

// Run 启动解析循环。阻塞直到 ctx 取消或 lineChan 关闭。
// 流结束时丢弃残留的孤儿标签（有标签无消息）。
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

// processLine 处理单行，维护 pendingLabels 状态机。
// 三种情况：
//  1. 当前行是标签行 → 更新 pendingLabels（旧标签作为孤儿丢弃）
//  2. 当前行非标签且有待定标签 → 组装 LogEntry 发送
//  3. 当前行非标签且无待定标签 → 跳过并警告
func (p *Parser) processLine(line LogLine) {
	content := strings.TrimSpace(line.Content)

	// 空行只做分隔，不影响待定状态
	if len(content) == 0 {
		return
	}

	// 尝试作为标签行解析
	labels := parseLabelLine(content)
	if len(labels) > 0 {
		if len(p.pendingLabels) > 0 {
			log.Printf("[Parser] 丢弃孤儿标签 (来自 %s): %v", p.pendingSource, p.pendingLabels)
		}
		p.pendingLabels = labels
		p.pendingSource = line.FilePath
		return
	}

	// 非标签行：如果有待定标签，组装 entry 发送
	if len(p.pendingLabels) > 0 {
		entry := LogEntry{
			Labels:    p.pendingLabels,
			Message:   content,
			Timestamp: line.Timestamp,
		}
		p.pendingLabels = nil

		// 非阻塞发送，channel 满时丢弃（背压保护）
		select {
		case p.entryChan <- entry:
		default:
			log.Printf("[Parser] entryChan 已满，丢弃 entry: %s", content)
		}
		return
	}

	// 无前置标签的消息行，跳过
	log.Printf("[Parser] 跳过无标签的行: %s", content)
}

// parseLabelLine 将 "key1=val1,key2=val2" 解析为 map[string]string。
//
// 解析规则：
//   - 第一个 '=' 为 key/value 分隔符（value 中可以包含 '='）
//   - 空 key（=val 或 ,=val）的键值对被跳过
//   - 重复 key 取最后一次出现的值
//   - 不含 '=' 的行返回 nil
func parseLabelLine(line string) map[string]string {
	if len(strings.TrimSpace(line)) == 0 {
		return nil
	}
	if !strings.Contains(line, "=") {
		return nil // 不含 '=' 的不是标签行
	}

	labels := make(map[string]string)
	for _, pair := range strings.Split(line, ",") {
		pair = strings.TrimSpace(pair)
		if len(pair) == 0 {
			continue
		}
		idx := strings.Index(pair, "=")
		if idx < 0 {
			continue // 无等号，跳过
		}
		key := strings.TrimSpace(pair[:idx])
		if len(key) == 0 {
			continue // 空 key，跳过
		}
		labels[key] = strings.TrimSpace(pair[idx+1:])
	}

	if len(labels) == 0 {
		return nil
	}
	return labels
}

// ParseLogStream 一次性读取整个日志流并返回所有解析后的 entry。
// 用于测试和批量处理，末尾不完整的标签会被丢弃。
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
			pendingLabels = labels // 覆盖旧标签（孤儿标签自动丢弃）
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
