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

// Parser 读取原始 LogLine 输入，按照 label=value,\nmessage\n\n 格式组装为 LogEntry 对象。
//
// 日志格式规范：
//
//	label1=value1,label2=value2
//	message content
//
//	label3=value3
//	another message
//
// 规则：
//   - 标签行包含一个或多个逗号分隔的 key=value 对。
//   - 消息行紧跟在标签行之后。
//   - 条目之间由一个或多个空白行分隔。
//   - 标签的 key 和 value 会去除首尾空白。
//   - 不匹配格式的行会被跳过并记录警告日志。
//   - 每当解析出一组完整的"标签+消息"对，立即发送一条 LogEntry。
//
// 边界条件：
//   - 空输入流：不产生条目，不报错。
//   - 有标签行但无消息：等待下一条非空非标签行作为消息（流结束时丢弃）。
//   - 有消息行但无标签：跳过该行并记录警告。
//   - Label 行含无效的 key=value 对：跳过该对，保留有效的。
//   - 重复的标签 key：最后一次出现的值生效。
//   - 超长行（> bufio.MaxScanTokenSize）：由 bufio.Scanner 处理。
type Parser struct {
	lineChan <-chan LogLine
	// entryChan 向下游发送解析后的 LogEntry 对象
	entryChan chan<- LogEntry

	// pendingLabels 保存当前累积的标签集合。
	// 当下一行是非空非标签行时，该行成为待定标签的消息，并发送一条 entry。
	pendingLabels map[string]string
	pendingSource string
	mu            sync.Mutex
}

// NewParser 创建一个从 lineChan 读取、向 entryChan 写入的 Parser。
//
// 参数：
//   - lineChan: 原始 LogLine 输入 channel（通常来自 TailReader）
//   - entryChan: 解析后的 LogEntry 输出 channel（通常流向 EntryBatcher）
//
// entryChan 应该是带缓冲的 channel，以避免慢消费者阻塞解析器。
func NewParser(lineChan <-chan LogLine, entryChan chan<- LogEntry) *Parser {
	return &Parser{
		lineChan:      lineChan,
		entryChan:     entryChan,
		pendingLabels: make(map[string]string),
	}
}

// Run 启动解析循环，阻塞直到 ctx 被取消或 lineChan 被关闭。
// 输入流结束时，任何没有消息的待定标签会被丢弃。
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

// processLine 处理单行输入，组装 entry。
func (p *Parser) processLine(line LogLine) {
	content := strings.TrimSpace(line.Content)

	// 空行仅作为分隔符。如果有待定标签但遇到了空行，
	// 继续等待消息行。只有收到非标签非空行时才发送 entry。
	if len(content) == 0 {
		return
	}

	// 先尝试作为标签行解析
	labels := parseLabelLine(content)
	if len(labels) > 0 {
		// 丢弃之前未完成的待定标签（两条连续标签行的情况，第一条视为无效）
		if len(p.pendingLabels) > 0 {
			log.Printf("[Parser] 丢弃孤儿标签 (来自 %s): %v", p.pendingSource, p.pendingLabels)
		}

		p.pendingLabels = labels
		p.pendingSource = line.FilePath
		return
	}

	// 不是标签行。如果有待定标签，则本行即为消息。
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
			log.Printf("[Parser] entryChan 已满，丢弃 entry: %s", content)
		}
		return
	}

	// 消息行没有前置标签：跳过并记录警告
	log.Printf("[Parser] 跳过无标签的行: %s", content)
}

// parseLabelLine 将类似 "key1=val1,key2=val2" 的行解析为 map。
// 如果该行不是合法的标签行（不含 =），返回 nil。
//
// 边界条件：
//   - 空输入：返回 nil
//   - 行中无 '=' 字符：返回 nil
//   - key 无 value（"key="）：value 为空字符串
//   - value 无 key（"=val"）：跳过该对
//   - 逗号和等号周围的空白：去除
//   - 重复 key：最后一次出现的值生效
//   - 当前不支持 value 中的逗号转义（逗号始终作为键值对分隔符）
func parseLabelLine(line string) map[string]string {
	if len(strings.TrimSpace(line)) == 0 {
		return nil
	}

	// 标签行至少含有一个 '='
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
			// 不含等号，跳过
			continue
		}

		key := strings.TrimSpace(pair[:idx])
		if len(key) == 0 {
			// key 为空，跳过
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

// ParseLogStream 读取整个日志流并返回所有解析后的 entry。
// 这是一个便捷函数，用于测试和批量处理。
//
// 用法：
//
//	reader := strings.NewReader("app=api\nstarted\n\napp=db\nerror\n")
//	entries, err := ParseLogStream(reader, "test.log")
//
// 边界条件：
//   - 空 reader：返回 nil entries 和 nil error
//   - 末尾不完整 entry（有标签无消息）：标签被丢弃
//   - 最大行长度：受 bufio.Scanner 默认值（64KB）限制
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
				// 丢弃孤儿标签
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
