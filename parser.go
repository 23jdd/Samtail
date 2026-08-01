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

// Parser 按 label=value,\nmessage\n\n 格式将 LogLine 流解析为 LogEntry。
//
//   - 标签行 = 逗号分隔的 key=value 对
//   - 消息行紧跟在标签行之后
//   - 空行分隔不同条目
type Parser struct {
	lineChan  <-chan LogLine
	entryChan chan<- LogEntry

	pendingLabels map[string]string
	pendingSource string
	mu            sync.Mutex
}

func NewParser(lineChan <-chan LogLine, entryChan chan<- LogEntry) *Parser {
	return &Parser{
		lineChan:      lineChan,
		entryChan:     entryChan,
		pendingLabels: make(map[string]string),
	}
}

// Run 阻塞直到 ctx 取消或 lineChan 关闭。流结束时丢弃剩余孤儿标签。
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

func (p *Parser) processLine(line LogLine) {
	content := strings.TrimSpace(line.Content)

	if len(content) == 0 {
		return
	}

	labels := parseLabelLine(content)
	if len(labels) > 0 {
		if len(p.pendingLabels) > 0 {
			log.Printf("[Parser] 丢弃孤儿标签 (来自 %s): %v", p.pendingSource, p.pendingLabels)
		}
		p.pendingLabels = labels
		p.pendingSource = line.FilePath
		return
	}

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

	log.Printf("[Parser] 跳过无标签的行: %s", content)
}

// parseLabelLine 将 "key1=val1,key2=val2" 解析为 map。不含 '=' 则返回 nil。
// 空 key 跳过，重复 key 取最后一个，逗号周围空白去除。
func parseLabelLine(line string) map[string]string {
	if len(strings.TrimSpace(line)) == 0 {
		return nil
	}
	if !strings.Contains(line, "=") {
		return nil
	}

	labels := make(map[string]string)
	for _, pair := range strings.Split(line, ",") {
		pair = strings.TrimSpace(pair)
		if len(pair) == 0 {
			continue
		}
		idx := strings.Index(pair, "=")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(pair[:idx])
		if len(key) == 0 {
			continue
		}
		labels[key] = strings.TrimSpace(pair[idx+1:])
	}

	if len(labels) == 0 {
		return nil
	}
	return labels
}

// ParseLogStream 一次性读取整个日志流并返回所有解析后的 entry，用于测试和批量处理。
// 末尾不完整的标签会被丢弃。
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
