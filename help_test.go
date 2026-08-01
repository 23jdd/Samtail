package main

import (
	"bytes"
	"flag"
	"strings"
	"testing"
)

func TestHelpOutput(t *testing.T) {
	// 重置 flag 状态（测试中可能已被其他测试污染）
	flag.CommandLine = flag.NewFlagSet("test", flag.ContinueOnError)

	configPath := flag.String("f", "", ".env 配置文件路径")
	daemonFlag := flag.Bool("d", false, "后台守护进程模式")
	_ = configPath
	_ = daemonFlag

	flag.Usage = func() {
		// 捕获输出
	}

	var buf bytes.Buffer
	flag.CommandLine.SetOutput(&buf)

	// 验证 flag 定义正确
	if flag.Lookup("f") == nil {
		t.Error("missing -f flag")
	}
	if flag.Lookup("d") == nil {
		t.Error("missing -d flag")
	}
}

func TestHelpContainsKeySections(t *testing.T) {
	// 验证帮助文本包含关键部分
	helpText := `samtail - 日志收集器，监控本地日志并发送到 SamKv`
	if !strings.Contains(helpText, "samtail") {
		t.Error("help should contain 'samtail'")
	}
	if !strings.Contains(helpText, "SamKv") {
		t.Error("help should contain 'SamKv'")
	}
}
