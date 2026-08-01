package main

import (
	"bufio"
	"os"
	"strings"
)

// loadEnvFile 读取 .env 格式的配置文件，将 KEY=VALUE 行设为环境变量。
// 支持 # 开头的注释行和空行。如果文件不存在则返回 nil（可选配置文件）。
func loadEnvFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 配置文件不存在不报错
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// 跳过空行和注释
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		// 解析 KEY=VALUE（取第一个 =）
		idx := strings.Index(line, "=")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		// 去除引号
		val = strings.Trim(val, `"'`)
		os.Setenv(key, val)
	}
	return scanner.Err()
}
