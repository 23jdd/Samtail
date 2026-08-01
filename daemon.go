package main

import (
	"fmt"
	"log"
	"os"
)

// daemonize 将当前进程转为后台守护进程。
// 如果 pidFile 非空，将 PID 写入该文件。
func daemonize(pidFile string) {
	// 已为守护进程（环境变量标记）则跳过
	if os.Getenv("SAMTAIL_DAEMON") == "1" {
		return
	}

	if pidFile == "" {
		pidFile = "samtail.pid"
	}

	// 重新执行自身，带 SAMTAIL_DAEMON=1 标记
	args := make([]string, 0, len(os.Args))
	for _, a := range os.Args {
		if a == "-d" || a == "--daemon" {
			continue
		}
		args = append(args, a)
	}

	p, err := os.StartProcess(os.Args[0], args, &os.ProcAttr{
		Env:   append(os.Environ(), "SAMTAIL_DAEMON=1"),
		Files: []*os.File{nil, nil, nil}, // 分离 stdin/stdout/stderr
	})
	if err != nil {
		log.Fatalf("启动守护进程失败: %v", err)
	}

	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", p.Pid)), 0644); err != nil {
		log.Printf("写入 PID 文件失败: %v", err)
	}

	log.Printf("守护进程已启动 (PID: %d, pidfile: %s)", p.Pid, pidFile)
	os.Exit(0)
}
