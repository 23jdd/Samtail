package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

const (
	daemonEnv   = "_SAMTAIL_DAEMON"
	pidFileName = "samtail.pid"
)

// startDaemon 重新执行当前二进制，将输出追加到日志文件。
// 成功后父进程调用 os.Exit(0)。
func startDaemon(args ...string) error {
	if isRunning() {
		return fmt.Errorf("daemon already running")
	}

	exe, err := os.Executable()
	if err != nil {
		return err
	}
	exe, _ = filepath.Abs(exe)

	logFile := getLogFile()
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer f.Close()

	cmd := exec.Command(exe, args...)
	cmd.Env = append(os.Environ(), daemonEnv+"=1")
	cmd.Stdin = nil
	cmd.Stdout = f
	cmd.Stderr = f
	setProAttr(cmd)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("fork failed: %w", err)
	}

	savePID(cmd.Process.Pid)
	fmt.Printf("Daemon started, PID: %d\n", cmd.Process.Pid)
	fmt.Printf("Log file: %s\n", logFile)
	os.Exit(0)
	return nil
}

// stopDaemon 根据 PID 文件发送终止信号。
// Windows 使用 Kill，Unix 使用 SIGTERM。
func stopDaemon() error {
	pid, err := readPID()
	if err != nil {
		return fmt.Errorf("daemon not running")
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		os.Remove(getPIDFile())
		return fmt.Errorf("process not found")
	}

	if runtime.GOOS == "windows" {
		err = proc.Kill()
	} else {
		err = proc.Signal(syscall.SIGTERM)
	}

	if err != nil {
		return fmt.Errorf("send signal: %w", err)
	}

	os.Remove(getPIDFile())
	fmt.Printf("Daemon stopped, PID: %d\n", pid)
	return nil
}

// statusDaemon 检查守护进程运行状态。
func statusDaemon() {
	pid, err := readPID()
	if err != nil {
		fmt.Println("Daemon is not running")
		return
	}

	if runtime.GOOS != "windows" {
		proc, _ := os.FindProcess(pid)
		if proc != nil {
			if err := proc.Signal(syscall.Signal(0)); err != nil {
				fmt.Println("Daemon is not running (stale PID file)")
				os.Remove(getPIDFile())
				return
			}
		}
	}

	fmt.Printf("Daemon is running, PID: %d\n", pid)
	fmt.Printf("Log file: %s\n", getLogFile())
}

func isRunning() bool {
	_, err := readPID()
	return err == nil
}

func savePID(pid int) {
	path := getPIDFile()
	os.MkdirAll(filepath.Dir(path), 0755)
	os.WriteFile(path, []byte(strconv.Itoa(pid)), 0644)
}

func readPID() (int, error) {
	data, err := os.ReadFile(getPIDFile())
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(data))
}

func getPIDFile() string {
	if runtime.GOOS == "windows" {
		return filepath.Join(os.TempDir(), pidFileName)
	}
	if _, err := os.Stat("/var/run"); err == nil {
		return "/var/run/" + pidFileName
	}
	return filepath.Join(os.TempDir(), pidFileName)
}

func getLogFile() string {
	if runtime.GOOS == "windows" {
		return filepath.Join(os.TempDir(), "samtail.log")
	}
	return "/var/log/samtail.log"
}

func logf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	f, err := os.OpenFile(getLogFile(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "[%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), msg)
}
