//go:build unix

package main

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Get 返回 path 指向的文件唯一标识（跟随符号链接）
func Get(path string) (string, error) {
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return "", fmt.Errorf("stat %s: %w", path, err)
	}
	return ID{A: uint64(stat.Dev), B: stat.Ino}.String(), nil
}

// GetLstat 返回 path 本身的唯一标识（不跟随符号链接）
func GetLstat(path string) (string, error) {
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil {
		return "", fmt.Errorf("lstat %s: %w", path, err)
	}
	return ID{A: uint64(stat.Dev), B: stat.Ino}.String(), nil
}

// Same 判断两个路径是否指向同一文件
func Same(a, b string) (bool, error) {
	ida, err := Get(a)
	if err != nil {
		return false, err
	}
	idb, err := Get(b)
	if err != nil {
		return false, err
	}
	return ida == idb, nil
}
