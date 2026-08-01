//go:build: windows

package main

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// Get 返回 path 指向的文件唯一标识
func Get(path string) (string, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return "", fmt.Errorf("invalid path %s: %w", path, err)
	}

	// FILE_FLAG_BACKUP_SEMANTICS 允许打开目录
	h, err := windows.CreateFile(
		p,
		0, // dwDesiredAccess: 不需要读/写
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer windows.CloseHandle(h)

	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil {
		return "", fmt.Errorf("get info %s: %w", path, err)
	}

	vol := uint64(info.VolumeSerialNumber)
	idx := uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow)

	return ID{A: vol, B: idx}.String(), nil
}

// GetLstat 在 Windows 上等同于 Get（符号链接本身没有独立的 FileIndex）
func GetLstat(path string) (string, error) {
	return Get(path)
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
