package main

import (
	"fmt"

	"github.com/djherbis/times"
)

// ID 表示系统范围内唯一的文件标识
type ID struct {
	// Unix: (Dev, Ino)
	// Windows: (VolumeSerial, FileIndex)
	Volume    uint64
	FileIndex uint64
	Path      string
}

func (id ID) String() string {
	t, err := times.Stat(id.Path)
	// not support seek file create time
	if err != nil {
		return fmt.Sprintf("%016x:%016x:", id.Volume, id.FileIndex)
	}
	if !t.HasBirthTime() {
		return fmt.Sprintf("%016x:%016x:", id.Volume, id.FileIndex)
	}
	return fmt.Sprintf("%016x:%016x:%v", id.Volume, id.FileIndex, t.BirthTime())
}
