package main

import (
	"fmt"
)

// ID 表示系统范围内唯一的文件标识
type ID struct {
	// Unix: (Dev, Ino)
	// Windows: (VolumeSerial, FileIndex)
	A uint64
	B uint64
}

func (id ID) String() string {
	return fmt.Sprintf("%016x:%016x", id.A, id.B)
}
