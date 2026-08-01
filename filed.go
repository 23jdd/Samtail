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

// IsZero 判断是否为无效标识
func (id ID) IsZero() bool {
	return id.A == 0 && id.B == 0
}

// Equal 比较两个 ID 是否相同
func (id ID) Equal(other ID) bool {
	return id.A == other.A && id.B == other.B
}