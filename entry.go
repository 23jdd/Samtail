package main

import (
	"encoding/json"
	"sort"
	"time"
)

// LogEntry 表示一条解析后的日志条目，包含标签和消息。
//
// 它是整个管道中统一的内部表示：从文件读取的日志解析为 LogEntry，
// 然后由 HTTPWriter 发送给 SamKv。
//
// 原始日志文件格式示例：
//
//	app=api,level=INFO
//	request started
//
//	app=api,level=ERROR
//	request failed
//
// 上述格式会解析为两个 LogEntry 对象：
//
//	{Labels: {"app":"api","level":"INFO"}, Message: "request started"}
//	{Labels: {"app":"api","level":"ERROR"}, Message: "request failed"}
type LogEntry struct {
	Labels    map[string]string `json:"labels"`
	Message   string            `json:"message"`
	Timestamp time.Time         `json:"timestamp,omitempty"`
}

// NewLogEntry 创建一个 LogEntry，labels 为 nil 时自动初始化为空 map。
//
// 边界条件：
//   - labels 可以为 nil（自动创建空 map）
//   - message 可以为空（合法，表示空日志行）
//   - timestamp 默认为零值时间
func NewLogEntry(labels map[string]string, message string) LogEntry {
	if labels == nil {
		labels = make(map[string]string)
	}
	return LogEntry{
		Labels:  labels,
		Message: message,
	}
}

// WithTimestamp 返回设置了指定时间戳的副本。
//
// 用法：
//
//	entry := NewLogEntry(map[string]string{"app":"api"}, "started").WithTimestamp(time.Now())
func (e LogEntry) WithTimestamp(t time.Time) LogEntry {
	e.Timestamp = t
	return e
}

// GetLabel 根据 key 返回标签值，不存在时返回空字符串。
func (e LogEntry) GetLabel(key string) string {
	if e.Labels == nil {
		return ""
	}
	return e.Labels[key]
}

// HasLabel 判断 entry 是否包含给定的标签 key。
func (e LogEntry) HasLabel(key string) bool {
	_, ok := e.Labels[key]
	return ok
}

// LabelKeys 返回所有标签 key 的排序列表，便于一致的遍历。
func (e LogEntry) LabelKeys() []string {
	keys := make([]string, 0, len(e.Labels))
	for k := range e.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Validate 检查 entry 是否合法。
//
// 边界条件：
//   - Message 为空时返回错误（无有效日志内容）
//   - Labels 可以为空（合法但少见）
//   - Label key 或 value 为空时被拒绝
func (e LogEntry) Validate() error {
	if len(e.Message) == 0 {
		return &ValidationError{Field: "message", Reason: "message must not be empty"}
	}
	for k, v := range e.Labels {
		if len(k) == 0 {
			return &ValidationError{Field: "labels", Reason: "label key must not be empty"}
		}
		if len(v) == 0 {
			return &ValidationError{Field: "labels", Reason: "label value must not be empty for key \"" + "'\" (empty key)"}
		}
	}
	return nil
}

// ValidationError 在 LogEntry 校验不通过时返回。
type ValidationError struct {
	Field  string
	Reason string
}

func (ve *ValidationError) Error() string {
	return "validation error: " + ve.Field + ": " + ve.Reason
}

// BatchRequest 是 POST /logs/batch 端点的顶层 JSON 结构。
//
// JSON 请求体示例：
//
//	{
//	  "entries": [
//	    {"labels":{"app":"api"},"message":"request started"},
//	    {"labels":{"app":"api","level":"ERROR"},"message":"request failed"}
//	  ]
//	}
type BatchRequest struct {
	Entries []LogEntry `json:"entries"`
}

// UnmarshalJSON 实现 BatchRequest 的自定义 JSON 反序列化，用于解析后校验 entries。
func (br *BatchRequest) UnmarshalJSON(data []byte) error {
	type alias BatchRequest
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*br = BatchRequest(a)
	return nil
}

// BatchResponse 是 SamKv 返回的自动分配序列号数组，每个元素对应提交批次中的一条 entry，按顺序排列。
//
// 响应体示例：
//
//	[1, 2, 3]
//
// 表示提交的 3 条 entry 分别被分配了序列号 1、2、3。
type BatchResponse []int64
