package main

import (
	"encoding/json"
	"sort"
	"time"
)

// LogEntry 一条解析后的日志条目，包含标签和消息。
type LogEntry struct {
	Labels    map[string]string `json:"labels"`
	Message   string            `json:"message"`
	Timestamp time.Time         `json:"timestamp,omitempty"`
}

// NewLogEntry 创建 LogEntry，labels 为 nil 时自动初始化为空 map。
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

// HasLabel 判断是否包含给定的标签 key。
func (e LogEntry) HasLabel(key string) bool {
	_, ok := e.Labels[key]
	return ok
}

// LabelKeys 返回所有标签 key 的排序列表。
func (e LogEntry) LabelKeys() []string {
	keys := make([]string, 0, len(e.Labels))
	for k := range e.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Validate 校验 entry：message 不可为空，label key/value 不可为空。
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

// ValidationError LogEntry 校验失败时返回。
type ValidationError struct {
	Field  string
	Reason string
}

func (ve *ValidationError) Error() string {
	return "validation error: " + ve.Field + ": " + ve.Reason
}

// BatchRequest POST /logs/batch 的请求体结构。
type BatchRequest struct {
	Entries []LogEntry `json:"entries"`
}

func (br *BatchRequest) UnmarshalJSON(data []byte) error {
	type alias BatchRequest
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*br = BatchRequest(a)
	return nil
}

// BatchResponse SamKv 返回的序列号数组，每元素对应一条 entry，按顺序排列。如 [1,2,3]。
type BatchResponse []int64
