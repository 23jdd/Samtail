# Samtail

samtail 是一个日志收集器，监控本地日志文件并批量发送到 [SamKv](https://github.com/23jdd/SamKv) 数据库。

## 架构

```
本地日志文件 ──→ Watcher ──→ TailReader ──→ Parser ──→ EntryBatcher ──→ HTTPWriter ──→ SamKv
                                                                         ├──→ 本地备份 (JSONL)
                                                                         └──→ 多后端扇出
```

### 管道阶段

| 组件 | 职责 |
|------|------|
| **Watcher** | 使用 fsnotify 监控目标目录，检测 `.log`/`.txt` 文件的创建、写入、重命名、删除 |
| **TailReader** | 按 inode 跟踪文件读取偏移量，断点续读，每 500ms 将状态写入 `meta.json` 持久化 |
| **Parser** | 将原始行解析为 `label=value,key=value\nmessage\n\n` 格式的结构化 `LogEntry` |
| **EntryBatcher** | 缓冲日志条目，达到批次大小或时间间隔时刷新到数据库 |
| **HTTPWriter** | POST JSON 到 SamKv 数据库端点，解析返回的序列号 |
| **DatabaseWriter** | 插件式后端：HTTP POST 到 SamKv、本地 JSONL 备份、多后端扇出 |

### 与 SamKv 的交互

samtail 作为**纯客户端**运行，向 SamKv 的 `/logs/batch` 端点发送 POST 请求：

```
samtail ──── POST /logs/batch ────→ SamKv
        ←──── 201 {"sequence":N} ────
```

- 请求格式：`{"entries":[{"labels":{...},"message":"..."}, ...]}`
- 成功响应：`201 Created`，body 为 `{"sequence":N}`，N 是 SamKv 自动分配的唯一序列号
- 序列号由 samtail 记录到日志中，用于可观测性和调试

---

## 日志格式

samtail 监控的日志文件遵循**标签行 + 消息行**格式：

```
label1=value1,label2=value2
message content

label3=value3
another message
```

### 格式规则

| 规则 | 说明 |
|------|------|
| 标签行 | 一个或多个逗号分隔的 `key=value` 对 |
| 消息行 | 紧跟在标签行之后的一行文本 |
| 分隔符 | 空白行分隔不同条目 |
| 空白处理 | 标签的 key 和 value 会被 TrimSpace |
| 重复 key | 最后一次出现的值生效 |
| 无效行 | 不含 `=` 的消息行会被跳过（带警告） |
| 孤儿标签 | 没有对应消息的标签行被丢弃 |

### 边界条件

| 场景 | 行为 |
|------|------|
| 空文件 | 不产生任何条目，不报错 |
| 文件末尾有标签无消息 | 标签被丢弃 |
| 连续两条标签行 | 第一条被丢弃，第二条保留 |
| 标签 key 为空 (`=value`) | 该键值对被跳过 |
| 标签 value 为空 (`key=`) | value 作为空字符串保留 |
| 只有空行的文件 | 不产生条目 |
| 消息包含特殊字符（`[`, `]`, URL 等） | 原样保留 |

### 日志文件示例

```
app=api,level=INFO,env=production
[2024-01-15] GET /users - 200 OK

app=api,level=ERROR,env=production
[2024-01-15] POST /login - 500 Internal Server Error

app=worker,env=production
Background job #42 completed successfully
```

上述文件解析后向 SamKv 发送 3 条结构化日志：

```json
{"entries":[
  {"labels":{"app":"api","env":"production","level":"INFO"},"message":"[2024-01-15] GET /users - 200 OK"},
  {"labels":{"app":"api","env":"production","level":"ERROR"},"message":"[2024-01-15] POST /login - 500 Internal Server Error"},
  {"labels":{"app":"worker","env":"production"},"message":"Background job #42 completed successfully"}
]}
```

---

## 快速开始

### 构建

```bash
# 需要 Go 1.25+
go build -o samtail .

# 运行测试
go test -v ./...
```

### 基本运行

```bash
# 使用默认配置：监控 ./logs，发送到本机 SamKv
./samtail

# 通过环境变量配置
SAMTAIL_DIR="./app_logs" \
SAMTAIL_DB_URL="http://samkv.example.com:6379/logs/batch" \
SAMTAIL_BATCH_SIZE=500 \
SAMTAIL_FLUSH_SECS=5 \
./samtail
```

### 准备监控目录

```bash
mkdir -p logs
echo "app=api,level=INFO" > logs/app.log
echo "request started" >> logs/app.log
./samtail  # 自动扫描并开始监控，批量发送到 SamKv
```

---

## 环境变量配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SAMTAIL_DIR` | `logs` | 监控的日志文件目录 |
| `SAMTAIL_OUTPUT` | `./output` | 本地备份文件目录 |
| `SAMTAIL_DB_URL` | `http://127.0.0.1:6379/logs/batch` | SamKv 数据库 HTTP 端点 |
| `SAMTAIL_BATCH_SIZE` | `1000` | 触发刷新的最大条目数 |
| `SAMTAIL_FLUSH_SECS` | `2` | 定时刷新的时间间隔（秒） |

---

## DatabaseWriter 后端

系统支持多种后端同时写入：

```go
// SamKv 数据库 + 本地备份
db := NewMultiWriter(
    NewHTTPWriter("http://samkv:6379/logs/batch", 10*time.Second),
    fileWriter,
)
```

| 实现 | 用途 | 特性 |
|------|------|------|
| `HTTPWriter` | 发送到 SamKv | POST JSON，期望 `201` + `{"sequence":N}`，3 次重试+指数退避，5xx 重试/4xx 不重试，超时控制 |
| `FileWriter` | 本地审计 | JSONL 追加写入，每次写入后 fsync，线程安全 |
| `MultiWriter` | 多后端 | 扇出写入到多个后端，单个失败不影响其他 |
| `NoopWriter` | 测试/开发 | 丢弃所有条目，始终成功 |

### HTTPWriter 重试策略

```
第 1 次失败 (5xx/网络) → 等待 100ms → 重试
第 2 次失败 (5xx/网络) → 等待 200ms → 重试
第 3 次失败 (5xx/网络) → 等待 400ms → 重试
第 4 次失败 → 返回错误
4xx 客户端错误 → 不重试，直接返回错误
```

---

## 项目结构

```
samtail/
├── Samtail.go         # 主入口：管道组装、Watcher、TailReader
├── entry.go           # LogEntry 类型、BatchRequest/BatchResponse
├── parser.go          # 日志格式解析器（标签行+消息行）
├── batcher.go         # EntryBatcher：大小/时间触发的批量刷新
├── database.go        # DatabaseWriter 接口及实现（HTTPWriter、FileWriter 等）
├── filed.go           # 跨平台文件 ID 类型定义
├── filed_unix.go      # Unix 文件标识（Dev + Ino）
├── filed_windows.go   # Windows 文件标识（VolumeSerial + FileIndex）
├── *_test.go          # 单元测试和集成测试
├── go.mod / go.sum    # Go 模块定义
└── README.md
```

---

## 测试

```bash
# 运行全部测试
go test -v ./...

# 按模块运行
go test -v -run TestParseLabelLine ./...
go test -v -run TestIntegration ./...
go test -v -run TestHTTPWriter ./...
```

### 测试覆盖范围

| 模块 | 测试文件 | 关键用例 |
|------|---------|---------|
| LogEntry | `entry_test.go` | 构造、验证、JSON 往返、边界条件 |
| Parser | `parser_test.go` | 标签行解析（18 场景）、流解析（11 场景）、管道实时解析、孤儿标签、优雅关闭 |
| DatabaseWriter | `database_test.go` | NoopWriter、FileWriter 读写与并发、MultiWriter 扇出与部分失败 |
| HTTPWriter | `httpdb_test.go` | SamKv 201 响应、序列号解析、重试、不重试 4xx、上下文取消、空批次、关闭后拒绝 |
| EntryBatcher | `batcher_test.go` | 按数量刷新、按时间刷新、批量添加、失败重入队、关闭后拒绝、并发写入 |
| 集成测试 | `integration_test.go` | 完整管道、HTTPWriter→SamKv、日志格式端到端、批次刷新、MultiWriter 扇出 |
| 使用示例 | `example_test.go` | 解析、批量写入、HTTPWriter、多后端、完整管道、验证 |

### 关键边界条件测试

- **并发写入**：10 个 goroutine × 100 个条目并发写入 FileWriter
- **重试机制**：HTTPWriter 在 5xx 错误时重试，4xx 错误时不重试
- **上下文取消**：HTTPWriter 在重试等待期间响应 context 取消
- **失败重入队**：数据库写入失败时条目自动重新入队，防止数据丢失
- **空批次**：所有实现正确处理空 entries（无 HTTP 请求、无文件写入）
- **关闭后写入**：关闭后的 writer 和 batcher 拒绝新的写入，返回错误

---

## 许可

MIT



