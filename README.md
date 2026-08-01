# Samtail

一个轻量级日志收集器，监控本地日志文件并将其批量发送到数据库后端。

## 架构

```
本地日志文件 ──→ Watcher ──→ TailReader ──→ Parser ──┐
                                                       ├──→ EntryBatcher ──→ DatabaseWriter ──→ 远程数据库
外部服务 ────→ POST /logs/batch ──→ HTTP Server ───────┘                      ├──→ 本地备份文件 (JSONL)
                                                                              └──→ 多后端扇出
```

### 管道阶段

| 组件 | 职责 |
|------|------|
| **Watcher** | 使用 fsnotify 监控目标目录，检测 `.log`/`.txt` 文件的创建、写入、重命名、删除 |
| **TailReader** | 按 inode 跟踪文件读取偏移量，断点续读，每 500ms 将状态写入 `meta.json` 持久化 |
| **Parser** | 将原始行解析为 `label=value,key=value\nmessage\n\n` 格式的结构化 `LogEntry` |
| **EntryBatcher** | 缓冲日志条目，达到批次大小或时间间隔时刷新到数据库 |
| **DatabaseWriter** | 写入后端：HTTP POST 到远程数据库、本地 JSONL 备份文件、多后端扇出 |
| **HTTP Server** | 提供 `POST /logs/batch` 端点接收外部日志条目 |

---

## 日志格式

支持从文件读取和通过 HTTP 发送两种输入。日志文件遵循**标签行 + 消息行**的格式：

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

上述文件解析后产生 3 条结构化日志：

```json
{"labels":{"app":"api","env":"production","level":"INFO"},"message":"[2024-01-15] GET /users - 200 OK"}
{"labels":{"app":"api","env":"production","level":"ERROR"},"message":"[2024-01-15] POST /login - 500 Internal Server Error"}
{"labels":{"app":"worker","env":"production"},"message":"Background job #42 completed successfully"}
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
# 使用默认配置
./samtail

# 通过环境变量配置
SAMTAIL_DIR="./app_logs" \
SAMTAIL_DB_URL="http://db.example.com:8080/logs/batch" \
SAMTAIL_LISTEN=":8080" \
SAMTAIL_BATCH_SIZE=500 \
SAMTAIL_FLUSH_SECS=5 \
./samtail
```

### 监控目录准备

```bash
mkdir -p logs
echo "app=api,level=INFO" > logs/app.log
echo "request started" >> logs/app.log
./samtail  # 自动扫描并开始监控
```

---

## 环境变量配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SAMTAIL_DIR` | `logs` | 监控的日志文件目录 |
| `SAMTAIL_OUTPUT` | `./output` | 本地备份文件目录 |
| `SAMTAIL_DB_URL` | `http://127.0.0.1:9999/logs/batch` | 远程数据库 HTTP 端点 |
| `SAMTAIL_LISTEN` | `:9999` | HTTP 服务器监听地址 |
| `SAMTAIL_BATCH_SIZE` | `1000` | 触发刷新的最大条目数 |
| `SAMTAIL_FLUSH_SECS` | `2` | 定时刷新的时间间隔（秒） |

---

## HTTP API

### POST /logs/batch

接收一批日志条目。

**请求：**

```bash
curl -X POST http://127.0.0.1:9999/logs/batch \
  -H "Content-Type: application/json" \
  -d '{
    "entries":[
      {"labels":{"app":"api"},"message":"request started"},
      {"labels":{"app":"api","level":"ERROR"},"message":"request failed"}
    ]
  }'
```

**请求体结构：**
```json
{
  "entries": [
    {
      "labels": {"key1": "value1", "key2": "value2"},
      "message": "log message text",
      "timestamp": "2024-01-15T10:30:00Z"  // 可选，不提供则自动填充
    }
  ]
}
```

**成功响应 (200):**
```json
{"accepted": 2, "status": "ok"}
```

**错误响应：**

| 状态码 | 原因 |
|--------|------|
| 400 | JSON 格式无效 |
| 405 | 请求方法不是 POST |
| 413 | 请求体超过 1MB |
| 415 | Content-Type 不是 `application/json` |

**边界条件：**

| 场景 | 行为 |
|------|------|
| `entries` 为空数组 `[]` | 返回 200，`accepted=0`，不执行写入 |
| 某条 entry 的 message 为空 | 该条目被跳过，有效条目正常处理 |
| 某条 entry 的 label key 为空 | 该条目被跳过 |
| 全部条目无效 | 返回 200，`accepted=0` |
| timestamp 未提供 | 自动填充为服务器当前时间 |

---

### GET /health

健康检查端点。

```bash
curl http://127.0.0.1:9999/health
# {"status": "ok", "time": "2024-01-15T10:30:00Z"}
```

---

### GET /metrics

基本指标端点。

```bash
curl http://127.0.0.1:9999/metrics
# {"queue_depth": 42, "time": "2024-01-15T10:30:00Z"}
```

---

## DatabaseWriter 后端

系统支持多种后端同时写入：

```go
// 远程数据库 + 本地备份
db := NewMultiWriter(
    NewHTTPWriter("http://db.example.com/logs", 10*time.Second),
    fileWriter,
)
```

| 实现 | 用途 | 特性 |
|------|------|------|
| `HTTPWriter` | 远程数据库 | POST JSON 到 HTTP 端点，3 次重试+指数退避，5xx 重试/4xx 不重试，请求超时控制 |
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
├── Samtail.go           # 主入口：管道组装、Watcher、TailReader
├── entry.go             # LogEntry 类型、验证、JSON 序列化
├── parser.go            # 日志格式解析器（标签行+消息行）
├── batcher.go           # EntryBatcher：大小/时间触发的批量刷新
├── database.go          # DatabaseWriter 接口及实现
├── server.go            # HTTP 服务器（/logs/batch、/health、/metrics）
├── filed.go             # 跨平台文件 ID 类型定义
├── filed_unix.go        # Unix 文件标识（Dev + Ino）
├── filed_windows.go     # Windows 文件标识（VolumeSerial + FileIndex）
├── *_test.go            # 单元测试和集成测试（50+ 用例）
├── go.mod / go.sum      # Go 模块定义
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
go test -v -run TestServer ./...
```

### 测试覆盖范围

| 模块 | 测试文件 | 用例数 |
|------|---------|--------|
| LogEntry | `entry_test.go` | 16 |
| Parser | `parser_test.go` | 29 |
| DatabaseWriter | `database_test.go` | 12 |
| HTTPWriter | `httpdb_test.go` | 7 |
| EntryBatcher | `batcher_test.go` | 10 |
| HTTP Server | `server_test.go` | 14 |
| 集成测试 | `integration_test.go` | 5 |
| 使用示例 | `example_test.go` | 6 |

### 关键边界条件测试

- **并发写入**：10 个 goroutine × 100 个条目并发写入 FileWriter
- **重试机制**：HTTPWriter 在 5xx 错误时重试，4xx 错误时不重试
- **上下文取消**：HTTPWriter 在重试等待期间响应 context 取消
- **失败重回队**：数据库写入失败时条目自动重新入队，防止数据丢失
- **空批次**：所有实现正确处理空 entries
- **关闭后写入**：关闭后的 writer 和 batcher 拒绝新的写入
- **超大请求**：超过 1MB 的请求体返回 413

---

## 许可

MIT



