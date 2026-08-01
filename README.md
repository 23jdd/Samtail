# Samtail

日志收集器，监控本地日志文件并批量发送到 [SamKv](https://github.com/23jdd/SamKv)。

## 快速开始

```bash
go build -o samtail .
./samtail                          # 默认配置
./samtail -f .env                  # 使用配置文件
./samtail -d                       # 后台守护进程
./samtail -d -f .env               # 守护进程 + 配置文件
```

配置文件示例见 [.env.example](.env.example)。

## 命令行参数

| 参数 | 说明 |
|------|------|
| `-f <path>` | 加载 .env 格式配置文件 |
| `-d` | 后台守护进程模式，PID 写入 `samtail.pid` |

## 配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SAMTAIL_DIR` | `logs` | 监控目录 |
| `SAMTAIL_DB_URL` | `http://127.0.0.1:6379/logs/batch` | SamKv 端点 |
| `SAMTAIL_OUTPUT` | `./output` | 本地备份目录 |
| `SAMTAIL_BATCH_SIZE` | `1000` | 批次大小 |
| `SAMTAIL_FLUSH_SECS` | `2` | 刷新间隔（秒） |

## 日志格式

```
label1=value1,label2=value2
消息内容

label3=value3
另一条消息
```

发送给 SamKv 的 JSON：

```json
{"entries":[
  {"labels":{"app":"api","level":"INFO"},"message":"request started"},
  {"labels":{"app":"api","level":"ERROR"},"message":"request failed"}
]}
```

SamKv 返回 `201 Created`，body 为序列号数组 `[1, 2, ...]`。

## 项目结构

```
samtail/
├── Samtail.go         # 主入口、Watcher、TailReader
├── entry.go           # LogEntry、BatchRequest/BatchResponse
├── parser.go          # label=value 格式解析
├── batcher.go         # 批量缓冲刷新
├── database.go        # HTTPWriter、FileWriter、MultiWriter
├── filed_*.go         # 跨平台文件标识
└── *_test.go          # 测试
```

## 测试

```bash
go test ./...
```

## 许可

MIT
