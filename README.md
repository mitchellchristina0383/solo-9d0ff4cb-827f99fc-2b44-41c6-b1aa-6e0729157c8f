# 日志检索值班台

FastAPI + SQLite（FTS5）实现的日志录入与检索服务，支持**不中断服务的在线索引重建**。前端为单页页面，Docker Compose 一键启动。

## 启动

```bash
docker compose up --build
```

浏览器打开 <http://localhost:8000>（可用 `PORT` 环境变量改端口）。数据保存在命名卷 `logdata` 的 `/data/logs.db`。

本地开发（Python 3.11+，SQLite 需带 FTS5）：

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --reload
# 测试
pytest
```

## 功能与接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 网页（录入 / 检索 / 重建状态） |
| POST | `/api/logs` | 录入 `{"service": ..., "body": ...}` |
| GET | `/api/logs?q=&service=&limit=&offset=` | 关键词 + 服务名检索（两者 AND） |
| GET | `/api/services` | 已有服务名及数量 |
| POST | `/api/reindex` | 发起后台重建（202；已有重建时 409） |
| GET | `/api/status` | 当前索引、重建进度、最近一次重建结果 |
| GET | `/healthz` | 健康检查 |

检索关键字支持中文与英文：英文按词前缀匹配（`timeout` 可命中 `timeouts`），中文按字及相邻两字组合建立倒排，因此 `登录`、`登录失败` 均可命中。关键字为空时只按服务名列出。

## 重建设计

- 每次重建生成一个独立的 FTS5 表（`index_generations.fts_table`），旧索引在重建期间继续对外服务。
- 重建开始时记录快照上界 `start_id`。后台线程分批把快照内日志写入新表（`build_cursor` 单调推进）。
- 重建期间的写入在**同一个事务**里同时写日志表、当前生效索引和新索引，因此新索引天然包含“重建开始后提交”的日志。
- 启用前在一个 `BEGIN IMMEDIATE` 事务内校验新表行数等于日志总数，然后原子切换 `active` / `retired`；读者在一个读事务里先取当前 generation id，再查询该 generation 的表，一次检索不会混用新旧索引。
- 启用后旧索引表保留（`retired`），保证已开始、尚未结束的检索不受影响；新进程启动时清理历史表。
- 重建失败或进程重启：`building` 记录标记为 `failed` 并丢弃半成品表，原 `active` 索引不受影响，可直接重新发起。

## 已验证内容

见 `tests/`：写入/检索/服务过滤、中文与英文关键词、重建期间交错写入、启用前最后一刻的写入、并发写入不重不漏、进行中的检索跨越索引切换、失败保留旧索引并可重试、重启丢弃半成品索引、批量失败、HTTP 层完整流程与 409/400/422 校验、压力脚本（并发写 + 并发查 + 连续重建）。

## 已知限制

- 单进程部署：重建任务是在进程内线程，`docker compose` 使用单个 uvicorn 进程，不可横向扩到多个 worker/副本共享同一个 SQLite。
- 检索为 AND 语义，不支持 OR / 短语 / 模糊纠错；中文按二元字组匹配，没有分词词典。
- 索引表越来越多时只在新进程启动时清理 retired 表，运行期间保留。
- 日志正文没有单独截断/脱敏，写满磁盘等极端故障未做专门处理。
