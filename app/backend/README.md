# GISDataMonitor 后端（SQLite）

后端基于 FastAPI，默认单机单用户运行，数据库仅依赖 SQLite。

## 一键启动

```bash
cd app/backend
pip install -e .
python scripts/start_all.py
```

PowerShell:

```powershell
.\scripts\start_all.ps1
```

## 启动参数

- `--host` 监听地址（默认 `0.0.0.0`）
- `--port` 监听端口（默认 `8080`）
- `--no-reload` 关闭热重载（推荐稳定运行时使用）
- `--force-ingest` 强制重导静态数据
- `--skip-sync` 跳过启动前实时同步
- `--no-auto-port` 端口占用时不自动切换

## 24h 全量拉取（一次性）

```bash
cd app/backend
python scripts/pull_last24h_once.py
```

输出文件：

- `app/backend/cache/offline/events_last24h.json`

## 离线回退

- 在线连接器抓取失败时，会自动尝试使用 `app/backend/cache/connectors/*.json` 的历史缓存。
- 前端在线接口失败时，会自动尝试加载本地缓存（浏览器 `localStorage`）。

## 主要接口

- `GET /api/v1/layers`
- `GET /api/v1/facilities`
- `GET /api/v1/facilities/{id}`
- `GET /api/v1/events`
- `GET /api/v1/events/enriched`
- `GET /api/v1/risk/snapshot`
- `GET /api/v1/risk/timeline`
- `GET /api/v1/risk/explain`
- `GET /api/v1/system/health`
- `GET /api/v1/system/monitor`
- `GET /api/v1/scenes`
- `GET /api/v1/scenes/{scene_id}/state`
- `GET /api/v1/timeline/playback`

## GDELT 连接器优化建议

- 优先使用代理：`GISDATAMONITOR_HTTP_PROXY` / `GISDATAMONITOR_HTTPS_PROXY`
- 建议参数：
  - `GISDATAMONITOR_GDELT_MAX_RECORDS=120`
  - `GISDATAMONITOR_GDELT_TIMEOUT_SEC=9`
  - `GISDATAMONITOR_GDELT_TIMESPAN=1day`
  - `GISDATAMONITOR_GDELT_RATE_LIMIT_COOLDOWN_MINUTES=20`
