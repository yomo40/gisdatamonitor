# GISDataMonitor

江西能源安全情报工作台，采用 `FastAPI + SQLite + MapLibre/Leaflet`，支持实时事件汇聚、风险分析与时间回放。

## 目录结构

- `app/backend`：后端服务与数据同步任务
- `app/frontend`：前端静态页面（可直接用于 GitHub Pages）
- `data/manifests`：数据清单与完整性元数据
- `docs`：架构、策略与视觉设计文档
- `scripts`：打包、测试与数据工具脚本

## 本地启动

```bash
cd app/backend
pip install -e .
python scripts/start_all.py
```

PowerShell：

```powershell
.\scripts\start_all.ps1
```

默认地址：

- `http://localhost:8080/`：主工作台
- `http://localhost:8080/leaflet`：Leaflet 兼容页
- `http://localhost:8080/monitor`：系统监控页

## GitHub Pages 发布

本仓库已支持将 `app/frontend` 直接发布为 GitHub Pages。

1. 推送到 `main`（或手动触发 Actions）。
2. 在仓库 `Settings -> Pages` 中选择 `GitHub Actions` 作为 Source。
3. 可选：配置仓库变量 `GISDATAMONITOR_API_BASE`（例如 `https://your-backend.example.com/api/v1`），Pages 会自动注入前端运行配置。

说明：

- 如果未配置 `GISDATAMONITOR_API_BASE`，Pages 站点会默认请求 `/api/v1`。
- 页面导航在后端路由模式与静态文件模式（`.html`）都会自动适配。

## 关键文档

- [项目计划](./PROJECT_PLAN.md)
- [后端说明](./app/backend/README.md)
- [前端说明](./app/frontend/README.md)
- [技术架构与约束](./docs/architecture/技术架构与实施约束.md)
- [GitHub 提交与发布清单](./PUBLISH_TO_GITHUB.md)

## 打包命令

仅数据包：

```bash
python scripts/package_data_full_zip.py --output dist/GISDataMonitor-data-full.zip --strict
```

离线完整运行包：

```bash
python scripts/build_full_offline_package.py --output dist/GISDataMonitor-full-offline-win11-x64.zip --strict
```
