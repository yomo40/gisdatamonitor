# GISDataMonitor

应付领导检查写的垃圾小项目，采用
`FastAPI + SQLite + MapLibre/Leaflet`，支持实时事件汇聚、风险分析与时间回放。

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

## 打包命令

仅数据包：

```bash
python scripts/package_data_full_zip.py --output dist/GISDataMonitor-data-full.zip --strict
```

离线完整运行包：

```bash
python scripts/build_full_offline_package.py --output dist/GISDataMonitor-full-offline-win11-x64.zip --strict
```
