# App Workspace

这里是 `GISDataMonitor` 正式产品代码区。

约定：

- `frontend/` 放正式监测界面
- `backend/` 放 API 与数据装配服务
- `shared/` 放配置、类型与图层元数据

当前已完成首版落地：

- `backend/` 已提供 FastAPI + PostGIS + 实时连接器 + 调度器 + 入库脚本
- `frontend/` 已提供 MapLibre 主界面与 Leaflet 兼容界面
- `shared/` 可继续沉淀图层元数据合同与跨端常量
