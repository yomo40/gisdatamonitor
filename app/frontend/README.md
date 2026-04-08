# GISDataMonitor 前端

前端包含三个页面，全部复用同一后端接口合同：

- `index.html`：MapLibre 主工作台
- `leaflet.html`：Leaflet 兼容/降级页
- `monitor.html`：同步与连接器监控页

## GitHub Pages 兼容

- 静态资源统一使用相对路径，可直接部署到 `https://<user>.github.io/<repo>/`。
- 导航链接会自动识别运行模式：
- 后端模式：`/`、`/leaflet`、`/monitor`、`/autoplay`
- 静态模式：`index.html`、`leaflet.html`、`monitor.html`、`autoplay.html`
- API 基址由 `assets/runtime-config.js` 提供（默认 `/api/v1`）。
- 在 GitHub Actions 中可通过仓库变量 `GISDATAMONITOR_API_BASE` 注入，例如：
- `https://your-backend.example.com/api/v1`

## 主工作台能力

- 场景预设：全球/金融/技术/稳定
- 运行态状态条（图层、同步时间、异常数、当前场景）
- 风险解释卡与事件简报卡
- 事件增强渲染（风险等级样式）
- 时间回放控制（播放/暂停/步进/速度/窗口）
- 回放联动设施高亮

## 共享数据接口

- `GET /api/v1/scenes`
- `GET /api/v1/scenes/{scene_id}/state`
- `GET /api/v1/events/enriched`
- `GET /api/v1/timeline/playback`
- `GET /api/v1/risk/explain`
- `GET /api/v1/system/health`
- `GET /api/v1/system/monitor`
