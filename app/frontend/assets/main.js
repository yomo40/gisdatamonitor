import { API_BASE, bindRuntimeNavLinks, isAutoplayMode } from "./runtime.js";

const REFRESH_MS = 10 * 60 * 1000;
const EVENT_WINDOW_HOURS = 24;
const EVENT_WINDOW_FALLBACK_HOURS = [24, 24 * 7, 24 * 30];
const AUTO_SCENE_ROTATE_MS = 22 * 1000;
const AUTO_CAMERA_ROTATE_MS = 16 * 1000;
const RANDOM_DETAIL_MS = 14 * 1000;
const JIANGXI_CENTER = [116.0, 27.6];
const CHINA_CENTER = [104.5, 35.3];
const AUTO_MODE = isAutoplayMode();
const OFFLINE_CACHE_KEY = "gisdatamonitor_offline_v1";
const OFFLINE_PLAYBACK_CACHE_KEY = "gisdatamonitor_playback_offline_v1";
const EMPTY_FILTER = ["==", ["get", "facility_id"], "__none__"];
const REMOTE_BASEMAP_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";
const LOCAL_OFFLINE_STYLE = {
  version: 8,
  name: "gisdatamonitor-offline-dark",
  sources: {},
  layers: [
    {
      id: "background",
      type: "background",
      paint: {
        "background-color": "#051427",
      },
    },
  ],
};

bindRuntimeNavLinks(document);

function readMainOfflineCacheSafe() {
  try {
    const text = window.localStorage.getItem(OFFLINE_CACHE_KEY);
    if (!text) return null;
    const payload = JSON.parse(text);
    return payload && typeof payload === "object" ? payload : null;
  } catch (error) {
    console.warn("offline cache parse failed", error);
    return null;
  }
}

function savePlaybackOfflineCacheSafe(payload) {
  try {
    const text = JSON.stringify(payload);
    window.localStorage.setItem(OFFLINE_PLAYBACK_CACHE_KEY, text);
  } catch (error) {
    console.warn("offline playback cache write failed", error);
  }
}

const SCENE_LABELS = {
  world: "全球视角",
  finance: "金融视角",
  tech: "技术视角",
  happy: "稳定视角",
};

const RISK_LABELS = {
  critical: "严重",
  high: "高",
  medium: "中",
  low: "低",
};

const CONNECTOR_STATUS_LABELS = {
  success: "成功",
  failed: "失败",
  skipped: "跳过",
  circuit_open: "熔断",
  running: "运行中",
};

const SOURCE_PROFILE = {
  usgs_earthquake: { label: "USGS 地震", quality: 5, grade: "A" },
  nasa_firms: { label: "NASA FIRMS", quality: 4, grade: "A-" },
  energy_announcement: { label: "行业公告", quality: 4, grade: "A-" },
  energy_market: { label: "能源市场", quality: 4, grade: "A-" },
  gdelt_events: { label: "GDELT 事件", quality: 2, grade: "B-" },
  ais_port_stub: { label: "AIS/港口", quality: 2, grade: "B-" },
};

const state = {
  scenes: [],
  sceneId: "world",
  sceneState: null,
  facilityType: "",
  eventSource: "",
  eventHours: EVENT_WINDOW_HOURS,
  eventDataWindowHours: EVENT_WINDOW_HOURS,
  riskLevel: "",
  dataTimestamp: null,
  autoRefreshTimer: null,
  autoSceneTimer: null,
  autoCameraTimer: null,
  playbackTimer: null,
  eventStreamTimer: null,
  pulseAnimationFrame: null,
  cachedEvents: [],
  jiangxiFacilityPool: [],
  randomDetailTimer: null,
  randomDetailBusy: false,
  detailPinnedUntil: 0,
  autoSceneBusy: false,
  autoCameraPhase: 0,
  workbenchInitialized: false,
  mapStyleFallbackUsed: false,
  playback: {
    frames: [],
    index: 0,
    window: "24h",
    stepMinutes: 30,
    playing: false,
  },
};

const map = new maplibregl.Map({
  container: "map",
  style: REMOTE_BASEMAP_STYLE,
  center: CHINA_CENTER,
  zoom: 4.2,
  attributionControl: false,
});

map.addControl(new maplibregl.NavigationControl({ showCompass: true }), "top-right");

const el = {
  sceneTabs: document.getElementById("sceneTabs"),
  eventHours: document.getElementById("eventHours"),
  eventSource: document.getElementById("eventSource"),
  facilityType: document.getElementById("facilityType"),
  riskLevel: document.getElementById("riskLevel"),
  toggleDem: document.getElementById("toggleDem"),
  toggleBoundary: document.getElementById("toggleBoundary"),
  toggleFacilities: document.getElementById("toggleFacilities"),
  toggleEvents: document.getElementById("toggleEvents"),
  togglePlayback: document.getElementById("togglePlayback"),
  detailCard: document.getElementById("detailCard"),
  riskExplainCard: document.getElementById("riskExplainCard"),
  eventBriefCard: document.getElementById("eventBriefCard"),
  riskSnapshot: document.getElementById("riskSnapshot"),
  systemHealth: document.getElementById("systemHealth"),
  statusScene: document.getElementById("statusScene"),
  statusLayers: document.getElementById("statusLayers"),
  statusSync: document.getElementById("statusSync"),
  statusAnomaly: document.getElementById("statusAnomaly"),
  statusTimestamp: document.getElementById("statusTimestamp"),
  timelineBars: document.getElementById("timelineBars"),
  playbackMeta: document.getElementById("playbackMeta"),
  playbackScrubber: document.getElementById("playbackScrubber"),
  playbackWindow: document.getElementById("playbackWindow"),
  playbackStep: document.getElementById("playbackStep"),
  playbackSpeed: document.getElementById("playbackSpeed"),
  playPauseBtn: document.getElementById("playPauseBtn"),
  stepBackBtn: document.getElementById("stepBackBtn"),
  stepForwardBtn: document.getElementById("stepForwardBtn"),
  manualRefresh: document.getElementById("manualRefresh"),
};

const hoverPopup = new maplibregl.Popup({
  closeButton: false,
  closeOnClick: false,
  maxWidth: "300px",
});

function 风险等级文本(level) {
  return RISK_LABELS[level] || "未知";
}

function 风险颜色(level) {
  if (level === "critical") return "#ff2b2b";
  if (level === "high") return "#ff665a";
  if (level === "medium") return "#f7be4f";
  return "#66dd95";
}

function 场景名称(sceneId, fallbackName) {
  return SCENE_LABELS[sceneId] || fallbackName || sceneId;
}

function 连接器状态文本(status) {
  return CONNECTOR_STATUS_LABELS[status] || status || "未知";
}

function 来源画像(source) {
  return SOURCE_PROFILE[source] || { label: source || "未知来源", quality: 1, grade: "C" };
}

function 简报关键词键(event) {
  const source = String(event?.source || "");
  const title = String(event?.title || event?.event_type || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 84);
  return `${source}:${title}`;
}

function 简报评分(event) {
  const riskLevel = String(event?.risk_level || "low");
  const riskBase = riskLevel === "critical" ? 120 : riskLevel === "high" ? 92 : riskLevel === "medium" ? 58 : 26;
  const profile = 来源画像(event?.source);
  const eventTs = new Date(event?.event_time || 0).getTime();
  const ageHours = Number.isFinite(eventTs) ? Math.max(0, (Date.now() - eventTs) / 3_600_000) : 72;
  const recencyScore = Math.max(0, 30 - ageHours);
  return riskBase + profile.quality * 7 + recencyScore;
}

function 生成高质量简报(items) {
  const dedup = new Map();
  for (const event of items || []) {
    if (!event) continue;
    const profile = 来源画像(event.source);
    if (profile.quality < 2) continue;
    const key = 简报关键词键(event);
    const score = 简报评分(event);
    const current = dedup.get(key);
    if (!current || score > current.__brief_score) {
      dedup.set(key, { ...event, __brief_score: score, __source_profile: profile });
    }
  }
  return Array.from(dedup.values())
    .sort((a, b) => b.__brief_score - a.__brief_score)
    .slice(0, 12);
}

function asFacilityFeatureCollection(items) {
  return {
    type: "FeatureCollection",
    features: (items || [])
      .filter((item) => item.geometry)
      .map((item) => ({
        type: "Feature",
        properties: {
          id: item.id,
          facility_id: item.facility_id,
          facility_type: item.facility_type,
          source_layer: item.source_layer,
          name: item.name,
          start_year: item.start_year,
          status: item.status,
          admin_city: item.admin_city,
        },
        geometry: item.geometry,
      })),
  };
}

function asEventFeatureCollection(items) {
  return {
    type: "FeatureCollection",
    features: (items || [])
      .filter((item) => item.geometry)
      .map((item) => ({
        type: "Feature",
        properties: {
          id: item.id,
          source: item.source,
          event_type: item.event_type,
          severity: item.severity,
          risk_level: item.risk_level,
          risk_score: item.risk_score,
          title: item.title,
          event_time: item.event_time,
          summary_zh: item.summary_zh,
        },
        geometry: item.geometry,
      })),
  };
}

async function fetchJSON(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`请求失败（HTTP ${response.status}）`);
  }
  return response.json();
}

function 保存离线缓存(payload) {
  try {
    const text = JSON.stringify(payload);
    window.localStorage.setItem(OFFLINE_CACHE_KEY, text);
  } catch (error) {
    console.warn("离线缓存写入失败", error);
  }
}

function 读取离线缓存() {
  try {
    const text = window.localStorage.getItem(OFFLINE_CACHE_KEY);
    if (!text) return null;
    const payload = JSON.parse(text);
    return payload && typeof payload === "object" ? payload : null;
  } catch (error) {
    console.warn("离线缓存读取失败", error);
    return null;
  }
}

function 应用离线缓存(payload) {
  if (!payload) return false;
  const boundary = payload.boundary;
  const facilities = payload.facilities || [];
  const events = payload.events || [];
  if (!boundary) return false;

  setSourceData("boundary", boundary);
  setSourceData("facilities", asFacilityFeatureCollection(facilities));
  streamEvents(events);
  state.eventDataWindowHours = Number(payload.eventHours || EVENT_WINDOW_HOURS);

  state.jiangxiFacilityPool = payload.jiangxiFacilities || [];
  state.cachedEvents = events;
  state.dataTimestamp = payload.cachedAt || new Date().toISOString();

  if (payload.snapshotItems) renderSnapshot(payload.snapshotItems);
  if (payload.riskExplain) renderRiskExplain(payload.riskExplain);
  if (payload.health) renderSystemHealth(payload.health);
  renderEventBrief(events, state.eventDataWindowHours);
  updateStatusBar({ sceneState: state.sceneState, health: payload.health || {} });
  applyLayerVisibility();

  el.detailCard.innerHTML = `<div class="detail-card muted">当前离线，已加载本地历史数据（${toLocalText(payload.cachedAt)}）。</div>`;
  return true;
}

function 保存离线回放缓存(payload) {
  try {
    const text = JSON.stringify(payload);
    window.localStorage.setItem(OFFLINE_PLAYBACK_CACHE_KEY, text);
  } catch (error) {
    console.warn("离线回放缓存写入失败", error);
  }
}

function 读取离线回放缓存() {
  try {
    const text = window.localStorage.getItem(OFFLINE_PLAYBACK_CACHE_KEY);
    if (!text) return null;
    const payload = JSON.parse(text);
    return payload && typeof payload === "object" ? payload : null;
  } catch (error) {
    console.warn("离线回放缓存读取失败", error);
    return null;
  }
}

function 应用离线回放缓存(payload) {
  const frames = payload?.frames;
  if (!Array.isArray(frames) || !frames.length) return false;
  state.playback.frames = frames;
  state.playback.window = payload.window || state.playback.window;
  state.playback.stepMinutes = Number(payload.step_minutes || state.playback.stepMinutes || 30);
  state.playback.index = lastEventFrameIndex(frames);
  setPlaybackFrame(state.playback.index);
  return true;
}

function 取几何代表点(geometry) {
  if (!geometry || !geometry.type) return null;
  const coords = geometry.coordinates;
  if (!coords) return null;
  if (geometry.type === "Point" && Array.isArray(coords) && coords.length >= 2) return coords;
  if (geometry.type === "MultiPoint" && Array.isArray(coords[0])) return coords[0];
  if (geometry.type === "LineString" && Array.isArray(coords[0])) return coords[Math.floor(coords.length / 2)] || coords[0];
  if (geometry.type === "MultiLineString" && Array.isArray(coords[0]?.[0])) return coords[0][Math.floor(coords[0].length / 2)] || coords[0][0];
  if (geometry.type === "Polygon" && Array.isArray(coords[0]?.[0])) return coords[0][0];
  if (geometry.type === "MultiPolygon" && Array.isArray(coords[0]?.[0]?.[0])) return coords[0][0][0];
  return null;
}

function 点在环内(point, ring) {
  if (!Array.isArray(ring) || ring.length < 3) return false;
  const x = Number(point[0]);
  const y = Number(point[1]);
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = Number(ring[i][0]);
    const yi = Number(ring[i][1]);
    const xj = Number(ring[j][0]);
    const yj = Number(ring[j][1]);
    const intersect = yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi + Number.EPSILON) + xi;
    if (intersect) inside = !inside;
  }
  return inside;
}

function 点在面内(point, polygonCoords) {
  if (!Array.isArray(polygonCoords) || !polygonCoords.length) return false;
  if (!点在环内(point, polygonCoords[0])) return false;
  for (let i = 1; i < polygonCoords.length; i += 1) {
    if (点在环内(point, polygonCoords[i])) return false;
  }
  return true;
}

function 点在江西边界内(point, boundaryGeometry) {
  if (!point || !boundaryGeometry || !boundaryGeometry.type) return false;
  if (boundaryGeometry.type === "Polygon") {
    return 点在面内(point, boundaryGeometry.coordinates);
  }
  if (boundaryGeometry.type === "MultiPolygon") {
    return (boundaryGeometry.coordinates || []).some((polygonCoords) => 点在面内(point, polygonCoords));
  }
  return false;
}

function 构建江西设施池(facilityItems, boundaryGeojson) {
  const boundaryGeometry = boundaryGeojson?.features?.[0]?.geometry;
  if (!boundaryGeometry) return [];
  return (facilityItems || []).filter((item) => {
    const point = 取几何代表点(item.geometry);
    return 点在江西边界内(point, boundaryGeometry);
  });
}

async function 随机展示江西设施详情() {
  if (state.randomDetailBusy) return;
  if (Date.now() < state.detailPinnedUntil) return;
  if (!state.jiangxiFacilityPool.length) return;
  state.randomDetailBusy = true;
  try {
    const index = Math.floor(Math.random() * state.jiangxiFacilityPool.length);
    const item = state.jiangxiFacilityPool[index];
    if (!item?.facility_id) return;
    const detail = await fetchJSON(`${API_BASE}/facilities/${encodeURIComponent(item.facility_id)}`);
    renderDetailCardFromFacility(detail);
  } catch (error) {
    el.detailCard.textContent = `随机设备详情加载失败：${error.message}`;
  } finally {
    state.randomDetailBusy = false;
  }
}

function 启动随机江西设施详情轮播() {
  if (state.randomDetailTimer) return;
  state.randomDetailTimer = window.setInterval(() => {
    随机展示江西设施详情();
  }, RANDOM_DETAIL_MS);
}

async function fetchAllEventsEnrichedByHours({ source, riskLevel, hours }) {
  const pageSize = 1000;
  const maxPages = 60;
  let page = 1;
  const allItems = [];
  let total = 0;

  while (page <= maxPages) {
    const params = new URLSearchParams({
      page: String(page),
      page_size: String(pageSize),
      hours: String(hours),
    });
    if (source) params.set("source", source);
    if (riskLevel) params.set("risk_level", riskLevel);

    const payload = await fetchJSON(`${API_BASE}/events/enriched?${params.toString()}`);
    const items = payload.items || [];
    total = Number(payload.total || 0);
    allItems.push(...items);

    if (!items.length || allItems.length >= total) break;
    page += 1;
  }

  const dedup = new Map();
  for (const item of allItems) {
    dedup.set(String(item.id), item);
  }
  return Array.from(dedup.values());
}

async function fetchEventsEnrichedWithFallback({ source, riskLevel }) {
  let lastError = null;
  for (const hours of EVENT_WINDOW_FALLBACK_HOURS) {
    try {
      const items = await fetchAllEventsEnrichedByHours({ source, riskLevel, hours });
      if (items.length > 0 || hours === EVENT_WINDOW_FALLBACK_HOURS[EVENT_WINDOW_FALLBACK_HOURS.length - 1]) {
        return { items, resolvedHours: hours, fallbackUsed: hours !== EVENT_WINDOW_HOURS };
      }
    } catch (error) {
      lastError = error;
    }
  }
  if (lastError) throw lastError;
  return { items: [], resolvedHours: EVENT_WINDOW_HOURS, fallbackUsed: false };
}

function toLocalText(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString();
}

function setSourceData(sourceId, data) {
  const source = map.getSource(sourceId);
  if (source) source.setData(data);
}

function renderSceneTabs() {
  el.sceneTabs.innerHTML = (state.scenes || [])
    .map((scene) => {
      const active = scene.scene_id === state.sceneId ? "active" : "";
      const label = 场景名称(scene.scene_id, scene.scene_name);
      return `<button class="scene-tab ${active}" data-scene="${scene.scene_id}" type="button">${label}</button>`;
    })
    .join("");
}

function applyLayerVisibility() {
  const demVisible = el.toggleDem.checked ? "visible" : "none";
  const boundaryVisible = el.toggleBoundary.checked ? "visible" : "none";
  const facilitiesVisible = el.toggleFacilities.checked ? "visible" : "none";
  const eventsVisible = el.toggleEvents.checked ? "visible" : "none";
  const playbackVisible = el.togglePlayback.checked ? "visible" : "none";

  if (map.getLayer("dem-hillshade")) map.setLayoutProperty("dem-hillshade", "visibility", demVisible);
  for (const layerId of ["boundary-fill", "boundary-line"]) {
    if (map.getLayer(layerId)) map.setLayoutProperty(layerId, "visibility", boundaryVisible);
  }
  for (const layerId of ["facility-point", "facility-line"]) {
    if (map.getLayer(layerId)) map.setLayoutProperty(layerId, "visibility", facilitiesVisible);
  }
  if (map.getLayer("event-point")) map.setLayoutProperty("event-point", "visibility", eventsVisible);
  if (map.getLayer("event-pulse")) map.setLayoutProperty("event-pulse", "visibility", eventsVisible);
  if (map.getLayer("playback-event-point")) map.setLayoutProperty("playback-event-point", "visibility", playbackVisible);
  if (map.getLayer("facility-highlight")) map.setLayoutProperty("facility-highlight", "visibility", playbackVisible);
}

function renderSnapshot(items) {
  if (!items?.length) {
    el.riskSnapshot.innerHTML = `<div class="detail-card muted">当前无风险快照数据。</div>`;
    return;
  }
  el.riskSnapshot.innerHTML = items
    .slice(0, 8)
    .map((row) => {
      const regionLevel = row.region_level === "province" ? "省级" : "地市级";
      return `<div class="detail-card">
        <strong>${regionLevel} / ${row.region_name}</strong><br>
        事件 ${row.total_events} | 高风险 ${row.high_events} | 得分 ${(row.weighted_score || 0).toFixed(1)}
      </div>`;
    })
    .join("");
}

function renderEventBrief(items, resolvedHours = EVENT_WINDOW_HOURS) {
  const curated = 生成高质量简报(items);
  const usingFallbackWindow = Number(resolvedHours || EVENT_WINDOW_HOURS) > EVENT_WINDOW_HOURS;
  const fallbackTip = usingFallbackWindow
    ? `<div class="detail-card muted">24h 数据不足，已自动扩展到近 ${Math.round(Number(resolvedHours) / 24)} 天窗口展示。</div>`
    : "";
  if (!curated.length) {
    el.eventBriefCard.innerHTML = `${fallbackTip}<div class="detail-card muted">当前筛选条件下无事件。</div>`;
    return;
  }
  el.eventBriefCard.innerHTML = `${fallbackTip}${curated
    .map((event) => {
      const level = event.risk_level || "low";
      const badgeColor = 风险颜色(level);
      const profile = event.__source_profile || 来源画像(event.source);
      return `<div class="detail-card">
        <strong>${event.title || event.event_type}</strong><br>
        <span class="badge" style="background:${badgeColor}22;border-color:${badgeColor};color:${badgeColor};">${风险等级文本(level)}</span>
        <span class="meta">${profile.label} · 信源 ${profile.grade} · ${toLocalText(event.event_time)}</span><br>
        <span>${event.summary_zh || event.description || ""}</span>
      </div>`;
    })
    .join("")}`;
}

function renderRiskExplain(payload) {
  const metrics = payload?.metrics || {};
  const breakdown = payload?.score_breakdown || {};
  el.riskExplainCard.innerHTML = `
    <strong>${payload?.region_name || "江西"} · ${payload?.window || "--"}</strong><br>
    综合风险分：${(metrics.composite_risk_score || 0).toFixed(1)}<br>
    总事件 ${metrics.total_events || 0} | 高风险 ${metrics.high_events || 0} | 中风险 ${metrics.medium_events || 0}<br>
    严重度权重 ${(breakdown.severity_weight || 0).toFixed(2)} · 距离权重 ${(breakdown.proximity_weight || 0).toFixed(2)}<br>
    时效权重 ${(breakdown.recency_weight || 0).toFixed(2)} · 来源权重 ${(breakdown.source_weight || 0).toFixed(2)}<br>
    ${payload?.explanation_zh || ""}
  `;
}

function renderSystemHealth(payload) {
  const connectors = payload?.connectors || [];
  const connectorsText = connectors
    .slice(0, 8)
    .map((item) => `${item.connector}: ${连接器状态文本(item.status)}`)
    .join("<br>");
  el.systemHealth.innerHTML = `
    <strong>${payload?.status === "ok" ? "正常" : payload?.status || "未知"}</strong><br>
    ${payload?.runtime?.last_analysis_time ? `最近分析：${toLocalText(payload.runtime.last_analysis_time)}<br>` : ""}
    ${payload?.runtime?.last_sync_time ? `最近同步：${toLocalText(payload.runtime.last_sync_time)}<br>` : ""}
    数据表：${(payload?.table_counts || []).map((x) => `${x.table_name}=${x.count}`).join("，")}<br>
    ${connectorsText || "暂无连接器日志"}
  `;
}

function renderDetailCardFromEvent(eventProps) {
  const riskLevel = eventProps.risk_level || "low";
  const riskScore = Number(eventProps.risk_score || 0);
  el.detailCard.innerHTML = `
    <strong>${eventProps.title || eventProps.event_type}</strong><br>
    来源：${eventProps.source}<br>
    事件类型：${eventProps.event_type}<br>
    严重度：${eventProps.severity}<br>
    风险：${风险等级文本(riskLevel)}（${riskScore.toFixed(1)}）<br>
    时间：${toLocalText(eventProps.event_time)}<br>
    ${eventProps.summary_zh || ""}
  `;
}

function renderDetailCardFromFacility(detail) {
  const facility = detail?.facility || {};
  const terrain = detail?.terrain_metrics || {};
  const recentEvents = detail?.recent_events || [];
  el.detailCard.innerHTML = `
    <strong>${facility.name || facility.facility_id || "-"}</strong><br>
    类型：${facility.facility_type || "-"}<br>
    来源图层：${facility.source_layer || "-"}<br>
    城市：${facility.admin_city || "-"}<br>
    投运年份：${facility.start_year || "-"}<br>
    状态：${facility.status || "-"}<br>
    海拔：${terrain.elevation_m != null ? Number(terrain.elevation_m).toFixed(2) : "N/A"} 米<br>
    坡度：${terrain.slope_deg != null ? Number(terrain.slope_deg).toFixed(2) : "N/A"}°<br>
    坡向：${terrain.aspect_deg != null ? Number(terrain.aspect_deg).toFixed(2) : "N/A"}°<br>
    阴影值：${terrain.hillshade != null ? Number(terrain.hillshade).toFixed(2) : "N/A"}<br>
    粗糙度：${terrain.roughness != null ? Number(terrain.roughness).toFixed(2) : "N/A"}<br>
    最近关联事件：${recentEvents.length}
  `;
}

function updateStatusBar({ sceneState, health }) {
  const currentSceneLabel = 场景名称(state.sceneId, state.sceneId);
  el.statusScene.textContent = currentSceneLabel;
  const baseLayers = Number(sceneState?.runtime?.loaded_layers || 0);
  const demLayer = el.toggleDem?.checked ? 1 : 0;
  el.statusLayers.textContent = String(baseLayers + demLayer);
  el.statusSync.textContent = toLocalText(sceneState?.runtime?.last_sync_time || health?.runtime?.last_sync_time);
  el.statusAnomaly.textContent = String(
    sceneState?.runtime?.connector_anomaly_count_24h ?? health?.runtime?.connector_anomaly_count_24h ?? 0,
  );
  el.statusTimestamp.textContent = toLocalText(state.dataTimestamp);
}

function attachMapLayers() {
  map.addSource("dem-raster", {
    type: "raster",
    tiles: [`${API_BASE}/map/tiles/dem/{z}/{x}/{y}.png?derivative=hillshade`],
    tileSize: 256,
  });
  map.addSource("boundary", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
  map.addSource("facilities", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
  map.addSource("events", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
  map.addSource("playback-events", { type: "geojson", data: { type: "FeatureCollection", features: [] } });

  map.addLayer({
    id: "dem-hillshade",
    type: "raster",
    source: "dem-raster",
    paint: {
      "raster-opacity": 0.68,
      "raster-contrast": 0.52,
      "raster-brightness-min": 0.08,
      "raster-brightness-max": 1.0,
      "raster-saturation": 0.55,
    },
  });
  map.addLayer({
    id: "boundary-fill",
    type: "fill",
    source: "boundary",
    paint: { "fill-color": "#0d5b98", "fill-opacity": 0.16 },
  });
  map.addLayer({
    id: "boundary-line",
    type: "line",
    source: "boundary",
    paint: { "line-color": "#68d4ff", "line-width": 1.4 },
  });
  map.addLayer({
    id: "facility-line",
    type: "line",
    source: "facilities",
    filter: ["any", ["==", ["geometry-type"], "LineString"], ["==", ["geometry-type"], "MultiLineString"]],
    paint: { "line-color": "#82f3de", "line-width": 1.4, "line-opacity": 0.76 },
  });
  map.addLayer({
    id: "facility-point",
    type: "circle",
    source: "facilities",
    filter: ["==", ["geometry-type"], "Point"],
    paint: {
      "circle-color": "#89fbe7",
      "circle-radius": 4.8,
      "circle-opacity": 0.82,
      "circle-stroke-width": 1,
      "circle-stroke-color": "#10323f",
    },
  });
  map.addLayer({
    id: "facility-highlight",
    type: "circle",
    source: "facilities",
    filter: EMPTY_FILTER,
    paint: {
      "circle-color": "#ff7d7d",
      "circle-radius": 9,
      "circle-opacity": 0.22,
      "circle-stroke-width": 1.3,
      "circle-stroke-color": "#ff4d4d",
    },
  });
  map.addLayer({
    id: "event-point",
    type: "circle",
    source: "events",
    paint: {
      "circle-radius": ["match", ["get", "risk_level"], "critical", 9, "high", 7, "medium", 5.4, 4.5],
      "circle-color": [
        "match",
        ["get", "risk_level"],
        "critical",
        "#ff2b2b",
        "high",
        "#ff665a",
        "medium",
        "#f7be4f",
        "#66dd95",
      ],
      "circle-stroke-width": 1,
      "circle-stroke-color": "#0a1729",
      "circle-opacity": 0.88,
    },
  });
  map.addLayer({
    id: "event-pulse",
    type: "circle",
    source: "events",
    paint: {
      "circle-radius": ["match", ["get", "risk_level"], "critical", 14, "high", 12, "medium", 10, 8],
      "circle-color": ["match", ["get", "risk_level"], "critical", "#ff2b2b", "high", "#ff665a", "medium", "#f7be4f", "#66dd95"],
      "circle-opacity": 0.2,
      "circle-stroke-width": 0,
      "circle-blur": 0.7,
    },
  });
  map.addLayer({
    id: "playback-event-point",
    type: "circle",
    source: "playback-events",
    paint: {
      "circle-radius": ["match", ["get", "risk_level"], "critical", 11, "high", 9, "medium", 7, 6],
      "circle-color": ["match", ["get", "risk_level"], "critical", "#ff2b2b", "high", "#ff665a", "medium", "#f7be4f", "#66dd95"],
      "circle-stroke-width": 1.4,
      "circle-stroke-color": "#ffffff",
      "circle-opacity": 0.62,
    },
  });

  const clickableLayers = ["facility-point", "event-point", "playback-event-point"];
  for (const layerId of clickableLayers) {
    map.on("mouseenter", layerId, () => {
      map.getCanvas().style.cursor = "pointer";
    });
    map.on("mouseleave", layerId, () => {
      map.getCanvas().style.cursor = "";
      hoverPopup.remove();
    });
  }

  map.on("mousemove", "facility-point", (event) => {
    const feature = event.features?.[0];
    if (!feature) return;
    hoverPopup
      .setLngLat(event.lngLat)
      .setHTML(`<strong>${feature.properties?.name || feature.properties?.facility_id || "设施对象"}</strong>`)
      .addTo(map);
  });

  map.on("mousemove", "event-point", (event) => {
    const feature = event.features?.[0];
    if (!feature) return;
    hoverPopup
      .setLngLat(event.lngLat)
      .setHTML(`<strong>${feature.properties?.title || feature.properties?.event_type || "事件对象"}</strong>`)
      .addTo(map);
  });

  map.on("mousemove", "playback-event-point", (event) => {
    const feature = event.features?.[0];
    if (!feature) return;
    hoverPopup
      .setLngLat(event.lngLat)
      .setHTML(`<strong>${feature.properties?.title || feature.properties?.event_type || "事件对象"}</strong>`)
      .addTo(map);
  });

  map.on("click", "facility-point", async (event) => {
    const feature = event.features?.[0];
    const facilityId = feature?.properties?.facility_id;
    if (!facilityId) return;
    try {
      state.detailPinnedUntil = Date.now() + 30_000;
      const detail = await fetchJSON(`${API_BASE}/facilities/${encodeURIComponent(facilityId)}`);
      renderDetailCardFromFacility(detail);
    } catch (error) {
      el.detailCard.textContent = `设施详情加载失败：${error.message}`;
    }
  });

  map.on("click", "event-point", (event) => {
    const feature = event.features?.[0];
    if (feature?.properties) {
      state.detailPinnedUntil = Date.now() + 30_000;
      renderDetailCardFromEvent(feature.properties);
    }
  });

  map.on("click", "playback-event-point", (event) => {
    const feature = event.features?.[0];
    if (feature?.properties) {
      state.detailPinnedUntil = Date.now() + 30_000;
      renderDetailCardFromEvent(feature.properties);
    }
  });
}

function startEventPulseAnimation() {
  if (state.pulseAnimationFrame) window.cancelAnimationFrame(state.pulseAnimationFrame);
  const animate = (timestamp) => {
    if (map.getLayer("event-pulse")) {
      const t = timestamp / 1000;
      const base = 0.14 + ((Math.sin(t * 2.5) + 1) / 2) * 0.1;
      map.setPaintProperty("event-pulse", "circle-opacity", base);
      map.setPaintProperty("event-pulse", "circle-radius", [
        "match",
        ["get", "risk_level"],
        "critical",
        13 + Math.sin(t * 3.2) * 2.5,
        "high",
        10 + Math.sin(t * 2.7) * 2.1,
        "medium",
        8 + Math.sin(t * 2.3) * 1.7,
        6 + Math.sin(t * 1.9) * 1.3,
      ]);
    }
    state.pulseAnimationFrame = window.requestAnimationFrame(animate);
  };
  state.pulseAnimationFrame = window.requestAnimationFrame(animate);
}

function stopEventStream() {
  if (state.eventStreamTimer) {
    window.clearInterval(state.eventStreamTimer);
    state.eventStreamTimer = null;
  }
}

function streamEvents(items) {
  stopEventStream();
  const sorted = [...(items || [])].sort((a, b) => new Date(a.event_time).getTime() - new Date(b.event_time).getTime());
  if (!sorted.length) {
    setSourceData("events", asEventFeatureCollection([]));
    return;
  }
  const chunkSize = Math.max(6, Math.ceil(sorted.length / 40));
  let cursor = 0;
  setSourceData("events", asEventFeatureCollection([]));
  state.eventStreamTimer = window.setInterval(() => {
    cursor = Math.min(sorted.length, cursor + chunkSize);
    setSourceData("events", asEventFeatureCollection(sorted.slice(0, cursor)));
    if (cursor >= sorted.length) {
      stopEventStream();
    }
  }, 140);
}

function pickAutoFocusEvent() {
  const frame = (state.playback.frames || [])[state.playback.index];
  const events = frame?.events || [];
  for (const event of events) {
    const geom = event?.geometry;
    if (geom?.type === "Point" && Array.isArray(geom.coordinates) && geom.coordinates.length >= 2) {
      return geom.coordinates;
    }
  }
  return null;
}

function autoCameraTick() {
  const point = pickAutoFocusEvent();
  state.autoCameraPhase += 1;
  const phase = state.autoCameraPhase;
  if (point) {
    map.easeTo({
      center: point,
      zoom: 5.8 + (phase % 3) * 0.3,
      pitch: 48,
      bearing: (phase * 22) % 360,
      duration: 6200,
      easing: (t) => t * (2 - t),
      essential: true,
    });
    return;
  }
  map.easeTo({
    center: CHINA_CENTER,
    zoom: 4.35 + ((phase % 2) ? 0.18 : 0),
    pitch: 36,
    bearing: (phase * 14) % 360,
    duration: 6200,
    easing: (t) => t * (2 - t),
    essential: true,
  });
}

function startAutoShowMode() {
  if (!AUTO_MODE) return;
  document.body.classList.add("auto-mode");
  if (el.manualRefresh) el.manualRefresh.style.display = "none";
  if (el.playPauseBtn) el.playPauseBtn.disabled = true;
  if (el.stepBackBtn) el.stepBackBtn.disabled = true;
  if (el.stepForwardBtn) el.stepForwardBtn.disabled = true;
  if (el.playbackScrubber) el.playbackScrubber.disabled = true;

  const sceneIds = (state.scenes || []).map((scene) => scene.scene_id);
  if (!sceneIds.length) return;
  let sceneCursor = (Math.max(0, sceneIds.indexOf(state.sceneId)) + 1) % sceneIds.length;

  const rotateScene = async () => {
    if (state.autoSceneBusy) return;
    state.autoSceneBusy = true;
    const nextScene = sceneIds[sceneCursor % sceneIds.length];
    sceneCursor += 1;
    stopPlayback();
    try {
      await applyScene(nextScene);
      await refreshAll();
      setPlaybackFrame(firstEventFrameIndex(state.playback.frames || []));
      startPlayback();
      autoCameraTick();
    } finally {
      state.autoSceneBusy = false;
    }
  };

  if (state.autoSceneTimer) window.clearInterval(state.autoSceneTimer);
  state.autoSceneTimer = window.setInterval(() => {
    rotateScene().catch((error) => {
      el.detailCard.textContent = `自动场景切换失败：${error.message}`;
    });
  }, AUTO_SCENE_ROTATE_MS);

  if (state.autoCameraTimer) {
    window.clearInterval(state.autoCameraTimer);
    state.autoCameraTimer = null;
  }
  state.autoCameraTimer = window.setInterval(() => {
    autoCameraTick();
  }, AUTO_CAMERA_ROTATE_MS);

  setPlaybackFrame(firstEventFrameIndex(state.playback.frames || []));
  startPlayback();
  autoCameraTick();
}

async function refreshFacilityTypes() {
  const payload = await fetchJSON(`${API_BASE}/layers`);
  const types = Array.from(new Set((payload.layers || []).map((row) => row.facility_type).filter(Boolean))).sort();
  el.facilityType.innerHTML = `<option value="">全部类型</option>${types.map((type) => `<option value="${type}">${type}</option>`).join("")}`;
  if (state.facilityType) el.facilityType.value = state.facilityType;
}

function sceneConfig(sceneId) {
  const item = (state.scenes || []).find((scene) => scene.scene_id === sceneId);
  return item?.config || {};
}

async function loadScenes() {
  const payload = await fetchJSON(`${API_BASE}/scenes`);
  state.scenes = payload.items || [];
  const defaultScene = payload.default_scene_id || (state.scenes[0] && state.scenes[0].scene_id) || "world";
  if (!state.scenes.some((scene) => scene.scene_id === state.sceneId)) {
    state.sceneId = defaultScene;
  }
  renderSceneTabs();
}

async function applyScene(sceneId) {
  state.sceneId = sceneId;
  const payload = await fetchJSON(`${API_BASE}/scenes/${encodeURIComponent(sceneId)}/state`);
  state.sceneState = payload;
  const config = payload?.scene?.config || sceneConfig(sceneId);

  state.eventHours = EVENT_WINDOW_HOURS;
  state.eventSource = config.event_source || "";
  state.facilityType = config.facility_type || "";
  state.playback.window = config.timeline_window || "24h";

  el.eventHours.value = String(EVENT_WINDOW_HOURS);
  el.eventHours.disabled = true;
  el.eventSource.value = state.eventSource;
  el.facilityType.value = state.facilityType;
  el.playbackWindow.value = state.playback.window;

  const layerSet = new Set(config.layers || []);
  el.toggleDem.checked = true;
  el.toggleBoundary.checked = layerSet.has("boundary");
  el.toggleFacilities.checked = layerSet.has("facilities");
  el.toggleEvents.checked = layerSet.has("events");
  el.togglePlayback.checked = true;

  renderSceneTabs();
  applyLayerVisibility();
}

async function refreshMainData() {
  state.eventHours = EVENT_WINDOW_HOURS;
  state.eventSource = el.eventSource.value;
  state.facilityType = el.facilityType.value;
  state.riskLevel = el.riskLevel.value;

  const paramsFacility = new URLSearchParams({ page: "1", page_size: "5000" });
  if (state.facilityType) paramsFacility.set("facility_type", state.facilityType);

  const riskWindow = state.playback.window || "24h";
  const [boundary, facilities, eventFetchResult] = await Promise.all([
    fetchJSON(`${API_BASE}/map/layers/boundary.geojson`),
    fetchJSON(`${API_BASE}/facilities?${paramsFacility.toString()}`),
    fetchEventsEnrichedWithFallback({ source: state.eventSource, riskLevel: state.riskLevel }),
  ]);
  const optionalResults = await Promise.allSettled([
    fetchJSON(`${API_BASE}/risk/snapshot?window=${encodeURIComponent(riskWindow)}`),
    fetchJSON(`${API_BASE}/risk/explain?window=${encodeURIComponent(riskWindow)}&region_level=province`),
    fetchJSON(`${API_BASE}/system/health`),
  ]);
  optionalResults.forEach((result, index) => {
    if (result.status === "rejected") {
      const apiName = index === 0 ? "risk/snapshot" : index === 1 ? "risk/explain" : "system/health";
      console.warn(`optional API failed: ${apiName}`, result.reason);
    }
  });
  const snapshot = optionalResults[0].status === "fulfilled" ? optionalResults[0].value : { items: [] };
  const explain = optionalResults[1].status === "fulfilled" ? optionalResults[1].value : {};
  const health = optionalResults[2].status === "fulfilled" ? optionalResults[2].value : {};
  const eventsEnrichedItems = eventFetchResult.items || [];
  state.eventDataWindowHours = Number(eventFetchResult.resolvedHours || EVENT_WINDOW_HOURS);

  setSourceData("boundary", boundary);
  setSourceData("facilities", asFacilityFeatureCollection(facilities.items || []));
  streamEvents(eventsEnrichedItems);
  state.cachedEvents = eventsEnrichedItems;
  state.jiangxiFacilityPool = 构建江西设施池(facilities.items || [], boundary);

  renderSnapshot(snapshot.items || []);
  renderRiskExplain(explain);
  renderEventBrief(eventsEnrichedItems, state.eventDataWindowHours);
  renderSystemHealth(health);

  state.dataTimestamp = new Date().toISOString();
  updateStatusBar({ sceneState: state.sceneState, health });
  applyLayerVisibility();
  保存离线缓存({
    cachedAt: state.dataTimestamp,
    eventHours: state.eventDataWindowHours,
    boundary,
    facilities: (facilities.items || []).slice(0, 2500),
    jiangxiFacilities: state.jiangxiFacilityPool.slice(0, 1200),
    events: eventsEnrichedItems,
    snapshotItems: snapshot.items || [],
    riskExplain: explain,
    health,
  });
  await 随机展示江西设施详情();
}

function renderTimelineBars(frames) {
  if (el.timelineBars) el.timelineBars.classList.toggle("dense", Array.isArray(frames) && frames.length > 120);
  if (!frames?.length) {
    el.timelineBars.innerHTML =
      '<div class="timeline-empty">暂无可回放事件，正在等待实时拉取或加载离线缓存…</div>';
    return;
  }
  if (!hasPlaybackEvents(frames)) {
    el.timelineBars.innerHTML =
      '<div class="timeline-empty">当前时间窗无事件，系统将继续自动拉取并优先使用离线历史数据。</div>';
    return;
  }
  const maxEventCount = Math.max(...frames.map((frame) => frameEventCount(frame)), 1);
  el.timelineBars.innerHTML = frames
    .map((frame, index) => {
      const events = frameEventCount(frame);
      const highRiskFacilityCount = Number(frame.high_risk_facility_count || 0);
      const height = events > 0 ? Math.max(5, Math.round((events / maxEventCount) * 94)) : 2;
      const cssClass = events <= 0 ? "empty" : highRiskFacilityCount > 0 ? "high" : "medium";
      return `<button
        class="bar ${cssClass} ${index === state.playback.index ? "active" : ""}"
        data-index="${index}"
        style="height:${height}px"
        title="${toLocalText(frame.frame_time)} | 事件 ${events} | 高风险设施 ${highRiskFacilityCount}"
      ></button>`;
    })
    .join("");
}

function keepActiveTimelineBarVisible() {
  if (!el.timelineBars?.classList.contains("dense")) return;
  const active = el.timelineBars?.querySelector(".bar.active");
  if (!active) return;
  active.scrollIntoView({ block: "nearest", inline: "center" });
}

function setPlaybackFrame(index) {
  const frames = state.playback.frames || [];
  if (!frames.length) {
    state.playback.index = 0;
    el.playbackScrubber.value = "0";
    el.playbackMeta.textContent = "帧 -- / --";
    setSourceData("playback-events", { type: "FeatureCollection", features: [] });
    map.setFilter("facility-highlight", EMPTY_FILTER);
    renderTimelineBars([]);
    return;
  }

  const clamped = Math.max(0, Math.min(index, frames.length - 1));
  state.playback.index = clamped;
  const frame = frames[clamped];
  const frameEvents = frame.events || [];
  setSourceData("playback-events", asEventFeatureCollection(frameEvents));

  const facilityIds = (frame.high_risk_facilities || []).map((item) => item.facility_id).filter(Boolean);
  if (facilityIds.length > 0) {
    map.setFilter("facility-highlight", ["in", ["get", "facility_id"], ["literal", facilityIds]]);
  } else {
    map.setFilter("facility-highlight", EMPTY_FILTER);
  }

  const frameEventsCount = frameEventCount(frame);
  el.playbackMeta.textContent = `第 ${clamped + 1}/${frames.length} 帧 · ${toLocalText(frame.frame_time)} · 事件 ${
    frameEventsCount
  } · 高风险设施 ${frame.high_risk_facility_count || 0}`;
  const hasEvents = hasPlaybackEvents(frames);
  const maxPlayableIndex = hasEvents ? lastEventFrameIndex(frames) : Math.max(0, frames.length - 1);
  el.playbackScrubber.max = String(Math.max(0, maxPlayableIndex));
  el.playbackScrubber.value = String(clamped);
  renderTimelineBars(frames);
  keepActiveTimelineBarVisible();
}

function stopPlayback() {
  if (state.playbackTimer) {
    window.clearInterval(state.playbackTimer);
    state.playbackTimer = null;
  }
  state.playback.playing = false;
  el.playPauseBtn.textContent = "播放";
}

function playbackWindowMs(window) {
  if (window === "30d") return 30 * 24 * 60 * 60 * 1000;
  if (window === "7d") return 7 * 24 * 60 * 60 * 1000;
  return 24 * 60 * 60 * 1000;
}

function parseEventTimeMs(value) {
  const ts = new Date(value || 0).getTime();
  return Number.isFinite(ts) ? ts : null;
}

function frameEventCount(frame) {
  const listCount = Array.isArray(frame?.events) ? frame.events.length : 0;
  const numeric = Number(frame?.event_count ?? 0);
  if (Number.isFinite(numeric) && numeric > 0) return numeric;
  return listCount;
}

function hasPlaybackEvents(frames) {
  if (!Array.isArray(frames) || !frames.length) return false;
  return frames.some((frame) => frameEventCount(frame) > 0);
}

function playbackNonZeroRatio(frames) {
  if (!Array.isArray(frames) || !frames.length) return 0;
  const nonZero = frames.filter((frame) => frameEventCount(frame) > 0).length;
  return nonZero / frames.length;
}

function buildPlaybackFramesFromEvents(items, { window, stepMinutes, frameLimit = 360 }) {
  const source = Array.isArray(items) ? items : [];
  if (!source.length) return [];

  const stepMs = Math.max(1, Number(stepMinutes || 30)) * 60 * 1000;
  const endTs = Date.now();
  const startTs = endTs - playbackWindowMs(window);

  const events = source
    .map((item) => {
      const ts = parseEventTimeMs(item?.event_time);
      if (!ts || ts < startTs || ts > endTs) return null;
      return { ...item, __ts: ts };
    })
    .filter(Boolean)
    .sort((a, b) => a.__ts - b.__ts);

  if (!events.length) return [];

  const frameTimes = [];
  let cursor = startTs + stepMs;
  while (cursor <= endTs) {
    frameTimes.push(cursor);
    if (frameTimes.length >= Math.max(1, Number(frameLimit || 360))) break;
    cursor += stepMs;
  }
  if (!frameTimes.length) frameTimes.push(endTs);

  return frameTimes.map((frameTs) => {
    const frameStartTs = frameTs - stepMs;
    const frameEvents = events
      .filter((event) => event.__ts > frameStartTs && event.__ts <= frameTs)
      .sort((a, b) => Number(b?.risk_score || 0) - Number(a?.risk_score || 0))
      .slice(0, 200)
      .map((event) => {
        const copied = { ...event };
        delete copied.__ts;
        return copied;
      });

    return {
      frame_time: new Date(frameTs).toISOString(),
      window_start: new Date(frameStartTs).toISOString(),
      event_count: frameEvents.length,
      high_risk_facility_count: 0,
      events: frameEvents,
      high_risk_facilities: [],
    };
  });
}

function firstEventFrameIndex(frames) {
  if (!Array.isArray(frames) || !frames.length) return 0;
  const idx = frames.findIndex((frame) => frameEventCount(frame) > 0);
  return idx >= 0 ? idx : 0;
}

function lastEventFrameIndex(frames) {
  if (!Array.isArray(frames) || !frames.length) return 0;
  for (let i = frames.length - 1; i >= 0; i -= 1) {
    if (frameEventCount(frames[i]) > 0) return i;
  }
  return Math.max(0, frames.length - 1);
}

function startPlayback() {
  const frames = state.playback.frames || [];
  if (!frames.length) return;
  stopPlayback();
  state.playback.playing = true;
  el.playPauseBtn.textContent = "暂停";
  const speed = Number(el.playbackSpeed.value || 1);
  const intervalMs = Math.max(160, Math.round(1200 / speed));
  const hasEvents = hasPlaybackEvents(frames);
  const maxPlayableIndex = hasEvents ? lastEventFrameIndex(frames) : Math.max(0, frames.length - 1);
  state.playbackTimer = window.setInterval(() => {
    const next = state.playback.index + 1;
    if (next > maxPlayableIndex) {
      if (AUTO_MODE) {
        setPlaybackFrame(firstEventFrameIndex(frames));
        return;
      }
      stopPlayback();
      return;
    }
    setPlaybackFrame(next);
  }, intervalMs);
}

function togglePlayback() {
  if (state.playback.playing) {
    stopPlayback();
  } else {
    startPlayback();
  }
}

async function refreshPlayback() {
  const sceneWindow = el.playbackWindow.value;
  let stepMinutes = Number(el.playbackStep.value || 30);
  state.playback.window = sceneWindow;
  state.playback.stepMinutes = stepMinutes;
  let payload = await fetchJSON(
    `${API_BASE}/timeline/playback?scene_id=${encodeURIComponent(state.sceneId)}&window=${encodeURIComponent(sceneWindow)}&step_minutes=${stepMinutes}&frame_limit=360`,
  );
  let frames = payload.frames || [];
  const ratio = playbackNonZeroRatio(frames);
  const shouldCoarsenStep = (sceneWindow === "7d" || sceneWindow === "30d") && stepMinutes < 60 && ratio > 0 && ratio < 0.12;
  if (shouldCoarsenStep) {
    stepMinutes = 60;
    payload = await fetchJSON(
      `${API_BASE}/timeline/playback?scene_id=${encodeURIComponent(state.sceneId)}&window=${encodeURIComponent(sceneWindow)}&step_minutes=${stepMinutes}&frame_limit=360`,
    );
    frames = payload.frames || [];
    state.playback.stepMinutes = stepMinutes;
    if (el.playbackStep.value !== "60") {
      el.playbackStep.value = "60";
      el.detailCard.innerHTML = '<div class="detail-card muted">检测到回放数据较稀疏，已自动切换为 60 分钟步长以提升可读性。</div>';
    }
  }
  if (!hasPlaybackEvents(frames)) {
    const rebuiltFrames = buildPlaybackFramesFromEvents(state.cachedEvents || [], {
      window: sceneWindow,
      stepMinutes,
      frameLimit: 360,
    });
    if (hasPlaybackEvents(rebuiltFrames)) {
      frames = rebuiltFrames;
    }
  }
  state.playback.frames = frames;
  state.playback.index = lastEventFrameIndex(state.playback.frames || []);
  setPlaybackFrame(state.playback.index);
  保存离线回放缓存({
    scene_id: state.sceneId,
    window: sceneWindow,
    step_minutes: stepMinutes,
    frames: state.playback.frames || [],
    data_quality: payload.data_quality || null,
    cachedAt: new Date().toISOString(),
  });

  if (!hasPlaybackEvents(state.playback.frames)) {
    const offlineMain = 读取离线缓存();
    const offlineFrames = buildPlaybackFramesFromEvents(offlineMain?.events || [], {
      window: sceneWindow,
      stepMinutes,
      frameLimit: 360,
    });
    if (hasPlaybackEvents(offlineFrames)) {
      state.playback.frames = offlineFrames;
      state.playback.index = lastEventFrameIndex(offlineFrames);
      setPlaybackFrame(state.playback.index);
      保存离线回放缓存({
        scene_id: state.sceneId,
        window: sceneWindow,
        step_minutes: stepMinutes,
        frames: offlineFrames,
        data_quality: { source_mode: "local_offline_cache", fallback_reason: "rebuilt_from_offline_events" },
        cachedAt: new Date().toISOString(),
      });
    }
  }
}

async function refreshAll() {
  try {
    await refreshMainData();
  } catch (error) {
    const offline = 读取离线缓存();
    const ok = 应用离线缓存(offline);
    if (!ok) {
      const offlineMain = readMainOfflineCacheSafe();
      const rebuiltFrames = buildPlaybackFramesFromEvents(offlineMain?.events || [], {
        window: state.playback.window || "24h",
        stepMinutes: Number(el.playbackStep.value || 30),
        frameLimit: 360,
      });
      if (!hasPlaybackEvents(rebuiltFrames)) throw error;
      state.playback.frames = rebuiltFrames;
      state.playback.index = lastEventFrameIndex(rebuiltFrames);
      setPlaybackFrame(state.playback.index);
      savePlaybackOfflineCacheSafe({
        scene_id: state.sceneId,
        window: state.playback.window || "24h",
        step_minutes: Number(el.playbackStep.value || 30),
        frames: rebuiltFrames,
        data_quality: { source_mode: "local_offline_cache", fallback_reason: "rebuilt_after_playback_api_error" },
        cachedAt: new Date().toISOString(),
      });
    }
    return;
  }
  try {
    await refreshPlayback();
  } catch (error) {
    const playbackOffline = 读取离线回放缓存();
    const okPlayback = 应用离线回放缓存(playbackOffline);
    if (okPlayback) {
      el.detailCard.innerHTML = `<div class="detail-card muted">回放接口离线，已加载本地回放缓存（${toLocalText(playbackOffline?.cachedAt)}）。</div>`;
      return;
    }
    const offline = 读取离线缓存();
    const ok = 应用离线缓存(offline);
    if (!ok) {
      const offlineMain = readMainOfflineCacheSafe();
      const rebuiltFrames = buildPlaybackFramesFromEvents(offlineMain?.events || [], {
        window: state.playback.window || "24h",
        stepMinutes: Number(el.playbackStep.value || 30),
        frameLimit: 360,
      });
      if (!hasPlaybackEvents(rebuiltFrames)) throw error;
      state.playback.frames = rebuiltFrames;
      state.playback.index = lastEventFrameIndex(rebuiltFrames);
      setPlaybackFrame(state.playback.index);
      savePlaybackOfflineCacheSafe({
        scene_id: state.sceneId,
        window: state.playback.window || "24h",
        step_minutes: Number(el.playbackStep.value || 30),
        frames: rebuiltFrames,
        data_quality: { source_mode: "local_offline_cache", fallback_reason: "rebuilt_after_playback_api_error" },
        cachedAt: new Date().toISOString(),
      });
    }
  }
}

function bindUI() {
  el.sceneTabs.addEventListener("click", async (event) => {
    const target = event.target.closest("[data-scene]");
    if (!target) return;
    const sceneId = target.getAttribute("data-scene");
    if (!sceneId || sceneId === state.sceneId) return;
    stopPlayback();
    try {
      await applyScene(sceneId);
      await refreshAll();
    } catch (error) {
      el.detailCard.textContent = `场景切换失败：${error.message}`;
    }
  });

  el.eventSource.addEventListener("change", () => refreshMainData().catch((err) => (el.detailCard.textContent = `数据刷新失败：${err.message}`)));
  el.facilityType.addEventListener("change", () => refreshMainData().catch((err) => (el.detailCard.textContent = `数据刷新失败：${err.message}`)));
  el.riskLevel.addEventListener("change", () => refreshMainData().catch((err) => (el.detailCard.textContent = `数据刷新失败：${err.message}`)));

  el.toggleDem.addEventListener("change", applyLayerVisibility);
  el.toggleBoundary.addEventListener("change", applyLayerVisibility);
  el.toggleFacilities.addEventListener("change", applyLayerVisibility);
  el.toggleEvents.addEventListener("change", applyLayerVisibility);
  el.togglePlayback.addEventListener("change", applyLayerVisibility);

  el.playPauseBtn.addEventListener("click", togglePlayback);
  el.stepBackBtn.addEventListener("click", () => {
    stopPlayback();
    setPlaybackFrame(state.playback.index - 1);
  });
  el.stepForwardBtn.addEventListener("click", () => {
    stopPlayback();
    const frames = state.playback.frames || [];
    const maxPlayableIndex = hasPlaybackEvents(frames) ? lastEventFrameIndex(frames) : Math.max(0, frames.length - 1);
    setPlaybackFrame(Math.min(state.playback.index + 1, maxPlayableIndex));
  });
  el.playbackScrubber.addEventListener("input", () => {
    stopPlayback();
    setPlaybackFrame(Number(el.playbackScrubber.value || 0));
  });
  el.playbackSpeed.addEventListener("change", () => {
    if (state.playback.playing) startPlayback();
  });
  el.playbackWindow.addEventListener("change", () => {
    stopPlayback();
    refreshPlayback().catch((err) => (el.detailCard.textContent = `回放刷新失败：${err.message}`));
    refreshMainData().catch((err) => (el.detailCard.textContent = `数据刷新失败：${err.message}`));
  });
  el.playbackStep.addEventListener("change", () => {
    stopPlayback();
    refreshPlayback().catch((err) => (el.detailCard.textContent = `回放刷新失败：${err.message}`));
  });

  el.timelineBars.addEventListener("click", (event) => {
    const button = event.target.closest("[data-index]");
    if (!button) return;
    stopPlayback();
    setPlaybackFrame(Number(button.getAttribute("data-index") || 0));
  });

  el.manualRefresh.addEventListener("click", () => {
    stopPlayback();
    refreshAll().catch((err) => (el.detailCard.textContent = `手动刷新失败：${err.message}`));
  });
}

async function initializeWorkbench() {
  attachMapLayers();
  startEventPulseAnimation();
  bindUI();
  await loadScenes();
  await refreshFacilityTypes();
  await applyScene(state.sceneId);
  await refreshAll();
  启动随机江西设施详情轮播();
  if (state.autoRefreshTimer) window.clearInterval(state.autoRefreshTimer);
  state.autoRefreshTimer = window.setInterval(() => {
    refreshAll().catch((err) => {
      el.detailCard.textContent = `自动刷新失败：${err.message}`;
    });
  }, REFRESH_MS);
  startAutoShowMode();
}

async function initializeWorkbenchOnce() {
  if (state.workbenchInitialized) return;
  state.workbenchInitialized = true;
  try {
    await initializeWorkbench();
  } catch (error) {
    state.workbenchInitialized = false;
    throw error;
  }
}

map.on("error", (event) => {
  if (state.mapStyleFallbackUsed || state.workbenchInitialized) return;
  const message = String(event?.error?.message || "");
  const isLikelyStyleLoadError =
    message.includes("Failed to fetch") ||
    message.includes("NetworkError") ||
    message.includes("ERR_") ||
    message.includes("style");
  if (!isLikelyStyleLoadError) return;
  state.mapStyleFallbackUsed = true;
  map.setStyle(LOCAL_OFFLINE_STYLE, { diff: false });
  if (el.detailCard) {
    el.detailCard.innerHTML = '<div class="detail-card muted">底图源暂不可用，已切换离线底图并继续渲染业务图层。</div>';
  }
});

map.on("load", () => {
  initializeWorkbenchOnce().catch((error) => {
    el.detailCard.textContent = `初始化失败：${error.message}`;
  });
});
