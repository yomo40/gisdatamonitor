import { API_BASE, bindRuntimeNavLinks } from "./runtime.js";

bindRuntimeNavLinks(document);

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

const map = L.map("leafletMap", {
  center: [35.3, 104.5],
  zoom: 4,
  zoomControl: true,
});

const baseTileLayer = L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
  maxZoom: 19,
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> 贡献者',
}).addTo(map);

L.tileLayer(`${API_BASE}/map/tiles/dem/{z}/{x}/{y}.png?derivative=hillshade`, {
  maxZoom: 12,
  opacity: 0.36,
  attribution: "DEM 江西",
}).addTo(map);

const layerGroup = {
  boundary: L.geoJSON(null, {
    style: { color: "#7bc8ff", weight: 1.4, fillColor: "#2f7fc9", fillOpacity: 0.12 },
  }).addTo(map),
  facilities: L.geoJSON(null, {
    pointToLayer: (_, latlng) =>
      L.circleMarker(latlng, {
        radius: 4,
        color: "#10323f",
        weight: 1,
        fillColor: "#7bf2d7",
        fillOpacity: 0.85,
      }),
    style: { color: "#66ddff", weight: 1.6, opacity: 0.75 },
  }).addTo(map),
  events: L.geoJSON(null, {
    pointToLayer: (feature, latlng) => {
      const risk = feature.properties?.risk_level || "low";
      const color = risk === "critical" ? "#ff2b2b" : risk === "high" ? "#ff665a" : risk === "medium" ? "#f7be4f" : "#66dd95";
      const radius = risk === "critical" ? 8 : risk === "high" ? 7 : risk === "medium" ? 5 : 4;
      return L.circleMarker(latlng, {
        radius,
        color: "#092034",
        weight: 1,
        fillColor: color,
        fillOpacity: 0.9,
      });
    },
  }).addTo(map),
};

const statusEl = document.getElementById("leafletStatus");
const eventsEl = document.getElementById("leafletEvents");
const refreshBtn = document.getElementById("leafletRefresh");
const sceneSelect = document.getElementById("leafletScene");

const state = {
  scenes: [],
  sceneId: "world",
  basemapOfflineHinted: false,
};

baseTileLayer.on("tileerror", () => {
  if (state.basemapOfflineHinted) return;
  state.basemapOfflineHinted = true;
  if (!statusEl) return;
  const existed = String(statusEl.innerHTML || "").trim();
  const hint = '<span class="meta">在线底图暂不可用，已切换为离线数据视图。</span>';
  statusEl.innerHTML = existed ? `${existed}<br>${hint}` : hint;
});

async function fetchJSON(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`请求失败（HTTP ${response.status}）`);
  return response.json();
}

async function fetchAllEventsEnriched24h(source) {
  const pageSize = 1000;
  let page = 1;
  let total = 0;
  const items = [];
  while (page <= 60) {
    const params = new URLSearchParams({
      page: String(page),
      page_size: String(pageSize),
      hours: "24",
    });
    if (source) params.set("source", source);
    const payload = await fetchJSON(`${API_BASE}/events/enriched?${params.toString()}`);
    const chunk = payload.items || [];
    total = Number(payload.total || 0);
    items.push(...chunk);
    if (!chunk.length || items.length >= total) break;
    page += 1;
  }
  return items;
}

function asFeatureCollection(items) {
  return {
    type: "FeatureCollection",
    features: (items || [])
      .filter((item) => item.geometry)
      .map((item) => ({
        type: "Feature",
        properties: item,
        geometry: item.geometry,
      })),
  };
}

function toLocalText(value) {
  if (!value) return "--";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
}

function sceneName(sceneId, fallback) {
  return SCENE_LABELS[sceneId] || fallback || sceneId;
}

function riskName(level) {
  return RISK_LABELS[level] || level || "未知";
}

function populateSceneOptions() {
  sceneSelect.innerHTML = (state.scenes || [])
    .map((scene) => `<option value="${scene.scene_id}">${sceneName(scene.scene_id, scene.scene_name)}</option>`)
    .join("");
  sceneSelect.value = state.sceneId;
}

async function loadScenes() {
  const payload = await fetchJSON(`${API_BASE}/scenes`);
  state.scenes = payload.items || [];
  state.sceneId = payload.default_scene_id || (state.scenes[0] && state.scenes[0].scene_id) || "world";
  populateSceneOptions();
}

async function refreshLeafletData() {
  const sceneState = await fetchJSON(`${API_BASE}/scenes/${encodeURIComponent(state.sceneId)}/state`);
  const eventSource = sceneState?.filters?.event_source || "";
  const facilityType = sceneState?.filters?.facility_type || "";

  const facilityParams = new URLSearchParams({ page: "1", page_size: "5000" });
  if (facilityType) facilityParams.set("facility_type", facilityType);
  const [boundary, facilities, events, health] = await Promise.all([
    fetchJSON(`${API_BASE}/map/layers/boundary.geojson`),
    fetchJSON(`${API_BASE}/facilities?${facilityParams.toString()}`),
    fetchAllEventsEnriched24h(eventSource),
    fetchJSON(`${API_BASE}/system/health`),
  ]);

  layerGroup.boundary.clearLayers().addData(boundary);
  layerGroup.facilities.clearLayers().addData(asFeatureCollection(facilities.items || []));
  layerGroup.events.clearLayers().addData(asFeatureCollection(events));

  statusEl.innerHTML = `
    <strong>${health.status === "ok" ? "正常" : health.status}</strong><br>
    场景：${sceneName(state.sceneId, state.sceneId)}<br>
    最近同步：${toLocalText(health.runtime?.last_sync_time)}<br>
    连接器异常(24h)：${health.runtime?.connector_anomaly_count_24h ?? 0}<br>
    设施总数：${(health.table_counts || []).find((x) => x.table_name === "baker_facilities")?.count ?? 0}<br>
    事件总数：${(health.table_counts || []).find((x) => x.table_name === "event_normalized")?.count ?? 0}
  `;

  eventsEl.innerHTML = (events || [])
    .slice(0, 18)
    .map(
      (item) =>
        `${toLocalText(item.event_time)} | ${item.source} | ${item.event_type} | ${riskName(item.risk_level)}（${Number(item.risk_score || 0).toFixed(1)}）`,
    )
    .join("<br>");
}

sceneSelect.addEventListener("change", () => {
  state.sceneId = sceneSelect.value;
  refreshLeafletData().catch((error) => {
    statusEl.textContent = `刷新失败：${error.message}`;
  });
});

refreshBtn.addEventListener("click", () => {
  refreshLeafletData().catch((error) => {
    statusEl.textContent = `刷新失败：${error.message}`;
  });
});

async function initializeLeafletFallback() {
  await loadScenes();
  await refreshLeafletData();
}

initializeLeafletFallback().catch((error) => {
  statusEl.textContent = `初始化失败：${error.message}`;
});
