import { API_BASE, bindRuntimeNavLinks } from "./runtime.js";

bindRuntimeNavLinks(document);

const STATUS_LABELS = {
  success: "成功",
  failed: "失败",
  skipped: "跳过",
  circuit_open: "熔断",
  running: "运行中",
};

const el = {
  monitorHours: document.getElementById("monitorHours"),
  monitorRefresh: document.getElementById("monitorRefresh"),
  mLastSync: document.getElementById("mLastSync"),
  mLastAnalysis: document.getElementById("mLastAnalysis"),
  mAnomaly: document.getElementById("mAnomaly"),
  mSyncRows: document.getElementById("mSyncRows"),
  mThroughputRows: document.getElementById("mThroughputRows"),
  connectorAvailability: document.getElementById("connectorAvailability"),
  throughputBars: document.getElementById("throughputBars"),
  syncJobs: document.getElementById("syncJobs"),
  analysisJobs: document.getElementById("analysisJobs"),
};

function toLocalText(value) {
  if (!value) return "--";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
}

function statusText(value) {
  return STATUS_LABELS[value] || value || "未知";
}

async function fetchJSON(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`请求失败（HTTP ${response.status}）`);
  return response.json();
}

function renderConnectorAvailability(rows) {
  if (!rows?.length) {
    el.connectorAvailability.innerHTML = `<div class="detail-card muted">暂无连接器指标。</div>`;
    return;
  }
  el.connectorAvailability.innerHTML = rows
    .map((row) => {
      const percentage = (Number(row.availability || 0) * 100).toFixed(1);
      const alertCount = Number(row.counts?.failed || 0) + Number(row.counts?.circuit_open || 0);
      const css = alertCount > 0 ? "high" : Number(percentage) < 80 ? "medium" : "low";
      return `<div class="detail-card">
        <strong>${row.connector}</strong><br>
        可用率：${percentage}%<br>
        平均延迟：${Number(row.avg_latency_ms || 0).toFixed(1)} ms<br>
        成功 ${row.counts?.success || 0} / 失败 ${row.counts?.failed || 0} / 熔断 ${row.counts?.circuit_open || 0}
        <div class="dot ${css}" style="margin-top:6px;"></div>
      </div>`;
    })
    .join("");
}

function renderThroughput(rows) {
  if (!rows?.length) {
    el.throughputBars.innerHTML = `<div class="timeline-empty">暂无吞吐数据。</div>`;
    return;
  }
  const maxValue = Math.max(...rows.map((row) => Number(row.event_count || 0)), 1);
  el.throughputBars.innerHTML = rows
    .slice()
    .reverse()
    .map((row) => {
      const value = Number(row.event_count || 0);
      const height = Math.max(6, Math.round((value / maxValue) * 100));
      const cls = value > maxValue * 0.66 ? "high" : value > maxValue * 0.33 ? "medium" : "low";
      return `<div class="bar ${cls}" style="height:${height}px" title="${row.bucket} · ${value}"></div>`;
    })
    .join("");
}

function renderJobList(rows, target) {
  if (!rows?.length) {
    target.innerHTML = "暂无任务记录。";
    return;
  }
  target.innerHTML = rows
    .slice(0, 25)
    .map((row) => {
      const extra = [];
      if (row.connector) extra.push(`连接器=${row.connector}`);
      if (row.records_fetched != null) extra.push(`抓取=${row.records_fetched}`);
      if (row.records_inserted != null) extra.push(`入库=${row.records_inserted}`);
      if (row.analyzed_count != null) extra.push(`分析=${row.analyzed_count}`);
      if (row.failed_count != null) extra.push(`失败=${row.failed_count}`);
      if (row.error_message) extra.push(`错误=${row.error_message}`);
      return `${toLocalText(row.started_at)} | 状态=${statusText(row.status)} | ${extra.join(" | ")}`;
    })
    .join("<br>");
}

async function refreshMonitor() {
  const hours = Number(el.monitorHours.value || 24);
  const [monitor, health] = await Promise.all([
    fetchJSON(`${API_BASE}/system/monitor?hours=${hours}`),
    fetchJSON(`${API_BASE}/system/health`),
  ]);

  el.mLastSync.textContent = toLocalText(health.runtime?.last_sync_time);
  el.mLastAnalysis.textContent = toLocalText(health.runtime?.last_analysis_time);
  el.mAnomaly.textContent = String(health.runtime?.connector_anomaly_count_24h ?? 0);
  el.mSyncRows.textContent = String((monitor.sync_jobs || []).length);
  el.mThroughputRows.textContent = String((monitor.event_throughput || []).length);

  renderConnectorAvailability(monitor.connector_availability || []);
  renderThroughput(monitor.event_throughput || []);
  renderJobList(monitor.sync_jobs || [], el.syncJobs);
  renderJobList(monitor.analysis_jobs || [], el.analysisJobs);
}

el.monitorRefresh.addEventListener("click", () => {
  refreshMonitor().catch((error) => {
    el.syncJobs.textContent = `刷新失败：${error.message}`;
  });
});

el.monitorHours.addEventListener("change", () => {
  refreshMonitor().catch((error) => {
    el.syncJobs.textContent = `刷新失败：${error.message}`;
  });
});

refreshMonitor().catch((error) => {
  el.syncJobs.textContent = `初始化失败：${error.message}`;
});
