function normalizeApiBase(value) {
  const text = String(value || "").trim();
  if (!text) return "/api/v1";
  if (text.startsWith("http://") || text.startsWith("https://")) {
    return text.replace(/\/+$/, "");
  }
  const withSlash = text.startsWith("/") ? text : `/${text}`;
  return withSlash.replace(/\/+$/, "");
}

function isStaticHtmlMode() {
  if (window.location.protocol === "file:") return true;
  const host = String(window.location.hostname || "").toLowerCase();
  if (host.endsWith(".github.io")) return true;
  return /\.html$/i.test(String(window.location.pathname || ""));
}

export const STATIC_HTML_MODE = isStaticHtmlMode();
export const API_BASE = normalizeApiBase(window.GISDATAMONITOR_RUNTIME?.apiBase || "/api/v1");

export function resolveNavHref(target) {
  if (!STATIC_HTML_MODE) {
    if (target === "autoplay") return "/autoplay";
    if (target === "leaflet") return "/leaflet";
    if (target === "monitor") return "/monitor";
    return "/";
  }
  if (target === "autoplay") return "./autoplay.html";
  if (target === "leaflet") return "./leaflet.html";
  if (target === "monitor") return "./monitor.html";
  return "./index.html";
}

export function bindRuntimeNavLinks(root = document) {
  const anchors = root.querySelectorAll("a[data-nav-target]");
  for (const anchor of anchors) {
    const target = anchor.getAttribute("data-nav-target");
    if (!target) continue;
    anchor.setAttribute("href", resolveNavHref(target));
  }
}

export function isAutoplayMode() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("autoplay") === "1") return true;
  const pathname = String(window.location.pathname || "");
  return pathname === "/autoplay" || pathname.endsWith("/autoplay.html");
}
