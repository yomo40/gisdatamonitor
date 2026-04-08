window.GISDATAMONITOR_RUNTIME = window.GISDATAMONITOR_RUNTIME || {};

if (typeof window.GISDATAMONITOR_RUNTIME.apiBase !== "string") {
  // Default backend prefix; GitHub Pages workflow can overwrite this file.
  window.GISDATAMONITOR_RUNTIME.apiBase = "/api/v1";
}
