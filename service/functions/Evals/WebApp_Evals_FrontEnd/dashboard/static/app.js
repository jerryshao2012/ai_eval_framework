let trendChart;
const trendCache = {};
const apiCache = new Map();

const CACHE_TTL_MS = {
  latest: 5000,
  alerts: 5000,
  batchCurrent: 5000,
  batchHistory: 8000,
  trends: 60000
};

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed for ${url}`);
  }
  return response.json();
}

async function fetchJsonCached(url, ttlMs = 0) {
  const now = Date.now();
  const cached = apiCache.get(url);
  if (cached && cached.expiresAt > now) {
    return cached.value;
  }
  const value = await fetchJson(url);
  if (ttlMs > 0) {
    apiCache.set(url, { value, expiresAt: now + ttlMs });
  }
  return value;
}

function buildThresholdQuery() {
  const params = new URLSearchParams();
  const enabled = document.getElementById("dynamicThresholds").checked;
  if (!enabled) {
    return "";
  }

  params.set("dynamic_thresholds", "1");

  const precisionWarning = document.getElementById("precisionWarning").value;
  const latencyWarning = document.getElementById("latencyWarning").value;
  const toxicityWarning = document.getElementById("toxicityWarning").value;

  if (precisionWarning !== "") {
    params.set("threshold.performance_precision_coherence.warning", precisionWarning);
    params.set("direction.performance_precision_coherence.warning", "min");
  }
  if (latencyWarning !== "") {
    params.set("threshold.system_reliability_latency.warning", latencyWarning);
    params.set("direction.system_reliability_latency.warning", "max");
  }
  if (toxicityWarning !== "") {
    params.set("threshold.safety_toxicity.warning", toxicityWarning);
    params.set("direction.safety_toxicity.warning", "min");
  }

  return `?${params.toString()}`;
}

function renderApps(items) {
  const container = document.getElementById("apps");
  container.innerHTML = "";

  items.forEach((item) => {
    const card = document.createElement("div");
    card.className = "app-card";

    const metrics = item.metrics
      .map((m) => `${m.metric_type ?? "Unknown Type"} | ${m.metric_name}: ${Number(m.value).toFixed(3)}`)
      .join(" | ");

    card.innerHTML = `
      <h3>${item.app_id}</h3>
      <div class="status-${item.status}">${item.status.toUpperCase()}</div>
      <div>Timestamp: ${item.timestamp}</div>
      <div>${metrics}</div>
    `;
    container.appendChild(card);
  });
}

function renderBatchCurrent(run) {
  const container = document.getElementById("batchCurrent");
  if (!run) {
    container.textContent = "No batch runs yet.";
    return;
  }
  container.innerHTML = `
    <div><strong>Run:</strong> ${run.run_id}</div>
    <div><strong>Status:</strong> ${run.status}</div>
    <div><strong>Started:</strong> ${run.started_at}</div>
    <div><strong>Ended:</strong> ${run.ended_at ?? "in progress"}</div>
    <div class="batch-stat">
      total=${run.stats.total_items}, completed=${run.stats.completed_items},
      failed=${run.stats.failed_items}, running=${run.stats.running_items},
      pending=${run.stats.pending_items}, policy_runs=${run.stats.total_policy_runs},
      breaches=${run.stats.total_breaches}, success_rate=${(run.stats.success_rate * 100).toFixed(1)}%
    </div>
  `;
}

let currentBatchPage = 1;
const batchPageSize = 5;
let totalBatchHistoryItems = 0;
let totalBatchHistoryPages = 1;

function renderBatchHistory(payload) {
  const container = document.getElementById("batchHistory");
  container.innerHTML = "";

  const pageRuns = Array.isArray(payload) ? payload : payload.items || [];
  totalBatchHistoryItems = Array.isArray(payload) ? pageRuns.length : Number(payload.total || 0);
  totalBatchHistoryPages = Array.isArray(payload)
    ? Math.max(1, Math.ceil(totalBatchHistoryItems / batchPageSize))
    : Math.max(1, Number(payload.total_pages || 1));

  if (!pageRuns.length) {
    container.textContent = "No history.";
    renderBatchPagination();
    return;
  }

  pageRuns.forEach((run) => {
    const row = document.createElement("div");
    row.className = "batch-row";

    const failedItems = (run.items || []).filter((item) => item.status === "failed");
    const failButtons = failedItems
      .map(
        (item) =>
          `<button class="btn-trace" data-run-id="${run.run_id}" data-item-id="${item.item_id}">Trace ${item.item_id}</button>`
      )
      .join(" ");

    row.innerHTML = `
      <div><strong>${run.run_id}</strong> | ${run.status}</div>
      <div class="batch-stat">${run.started_at} -> ${run.ended_at ?? "in progress"}</div>
      <div class="batch-stat">total=${run.stats.total_items}, ok=${run.stats.completed_items}, failed=${run.stats.failed_items}</div>
      <div>${failButtons || ""}</div>
    `;
    container.appendChild(row);
  });

  container.querySelectorAll(".btn-trace").forEach((btn) => {
    btn.addEventListener("click", async (event) => {
      const runId = event.target.getAttribute("data-run-id");
      const itemId = event.target.getAttribute("data-item-id");
      await loadTrace(runId, itemId);
    });
  });

  renderBatchPagination();
}

function renderBatchPagination() {
  const container = document.getElementById("batchPagination");
  if (!container) return;
  container.innerHTML = "";
  if (totalBatchHistoryPages <= 1) {
    return; // No pagination needed
  }

  const prevBtn = document.createElement("button");
  prevBtn.textContent = "Previous";
  prevBtn.disabled = currentBatchPage === 1;
  prevBtn.onclick = async () => {
    if (currentBatchPage > 1) {
      currentBatchPage--;
      await loadBatchHistory(currentBatchPage);
    }
  };

  const nextBtn = document.createElement("button");
  nextBtn.textContent = "Next";
  nextBtn.disabled = currentBatchPage === totalBatchHistoryPages;
  nextBtn.onclick = async () => {
    if (currentBatchPage < totalBatchHistoryPages) {
      currentBatchPage++;
      await loadBatchHistory(currentBatchPage);
    }
  };

  const info = document.createElement("span");
  info.textContent = `Page ${currentBatchPage} of ${totalBatchHistoryPages} (Total: ${totalBatchHistoryItems})`;

  container.appendChild(prevBtn);
  container.appendChild(info);
  container.appendChild(nextBtn);
}

async function loadTrace(runId, itemId) {
  const header = document.getElementById("traceHeader");
  const body = document.getElementById("traceLogs");
  const detail = await fetchJson(`/api/batch/run/${runId}/item/${itemId}/logs`);
  header.textContent = `Run=${runId}, Item=${itemId}, Status=${detail.status}`;

  const logLines = (detail.logs || []).map(
    (entry) => `[${entry.timestamp}] ${entry.level} ${entry.message}`
  );
  const trace = detail.traceback ? `\n\nTraceback:\n${detail.traceback}` : "";
  body.textContent = `${logLines.join("\n")}${trace}`;
}

function renderAlerts(alerts) {
  const list = document.getElementById("alerts");
  list.innerHTML = "";

  if (!alerts.length) {
    const li = document.createElement("li");
    li.textContent = "No threshold breaches";
    list.appendChild(li);
    return;
  }

  alerts.forEach((alert) => {
    const li = document.createElement("li");
    li.textContent = `${alert.metric_name} ${alert.level}: actual=${alert.actual_value}, threshold=${alert.threshold_value}, direction=${alert.direction}`;
    list.appendChild(li);
  });
}

async function loadTrend(appId) {
  if (!trendCache[appId]) {
    trendCache[appId] = await fetchJsonCached(`/api/trends/${appId}`, CACHE_TTL_MS.trends);
  }
  const trend = trendCache[appId];
  const labels = trend.map((x) => x.timestamp.slice(0, 10));

  const precision = trend.map((x) => x.performance_precision_coherence ?? null);
  const latency = trend.map((x) => x.system_reliability_latency ?? null);
  const toxicity = trend.map((x) => x.safety_toxicity ?? null);

  const datasets = [
    { label: "Performance Precision Coherence", data: precision, yAxisID: "y", borderColor: "#1f8a70" },
    { label: "System Reliability Latency", data: latency, yAxisID: "y1", borderColor: "#f07167" },
    { label: "Safety Toxicity", data: toxicity, yAxisID: "y", borderColor: "#3d5a80" }
  ];

  if (!trendChart) {
    trendChart = new Chart(document.getElementById("trendChart"), {
      type: "line",
      data: { labels, datasets },
      options: {
        responsive: true,
        animation: false,
        scales: {
          y: { type: "linear", position: "left" },
          y1: { type: "linear", position: "right", grid: { drawOnChartArea: false } }
        }
      }
    });
    return;
  }

  trendChart.data.labels = labels;
  trendChart.data.datasets = datasets;
  trendChart.update("none");
}

function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

const debouncedLoadTrend = debounce((appId) => loadTrend(appId), 250);

async function loadBatchHistory(page = 1) {
  const payload = await fetchJsonCached(
    `/api/batch/history?page=${page}&page_size=${batchPageSize}`,
    CACHE_TTL_MS.batchHistory
  );
  currentBatchPage = Number(payload.page || page);
  renderBatchHistory(payload);
}

async function reloadDashboard() {
  const query = buildThresholdQuery();

  // Clear the trend cache on explicitly reloading the dashboard
  Object.keys(trendCache).forEach(key => delete trendCache[key]);

  const [latest, alerts, currentRun, history] = await Promise.all([
    fetchJsonCached(`/api/latest${query}`, CACHE_TTL_MS.latest),
    fetchJsonCached(`/api/alerts${query}`, CACHE_TTL_MS.alerts),
    fetchJsonCached("/api/batch/current", CACHE_TTL_MS.batchCurrent),
    fetchJsonCached(
      `/api/batch/history?page=${currentBatchPage}&page_size=${batchPageSize}`,
      CACHE_TTL_MS.batchHistory
    )
  ]);

  renderApps(latest);
  renderAlerts(alerts);
  renderBatchCurrent(currentRun);
  renderBatchHistory(history);

  const select = document.getElementById("appSelect");
  const currentValue = select.value;
  select.innerHTML = "";
  latest.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.app_id;
    option.textContent = item.app_id;
    select.appendChild(option);
  });

  if (latest.length) {
    if (currentValue && latest.some(i => i.app_id === currentValue)) {
      select.value = currentValue;
      debouncedLoadTrend(currentValue);
    } else {
      debouncedLoadTrend(latest[0].app_id);
    }
  }
}

async function boot() {
  await reloadDashboard();
  const select = document.getElementById("appSelect");
  const applyButton = document.getElementById("applyThresholds");

  select.addEventListener("change", (event) => {
    debouncedLoadTrend(event.target.value);
  });
  applyButton.addEventListener("click", reloadDashboard);
}

boot().catch((err) => {
  console.error(err);
});
