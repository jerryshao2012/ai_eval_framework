let trendChart;

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed for ${url}`);
  }
  return response.json();
}

function buildThresholdQuery() {
  const params = new URLSearchParams();
  const enabled = document.getElementById("dynamicThresholds").checked;
  if (!enabled) {
    return "";
  }

  params.set("dynamic_thresholds", "1");

  const accuracyWarning = document.getElementById("accuracyWarning").value;
  const latencyWarning = document.getElementById("latencyWarning").value;
  const driftWarning = document.getElementById("driftWarning").value;

  if (accuracyWarning !== "") {
    params.set("threshold.accuracy.warning", accuracyWarning);
    params.set("direction.accuracy.warning", "min");
  }
  if (latencyWarning !== "") {
    params.set("threshold.latency_p95_ms.warning", latencyWarning);
    params.set("direction.latency_p95_ms.warning", "max");
  }
  if (driftWarning !== "") {
    params.set("threshold.input_length_drift.warning", driftWarning);
    params.set("direction.input_length_drift.warning", "max");
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
      .map((m) => `${m.metric_name}: ${Number(m.value).toFixed(3)}`)
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
  const trend = await fetchJson(`/api/trends/${appId}`);
  const labels = trend.map((x) => x.timestamp.slice(0, 10));

  const accuracy = trend.map((x) => x.accuracy ?? null);
  const latency = trend.map((x) => x.latency_p95_ms ?? null);
  const drift = trend.map((x) => x.input_length_drift ?? null);

  const datasets = [
    { label: "Accuracy", data: accuracy, yAxisID: "y", borderColor: "#1f8a70" },
    { label: "P95 Latency (ms)", data: latency, yAxisID: "y1", borderColor: "#f07167" },
    { label: "Input Drift", data: drift, yAxisID: "y", borderColor: "#3d5a80" }
  ];

  if (trendChart) {
    trendChart.destroy();
  }

  trendChart = new Chart(document.getElementById("trendChart"), {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      scales: {
        y: { type: "linear", position: "left" },
        y1: { type: "linear", position: "right", grid: { drawOnChartArea: false } }
      }
    }
  });
}

async function reloadDashboard() {
  const query = buildThresholdQuery();
  const latest = await fetchJson(`/api/latest${query}`);
  const alerts = await fetchJson(`/api/alerts${query}`);

  renderApps(latest);
  renderAlerts(alerts);

  const select = document.getElementById("appSelect");
  select.innerHTML = "";
  latest.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.app_id;
    option.textContent = item.app_id;
    select.appendChild(option);
  });

  if (latest.length) {
    await loadTrend(latest[0].app_id);
  }
}

async function boot() {
  await reloadDashboard();
  const select = document.getElementById("appSelect");
  const applyButton = document.getElementById("applyThresholds");

  select.addEventListener("change", async (event) => {
    await loadTrend(event.target.value);
  });
  applyButton.addEventListener("click", reloadDashboard);
}

boot().catch((err) => {
  console.error(err);
});
