"use strict";

const $ = (id) => document.getElementById(id);

async function call(method, url, payload) {
  const options = { method, headers: {} };
  if (payload !== undefined) {
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(payload);
  }
  const response = await fetch(url, options);
  let data = null;
  try {
    data = await response.json();
  } catch (_) {
    /* empty body */
  }
  if (!response.ok) {
    const detail = data && data.detail ? data.detail : `HTTP ${response.status}`;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return data;
}

function fmtTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

const STATUS_TEXT = {
  active: "已启用",
  building: "重建中",
  failed: "失败",
  retired: "已退役",
};

function renderStatus(status) {
  const box = $("status-box");
  const details = $("index-details");
  box.textContent = "";
  details.textContent = "";

  const active = status.active;
  if (!active) {
    box.innerHTML = '<span class="dot err"></span>没有可用索引';
  } else if (status.building) {
    const pct = status.progress ? status.progress.percent : 0;
    box.innerHTML =
      `<span class="dot warn"></span>索引重建中 ${pct}% · 检索仍使用旧索引 #${active.id}`;
  } else {
    box.innerHTML = `<span class="dot ok"></span>索引就绪 #${active.id}`;
  }

  const add = (label, value) => {
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.textContent = value;
    details.append(dt, dd);
  };

  add("日志总数", String(status.total_logs));
  if (active) {
    add("当前索引", `#${active.id}（${STATUS_TEXT[active.status] || active.status}）`);
    add("已收录", `${active.doc_count} 条`);
    add("启用时间", fmtTime(active.activated_at));
  }
  if (status.building) {
    const b = status.building;
    add("正在重建", `#${b.id}，已扫描 ${b.build_cursor} / ${b.start_id}`);
    const bar = document.createElement("progress");
    bar.max = 100;
    bar.value = status.progress ? status.progress.percent : 0;
    const dt = document.createElement("dt");
    dt.textContent = "进度";
    const dd = document.createElement("dd");
    dd.appendChild(bar);
    details.append(dt, dd);
  }
  const latest = status.latest;
  if (latest && latest.status === "failed") {
    add("最近一次重建", `失败：${latest.error || "未知错误"}`);
  }

  $("rebuild-button").disabled = Boolean(status.building);
}

function renderResults(result, q, service) {
  const list = $("results");
  const meta = $("search-meta");
  list.textContent = "";
  const filters = [];
  if (service) filters.push(`服务 ${service}`);
  if (q) filters.push(`关键词 ${q}`);
  meta.textContent =
    `命中 ${result.total} 条` +
    (filters.length ? `（${filters.join("，")}）` : "") +
    ` · 索引 #${result.generation.id}`;

  for (const item of result.items) {
    const li = document.createElement("li");
    const metaRow = document.createElement("div");
    metaRow.className = "meta";
    const svc = document.createElement("span");
    svc.className = "service";
    svc.textContent = item.service;
    const time = document.createElement("span");
    time.textContent = fmtTime(item.created_at);
    const id = document.createElement("span");
    id.textContent = `#${item.id}`;
    metaRow.append(svc, time, id);
    const pre = document.createElement("pre");
    pre.textContent = item.body;
    li.append(metaRow, pre);
    list.appendChild(li);
  }
  if (result.items.length === 0) {
    const li = document.createElement("li");
    li.className = "hint";
    li.textContent = "没有匹配的日志。";
    list.appendChild(li);
  }
}

async function loadServices() {
  try {
    const data = await call("GET", "/api/services");
    const list = $("service-list");
    list.textContent = "";
    for (const item of data.items) {
      const option = document.createElement("option");
      option.value = item.service;
      option.label = `${item.count} 条`;
      list.appendChild(option);
    }
  } catch (_) {
    /* service list is a convenience only */
  }
}

async function refreshStatus() {
  try {
    renderStatus(await call("GET", "/api/status"));
  } catch (error) {
    $("status-box").innerHTML =
      `<span class="dot err"></span>无法获取索引状态：${error.message}`;
  }
}

$("log-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = $("submit-log");
  const hint = $("log-hint");
  button.disabled = true;
  hint.textContent = "写入中…";
  try {
    const saved = await call("POST", "/api/logs", {
      service: $("log-service").value,
      body: $("log-body").value,
    });
    hint.textContent = `已写入 #${saved.id}（${saved.service}）`;
    $("log-body").value = "";
    void loadServices();
    void refreshStatus();
  } catch (error) {
    hint.textContent = `写入失败：${error.message}`;
  } finally {
    button.disabled = false;
  }
});

$("search-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const q = $("search-q").value.trim();
  const service = $("search-service").value.trim();
  const hint = $("search-meta");
  if (!q && !service) {
    hint.textContent = "请至少填写服务名或关键词。";
    $("results").textContent = "";
    return;
  }
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  if (service) params.set("service", service);
  try {
    const result = await call("GET", `/api/logs?${params}`);
    renderResults(result, q, service);
  } catch (error) {
    hint.textContent = `检索失败：${error.message}`;
    $("results").textContent = "";
  }
});

$("rebuild-button").addEventListener("click", async () => {
  const button = $("rebuild-button");
  const hint = $("rebuild-hint");
  button.disabled = true;
  try {
    const data = await call("POST", "/api/reindex");
    hint.textContent = `已发起重建 #${data.building.id}，期间可继续录入和检索。`;
  } catch (error) {
    hint.textContent = `无法发起重建：${error.message}`;
  } finally {
    await refreshStatus();
  }
});

void refreshStatus();
void loadServices();
setInterval(refreshStatus, 1000);
setInterval(loadServices, 15000);
