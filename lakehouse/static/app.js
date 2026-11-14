"use strict";

const state = {
  crashPoints: {},
  tables: {},
  taskIds: { A: null, B: null },
  selectedSeq: null,
  busy: false,
  retain: 0,
  lastState: null,
};

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

async function api(path, options = {}) {
  const opts = { headers: { "Content-Type": "application/json" }, ...options };
  if (opts.body && typeof opts.body !== "string") opts.body = JSON.stringify(opts.body);
  const res = await fetch(path, opts);
  let data = null;
  try { data = await res.json(); } catch (_) { data = null; }
  if (!res.ok) {
    const detail = data && data.detail ? data.detail : res.statusText;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return data;
}

function toast(message, kind = "") {
  const el = $("#toast");
  el.textContent = message;
  el.className = "toast show " + kind;
  clearTimeout(toast._t);
  toast._t = setTimeout(() => { el.className = "toast " + kind; }, 4200);
}

function esc(value) {
  return String(value === null || value === undefined ? "" : value)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function shortTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  return isNaN(d) ? iso : d.toLocaleTimeString("zh-CN", { hour12: false });
}

function table(columns, rows, emptyText) {
  if (!rows || !rows.length) {
    return `<table><thead><tr>${columns.map(c => `<th>${esc(c.label)}</th>`).join("")}
      </tr></thead><tbody><tr><td class="empty-row" colspan="${columns.length}">
      ${esc(emptyText || "无数据")}</td></tr></tbody></table>`;
  }
  const head = columns.map(c => `<th class="${c.cls || ""}">${esc(c.label)}</th>`).join("");
  const body = rows.map(r => `<tr>${columns.map(c =>
    `<td class="${c.cls || ""}">${c.render ? c.render(r) : esc(r[c.key])}</td>`).join("")}</tr>`)
    .join("");
  return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function tag(value) {
  return `<span class="tag tag-${esc(value)}">${esc(value)}</span>`;
}

function btn(label, onclick, cls = "btn btn-ghost") {
  const el = document.createElement("button");
  el.className = cls;
  el.textContent = label;
  el.addEventListener("click", onclick);
  return el;
}

/* ------------------------------------------------------------- writers */
function readWriter(letter) {
  const root = $("#writer-" + letter);
  const parts = $$('[data-field="partitions"] input:checked', root).map(i => i.value);
  return {
    type: $('[data-field="type"]', root).value,
    partitions: parts,
    rows: parseInt($('[data-field="rows"]', root).value || "0", 10),
    tag: $('[data-field="tag"]', root).value || letter,
    crash_point: $('[data-field="crash_point"]', root).value,
  };
}

function fillCrashPoints(points) {
  if (!points) return;
  const keys = Object.keys(points);
  const same = JSON.stringify(keys) === JSON.stringify(state.crashPoints ? Object.keys(state.crashPoints) : []);
  state.crashPoints = points;
  $$('[data-role="crash"]').forEach(sel => {
    // Auto-refresh must not rebuild this control: rebuilding loses whatever the
    // user picked. Only (re)populate when the option set is new or empty.
    if (same && sel.options.length === keys.length) return;
    const previous = sel.value;
    sel.innerHTML = keys
      .map(k => `<option value="${esc(k)}">${esc(points[k])}</option>`).join("");
    if (keys.includes(previous)) sel.value = previous;
  });
}

function setWriterState(letter, task) {
  const root = $("#writer-" + letter);
  const stateEl = $('[data-role="state"]', root);
  const metaEl = $('[data-role="meta"]', root);
  const commitBtn = $('[data-action="commit"]', root);
  if (!task) {
    stateEl.textContent = "未创建";
    metaEl.innerHTML = "";
    commitBtn.disabled = true;
    root.dataset.live = "false";
    return;
  }
  stateEl.innerHTML = tag(task.state);
  root.dataset.live = "true";
  const base = task.base_seq !== null && task.base_seq !== undefined
    ? `base seq ${task.base_seq}` : "";
  const res = task.result_seq ? `→ seq ${task.result_seq}` : "";
  metaEl.innerHTML = `${esc(task.id)}<br>${esc(base)} ${esc(res)}` +
    (task.recomputes ? ` · 重算 ${task.recomputes} 次` : "");
  commitBtn.disabled = task.state !== "prepared";
}

/* -------------------------------------------------------------- render */
async function refresh() {
  const retain = parseInt($("#retain-versions").value || "0", 10);
  const query = retain > 0 ? `?table=events&retain_versions=${retain}` : "?table=events";
  const data = await api("/api/state" + query);
  state.lastState = data;
  state.leaseTtl = data.lease_ttl;
  fillCrashPoints(data.crash_points);
  renderVersions(data);
  renderFiles(data);
  renderGcPending(data);
  renderIntents(data);
  renderEvents(data);
  syncTasks(data);
  renderLeasesReaders(data);
  $("#health").textContent = "SQLite 已连接 · 事件表";
  $("#health").className = "pill pill-ok";
  return data;
}

function syncTasks(data) {
  ["A", "B"].forEach(letter => {
    const id = state.taskIds[letter];
    setWriterState(letter, id ? data.tasks.find(t => t.id === id) : null);
  });
}

function renderVersions(data) {
  const el = $("#versions");
  if (!data.versions.length) { el.innerHTML = `<p class="hint">没有版本</p>`; return; }
  el.innerHTML = data.versions.map(v => {
    const selected = state.selectedSeq === v.seq ? " selected" : "";
    const pruned = v.retained ? "" : " pruned";
    const flags = [];
    if (v.stats.recomputes) flags.push(`重算 ${v.stats.recomputes}`);
    if (v.stats.attempt > 1) flags.push(`第 ${v.stats.attempt} 次尝试提交`);
    if (!v.retained) flags.push("已修剪");
    return `<div class="version-row${selected}${pruned}" data-seq="${v.seq}">
      <span class="ver-seq">#${v.seq}<small>${v.retained ? "" : " ✂"}</small></span>
      <span class="ver-kind">${tag(v.kind)}</span>
      <span class="ver-msg" title="${esc(v.message)}">${esc(v.message)}</span>
      <span class="ver-rows">${v.stats.rows} 行</span>
      ${flags.length ? `<span class="ver-flags">${esc(flags.join(" · "))}</span>` : ""}
      <button class="btn btn-ghost pin-btn" data-pin="${v.seq}"
        title="打开一个固定此版本的读者引用">固定为读者</button>
    </div>`;
  }).join("");
  $$(".version-row", el).forEach(row => row.addEventListener("click", () => {
    state.selectedSeq = parseInt(row.dataset.seq, 10);
    refresh().catch(e => toast(e.message, "bad"));
  }));
  $$("[data-pin]", el).forEach(button => button.addEventListener("click", async event => {
    event.stopPropagation();
    const seq = parseInt(button.dataset.pin, 10);
    try {
      await api("/api/readers", {
        method: "POST",
        body: { table: "events", version_seq: seq, label: `pin-seq-${seq}`, ttl: 120 },
      });
      toast(`已打开固定版本 #${seq} 的读者，GC 不会删除它引用的文件`, "ok");
      await refresh();
    } catch (e) { toast(e.message, "bad"); }
  }));
}

function renderFiles(data) {
  const rows = data.files.slice().sort((a, b) => {
    const order = { active: 0, staging: 1, pending: 2, orphaned: 3, superseded: 4, reclaimed: 5 };
    return (order[a.state] ?? 9) - (order[b.state] ?? 9);
  });
  $("#files").innerHTML = table([
    { key: "state", label: "状态", render: r => tag(r.state) },
    { key: "rel_path", label: "文件路径", cls: "mono", render: r =>
      `<span title="${esc(r.rel_path)}">${esc(r.rel_path.replace(/^data\//, ""))}</span>` +
      (r.staging_rel_path ? `<br><span class="hint">临时: ${esc(r.staging_rel_path)}</span>` : "") },
    { key: "partitions", label: "分区", cls: "mono", render: r =>
      esc((r.partitions && r.partitions.region) || "-") },
    { key: "rows", label: "行数", cls: "mono" },
    { key: "referenced_by", label: "被引用版本", cls: "mono", render: r =>
      r.referenced_by.length ? r.referenced_by.map(s => "#" + s).join(" ") : "—" },
    { key: "owner_task_id", label: "写入者", cls: "mono", render: r =>
      r.owner_task_id ? esc(r.owner_task_id.replace("task-", "")) : "—" },
    { key: "note", label: "备注", render: r => esc(r.note || "") },
  ], rows, "尚无文件");
}

function reasonBadge(c) {
  return `<span class="reason reason-${esc(c.reason)}">${esc(c.reason)}</span>` +
    (c.protected ? "" : " ⚠");
}

function renderGcPending(data) {
  const scan = data.gc_pending;
  const total = scan.candidates.length;
  $("#gc-summary").textContent =
    `${scan.reclaimable.length} 个可回收 / ${total} 个候选 · ${formatBytes(scan.reclaimable_bytes)}`;
  const rows = scan.candidates.slice().sort((a, b) => (a.protected ? 1 : 0) - (b.protected ? 1 : 0));
  $("#gc-pending").innerHTML = table([
    { key: "path", label: "文件", cls: "mono", render: c =>
      `<span title="${esc(c.path)}">${esc(c.path.replace(/^data\//, ""))}</span>` +
      (c.staging_path ? `<br><span class="hint">临时: ${esc(c.staging_path)}</span>` : "") },
    { key: "state", label: "状态", render: c => tag(c.state) },
    { key: "reason", label: "判定", render: c => reasonBadge(c) },
    { key: "detail", label: "依据", render: c => `<span class="hint">${esc(c.detail)}</span>` },
    { key: "bytes", label: "大小", cls: "mono", render: c => formatBytes(c.bytes) },
  ], rows, "没有需要回收的文件");
}

function renderLeasesReaders(data) {
  const now = Date.now();
  const leaseRows = data.leases.map(l => {
    const remaining = Math.max(0, Math.round((new Date(l.expires_at * 1000) - now) / 1000));
    return {
      ...l,
      remaining: `${remaining}s`,
      ownerTask: l.task_id ? l.task_id.replace("task-", "") : "—",
    };
  });
  const readerRows = data.readers.map(r => ({
    ...r,
    files: (r.file_ids || []).length,
    refs: (r.file_ids || []).slice(0, 3).map(f => f.replace("file-", "")).join(" "),
    stateLabel: r.effective_state || r.state,
  }));
  $("#leases-readers").innerHTML = `
    <div>
      <div class="subhead">写入租约 <span class="hint">TTL ${state.leaseTtl || ""}s</span></div>
      ${table([
        { key: "id", label: "租约", cls: "mono", render: l => esc(l.id) },
        { key: "ownerTask", label: "任务", cls: "mono" },
        { key: "state", label: "状态", render: l => tag(l.state) },
        { key: "remaining", label: "剩余", cls: "mono" },
      ], leaseRows, "无租约")}
    </div>
    <div>
      <div class="subhead">读者引用 <span class="hint">打开读者会固定版本，保护其文件</span></div>
      ${table([
        { key: "id", label: "读者", cls: "mono" },
        { key: "label", label: "标签" },
        { key: "version_seq", label: "固定版本", cls: "mono", render: r =>
          r.version_seq ? `#${r.version_seq}` : "—" },
        { key: "files", label: "引用文件", cls: "mono" },
        { key: "stateLabel", label: "状态", render: r => tag(r.stateLabel) },
        { key: "live", label: "", render: r => r.live
          ? `<button class="btn btn-ghost" data-close-reader="${esc(r.id)}">释放</button>` : "" },
      ], readerRows, "没有打开的读者")}
    </div>`;
  $$("[data-close-reader]", $("#leases-readers")).forEach(b => b.addEventListener("click", async () => {
    try {
      await api(`/api/readers/${b.dataset.closeReader}/close`, { method: "POST" });
      toast("读者已关闭，其固定引用解除", "ok");
      await refresh();
    } catch (e) { toast(e.message, "bad"); }
  }));
}

function renderIntents(data) {
  const rows = data.intents.map(i => ({
    ...i,
    base: `#${i.base_seq}`,
    result: i.result_seq ? `#${i.result_seq}` : "—",
    task: i.task_id.replace("task-", ""),
    files: i.file_ids.length,
  }));
  $("#intents").innerHTML = table([
    { key: "id", label: "意图", cls: "mono", render: i => i.id.replace("intent-", "") },
    { key: "task", label: "任务", cls: "mono" },
    { key: "op", label: "操作", render: i => `${esc(i.op.type)} ${esc((i.op.partitions || []).join(","))}` },
    { key: "base", label: "基线", cls: "mono" },
    { key: "state", label: "状态", render: i => tag(i.state) },
    { key: "attempts", label: "尝试", cls: "mono" },
    { key: "recomputes", label: "重算", cls: "mono" },
    { key: "result", label: "结果", cls: "mono" },
  ], rows, "没有提交意图") +
    taskTable(data.tasks);
}

function taskTable(tasks) {
  const rows = tasks.map(t => ({
    ...t, short: t.id.replace("task-", ""), base: t.base_seq ? `#${t.base_seq}` : "—",
  }));
  return `<div style="margin-top:12px" class="subhead">写入任务</div>` + table([
    { key: "short", label: "任务", cls: "mono" },
    { key: "name", label: "名称" },
    { key: "op", label: "操作", render: t => `${esc(t.op.type)} ${esc((t.op.partitions || []).join(","))} ×${t.op.rows}` },
    { key: "base", label: "基线", cls: "mono" },
    { key: "state", label: "状态", render: t => tag(t.state) },
    { key: "crash_point", label: "崩溃点", cls: "mono" },
    { key: "log", label: "日志", render: t => t.log.length
      ? `<button class="btn btn-ghost" data-task-log="${esc(t.id)}">${t.log.length} 条</button>` : "—" },
  ], rows, "没有任务");
}

function renderEvents(data) {
  $("#events").innerHTML = data.events.map(e => `
    <div class="event ${esc(e.level)}">
      <span class="ts">${shortTime(e.ts)}</span>
      <span class="lvl">${esc(e.level)}</span>
      <span class="msg">${esc(e.message)}</span>
    </div>`).join("") || `<p class="hint">无事件</p>`;
  $$("[data-task-log]").forEach(b => b.addEventListener("click", () => {
    const task = state.lastState.tasks.find(t => t.id === b.dataset.taskLog);
    openLog(task);
  }));
}

function openLog(task) {
  if (!task) return;
  const el = $("#task-log");
  el.className = "output-block task-log";
  $("#task-log-title").textContent =
    `${task.name} · ${task.state}${task.result_seq ? " · seq " + task.result_seq : ""}`;
  el.innerHTML = task.log.map(l =>
    `<div class="log-line log-${esc(l.level)}"><span class="ts">${shortTime(l.ts)}</span>` +
    `<span class="lvl">${esc(l.level)}</span><span class="msg">${esc(l.message)}</span></div>`
  ).join("") || `<p class="hint">该任务尚无日志</p>`;
}

function formatBytes(bytes) {
  const n = Number(bytes || 0);
  if (n < 1024) return n + " B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
  return (n / 1024 / 1024).toFixed(2) + " MB";
}

async function queryAllVersions() {
  const data = await api("/api/query-all?table=events");
  const out = $("#query-all-out");
  const allOk = data.versions.every(v => v.ok && v.rows_match);
  out.className = "output-block " + (allOk ? "ok" : "bad");
  out.innerHTML = `
    <div class="banner ${allOk ? "ok" : "bad"}">
      逐版本实际查询：${data.versions.filter(v => v.ok).length}/${data.versions.length}
      个版本可读${allOk ? "，行数与清单全部一致" : "，存在不一致或不可读版本"}
    </div>
    ${table([
      { key: "seq", label: "版本", cls: "mono", render: v => `#${v.seq}` },
      { key: "ok", label: "可读", render: v => v.ok ? "✓" : "✗" },
      { key: "rows", label: "查询行数", cls: "mono" },
      { key: "expected_rows", label: "清单行数", cls: "mono" },
      { key: "rows_match", label: "一致", render: v => v.ok && v.rows_match ? "✓" : "✗" },
      { key: "file_count", label: "文件", cls: "mono" },
      { key: "by_region", label: "分区行数（us / eu / apac）", cls: "mono", render: v =>
        (v.by_region || []).map(r => `${r.region}:${r.rows}`).join("  ") },
    ], data.versions, "没有版本")}`;
  $("#query-all-caption").textContent =
    `已核对 ${data.versions.length} 个版本 · ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;
  toast(allOk ? "所有已提交版本均可查询且行数一致" : "存在不可读或行数不一致的版本",
    allOk ? "ok" : "bad");
}

/* -------------------------------------------------------------- query */
async function querySeq(seq) {
  state.selectedSeq = seq;
  $("#query-caption").textContent = `版本 #${seq}`;
  const out = $("#query-out");
  out.className = "output-block query-out";
  out.textContent = "查询中…";
  try {
    const data = await api(`/api/query?table=events&seq=${seq}&limit=20`);
    if (!data.ok) {
      out.textContent = data.error === "version_pruned"
        ? `版本 #${seq} 已被保留策略修剪，其文件不再可读。`
        : `查询失败：${data.error || "未知错误"} ${(data.missing || []).join(", ")}`;
      out.classList.add("bad");
      return;
    }
    const rows = data.by_region.map(r => ({
      region: r.region, rows: r.rows, sum: r.amount_sum,
    }));
    const sample = data.sample.map(s => ({
      id: s.id, region: s.region, amount: s.amount, note: s.note, seq: s.base_seq,
    }));
    out.innerHTML = `
      <div class="banner ok">
        DuckDB 实时查询版本 #${data.seq} · ${data.file_count} 个 Parquet 文件 ·
        ${data.rows} 行（清单声明 ${data.expected_rows} 行，${data.rows_match ? "一致" : "不一致"}）·
        amount 合计 ${data.amount_sum}
      </div>
      <div class="subhead">按分区汇总</div>
      ${table([
        { key: "region", label: "分区" },
        { key: "rows", label: "行数", cls: "mono" },
        { key: "sum", label: "amount 合计", cls: "mono" },
      ], rows, "无数据")}
      <div class="subhead" style="margin-top:12px">行样本（最多 20 行）</div>
      ${table([
        { key: "id", label: "id", cls: "mono" },
        { key: "region", label: "region", cls: "mono" },
        { key: "amount", label: "amount", cls: "mono" },
        { key: "seq", label: "写入时基线", cls: "mono" },
        { key: "note", label: "note" },
      ], sample, "无行")}`;
  } catch (e) {
    out.classList.add("bad");
    out.textContent = e.message;
  }
}

/* ------------------------------------------------------------- actions */
let actionChain = Promise.resolve();

function withBusy(fn) {
  // Queue user actions instead of silently dropping clicks that arrive while a
  // previous request or the auto-refresh is still running.
  actionChain = actionChain.then(async () => {
    state.busy = true;
    try { await fn(); } catch (e) { toast(e.message, "bad"); }
    finally { state.busy = false; }
  });
  return actionChain;
}

async function prepare(letter) {
  const spec = readWriter(letter);
  if (!spec.partitions.length) { toast("请至少选择一个分区", "bad"); return; }
  const task = await api("/api/tasks", {
    method: "POST",
    body: { table: "events", name: `写入者 ${letter} · ${spec.type}`, op: {
      type: spec.type, partitions: spec.partitions, rows: spec.rows, tag: spec.tag,
    }, crash_point: spec.crash_point },
  });
  state.taskIds[letter] = task.id;
  toast(task.state === "crashed"
    ? `任务已在 ${task.crash_point} 注入崩溃`
    : `任务已准备，基线 seq ${task.base_seq}`, task.state === "crashed" ? "bad" : "ok");
  await refresh();
  openLog(task);
}

async function commitOne(letter) {
  const id = state.taskIds[letter];
  if (!id) { toast("请先创建该写入任务", "bad"); return; }
  const task = await api(`/api/tasks/${id}/commit`, { method: "POST" });
  toast(task.state === "committed"
    ? `提交成功，版本 seq ${task.result_seq}`
    : `任务状态：${task.state}`, task.state === "committed" ? "ok" : "bad");
  await refresh();
  openLog(task);
  if (task.result_seq) await querySeq(task.result_seq);
}

async function commitBoth(order) {
  const ids = [state.taskIds.A, state.taskIds.B].filter(Boolean);
  if (ids.length < 2) { toast("请先创建两个写入任务", "bad"); return; }
  const ordered = order === "BA" ? [state.taskIds.B, state.taskIds.A]
    : order === "concurrent" ? [state.taskIds.A, state.taskIds.B] : ids;
  const data = await api("/api/commits", {
    method: "POST",
    body: { table: "events", task_ids: ordered, order: order === "concurrent" ? "concurrent" : "sequential" },
  });
  const errs = Object.keys(data.errors || {});
  const skips = Object.keys(data.skipped || {});
  let message = errs.length ? `部分任务失败：${data.errors[errs[0]]}`
    : order === "concurrent" ? "并发提交完成" : "顺序提交完成";
  if (skips.length) message += ` · ${skips.length} 个任务因状态不符被跳过`;
  toast(message, errs.length ? "bad" : "ok");
  await refresh();
  const latest = state.lastState.versions[0];
  if (latest) await querySeq(latest.seq);
}

async function runRecovery() {
  const out = $("#recovery-out");
  const data = await api("/api/recover", { method: "POST", body: { table: "events" } });
  out.className = "output-block " + (data.consistent ? "ok" : "bad");
  const actions = data.actions.length
    ? data.actions.map(a => `• [${a.action}] ${a.detail}`).join("\n")
    : "• 没有需要恢复的任务";
  out.textContent = `恢复完成（${data.run_id}）\n${actions}\n\n` +
    `恢复前核对: ${data.before.ok ? "通过" : "失败"} · 恢复后核对: ${data.after.ok ? "通过" : "失败"}`;
  toast(data.consistent ? "恢复完成，前后核对通过" : "恢复完成，但核对发现问题",
    data.consistent ? "ok" : "bad");
  await refresh();
}

async function runVerify() {
  const out = $("#verify-out");
  const data = await api("/api/verify", { method: "POST", body: { table: "events" } });
  out.className = "output-block " + (data.ok ? "ok" : "bad");
  const checks = data.checks.map(c =>
    `${c.ok ? "✓" : "✗"} ${c.name}${c.ok ? "" : " — " + JSON.stringify(c.detail)}`).join("\n");
  const fps = Object.entries(data.fingerprints || {}).map(([seq, fp]) => `  seq ${seq}: ${fp}`).join("\n");
  out.textContent = `一致性核对：${data.ok ? "全部通过" : "存在失败项"}\n${checks}` +
    (fps ? `\n\n内容指纹（行内容哈希，与此前记录相同表示数据未变）\n${fps}` : "");
  toast(data.ok ? "一致性核对通过" : "一致性核对失败", data.ok ? "ok" : "bad");
  await refresh();
}

function retainValue() {
  const raw = $("#retain-versions").value.trim();
  if (raw === "") return 0;
  return parseInt(raw, 10) || 0;
}

async function gcScan() {
  const retain = retainValue();
  const data = await api("/api/gc/scan", {
    method: "POST", body: { table: "events", retain_versions: retain > 0 ? retain : null },
  });
  const out = $("#gc-out");
  out.className = "output-block";
  const prot = data.protected.map(c => `• 保护 ${c.path} — ${c.reason}：${c.detail}`).join("\n");
  const rec = data.reclaimable.map(c => `• 可回收 ${c.path} — ${c.state}（${formatBytes(c.bytes)}）`).join("\n");
  const expire = (data.versions_to_expire || []).map(v => `#${v.seq}`).join(" ") || "无";
  const pinned = (data.versions_pinned || []).map(v => `#${v.seq}`).join(" ") || "无";
  out.textContent = `扫描结果：${data.reclaimable.length} 个可回收，${data.protected.length} 个受保护\n` +
    (prot ? prot + "\n" : "") + (rec ? rec + "\n" : "") +
    `\n保留策略：将修剪版本 ${expire}；被读者固定而保留 ${pinned}`;
  await refresh();
}

async function gcReclaim() {
  const retain = retainValue();
  const data = await api("/api/gc/reclaim", {
    method: "POST", body: { table: "events", retain_versions: retain > 0 ? retain : null },
  });
  const out = $("#gc-out");
  out.className = "output-block " + (data.consistent ? "ok" : "bad");
  out.textContent = `回收完成（${data.run_id}）\n` +
    `• 删除文件 ${data.reclaimed.length} 个：\n` +
    (data.reclaimed.map(c => `   - ${c.path}（${c.reason}）`).join("\n") || "   （无）") +
    `\n• 因保护跳过 ${data.skipped.length} 个：\n` +
    (data.skipped.map(c => `   - ${c.path} — ${c.reason}`).join("\n") || "   （无）") +
    `\n• 修剪版本：${(data.expired_versions || []).map(v => "#" + v.seq).join(" ") || "无"}` +
    `\n\n回收前核对: ${data.before.ok ? "通过" : "失败"} · 回收后核对: ${data.after.ok ? "通过" : "失败"}`;
  toast(data.consistent ? "回收完成，前后一致性核对通过" : "回收完成但核对失败",
    data.consistent ? "ok" : "bad");
  await refresh();
  const latest = state.lastState.versions[0];
  if (latest) await querySeq(latest.seq);
}

/* --------------------------------------------------------------- wiring */
function wire() {
  ["A", "B"].forEach(letter => {
    const root = $("#writer-" + letter);
    $('[data-action="prepare"]', root).addEventListener("click", () =>
      withBusy(() => prepare(letter)));
    $('[data-action="commit"]', root).addEventListener("click", () =>
      withBusy(() => commitOne(letter)));
    $('[data-field="type"]', root).addEventListener("change", () => {
      const isReplace = $('[data-field="type"]', root).value === "replace";
      $('[data-field="rows"]', root).disabled = isReplace;
      $('[data-field="rows"]', root).title = isReplace
        ? "替换操作按基线分区内容派生，行数由基线决定" : "";
    });
  });
  $("#btn-commit-ab").addEventListener("click", () => withBusy(() => commitBoth("AB")));
  $("#btn-commit-ba").addEventListener("click", () => withBusy(() => commitBoth("BA")));
  $("#btn-commit-both").addEventListener("click", () => withBusy(() => commitBoth("concurrent")));
  $("#btn-recover").addEventListener("click", () => withBusy(runRecovery));
  $("#btn-verify").addEventListener("click", () => withBusy(runVerify));
  $("#btn-gc-scan").addEventListener("click", () => withBusy(gcScan));
  $("#btn-gc-reclaim").addEventListener("click", () => withBusy(gcReclaim));
  $("#btn-query-all").addEventListener("click", () => withBusy(queryAllVersions));
  $("#btn-refresh").addEventListener("click", () => withBusy(refresh));
  $("#btn-reset").addEventListener("click", () => withBusy(async () => {
    await api("/api/demo/reset", { method: "POST", body: {} });
    state.taskIds = { A: null, B: null };
    state.selectedSeq = null;
    setWriterState("A", null); setWriterState("B", null);
    $("#query-out").textContent = "";
    $("#gc-out").textContent = "";
    $("#recovery-out").textContent = "";
    $("#verify-out").textContent = "";
    toast("演示数据已重置", "ok");
    await refresh();
    await querySeq(1);
  }));
  $("#retain-versions").addEventListener("change", () => withBusy(refresh));
  setInterval(() => { if (!state.busy) refresh().catch(() => {}); }, 4000);
}

wire();
refresh().then(() => querySeq(1)).catch(e => {
  $("#health").textContent = "连接失败";
  $("#health").className = "pill pill-bad";
  toast(e.message, "bad");
});
