const CLIENT_DRAFT_KEY = "bb_client_request_draft_v2";

let draftStatusEl = null;
let qolToastEl = null;
let requestSearchInput = null;
let requestStatusFilter = null;
let requestsObserver = null;

function escHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function showToast(message) {
  if (!qolToastEl) {
    qolToastEl = document.createElement("div");
    qolToastEl.className = "qol-toast";
    document.body.appendChild(qolToastEl);
  }
  qolToastEl.textContent = message;
  qolToastEl.classList.add("visible");
  window.clearTimeout(showToast._timer);
  showToast._timer = window.setTimeout(() => qolToastEl.classList.remove("visible"), 1500);
}

function setDraftStatus(message) {
  if (!draftStatusEl) return;
  draftStatusEl.textContent = message || "";
}

function requestFormElements() {
  return {
    form: document.getElementById("clientRequestForm"),
    title: document.getElementById("crTitle"),
    description: document.getElementById("crDescription"),
    category: document.getElementById("crCategory"),
    priority: document.getElementById("crPriority"),
    status: document.getElementById("crStatus"),
  };
}

function readDraft() {
  try {
    const raw = localStorage.getItem(CLIENT_DRAFT_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    return parsed;
  } catch {
    return null;
  }
}

function writeDraft(payload) {
  try {
    localStorage.setItem(CLIENT_DRAFT_KEY, JSON.stringify(payload));
  } catch {
    // Ignore storage failures to avoid blocking form usage.
  }
}

function clearDraft() {
  try {
    localStorage.removeItem(CLIENT_DRAFT_KEY);
  } catch {
    // Ignore storage failures.
  }
}

function installRequestTemplates() {
  const { form, title, description, category, priority } = requestFormElements();
  if (!form || !title || !description || !category || !priority) return;

  const tools = document.createElement("div");
  tools.className = "qol-request-tools";
  tools.innerHTML = `
    <div class="qol-request-tools-head">
      <div class="qol-request-tools-title">Quick Request Templates</div>
      <button type="button" class="qol-ghost-btn" id="qolClearDraftBtn">Clear Draft</button>
    </div>
    <div class="qol-chip-row">
      <button type="button" class="qol-chip" data-template="issue">Report Issue</button>
      <button type="button" class="qol-chip" data-template="website">Website Tweak</button>
      <button type="button" class="qol-chip" data-template="automation">Automation Update</button>
      <button type="button" class="qol-chip" data-template="content">Content Refresh</button>
    </div>
    <div class="qol-draft-status" id="qolDraftStatus"></div>
  `;
  form.insertBefore(tools, form.firstElementChild);
  draftStatusEl = document.getElementById("qolDraftStatus");

  const templates = {
    issue: {
      category: "issue",
      priority: "high",
      title: "Issue: ",
      description:
        "What happened:\nExpected behavior:\nUrgency/impact:\nLinks or screenshots (if any):",
    },
    website: {
      category: "website_change",
      priority: "normal",
      title: "Website change: ",
      description:
        "Page/section:\nRequested change:\nPreferred wording/design direction:\nDeadline (if any):",
    },
    automation: {
      category: "automation_adjustment",
      priority: "normal",
      title: "Automation update: ",
      description:
        "Current workflow:\nNeeded adjustment:\nDesired outcome:\nAny blockers/errors:",
    },
    content: {
      category: "content_update",
      priority: "low",
      title: "Content update: ",
      description:
        "Content location:\nNew content:\nTone/style notes:\nPriority timing:",
    },
  };

  tools.querySelectorAll(".qol-chip").forEach((btn) => {
    btn.addEventListener("click", () => {
      const key = String(btn.getAttribute("data-template") || "");
      const tpl = templates[key];
      if (!tpl) return;
      category.value = tpl.category;
      priority.value = tpl.priority;
      if (!title.value.trim()) title.value = tpl.title;
      if (!description.value.trim()) description.value = tpl.description;
      title.dispatchEvent(new Event("input", { bubbles: true }));
      description.dispatchEvent(new Event("input", { bubbles: true }));
      setDraftStatus("Template applied.");
    });
  });

  document.getElementById("qolClearDraftBtn")?.addEventListener("click", () => {
    clearDraft();
    title.value = "";
    description.value = "";
    category.value = "issue";
    priority.value = "normal";
    title.dispatchEvent(new Event("input", { bubbles: true }));
    description.dispatchEvent(new Event("input", { bubbles: true }));
    setDraftStatus("Draft cleared.");
  });
}

function installCharCounters() {
  const titleInput = document.getElementById("crTitle");
  const descInput = document.getElementById("crDescription");
  if (!titleInput || !descInput) return;

  const titleCounter = document.createElement("div");
  const descCounter = document.createElement("div");
  titleCounter.className = "qol-counter";
  descCounter.className = "qol-counter";
  titleInput.parentElement?.appendChild(titleCounter);
  descInput.parentElement?.appendChild(descCounter);

  function applyCounterState(el, count, max) {
    const ratio = max ? count / max : 0;
    el.classList.toggle("warn", ratio >= 0.8 && ratio < 0.95);
    el.classList.toggle("danger", ratio >= 0.95);
  }

  function updateCounters() {
    const titleLen = String(titleInput.value || "").length;
    const descLen = String(descInput.value || "").length;
    const titleMax = Number(titleInput.maxLength || 140);
    const descMax = Number(descInput.maxLength || 4000);
    titleCounter.textContent = `${titleLen}/${titleMax}`;
    descCounter.textContent = `${descLen}/${descMax}`;
    applyCounterState(titleCounter, titleLen, titleMax);
    applyCounterState(descCounter, descLen, descMax);
  }

  titleInput.addEventListener("input", updateCounters);
  descInput.addEventListener("input", updateCounters);
  updateCounters();
}

function installDraftAutosave() {
  const { title, description, category, priority, status } = requestFormElements();
  if (!title || !description || !category || !priority) return;

  const draft = readDraft();
  if (draft && !title.value.trim() && !description.value.trim()) {
    title.value = String(draft.title || "");
    description.value = String(draft.description || "");
    if (draft.category) category.value = String(draft.category);
    if (draft.priority) priority.value = String(draft.priority);
    title.dispatchEvent(new Event("input", { bubbles: true }));
    description.dispatchEvent(new Event("input", { bubbles: true }));
    setDraftStatus("Draft restored.");
  }

  let writeTimer = null;
  const queueWrite = () => {
    window.clearTimeout(writeTimer);
    writeTimer = window.setTimeout(() => {
      const next = {
        title: title.value.trim(),
        description: description.value.trim(),
        category: category.value || "general_support",
        priority: priority.value || "normal",
        updatedAt: Date.now(),
      };
      if (!next.title && !next.description) {
        clearDraft();
        return;
      }
      writeDraft(next);
      setDraftStatus("Draft saved locally.");
    }, 220);
  };

  [title, description, category, priority].forEach((field) => {
    field.addEventListener("input", queueWrite);
    field.addEventListener("change", queueWrite);
  });

  if (status) {
    const obs = new MutationObserver(() => {
      const text = String(status.textContent || "");
      const isError = status.classList.contains("error");
      if (!isError && text.toLowerCase().includes("request submitted")) {
        clearDraft();
        setDraftStatus("Draft cleared after submit.");
      }
    });
    obs.observe(status, { childList: true, characterData: true, subtree: true, attributes: true });
  }
}

function statusLabelToKey(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) return "submitted";
  return normalized.replace(/\s+/g, "_");
}

function applyRequestTableFilters() {
  const tbody = document.getElementById("clientRequestsBody");
  if (!tbody || !requestSearchInput || !requestStatusFilter) return;
  const searchNeedle = requestSearchInput.value.trim().toLowerCase();
  const statusNeedle = requestStatusFilter.value;

  Array.from(tbody.querySelectorAll("tr")).forEach((row) => {
    const cells = row.querySelectorAll("td");
    if (!cells.length) return;
    if (cells.length === 1 && cells[0].getAttribute("colspan")) {
      row.style.display = "";
      return;
    }
    const title = String(cells[1]?.textContent || "").toLowerCase();
    const category = String(cells[2]?.textContent || "").toLowerCase();
    const statusKey = statusLabelToKey(cells[4]?.textContent || "");
    const matchesSearch = !searchNeedle || title.includes(searchNeedle) || category.includes(searchNeedle);
    const matchesStatus = statusNeedle === "all" || statusKey === statusNeedle;
    row.style.display = matchesSearch && matchesStatus ? "" : "none";
  });
}

function installRequestFilters() {
  const table = document.querySelector(".requests-table");
  if (!table) return;
  const wrap = table.closest(".table-wrap");
  if (!wrap || !wrap.parentElement) return;

  const filterRow = document.createElement("div");
  filterRow.className = "qol-requests-filters";
  filterRow.innerHTML = `
    <input id="qolReqSearch" type="text" placeholder="Search your request titles or categories">
    <select id="qolReqStatusFilter">
      <option value="all">All statuses</option>
      <option value="submitted">Submitted</option>
      <option value="in_review">In Review</option>
      <option value="scheduled">Scheduled</option>
      <option value="done">Done</option>
    </select>
  `;

  wrap.parentElement.insertBefore(filterRow, wrap);
  requestSearchInput = document.getElementById("qolReqSearch");
  requestStatusFilter = document.getElementById("qolReqStatusFilter");
  requestSearchInput?.addEventListener("input", applyRequestTableFilters);
  requestStatusFilter?.addEventListener("change", applyRequestTableFilters);

  const tbody = document.getElementById("clientRequestsBody");
  if (tbody) {
    requestsObserver?.disconnect?.();
    requestsObserver = new MutationObserver(() => applyRequestTableFilters());
    requestsObserver.observe(tbody, { childList: true, subtree: true });
  }
  applyRequestTableFilters();
}

function buildRequestSummaryText() {
  const body = document.getElementById("clientRequestDetailBody");
  if (!body) return "";

  const lines = [];
  body.querySelectorAll(".client-detail-field").forEach((field) => {
    const label = field.querySelector(".client-detail-label")?.textContent?.trim() || "";
    const value = field.querySelector(".client-detail-value")?.textContent?.trim() || "";
    if (label && value) lines.push(`${label}: ${value}`);
  });

  const description = body.querySelector(".client-detail-description")?.textContent?.trim() || "";
  if (description) {
    lines.push("");
    lines.push("Description:");
    lines.push(description);
  }

  return lines.join("\n").trim();
}

function installCopySummaryAction() {
  const head = document.querySelector(".client-modal-head");
  if (!head || document.getElementById("qolCopySummaryBtn")) return;

  const copyBtn = document.createElement("button");
  copyBtn.type = "button";
  copyBtn.id = "qolCopySummaryBtn";
  copyBtn.className = "btn btn-ghost qol-copy-btn";
  copyBtn.textContent = "Copy Summary";
  head.insertBefore(copyBtn, document.getElementById("clientRequestCloseBtn"));

  copyBtn.addEventListener("click", async () => {
    const summary = buildRequestSummaryText();
    if (!summary) {
      showToast("No request summary to copy yet.");
      return;
    }
    try {
      await navigator.clipboard.writeText(summary);
      showToast("Request summary copied.");
    } catch {
      showToast("Clipboard blocked. Try again.");
    }
  });
}

function initClientQol() {
  installRequestTemplates();
  installCharCounters();
  installDraftAutosave();
  installRequestFilters();
  installCopySummaryAction();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initClientQol);
} else {
  initClientQol();
}
