const GO_SEQUENCE_MS = 1600;
const HELP_SEEN_STORAGE_KEY = "bb_portal_help_seen_v1";

let qkOverlay = null;
let qkInput = null;
let qkList = null;
let qkToast = null;
let qkOpen = false;
let qkSelected = 0;
let qkActions = [];
let goModeUntil = 0;
let qkHelpButton = null;
let qkHelpPanel = null;
let qkHelpHint = null;
let qkHelpOpen = false;

function isTypingTarget(target) {
  if (!target) return false;
  const tag = String(target.tagName || "").toLowerCase();
  if (tag === "input" || tag === "textarea" || tag === "select") return true;
  return target.isContentEditable === true;
}

function workspaceVisible() {
  const app = document.getElementById("app");
  return Boolean(app?.classList.contains("visible"));
}

function currentPageName() {
  const active = document.querySelector(".page.active");
  if (!active?.id?.startsWith("page-")) return "pipeline";
  return active.id.slice(5);
}

function pageNavElement(page) {
  return (
    document.querySelector(`.nav-item[data-page="${page}"]`)
    || document.getElementById(`tnav-${page}`)
    || null
  );
}

function gotoPage(page) {
  if (typeof window.showPage !== "function") return;
  window.showPage(page, pageNavElement(page));
}

function runWhenWorkspaceReady(callback) {
  if (!workspaceVisible()) {
    showToast("Sign in to use workspace actions.");
    return;
  }
  callback();
}

function contextAwareAdd() {
  if (typeof window.openAddModal !== "function") return;
  const page = currentPageName();
  const supported = new Set(["pipeline", "tasks", "decisions", "clients"]);
  const mode = supported.has(page) ? page : (page === "mywork" ? "tasks" : "pipeline");
  window.openAddModal(mode);
}

function focusClientsSearch() {
  gotoPage("clients");
  const input = document.getElementById("clientsSearchInput");
  if (!input) return;
  input.focus();
  input.select?.();
}

function gotoMessagesRequests() {
  gotoPage("clients");
  window.requestAnimationFrame(() => {
    const requestsSection = document.querySelector(".clients-requests");
    requestsSection?.scrollIntoView({ behavior: "smooth", block: "start" });
  });
}

function setPipelineRadarFilter(filter, toastLabel) {
  gotoPage("pipeline");
  if (typeof window.setPipelineFocusFilter !== "function") {
    showToast("Pipeline radar filters are unavailable.");
    return;
  }
  window.setPipelineFocusFilter(filter);
  if (toastLabel) showToast(toastLabel);
}

function makeActions() {
  return [
    {
      id: "go_pipeline",
      label: "Go to Pipeline",
      hint: "G, P",
      keywords: "pipeline leads sales",
      run: () => runWhenWorkspaceReady(() => gotoPage("pipeline")),
    },
    {
      id: "pipeline_focus_stale",
      label: "Pipeline Focus: Stale Leads",
      hint: "Radar",
      keywords: "pipeline stale follow up aging leads",
      run: () => runWhenWorkspaceReady(() => setPipelineRadarFilter("stale", "Pipeline focus set: stale leads.")),
    },
    {
      id: "pipeline_focus_unassigned",
      label: "Pipeline Focus: Unassigned",
      hint: "Radar",
      keywords: "pipeline unassigned owner handoff",
      run: () => runWhenWorkspaceReady(() => setPipelineRadarFilter("unassigned", "Pipeline focus set: unassigned leads.")),
    },
    {
      id: "pipeline_focus_high_intent",
      label: "Pipeline Focus: High Intent",
      hint: "Radar",
      keywords: "pipeline high intent proposal conversation",
      run: () => runWhenWorkspaceReady(() => setPipelineRadarFilter("high_intent", "Pipeline focus set: high intent.")),
    },
    {
      id: "pipeline_focus_clear",
      label: "Pipeline Focus: Clear Filter",
      hint: "Radar",
      keywords: "pipeline clear reset radar filter",
      run: () => runWhenWorkspaceReady(() => setPipelineRadarFilter("all", "Pipeline focus cleared.")),
    },
    {
      id: "go_messages_requests",
      label: "Go to Messages / Requests",
      hint: "G, M",
      keywords: "messages requests client requests inbox",
      run: () => runWhenWorkspaceReady(() => gotoMessagesRequests()),
    },
    {
      id: "go_tasks",
      label: "Go to Tasks",
      hint: "G, T",
      keywords: "tasks todo assignment",
      run: () => runWhenWorkspaceReady(() => gotoPage("tasks")),
    },
    {
      id: "go_clients",
      label: "Go to Clients",
      hint: "G, C",
      keywords: "clients ops billing",
      run: () => runWhenWorkspaceReady(() => gotoPage("clients")),
    },
    {
      id: "go_dashboard",
      label: "Go to Dashboard",
      hint: "G, D",
      keywords: "dashboard my work overview",
      run: () => runWhenWorkspaceReady(() => gotoPage("mywork")),
    },
    {
      id: "go_decisions",
      label: "Go to Decisions Log",
      hint: "Quick action",
      keywords: "decisions meetings log",
      run: () => runWhenWorkspaceReady(() => gotoPage("decisions")),
    },
    {
      id: "focus_client_search",
      label: "Focus Client Search",
      hint: "/",
      keywords: "search client filter",
      run: () => runWhenWorkspaceReady(() => focusClientsSearch()),
    },
    {
      id: "open_add_modal",
      label: "Add New Item (Context Aware)",
      hint: "Shift+N",
      keywords: "add create new modal",
      run: () => runWhenWorkspaceReady(() => contextAwareAdd()),
    },
    {
      id: "refresh_all",
      label: "Refresh Workspace Data",
      hint: "Quick action",
      keywords: "refresh reload listeners",
      run: () => {
        runWhenWorkspaceReady(() => {
          if (typeof window.refreshAll === "function") window.refreshAll();
        });
      },
    },
    {
      id: "toggle_archived",
      label: "Toggle Archived Client Requests",
      hint: "Clients page",
      keywords: "archived requests toggle",
      run: () => {
        runWhenWorkspaceReady(() => {
          gotoPage("clients");
          if (typeof window.toggleArchivedRequests === "function") window.toggleArchivedRequests();
        });
      },
    },
    {
      id: "open_playbook",
      label: "Open Sales Playbook",
      hint: "Reference",
      keywords: "playbook sales script",
      run: () => {
        if (typeof window.openSalesPlaybook === "function") {
          window.openSalesPlaybook();
          return;
        }
        window.location.href = "sales_playbook.html";
      },
    },
  ];
}

function ensureQkUi() {
  if (qkOverlay) return;

  qkOverlay = document.createElement("div");
  qkOverlay.className = "qk-overlay";
  qkOverlay.innerHTML = `
    <div class="qk-modal" role="dialog" aria-modal="true" aria-label="Quick Actions">
      <div class="qk-head">
        <span>Team Quick Actions</span>
        <span class="qk-shortcut-pill">Ctrl/Cmd + K</span>
      </div>
      <div class="qk-input-wrap">
        <input class="qk-input" id="qkInput" type="text" placeholder="Search actions, pages, or tools">
      </div>
      <ul class="qk-list" id="qkList"></ul>
      <div class="qk-foot">Enter run | Arrow keys navigate | Esc close</div>
    </div>
  `;
  document.body.appendChild(qkOverlay);

  qkInput = document.getElementById("qkInput");
  qkList = document.getElementById("qkList");

  const launcher = document.createElement("button");
  launcher.type = "button";
  launcher.className = "qk-launcher";
  launcher.textContent = "Quick Actions";
  launcher.addEventListener("click", () => togglePalette(true));
  document.body.appendChild(launcher);

  qkHelpButton = document.createElement("button");
  qkHelpButton.type = "button";
  qkHelpButton.className = "qk-help-btn";
  qkHelpButton.textContent = "?";
  qkHelpButton.setAttribute("aria-expanded", "false");
  qkHelpButton.setAttribute("aria-label", "Open keyboard shortcuts help");
  qkHelpButton.addEventListener("click", () => toggleHelp(!qkHelpOpen));
  document.body.appendChild(qkHelpButton);

  qkHelpPanel = document.createElement("section");
  qkHelpPanel.className = "qk-help-panel";
  qkHelpPanel.setAttribute("role", "dialog");
  qkHelpPanel.setAttribute("aria-modal", "false");
  qkHelpPanel.setAttribute("aria-label", "Keyboard shortcuts and quick tools help");
  qkHelpPanel.innerHTML = `
    <div class="qk-help-head">
      <div class="qk-help-title">Keyboard + Quick Tools</div>
      <button type="button" class="qk-help-close" id="qkHelpClose" aria-label="Close help">x</button>
    </div>
    <div class="qk-help-body">
      <div class="qk-help-intro">
        Quick Actions Launcher and keyboard shortcuts help you move through the workspace faster.
      </div>
      <div class="qk-help-section-title">Quick Actions Launcher</div>
      <ul class="qk-help-list">
        <li class="qk-help-row">
          <span class="qk-help-key">Ctrl/Cmd + K</span>
          <span class="qk-help-desc">Opens the Quick Actions command palette.</span>
        </li>
      </ul>
      <div class="qk-help-section-title">Keyboard Navigation</div>
      <ul class="qk-help-list">
        <li class="qk-help-row"><span class="qk-help-key">G then P</span><span class="qk-help-desc">Jump to Pipeline page.</span></li>
        <li class="qk-help-row"><span class="qk-help-key">G then M</span><span class="qk-help-desc">Jump to Messages / requests.</span></li>
        <li class="qk-help-row"><span class="qk-help-key">G then T</span><span class="qk-help-desc">Jump to Tasks.</span></li>
        <li class="qk-help-row"><span class="qk-help-key">G then C</span><span class="qk-help-desc">Jump to Clients.</span></li>
        <li class="qk-help-row"><span class="qk-help-key">G then D</span><span class="qk-help-desc">Jump to Dashboard.</span></li>
        <li class="qk-help-row"><span class="qk-help-key">Shift + N</span><span class="qk-help-desc">Opens a context-aware add modal.</span></li>
        <li class="qk-help-row"><span class="qk-help-key">"/" key</span><span class="qk-help-desc">Focuses the client search input.</span></li>
      </ul>
      <div class="qk-help-tip">Tip: Press ? anytime to reopen this guide.</div>
    </div>
  `;
  document.body.appendChild(qkHelpPanel);

  qkToast = document.createElement("div");
  qkToast.className = "qk-toast";
  document.body.appendChild(qkToast);

  qkOverlay.addEventListener("click", (event) => {
    if (event.target === qkOverlay) togglePalette(false);
  });

  qkInput.addEventListener("input", () => {
    qkSelected = 0;
    renderActions();
  });

  document.getElementById("qkHelpClose")?.addEventListener("click", () => toggleHelp(false));
  document.addEventListener("click", (event) => {
    if (!qkHelpOpen || !qkHelpPanel || !qkHelpButton) return;
    const target = event.target;
    if (qkHelpPanel.contains(target) || qkHelpButton.contains(target)) return;
    toggleHelp(false);
  });

  maybeShowHelpNudge();
}

function filteredActions() {
  const query = String(qkInput?.value || "").trim().toLowerCase();
  if (!query) return qkActions;
  return qkActions.filter((action) => {
    const haystack = `${action.label} ${action.keywords || ""}`.toLowerCase();
    return haystack.includes(query);
  });
}

function renderActions() {
  if (!qkList) return;
  const actions = filteredActions();
  if (!actions.length) {
    qkList.innerHTML = `<li class="qk-empty">No actions match this search.</li>`;
    return;
  }
  if (qkSelected >= actions.length) qkSelected = actions.length - 1;
  qkList.innerHTML = actions
    .map((action, index) => `
      <li class="qk-item">
        <button class="qk-item-btn${index === qkSelected ? " active" : ""}" data-action-id="${escapeHtmlAttr(action.id)}">
          <span class="qk-item-label">${escapeHtml(action.label)}</span>
          <span class="qk-item-hint">${escapeHtml(action.hint || "")}</span>
        </button>
      </li>
    `)
    .join("");

  qkList.querySelectorAll(".qk-item-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = String(btn.getAttribute("data-action-id") || "");
      runActionById(id);
    });
  });
}

function runActionById(actionId) {
  const action = qkActions.find((candidate) => candidate.id === actionId);
  if (!action) return;
  togglePalette(false);
  action.run();
}

function runSelectedAction() {
  const actions = filteredActions();
  const selected = actions[qkSelected];
  if (!selected) return;
  togglePalette(false);
  selected.run();
}

function togglePalette(nextOpen) {
  ensureQkUi();
  qkOpen = nextOpen;
  if (qkOpen) {
    toggleHelp(false);
  }
  qkOverlay.classList.toggle("open", qkOpen);
  if (qkOpen) {
    qkSelected = 0;
    qkInput.value = "";
    renderActions();
    qkInput.focus();
  }
}

function showToast(message) {
  if (!qkToast) return;
  qkToast.textContent = message;
  qkToast.classList.add("visible");
  window.clearTimeout(showToast._timer);
  showToast._timer = window.setTimeout(() => {
    qkToast.classList.remove("visible");
  }, 1600);
}

function markHelpSeen() {
  try {
    localStorage.setItem(HELP_SEEN_STORAGE_KEY, "1");
  } catch {
    // Ignore localStorage errors.
  }
  qkHelpButton?.classList.remove("pulse");
  if (qkHelpHint) {
    qkHelpHint.remove();
    qkHelpHint = null;
  }
}

function toggleHelp(nextOpen) {
  if (!qkHelpPanel || !qkHelpButton) return;
  qkHelpOpen = Boolean(nextOpen);
  qkHelpPanel.classList.toggle("open", qkHelpOpen);
  qkHelpButton.classList.toggle("active", qkHelpOpen);
  qkHelpButton.setAttribute("aria-expanded", qkHelpOpen ? "true" : "false");
  if (qkHelpOpen) {
    markHelpSeen();
  }
}

function maybeShowHelpNudge() {
  let seen = false;
  try {
    seen = localStorage.getItem(HELP_SEEN_STORAGE_KEY) === "1";
  } catch {
    seen = false;
  }
  if (seen || !qkHelpButton) return;

  qkHelpButton.classList.add("pulse");
  qkHelpHint = document.createElement("button");
  qkHelpHint.type = "button";
  qkHelpHint.className = "qk-help-hint";
  qkHelpHint.textContent = "Shortcut Guide";
  qkHelpHint.addEventListener("click", () => toggleHelp(true));
  document.body.appendChild(qkHelpHint);

  window.setTimeout(() => {
    if (qkHelpHint) {
      qkHelpHint.remove();
      qkHelpHint = null;
    }
  }, 9000);
}

function handleGoSequence(event) {
  const now = Date.now();
  const key = String(event.key || "").toLowerCase();

  if (key === "g") {
    goModeUntil = now + GO_SEQUENCE_MS;
    showToast("Go to: P pipeline | M requests | T tasks | C clients | D dashboard");
    event.preventDefault();
    return true;
  }

  if (now > goModeUntil) return false;
  goModeUntil = 0;

  if (key === "p") gotoPage("pipeline");
  else if (key === "m") gotoMessagesRequests();
  else if (key === "t") gotoPage("tasks");
  else if (key === "c") gotoPage("clients");
  else if (key === "d") gotoPage("mywork");
  else if (key === "j") gotoPage("decisions");
  else return false;

  event.preventDefault();
  return true;
}

function setupKeyboardShortcuts() {
  document.addEventListener("keydown", (event) => {
    const key = String(event.key || "");
    const isMeta = event.ctrlKey || event.metaKey;

    if (isMeta && key.toLowerCase() === "k") {
      event.preventDefault();
      togglePalette(!qkOpen);
      return;
    }

    if (qkOpen) {
      if (key === "Escape") {
        event.preventDefault();
        togglePalette(false);
        return;
      }
      if (key === "ArrowDown") {
        event.preventDefault();
        qkSelected += 1;
        renderActions();
        return;
      }
      if (key === "ArrowUp") {
        event.preventDefault();
        qkSelected = Math.max(0, qkSelected - 1);
        renderActions();
        return;
      }
      if (key === "Enter") {
        event.preventDefault();
        runSelectedAction();
      }
      return;
    }

    if (qkHelpOpen && key === "Escape") {
      event.preventDefault();
      toggleHelp(false);
      return;
    }

    if (!workspaceVisible()) return;
    if (isTypingTarget(event.target)) return;

    if (key === "?") {
      event.preventDefault();
      toggleHelp(!qkHelpOpen);
      return;
    }

    if (event.shiftKey && key.toLowerCase() === "n") {
      event.preventDefault();
      contextAwareAdd();
      return;
    }

    if (key === "/") {
      event.preventDefault();
      focusClientsSearch();
      return;
    }

    handleGoSequence(event);
  });
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function escapeHtmlAttr(value) {
  return escapeHtml(value);
}

function initPortalQol() {
  qkActions = makeActions();
  ensureQkUi();
  renderActions();
  setupKeyboardShortcuts();
  showToast("Tip: Ctrl/Cmd + K opens Quick Actions");
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initPortalQol);
} else {
  initPortalQol();
}
