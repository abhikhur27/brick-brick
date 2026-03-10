// ─── FIREBASE SETUP ──────────────────────────────────────────────────────────
import { initializeApp }          from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getAuth, signInWithEmailAndPassword, signOut, onAuthStateChanged }
                                   from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { getFirestore, collection, addDoc, updateDoc, deleteDoc, doc, setDoc, getDoc,
         onSnapshot, serverTimestamp, query, orderBy }
                                   from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import { firebaseConfig }          from "./firebase-config.js";

const app  = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db   = getFirestore(app);

// ─── ANTI-FLASH GUARD ────────────────────────────────────────────────────────
// Body starts invisible; revealed after auth state resolves so there's never
// a flash of the workspace before Firebase confirms the user is logged in.
document.body.style.visibility = "hidden";

// ─── COLUMN CONFIG ───────────────────────────────────────────────────────────
const COLS = [
  { key: "leads",     label: "Leads"          },
  { key: "contacted", label: "In Conversation" },
  { key: "proposal",  label: "Proposal Sent"  },
  { key: "closed",    label: "Closed"         },
];

// ─── STATE CACHES (updated by real-time listeners) ───────────────────────────
let currentPipeline  = [];
let currentTasks     = [];
let currentDecisions = [];
let currentClients   = [];
let currentClientRequests = [];

// ─── TASK SORT STATE ─────────────────────────────────────────────────────────
let taskSortField = "due";     // "due" | "createdAt" | "priority" | "owner" | "done"
let taskSortAsc   = true;

// ─── LISTENER HANDLES ────────────────────────────────────────────────────────
let unsubPipeline   = null;
let unsubTasks      = null;
let unsubDecisions  = null;
let unsubClients    = null;
let unsubClientRequests = null;

// ─── MODAL STATE ─────────────────────────────────────────────────────────────
let modalMode    = "pipeline";
let modalContext = "";
let editingLeadId = null;   // ID of the lead currently open in detail modal
let editingTaskId = null;   // ID of the task currently open in detail modal
let editingRequestId = null;
let selectedClientId = null;
let currentUserRole = null;
let draggingPipelineCardId = null;
let suppressCardClickUntil = 0;

// ─── AUTH ─────────────────────────────────────────────────────────────────────
window.doLogin = async function () {
  const email    = document.getElementById("loginEmail").value.trim();
  const password = document.getElementById("loginPassword").value;
  const errEl    = document.getElementById("loginError");
  const btn      = document.getElementById("loginBtn");

  errEl.textContent = "";
  btn.textContent   = "SIGNING IN…";
  btn.disabled      = true;

  try {
    await signInWithEmailAndPassword(auth, email, password);
  } catch (err) {
    errEl.textContent = friendlyAuthError(err.code);
    btn.textContent   = "ENTER WORKSPACE";
    btn.disabled      = false;
  }
};

window.doLogout = async function () {
  if (unsubPipeline)  unsubPipeline();
  if (unsubTasks)     unsubTasks();
  if (unsubDecisions) unsubDecisions();
  if (unsubClients) unsubClients();
  if (unsubClientRequests) unsubClientRequests();
  await signOut(auth);
};

document.getElementById("loginPassword")?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") window.doLogin();
});

// Central auth-state observer — the ONLY place that shows/hides login vs workspace.
async function getUserRole(uid) {
  try {
    const snap = await getDoc(doc(db, "users", uid));
    return snap.exists() ? String(snap.data().role || "") : "";
  } catch (err) {
    console.error("getUserRole:", err);
    return "";
  }
}

// Central auth-state observer - the ONLY place that shows/hides login vs workspace.
onAuthStateChanged(auth, async (user) => {
  // Reveal the page now that auth state is known (prevents flash)
  document.body.style.visibility = "visible";

  if (user) {
    currentUserRole = await getUserRole(user.uid);
    if (currentUserRole !== "admin") {
      await signOut(auth);
      const errEl = document.getElementById("loginError");
      if (errEl) errEl.textContent = "Internal workspace access is for admin/team accounts only.";
      return;
    }

    document.getElementById("loginScreen").style.display = "none";
    document.getElementById("app").classList.add("visible");

    const el = document.getElementById("signedInAs");
    if (el) el.textContent = user.email;

    startListeners();
  } else {
    currentUserRole = null;
    document.getElementById("loginScreen").style.display = "flex";
    document.getElementById("app").classList.remove("visible");

    const btn = document.getElementById("loginBtn");
    if (btn) { btn.textContent = "ENTER WORKSPACE"; btn.disabled = false; }
  }
});

function friendlyAuthError(code) {
  const map = {
    "auth/invalid-email":      "Invalid email address.",
    "auth/user-not-found":     "No account found with that email.",
    "auth/wrong-password":     "Incorrect password.",
    "auth/invalid-credential": "Incorrect email or password.",
    "auth/too-many-requests":  "Too many attempts — try again later.",
    "auth/user-disabled":      "This account has been disabled.",
  };
  return map[code] || "Login failed. Check your credentials.";
}

// ─── REAL-TIME LISTENERS ──────────────────────────────────────────────────────
function startListeners() {
  unsubPipeline = onSnapshot(
    query(collection(db, "pipeline"), orderBy("createdAt", "desc")),
    (snap) => {
      currentPipeline = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      renderPipeline(currentPipeline);
    },
    (err) => console.error("Pipeline error:", err)
  );

  // Fetch all tasks; sort client-side so we can switch sort fields without re-querying
  unsubTasks = onSnapshot(
    query(collection(db, "tasks"), orderBy("createdAt", "asc")),
    (snap) => {
      currentTasks = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      renderTasks(currentTasks);
    },
    (err) => console.error("Tasks error:", err)
  );

  // Decisions ordered by real meetingDate (ISO string sorts lexicographically = chronologically)
  unsubDecisions = onSnapshot(
    query(collection(db, "decisions"), orderBy("meetingDate", "desc")),
    (snap) => {
      currentDecisions = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      renderDecisions(currentDecisions);
    },
    (err) => {
      // Fallback if old documents lack meetingDate — order by createdAt
      unsubDecisions = onSnapshot(
        query(collection(db, "decisions"), orderBy("createdAt", "desc")),
        (snap) => {
          currentDecisions = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
          renderDecisions(currentDecisions);
        }
      );
    }
  );

  unsubClients = onSnapshot(
    query(collection(db, "clients"), orderBy("companyName", "asc")),
    (snap) => {
      currentClients = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      if (selectedClientId && !currentClients.some((c) => c.id === selectedClientId)) {
        selectedClientId = null;
      }
      renderClientsWorkspace(currentClients);
    },
    (err) => console.error("Clients error:", err)
  );

  unsubClientRequests = onSnapshot(
    query(collection(db, "client_requests"), orderBy("createdAt", "desc")),
    (snap) => {
      currentClientRequests = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      renderClientRequestsAdmin(currentClientRequests);
    },
    (err) => console.error("Client requests error:", err)
  );

}

window.refreshAll = function () {
  if (unsubPipeline)  unsubPipeline();
  if (unsubTasks)     unsubTasks();
  if (unsubDecisions) unsubDecisions();
  if (unsubClients) unsubClients();
  if (unsubClientRequests) unsubClientRequests();
  startListeners();
};

window.openSalesPlaybook = function () {
  window.location.href = "sales_playbook.html";
};

// ─── PIPELINE RENDER ──────────────────────────────────────────────────────────
function renderPipeline(cards) {
  const board = document.getElementById("kanbanBoard");
  if (!board) return;
  board.innerHTML = "";

  COLS.forEach(({ key, label }) => {
    const colCards = cards.filter((c) => c.status === key);
    const colEl    = document.createElement("div");
    colEl.className = "kanban-col";
    colEl.innerHTML = `
      <div class="col-header">
        <span class="col-title">${label}</span>
        <span class="col-count">${colCards.length}</span>
      </div>
      <div id="col-${key}" class="kanban-dropzone"></div>
      <button class="add-card" onclick="openAddModal('pipeline','${key}')">+ Add</button>
    `;
    board.appendChild(colEl);

    const container = colEl.querySelector(`#col-${key}`);
    container.addEventListener("dragover", (e) => {
      e.preventDefault();
      container.classList.add("is-drop-target");
    });
    container.addEventListener("dragleave", () => {
      container.classList.remove("is-drop-target");
    });
    container.addEventListener("drop", async (e) => {
      e.preventDefault();
      clearPipelineDropTargets();
      const cardId =
        e.dataTransfer?.getData("text/plain")
        || e.dataTransfer?.getData("application/x-brick-card")
        || draggingPipelineCardId;
      if (!cardId) return;
      const card = currentPipeline.find((c) => c.id === cardId);
      if (!card || card.status === key) return;
      await moveCard(cardId, key);
    });

    colCards.forEach((card) => {
      const tagClass =
        card.service === "AI Workflow"   ? "tag-ai"  :
        card.service === "Website Build" ? "tag-web" : "tag-other";

      const moveButtons = COLS
        .filter((c) => c.key !== key)
        .map((c) => `<button class="card-btn" onclick="event.stopPropagation();moveCard('${card.id}','${c.key}')">${c.label}</button>`)
        .join("");

      const cardEl = document.createElement("div");
      cardEl.className = "card";
      cardEl.draggable = true;
      cardEl.addEventListener("dragstart", (e) => {
        draggingPipelineCardId = card.id;
        cardEl.classList.add("dragging");
        if (e.dataTransfer) {
          e.dataTransfer.effectAllowed = "move";
          e.dataTransfer.setData("text/plain", card.id);
          e.dataTransfer.setData("application/x-brick-card", card.id);
        }
      });
      cardEl.addEventListener("dragend", () => {
        draggingPipelineCardId = null;
        cardEl.classList.remove("dragging");
        clearPipelineDropTargets();
        // Prevent accidental modal open if mouseup after drag triggers click.
        suppressCardClickUntil = Date.now() + 120;
      });
      // Clicking the card body (not action buttons) opens detail modal
      cardEl.addEventListener("click", () => {
        if (Date.now() < suppressCardClickUntil) return;
        openLeadDetail(card.id);
      });
      cardEl.innerHTML = `
        <div class="card-title">${escHtml(card.title)}</div>
        ${card.company ? `<div class="card-company">${escHtml(card.company)}</div>` : ""}
        <div class="card-meta">${escHtml(card.note || "")}</div>
        <span class="card-tag ${tagClass}">${escHtml(card.service || "")}</span>
        <div class="card-actions">
          ${moveButtons}
          <button class="card-btn delete" onclick="event.stopPropagation();deleteCard('${card.id}')">✕</button>
        </div>
      `;
      container.appendChild(cardEl);
    });
  });
}

function clearPipelineDropTargets() {
  document.querySelectorAll(".kanban-dropzone.is-drop-target").forEach((el) => {
    el.classList.remove("is-drop-target");
  });
}

window.moveCard = async function (id, newStatus) {
  try {
    await updateDoc(doc(db, "pipeline", id), { status: newStatus, updatedAt: serverTimestamp() });
  } catch (err) { console.error("moveCard:", err); }
};

window.deleteCard = async function (id) {
  if (!confirm("Delete this lead?")) return;
  try { await deleteDoc(doc(db, "pipeline", id)); }
  catch (err) { console.error("deleteCard:", err); }
};

// ─── LEAD DETAIL MODAL ────────────────────────────────────────────────────────
window.openLeadDetail = function (id) {
  const card = currentPipeline.find((c) => c.id === id);
  if (!card) return;
  editingLeadId = id;

  const colOpts = COLS.map((c) =>
    `<option value="${c.key}" ${c.key === card.status ? "selected" : ""}>${c.label}</option>`
  ).join("");

  const addedStr = card.createdAt?.toDate
    ? card.createdAt.toDate().toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" })
    : "—";

  document.getElementById("leadDetailBody").innerHTML = `
    <div class="detail-grid">
      <div class="form-group">
        <label class="form-label">Lead / Client Name</label>
        <input class="form-input" id="ld_title" value="${escHtmlAttr(card.title || "")}">
      </div>
      <div class="form-group">
        <label class="form-label">Company</label>
        <input class="form-input" id="ld_company" value="${escHtmlAttr(card.company || "")}" placeholder="Company name">
      </div>
      <div class="form-group">
        <label class="form-label">Contact Person</label>
        <input class="form-input" id="ld_contact" value="${escHtmlAttr(card.contact || "")}" placeholder="Contact name / email">
      </div>
      <div class="form-group">
        <label class="form-label">Service Type</label>
        <select class="form-select" id="ld_service">
          <option value="AI Workflow"   ${card.service === "AI Workflow"   ? "selected" : ""}>AI Workflow</option>
          <option value="Website Build" ${card.service === "Website Build" ? "selected" : ""}>Website Build</option>
          <option value="Other"         ${card.service === "Other"         ? "selected" : ""}>Other</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Status</label>
        <select class="form-select" id="ld_status">${colOpts}</select>
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Notes</label>
        <textarea class="form-input form-textarea" id="ld_note" rows="4" placeholder="Context, requirements, next steps…">${escHtml(card.note || "")}</textarea>
      </div>
      <div class="form-group">
        <label class="form-label">Date Added</label>
        <div class="detail-meta-value">${addedStr}</div>
      </div>
    </div>
  `;

  // Wire up delete button inside the detail modal
  const delBtn = document.getElementById("leadDetailDeleteBtn");
  delBtn.onclick = async () => {
    if (!confirm("Permanently delete this lead?")) return;
    await deleteCard(editingLeadId);
    closeLeadDetail();
  };

  document.getElementById("leadDetailOverlay").classList.add("open");
};

window.saveLeadDetail = async function () {
  if (!editingLeadId) return;
  try {
    await updateDoc(doc(db, "pipeline", editingLeadId), {
      title:     document.getElementById("ld_title").value.trim(),
      company:   document.getElementById("ld_company").value.trim(),
      contact:   document.getElementById("ld_contact").value.trim(),
      service:   document.getElementById("ld_service").value,
      status:    document.getElementById("ld_status").value,
      note:      document.getElementById("ld_note").value.trim(),
      updatedAt: serverTimestamp(),
    });
    closeLeadDetail();
  } catch (err) {
    console.error("saveLeadDetail:", err);
    alert("Save failed: " + err.message);
  }
};

window.closeLeadDetail = function () {
  document.getElementById("leadDetailOverlay").classList.remove("open");
  editingLeadId = null;
};

document.getElementById("leadDetailOverlay")?.addEventListener("click", function (e) {
  if (e.target === this) window.closeLeadDetail();
});

// ─── TASKS ────────────────────────────────────────────────────────────────────
// Priority sort order helper
const PRIORITY_ORDER = { High: 0, Mid: 1, Low: 2 };

function sortedTasks(tasks) {
  const arr = [...tasks];
  arr.sort((a, b) => {
    let va, vb;
    switch (taskSortField) {
      case "due":
        va = a.due || "9999-99-99";
        vb = b.due || "9999-99-99";
        break;
      case "createdAt":
        va = a.createdAt?.seconds ?? 0;
        vb = b.createdAt?.seconds ?? 0;
        break;
      case "priority":
        va = PRIORITY_ORDER[a.priority] ?? 9;
        vb = PRIORITY_ORDER[b.priority] ?? 9;
        break;
      case "owner":
        va = (a.owner || "").toLowerCase();
        vb = (b.owner || "").toLowerCase();
        break;
      case "done":
        va = a.done ? 1 : 0;
        vb = b.done ? 1 : 0;
        break;
      default:
        va = vb = 0;
    }
    if (va < vb) return taskSortAsc ? -1 : 1;
    if (va > vb) return taskSortAsc ?  1 : -1;
    return 0;
  });
  return arr;
}

function renderTasks(tasks) {
  const tbody = document.getElementById("tasksBody");
  const completedBody = document.getElementById("completedTasksBody");
  const completedSection = document.getElementById("completedTasksSection");
  if (!tbody || !completedBody || !completedSection) return;
  tbody.innerHTML = "";
  completedBody.innerHTML = "";

  const sorted = sortedTasks(tasks);

  if (!sorted.length) {
    tbody.innerHTML = `<tr><td colspan="7"><div class="empty-state" style="margin:24px 0">No open tasks.</div></td></tr>`;
    completedSection.style.display = "none";
    return;
  }

  let openCount = 0;
  let completedCount = 0;

  sorted.forEach((task) => {
    const tr = document.createElement("tr");
    if (task.done) tr.classList.add("task-done-row");

    const isOverdue = !task.done && task.due && task.due < todayStr();
    if (isOverdue) tr.classList.add("task-overdue-row");
    const pClass    = task.priority === "High" ? "p-high" : task.priority === "Mid" ? "p-mid" : "p-low";
    const initials  = (task.owner || "?").split(" ").map((w) => w[0]).join("").toUpperCase().slice(0, 2);

    const addedStr = task.createdAt?.toDate
      ? task.createdAt.toDate().toLocaleDateString("en-US", { month: "short", day: "numeric" })
      : "—";

    tr.innerHTML = `
      <td><div class="checkbox ${task.done ? "checked" : ""}" onclick="toggleTask('${task.id}', ${!task.done})"></div></td>
      <td class="task-name">${escHtml(task.name)}</td>
      <td>
        <span class="owner-chip">
          <span class="avatar">${initials}</span>
          ${escHtml(task.owner || "Team")}
        </span>
      </td>
      <td><span class="date-text ${isOverdue ? "date-overdue" : ""}">${formatDate(task.due)}</span></td>
      <td><span class="date-text">${addedStr}</span></td>
      <td><span class="priority ${pClass}">${task.priority || "—"}</span></td>
      <td>
        <div class="task-actions">
          <button class="card-btn" onclick="openTaskDetail('${task.id}')">Edit</button>
          <button class="card-btn delete" onclick="deleteTask('${task.id}')">✕</button>
        </div>
      </td>
    `;
    if (task.done) {
      completedCount++;
      completedBody.appendChild(tr);
    } else {
      openCount++;
      tbody.appendChild(tr);
    }
  });

  if (!openCount) {
    tbody.innerHTML = `<tr><td colspan="7"><div class="empty-state" style="margin:24px 0">No open tasks.</div></td></tr>`;
  }
  completedSection.style.display = completedCount ? "block" : "none";
}

window.setSort = function (field, btnEl) {
  if (taskSortField === field) {
    taskSortAsc = !taskSortAsc;
  } else {
    taskSortField = field;
    taskSortAsc   = true;
    document.querySelectorAll(".sort-pill").forEach((p) => p.classList.remove("active"));
    btnEl.classList.add("active");
  }
  const dirBtn = document.getElementById("sortDirBtn");
  if (dirBtn) dirBtn.textContent = taskSortAsc ? "↑" : "↓";
  renderTasks(currentTasks);
};

window.toggleSortDir = function () {
  taskSortAsc = !taskSortAsc;
  const dirBtn = document.getElementById("sortDirBtn");
  if (dirBtn) dirBtn.textContent = taskSortAsc ? "↑" : "↓";
  renderTasks(currentTasks);
};

window.toggleTask = async function (id, newDone) {
  try { await updateDoc(doc(db, "tasks", id), { done: newDone }); }
  catch (err) { console.error("toggleTask:", err); }
};

window.deleteTask = async function (id) {
  if (!confirm("Delete this task?")) return;
  try { await deleteDoc(doc(db, "tasks", id)); }
  catch (err) { console.error("deleteTask:", err); }
};

// ─── DECISIONS ────────────────────────────────────────────────────────────────
window.openTaskDetail = function (id) {
  const task = currentTasks.find((t) => t.id === id);
  if (!task) return;
  editingTaskId = id;

  const addedStr = task.createdAt?.toDate
    ? task.createdAt.toDate().toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" })
    : "—";

  document.getElementById("taskDetailBody").innerHTML = `
    <div class="detail-grid">
      <div class="form-group form-group-full">
        <label class="form-label">Task</label>
        <input class="form-input" id="td_name" value="${escHtmlAttr(task.name || "")}" placeholder="What needs to get done?">
      </div>
      <div class="form-group">
        <label class="form-label">Owner</label>
        <input class="form-input" id="td_owner" value="${escHtmlAttr(task.owner || "")}" placeholder="Athan / Team / etc.">
      </div>
      <div class="form-group">
        <label class="form-label">Due Date</label>
        <input class="form-input" type="date" id="td_due" value="${escHtmlAttr(task.due || "")}" style="color-scheme:dark">
      </div>
      <div class="form-group">
        <label class="form-label">Priority</label>
        <select class="form-select" id="td_priority">
          <option value="High" ${task.priority === "High" ? "selected" : ""}>High</option>
          <option value="Mid"  ${task.priority === "Mid"  ? "selected" : ""}>Mid</option>
          <option value="Low"  ${task.priority === "Low"  ? "selected" : ""}>Low</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Status</label>
        <select class="form-select" id="td_done">
          <option value="false" ${task.done ? "" : "selected"}>Open</option>
          <option value="true"  ${task.done ? "selected" : ""}>Completed</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Date Added</label>
        <div class="detail-meta-value">${addedStr}</div>
      </div>
    </div>
  `;

  const delBtn = document.getElementById("taskDetailDeleteBtn");
  delBtn.onclick = async () => {
    if (!confirm("Permanently delete this task?")) return;
    await deleteTask(editingTaskId);
    closeTaskDetail();
  };

  document.getElementById("taskDetailOverlay").classList.add("open");
};

window.saveTaskDetail = async function () {
  if (!editingTaskId) return;
  const name = document.getElementById("td_name").value.trim();
  if (!name) { alert("Task name is required."); return; }

  try {
    await updateDoc(doc(db, "tasks", editingTaskId), {
      name,
      owner:     document.getElementById("td_owner").value.trim() || "Team",
      due:       document.getElementById("td_due").value || "",
      priority:  document.getElementById("td_priority").value,
      done:      document.getElementById("td_done").value === "true",
      updatedAt: serverTimestamp(),
    });
    closeTaskDetail();
  } catch (err) {
    console.error("saveTaskDetail:", err);
    alert("Save failed: " + err.message);
  }
};

window.closeTaskDetail = function () {
  document.getElementById("taskDetailOverlay").classList.remove("open");
  editingTaskId = null;
};

document.getElementById("taskDetailOverlay")?.addEventListener("click", function (e) {
  if (e.target === this) window.closeTaskDetail();
});

function clientStatusLabel(status) {
  if (status === "active") return "Active";
  if (status === "inactive") return "Inactive";
  if (status === "completed") return "Completed";
  if (status === "one-time") return "One-Time";
  return "Active";
}

function clientStatusClass(status) {
  if (status === "active") return "sp-active";
  if (status === "inactive") return "sp-inactive";
  if (status === "completed") return "sp-completed";
  if (status === "one-time") return "sp-one-time";
  return "sp-active";
}

function isMutedClient(client) {
  return client.status === "inactive" || client.status === "completed" || client.status === "one-time" || !client.recurring;
}

function renderClientsWorkspace(clients) {
  const activeList = document.getElementById("clientsActiveList");
  const oneTimeList = document.getElementById("clientsOneTimeList");
  const inactiveList = document.getElementById("clientsInactiveList");
  if (!activeList || !oneTimeList || !inactiveList) return;

  const inactive = clients.filter((c) => c.status === "inactive" || c.status === "completed");
  const oneTime = clients.filter((c) =>
    !(c.status === "inactive" || c.status === "completed")
    && (c.status === "one-time" || !c.recurring)
  );
  const activeRecurring = clients.filter((c) =>
    !(c.status === "inactive" || c.status === "completed")
    && !(c.status === "one-time" || !c.recurring)
  );

  renderClientListGroup(activeList, activeRecurring, "No active recurring clients.");
  renderClientListGroup(oneTimeList, oneTime, "No one-time clients.");
  renderClientListGroup(inactiveList, inactive, "No inactive clients.");
  renderClientDetailPane();
}

function renderClientListGroup(container, list, emptyText) {
  container.innerHTML = "";
  if (!list.length) {
    container.innerHTML = `<div class="empty-state" style="padding:14px;font-size:10px">${emptyText}</div>`;
    return;
  }

  list.forEach((client) => {
    const row = document.createElement("div");
    row.className = `client-row ${selectedClientId === client.id ? "selected" : ""} ${isMutedClient(client) ? "muted" : ""}`;
    row.innerHTML = `
      <div class="client-row-head">
        <div class="client-row-title">${escHtml(client.companyName || "Unnamed Client")}</div>
        <span class="status-pill ${clientStatusClass(client.status || "active")}">${clientStatusLabel(client.status || "active")}</span>
      </div>
      <div class="client-row-sub">
        ${(client.planName || "No plan set")} · ${client.paid ? "Paid" : "Unpaid"}<br>
        Updates left: ${Number.isFinite(client.updatesRemaining) ? client.updatesRemaining : "—"}
      </div>
    `;
    row.addEventListener("click", () => {
      selectedClientId = client.id;
      renderClientsWorkspace(currentClients);
    });
    container.appendChild(row);
  });
}

function renderClientDetailPane() {
  const pane = document.getElementById("clientDetailPane");
  if (!pane) return;
  if (!selectedClientId) {
    pane.classList.add("empty-state");
    pane.textContent = "Select a client record to edit plan, billing, and notes.";
    return;
  }

  const client = currentClients.find((c) => c.id === selectedClientId);
  if (!client) {
    pane.classList.add("empty-state");
    pane.textContent = "Client not found.";
    return;
  }

  pane.classList.remove("empty-state");
  const history = Array.isArray(client.billingHistory) ? client.billingHistory : [];
  const historyHtml = history.length
    ? history.map((entry) => `
        <div class="billing-entry">
          ${escHtml(entry.date || "—")} · ${escHtml(entry.note || "")}
        </div>
      `).join("")
    : `<div class="billing-entry">No billing history entries yet.</div>`;

  pane.innerHTML = `
    <div class="client-detail-grid">
      <div class="form-group">
        <label class="form-label">Company</label>
        <input class="form-input" id="cd_companyName" value="${escHtmlAttr(client.companyName || "")}">
      </div>
      <div class="form-group">
        <label class="form-label">Contact Name</label>
        <input class="form-input" id="cd_contactName" value="${escHtmlAttr(client.contactName || "")}">
      </div>
      <div class="form-group">
        <label class="form-label">Contact Email</label>
        <input class="form-input" id="cd_email" value="${escHtmlAttr(client.email || "")}">
      </div>
      <div class="form-group">
        <label class="form-label">Client Auth UID</label>
        <input class="form-input" id="cd_authUid" value="${escHtmlAttr(client.authUid || "")}" placeholder="Firebase auth UID">
      </div>
      <div class="form-group">
        <label class="form-label">Plan Name</label>
        <input class="form-input" id="cd_planName" value="${escHtmlAttr(client.planName || "")}">
      </div>
      <div class="form-group">
        <label class="form-label">Status</label>
        <select class="form-select" id="cd_status">
          <option value="active" ${client.status === "active" ? "selected" : ""}>Active</option>
          <option value="inactive" ${client.status === "inactive" ? "selected" : ""}>Inactive</option>
          <option value="one-time" ${client.status === "one-time" ? "selected" : ""}>One-Time</option>
          <option value="completed" ${client.status === "completed" ? "selected" : ""}>Completed</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Recurring</label>
        <select class="form-select" id="cd_recurring">
          <option value="true" ${client.recurring ? "selected" : ""}>Recurring</option>
          <option value="false" ${client.recurring ? "" : "selected"}>One-Time</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Paid</label>
        <select class="form-select" id="cd_paid">
          <option value="true" ${client.paid ? "selected" : ""}>Paid</option>
          <option value="false" ${client.paid ? "" : "selected"}>Unpaid</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Updates Per Month</label>
        <input class="form-input" id="cd_updatesPerMonth" type="number" min="0" value="${Number.isFinite(client.updatesPerMonth) ? client.updatesPerMonth : 0}">
      </div>
      <div class="form-group">
        <label class="form-label">Updates Remaining</label>
        <input class="form-input" id="cd_updatesRemaining" type="number" min="0" value="${Number.isFinite(client.updatesRemaining) ? client.updatesRemaining : 0}">
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Last Payment Note</label>
        <input class="form-input" id="cd_lastPaymentNote" value="${escHtmlAttr(client.lastPaymentNote || "")}" placeholder="e.g. Paid Mar 2 via ACH">
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Billing Notes</label>
        <textarea class="form-input form-textarea" id="cd_billingNotes" rows="3">${escHtml(client.billingNotes || "")}</textarea>
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Internal Notes</label>
        <textarea class="form-input form-textarea" id="cd_notes" rows="4">${escHtml(client.notes || "")}</textarea>
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Billing History (Manual)</label>
        <div class="billing-list">${historyHtml}</div>
        <div class="inline-form-row">
          <input class="form-input" id="cd_historyDate" type="date" value="${todayStr()}">
          <input class="form-input" id="cd_historyNote" placeholder="Payment/event note">
          <button class="btn btn-ghost" type="button" onclick="addClientBillingEntry()">Add Entry</button>
        </div>
      </div>
    </div>
    <div class="client-detail-actions">
      <button class="btn btn-primary" type="button" onclick="saveClientDetail()">Save Client</button>
    </div>
  `;
}

window.saveClientDetail = async function () {
  if (!selectedClientId) return;
  try {
    const updates = {
      companyName: document.getElementById("cd_companyName").value.trim(),
      contactName: document.getElementById("cd_contactName").value.trim(),
      email: document.getElementById("cd_email").value.trim(),
      authUid: document.getElementById("cd_authUid").value.trim(),
      planName: document.getElementById("cd_planName").value.trim(),
      status: document.getElementById("cd_status").value,
      recurring: document.getElementById("cd_recurring").value === "true",
      paid: document.getElementById("cd_paid").value === "true",
      updatesPerMonth: Number(document.getElementById("cd_updatesPerMonth").value || 0),
      updatesRemaining: Number(document.getElementById("cd_updatesRemaining").value || 0),
      lastPaymentNote: document.getElementById("cd_lastPaymentNote").value.trim(),
      billingNotes: document.getElementById("cd_billingNotes").value.trim(),
      notes: document.getElementById("cd_notes").value.trim(),
      updatedAt: serverTimestamp(),
    };
    await updateDoc(doc(db, "clients", selectedClientId), updates);

    if (updates.authUid) {
      await setDoc(doc(db, "users", updates.authUid), {
        role: "client",
        clientId: selectedClientId,
        email: updates.email || "",
        updatedAt: serverTimestamp(),
      }, { merge: true });
    }
  } catch (err) {
    console.error("saveClientDetail:", err);
    alert("Save failed: " + err.message);
  }
};

window.addClientBillingEntry = async function () {
  if (!selectedClientId) return;
  const client = currentClients.find((c) => c.id === selectedClientId);
  if (!client) return;

  const date = document.getElementById("cd_historyDate")?.value || todayStr();
  const note = document.getElementById("cd_historyNote")?.value.trim() || "";
  if (!note) { alert("Enter a billing note first."); return; }

  const existing = Array.isArray(client.billingHistory) ? client.billingHistory : [];
  const next = [{ id: uid(), date, note }, ...existing].slice(0, 60);
  try {
    await updateDoc(doc(db, "clients", selectedClientId), {
      billingHistory: next,
      updatedAt: serverTimestamp(),
    });
    const noteEl = document.getElementById("cd_historyNote");
    if (noteEl) noteEl.value = "";
  } catch (err) {
    console.error("addClientBillingEntry:", err);
    alert("Could not add billing entry.");
  }
};

function requestStatusClass(status) {
  if (status === "in_review") return "rs-in_review";
  if (status === "scheduled") return "rs-scheduled";
  if (status === "done") return "rs-done";
  return "rs-submitted";
}

function requestStatusLabel(status) {
  if (status === "in_review") return "In Review";
  if (status === "scheduled") return "Scheduled";
  if (status === "done") return "Done";
  return "Submitted";
}

function renderClientRequestsAdmin(requests) {
  const body = document.getElementById("clientRequestsBody");
  if (!body) return;
  body.innerHTML = "";
  if (!requests.length) {
    body.innerHTML = `<tr><td colspan="7"><div class="empty-state" style="margin:24px 0">No client requests yet.</div></td></tr>`;
    return;
  }

  requests.forEach((req) => {
    const tr = document.createElement("tr");
    const updatedDate = req.updatedAt?.toDate
      ? req.updatedAt.toDate().toLocaleDateString("en-US", { month: "short", day: "numeric" })
      : (req.createdAt?.toDate
        ? req.createdAt.toDate().toLocaleDateString("en-US", { month: "short", day: "numeric" })
        : "—");
    tr.innerHTML = `
      <td>${escHtml(req.clientName || "Unknown")}</td>
      <td>${escHtml(req.title || "Untitled request")}</td>
      <td>${escHtml(req.category || "general")}</td>
      <td>${escHtml(req.priority || "normal")}</td>
      <td><span class="req-status ${requestStatusClass(req.status)}">${requestStatusLabel(req.status)}</span></td>
      <td><span class="date-text">${updatedDate}</span></td>
      <td><button class="card-btn" onclick="openRequestDetail('${req.id}')">Manage</button></td>
    `;
    body.appendChild(tr);
  });
}

window.openRequestDetail = function (id) {
  const req = currentClientRequests.find((r) => r.id === id);
  if (!req) return;
  editingRequestId = id;
  document.getElementById("requestDetailBody").innerHTML = `
    <div class="detail-grid">
      <div class="form-group">
        <label class="form-label">Client</label>
        <div class="detail-meta-value">${escHtml(req.clientName || "Unknown")}</div>
      </div>
      <div class="form-group">
        <label class="form-label">Category</label>
        <div class="detail-meta-value">${escHtml(req.category || "general")}</div>
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Title</label>
        <input class="form-input" id="rd_title" value="${escHtmlAttr(req.title || "")}" readonly>
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Description</label>
        <textarea class="form-input form-textarea" id="rd_description" rows="4" readonly>${escHtml(req.description || "")}</textarea>
      </div>
      <div class="form-group">
        <label class="form-label">Priority</label>
        <select class="form-select" id="rd_priority">
          <option value="low" ${req.priority === "low" ? "selected" : ""}>Low</option>
          <option value="normal" ${(!req.priority || req.priority === "normal") ? "selected" : ""}>Normal</option>
          <option value="high" ${req.priority === "high" ? "selected" : ""}>High</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Status</label>
        <select class="form-select" id="rd_status">
          <option value="submitted" ${req.status === "submitted" ? "selected" : ""}>Submitted</option>
          <option value="in_review" ${req.status === "in_review" ? "selected" : ""}>In Review</option>
          <option value="scheduled" ${req.status === "scheduled" ? "selected" : ""}>Scheduled</option>
          <option value="done" ${req.status === "done" ? "selected" : ""}>Done</option>
        </select>
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Internal Notes</label>
        <textarea class="form-input form-textarea" id="rd_internalNotes" rows="4">${escHtml(req.internalNotes || "")}</textarea>
      </div>
    </div>
  `;

  const pushBtn = document.getElementById("requestPushBtn");
  if (pushBtn) {
    pushBtn.textContent = req.pipelineId ? "Already in Pipeline" : "Push to Pipeline";
    pushBtn.disabled = Boolean(req.pipelineId);
  }

  document.getElementById("requestDetailOverlay").classList.add("open");
};

window.closeRequestDetail = function () {
  document.getElementById("requestDetailOverlay").classList.remove("open");
  editingRequestId = null;
};

window.saveRequestDetail = async function () {
  if (!editingRequestId) return;
  try {
    await updateDoc(doc(db, "client_requests", editingRequestId), {
      priority: document.getElementById("rd_priority").value,
      status: document.getElementById("rd_status").value,
      internalNotes: document.getElementById("rd_internalNotes").value.trim(),
      updatedAt: serverTimestamp(),
    });
    closeRequestDetail();
  } catch (err) {
    console.error("saveRequestDetail:", err);
    alert("Could not save request.");
  }
};

function requestCategoryToService(category) {
  const c = String(category || "").toLowerCase();
  if (c.includes("workflow") || c.includes("automation")) return "AI Workflow";
  if (c.includes("website") || c.includes("content")) return "Website Build";
  return "Other";
}

window.pushRequestToPipeline = async function () {
  if (!editingRequestId) return;
  const req = currentClientRequests.find((r) => r.id === editingRequestId);
  if (!req || req.pipelineId) return;
  try {
    const leadRef = await addDoc(collection(db, "pipeline"), {
      title: `${req.clientName || "Client"} — ${req.title || "Request"}`,
      company: req.clientName || "",
      contact: req.clientEmail || "",
      service: requestCategoryToService(req.category),
      status: "leads",
      note: [
        "Source: client request",
        `Category: ${req.category || "general"}`,
        `Priority: ${req.priority || "normal"}`,
        "",
        req.description || "",
      ].join("\n"),
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
    });

    await updateDoc(doc(db, "client_requests", editingRequestId), {
      pipelineId: leadRef.id,
      status: "in_review",
      updatedAt: serverTimestamp(),
    });

    const pushBtn = document.getElementById("requestPushBtn");
    if (pushBtn) {
      pushBtn.textContent = "Already in Pipeline";
      pushBtn.disabled = true;
    }
  } catch (err) {
    console.error("pushRequestToPipeline:", err);
    alert("Could not push request to pipeline.");
  }
};

document.getElementById("requestDetailOverlay")?.addEventListener("click", function (e) {
  if (e.target === this) window.closeRequestDetail();
});

// DECISIONS
// Schema (each document):
//   meetingTitle : string    — the meeting name / label
//   meetingDate  : string    — ISO date "YYYY-MM-DD" for chronological sorting
//   items        : Array<{ id: string, text: string, owner: string }>
//   createdAt    : timestamp
//
// Per-item delete logic:
//   • Remove the item from the items array.
//   • If remaining items.length > 0  → updateDoc with filtered array.
//   • If remaining items.length === 0 → deleteDoc (group becomes empty).

function renderDecisions(decisions) {
  const log = document.getElementById("decisionsLog");
  if (!log) return;
  log.innerHTML = "";

  if (!decisions.length) {
    log.innerHTML = `<div class="empty-state">No meetings logged yet.</div>`;
    return;
  }

  decisions.forEach((entry) => {
    // Support legacy documents that stored meetingTitle/meetingDate in a single "date" field
    const titleStr = entry.meetingTitle || entry.date || "Untitled Meeting";
    const dateStr  = entry.meetingDate
      ? formatDate(entry.meetingDate)
      : (entry.date || "");

    const el = document.createElement("div");
    el.className = "decision-entry";

    const itemsHtml = (entry.items || []).map((item) => `
      <div class="decision-item" data-item-id="${item.id}">
        <div class="decision-item-header">
          <div class="decision-item-text-wrap">
            <div class="decision-text">${escHtml(item.text)}</div>
            <div class="decision-owner">${escHtml(item.owner || "")}</div>
          </div>
          <button
            class="delete-entry-btn item-delete"
            onclick="deleteDecisionItem('${entry.id}', '${item.id}')"
            title="Delete this item"
          >✕</button>
        </div>
      </div>
    `).join("");

    el.innerHTML = `
      <div class="decision-date">
        <div class="decision-date-left">
          <span class="decision-title">${escHtml(titleStr)}</span>
          ${dateStr ? `<span class="decision-date-tag">${dateStr}</span>` : ""}
        </div>
      </div>
      ${itemsHtml}
    `;
    log.appendChild(el);
  });
}

/**
 * Delete a single decision item from a meeting document.
 * If no items remain, delete the whole document.
 */
window.deleteDecisionItem = async function (docId, itemId) {
  const entry = currentDecisions.find((d) => d.id === docId);
  if (!entry) return;

  const remaining = (entry.items || []).filter((i) => i.id !== itemId);

  if (remaining.length === 0) {
    // Last item removed — ask before deleting the whole meeting entry
    if (!confirm("This is the last item in this meeting entry. Delete the entire entry?")) return;
    try { await deleteDoc(doc(db, "decisions", docId)); }
    catch (err) { console.error("deleteDecisionItem (doc):", err); }
  } else {
    try { await updateDoc(doc(db, "decisions", docId), { items: remaining }); }
    catch (err) { console.error("deleteDecisionItem (update):", err); }
  }
};

// ─── ADD MODAL ────────────────────────────────────────────────────────────────
window.openAddModal = function (type, context) {
  modalMode    = type || currentPage;
  modalContext = context || "";

  const titles = {
    pipeline: "ADD LEAD",
    tasks: "ADD TASK",
    decisions: "LOG MEETING",
    clients: "ADD CLIENT",
  };
  document.getElementById("modalTitle").textContent = titles[modalMode] || "ADD";

  document.getElementById("modalBody").innerHTML = getModalForm(modalMode);
  document.getElementById("modalOverlay").classList.add("open");

  if (modalMode === "decisions") {
    resetDecisionRows();
  }
};

window.closeModal = function () {
  document.getElementById("modalOverlay").classList.remove("open");
};

document.getElementById("modalOverlay")?.addEventListener("click", function (e) {
  if (e.target === this) window.closeModal();
});

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    window.closeModal();
    window.closeLeadDetail();
    window.closeTaskDetail();
    window.closeRequestDetail();
  }
});

function getModalForm(mode) {
  if (mode === "pipeline") {
    const colOpts = COLS.map((c) =>
      `<option value="${c.key}" ${c.key === (modalContext || "leads") ? "selected" : ""}>${c.label}</option>`
    ).join("");
    return `
      <div class="form-group">
        <label class="form-label">Lead / Client Name</label>
        <input class="form-input" id="f_title" placeholder="e.g. Local coffee shop">
      </div>
      <div class="form-group">
        <label class="form-label">Company</label>
        <input class="form-input" id="f_company" placeholder="Company name (optional)">
      </div>
      <div class="form-group">
        <label class="form-label">Contact Person</label>
        <input class="form-input" id="f_contact" placeholder="Name or email (optional)">
      </div>
      <div class="form-group">
        <label class="form-label">Service Type</label>
        <select class="form-select" id="f_service">
          <option value="AI Workflow">AI Workflow</option>
          <option value="Website Build">Website Build</option>
          <option value="Other">Other</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Status</label>
        <select class="form-select" id="f_status">${colOpts}</select>
      </div>
      <div class="form-group">
        <label class="form-label">Notes</label>
        <input class="form-input" id="f_note" placeholder="What do they need?">
      </div>
    `;
  }

  if (mode === "tasks") {
    return `
      <div class="form-group">
        <label class="form-label">Task</label>
        <input class="form-input" id="f_name" placeholder="What needs to get done?">
      </div>
      <div class="form-group">
        <label class="form-label">Owner</label>
        <input class="form-input" id="f_owner" placeholder="Athan / Team / etc.">
      </div>
      <div class="form-group">
        <label class="form-label">Due Date</label>
        <input class="form-input" type="date" id="f_due" style="color-scheme:dark">
      </div>
      <div class="form-group">
        <label class="form-label">Priority</label>
        <select class="form-select" id="f_priority">
          <option value="High">High</option>
          <option value="Mid" selected>Mid</option>
          <option value="Low">Low</option>
        </select>
      </div>
    `;
  }

  if (mode === "decisions") {
    const today = todayStr();
    // NOTE: decisionRowsContainer starts EMPTY.
    // openAddModal() calls addDecisionRow() after rendering to inject the first
    // row programmatically — the same code path every subsequent row uses.
    // This avoids calling buildDecisionRow() inside a template literal, which
    // caused intermittent issues with function evaluation order.
    return `
      <div class="form-group">
        <label class="form-label">Meeting Title</label>
        <input class="form-input" id="f_mtitle" placeholder="e.g. Kickoff — Week 1">
      </div>
      <div class="form-group">
        <label class="form-label">Meeting Date</label>
        <input class="form-input" type="date" id="f_mdate" value="${today}" style="color-scheme:dark">
      </div>
      <div class="decisions-rows-header">
        <span class="form-label" style="margin-bottom:0">Decisions</span>
      </div>
      <div id="decisionRowsContainer"></div>
      <button type="button" class="btn btn-ghost add-decision-row-btn" onclick="addDecisionRow()">
        + Add new decision
      </button>
    `;
  }

  if (mode === "clients") {
    return `
      <div class="form-group">
        <label class="form-label">Company Name</label>
        <input class="form-input" id="f_companyName" placeholder="Client company">
      </div>
      <div class="form-group">
        <label class="form-label">Contact Name</label>
        <input class="form-input" id="f_contactName" placeholder="Primary contact">
      </div>
      <div class="form-group">
        <label class="form-label">Contact Email</label>
        <input class="form-input" id="f_email" placeholder="contact@company.com">
      </div>
      <div class="form-group">
        <label class="form-label">Client Auth UID</label>
        <input class="form-input" id="f_authUid" placeholder="Firebase Auth UID">
      </div>
      <div class="form-group">
        <label class="form-label">Plan Name</label>
        <input class="form-input" id="f_planName" placeholder="Growth Retainer">
      </div>
      <div class="form-group">
        <label class="form-label">Status</label>
        <select class="form-select" id="f_clientStatus">
          <option value="active" selected>Active</option>
          <option value="inactive">Inactive</option>
          <option value="one-time">One-Time</option>
          <option value="completed">Completed</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Recurring</label>
        <select class="form-select" id="f_recurring">
          <option value="true" selected>Recurring</option>
          <option value="false">One-Time</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Paid</label>
        <select class="form-select" id="f_paid">
          <option value="true">Paid</option>
          <option value="false" selected>Unpaid</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Updates / Month</label>
        <input class="form-input" id="f_updatesPerMonth" type="number" min="0" value="4">
      </div>
      <div class="form-group">
        <label class="form-label">Updates Remaining</label>
        <input class="form-input" id="f_updatesRemaining" type="number" min="0" value="4">
      </div>
      <div class="form-group">
        <label class="form-label">Last Payment Note</label>
        <input class="form-input" id="f_lastPaymentNote" placeholder="e.g. Paid Mar 9 via ACH">
      </div>
      <div class="form-group">
        <label class="form-label">Billing Notes</label>
        <textarea class="form-input form-textarea" id="f_billingNotes" rows="3" placeholder="Manual billing notes"></textarea>
      </div>
      <div class="form-group">
        <label class="form-label">Internal Notes</label>
        <textarea class="form-input form-textarea" id="f_clientNotes" rows="3" placeholder="General context"></textarea>
      </div>
    `;
  }
  return "";
}

// ─── DYNAMIC DECISION ROWS ───────────────────────────────────────────────────
// Each row is a self-contained block with a text field, owner field, and
// a remove button (hidden on the first row if it's the only one).

let decisionRowCount = 0;

function resetDecisionRows() {
  decisionRowCount = 0;
  const container = document.getElementById("decisionRowsContainer");
  if (!container) return;
  container.innerHTML = "";
  window.addDecisionRow();
}

function buildDecisionRow(n) {
  return `
    <div class="decision-row" data-decision-row="true" id="drow-${n}">
      <div class="decision-row-header">
        <span class="form-label" style="margin-bottom:0">Decision ${n}</span>
        <button
          type="button"
          class="btn-remove-row"
          onclick="removeDecisionRow(${n})"
          title="Remove this decision"
        >✕</button>
      </div>
      <div class="form-group" style="margin-bottom:8px">
        <input class="form-input decision-row-text" placeholder="What was decided?">
      </div>
      <div class="form-group">
        <input class="form-input decision-row-owner" placeholder="Owner / action">
      </div>
    </div>
  `;
}

window.addDecisionRow = function () {
  decisionRowCount++;
  const container = document.getElementById("decisionRowsContainer");
  if (!container) return;
  const div = document.createElement("div");
  div.innerHTML = buildDecisionRow(decisionRowCount);
  container.appendChild(div.firstElementChild);
  // Show all remove buttons once there are 2+ rows
  updateRemoveButtons();
};

window.removeDecisionRow = function (n) {
  const row = document.getElementById("drow-" + n);
  if (row) row.remove();
  updateRemoveButtons();
};

function updateRemoveButtons() {
  const rows = document.querySelectorAll("#decisionRowsContainer .decision-row");
  rows.forEach((row) => {
    const btn = row.querySelector(".btn-remove-row");
    if (btn) btn.style.display = rows.length > 1 ? "flex" : "none";
  });
}

window.saveModal = async function () {
  try {
    if (modalMode === "pipeline") {
      const title = document.getElementById("f_title").value.trim();
      if (!title) { alert("Lead name is required."); return; }
      await addDoc(collection(db, "pipeline"), {
        title,
        company:   document.getElementById("f_company").value.trim(),
        contact:   document.getElementById("f_contact").value.trim(),
        service:   document.getElementById("f_service").value,
        status:    document.getElementById("f_status").value,
        note:      document.getElementById("f_note").value.trim(),
        createdAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
      });
    }

    else if (modalMode === "tasks") {
      const name = document.getElementById("f_name").value.trim();
      if (!name) { alert("Task name is required."); return; }
      await addDoc(collection(db, "tasks"), {
        name,
        owner:     document.getElementById("f_owner").value.trim() || "Team",
        due:       document.getElementById("f_due").value || "",
        priority:  document.getElementById("f_priority").value,
        done:      false,
        createdAt: serverTimestamp(),
      });
    }

    else if (modalMode === "decisions") {
      const meetingTitle = document.getElementById("f_mtitle").value.trim();
      const meetingDate  = document.getElementById("f_mdate").value || todayStr();
      if (!meetingTitle) { alert("Meeting title is required."); return; }

      // Collect all dynamically added decision rows
      const items = [];
      document.querySelectorAll("#decisionRowsContainer .decision-row").forEach((row) => {
        const text  = row.querySelector(".decision-row-text")?.value.trim();
        const owner = row.querySelector(".decision-row-owner")?.value.trim();
        if (text) items.push({ id: uid(), text, owner: owner || "" });
      });
      if (!items.length) { alert("At least one decision is required."); return; }

      await addDoc(collection(db, "decisions"), {
        meetingTitle,
        meetingDate,
        items,
        createdAt: serverTimestamp(),
      });
    }

    else if (modalMode === "clients") {
      const companyName = document.getElementById("f_companyName").value.trim();
      if (!companyName) { alert("Company name is required."); return; }

      const authUid = document.getElementById("f_authUid").value.trim();
      const email = document.getElementById("f_email").value.trim();
      const updatesPerMonth = Math.max(0, Number(document.getElementById("f_updatesPerMonth").value || 0));
      const updatesRemaining = Math.max(0, Number(document.getElementById("f_updatesRemaining").value || 0));

      const clientRef = await addDoc(collection(db, "clients"), {
        companyName,
        contactName: document.getElementById("f_contactName").value.trim(),
        email,
        authUid,
        planName: document.getElementById("f_planName").value.trim(),
        recurring: document.getElementById("f_recurring").value === "true",
        paid: document.getElementById("f_paid").value === "true",
        status: document.getElementById("f_clientStatus").value,
        updatesPerMonth,
        updatesRemaining,
        notes: document.getElementById("f_clientNotes").value.trim(),
        lastPaymentNote: document.getElementById("f_lastPaymentNote").value.trim(),
        billingNotes: document.getElementById("f_billingNotes").value.trim(),
        billingHistory: [],
        createdAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
      });

      selectedClientId = clientRef.id;

      if (authUid) {
        await setDoc(doc(db, "users", authUid), {
          role: "client",
          clientId: clientRef.id,
          email: email || "",
          updatedAt: serverTimestamp(),
        }, { merge: true });
      }
    }

    window.closeModal();
  } catch (err) {
    console.error("saveModal:", err);
    alert("Save failed: " + err.message);
  }
};

// ─── PAGE NAVIGATION ──────────────────────────────────────────────────────────
let currentPage = "pipeline";

window.showPage = function (name, el) {
  currentPage = name;
  document.querySelectorAll(".page").forEach((p) => p.classList.remove("active"));
  document.querySelectorAll(".nav-item").forEach((n) => n.classList.remove("active"));
  document.querySelectorAll(".top-nav-item").forEach((n) => n.classList.remove("active"));

  document.getElementById("page-" + name)?.classList.add("active");

  const sidebarItem = document.querySelector(`.nav-item[data-page="${name}"]`);
  const topNavItem  = document.getElementById("tnav-" + name);
  if (sidebarItem) sidebarItem.classList.add("active");
  if (topNavItem)  topNavItem.classList.add("active");

  const titles = {
    pipeline: "PIPELINE",
    tasks: "TASKS",
    decisions: "DECISIONS LOG",
    clients: "CLIENT OPS",
  };
  const el2    = document.getElementById("pageTitle");
  if (el2) el2.textContent = titles[name] || name.toUpperCase();
};

// ─── UTILITIES ────────────────────────────────────────────────────────────────
function escHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// Alias for attribute contexts (same escaping, named for clarity)
function escHtmlAttr(str) { return escHtml(str); }

function formatDate(str) {
  if (!str) return "—";
  const d = new Date(str + "T00:00:00");
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

function uid() {
  return "_" + Math.random().toString(36).slice(2, 11);
}

