// ─── FIREBASE SETUP ──────────────────────────────────────────────────────────
import { initializeApp }          from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getAuth, signInWithEmailAndPassword, signOut, onAuthStateChanged }
                                   from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { getFirestore, collection, addDoc, updateDoc, deleteDoc, doc,
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

// ─── TASK SORT STATE ─────────────────────────────────────────────────────────
let taskSortField = "due";     // "due" | "createdAt" | "priority" | "owner" | "done"
let taskSortAsc   = true;

// ─── LISTENER HANDLES ────────────────────────────────────────────────────────
let unsubPipeline   = null;
let unsubTasks      = null;
let unsubDecisions  = null;

// ─── MODAL STATE ─────────────────────────────────────────────────────────────
let modalMode    = "pipeline";
let modalContext = "";
let editingLeadId = null;   // ID of the lead currently open in detail modal

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
  await signOut(auth);
};

document.getElementById("loginPassword")?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") window.doLogin();
});

// Central auth-state observer — the ONLY place that shows/hides login vs workspace.
onAuthStateChanged(auth, (user) => {
  // Reveal the page now that auth state is known (prevents flash)
  document.body.style.visibility = "visible";

  if (user) {
    document.getElementById("loginScreen").style.display = "none";
    document.getElementById("app").classList.add("visible");

    const el = document.getElementById("signedInAs");
    if (el) el.textContent = user.email;

    startListeners();
  } else {
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
    query(collection(db, "pipeline"), orderBy("createdAt", "asc")),
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
}

window.refreshAll = function () {
  if (unsubPipeline)  unsubPipeline();
  if (unsubTasks)     unsubTasks();
  if (unsubDecisions) unsubDecisions();
  startListeners();
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
      <div id="col-${key}"></div>
      <button class="add-card" onclick="openAddModal('pipeline','${key}')">+ Add</button>
    `;
    board.appendChild(colEl);

    const container = colEl.querySelector(`#col-${key}`);
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
      // Clicking the card body (not action buttons) opens detail modal
      cardEl.addEventListener("click", () => openLeadDetail(card.id));
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
  if (!tbody) return;
  tbody.innerHTML = "";

  const sorted = sortedTasks(tasks);

  if (!sorted.length) {
    tbody.innerHTML = `<tr><td colspan="7"><div class="empty-state" style="margin:24px 0">No tasks yet.</div></td></tr>`;
    return;
  }

  sorted.forEach((task) => {
    const tr = document.createElement("tr");
    if (task.done) tr.classList.add("task-done-row");

    const isOverdue = !task.done && task.due && task.due < todayStr();
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
      <td><button class="card-btn delete" onclick="deleteTask('${task.id}')">✕</button></td>
    `;
    tbody.appendChild(tr);
  });
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

  const titles = { pipeline: "ADD LEAD", tasks: "ADD TASK", decisions: "LOG MEETING" };
  document.getElementById("modalTitle").textContent = titles[modalMode] || "ADD";
  document.getElementById("modalBody").innerHTML    = getModalForm(modalMode);
  document.getElementById("modalOverlay").classList.add("open");
};

window.closeModal = function () {
  document.getElementById("modalOverlay").classList.remove("open");
};

document.getElementById("modalOverlay")?.addEventListener("click", function (e) {
  if (e.target === this) window.closeModal();
});

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") { window.closeModal(); window.closeLeadDetail(); }
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
    // Default date to today
    const today = todayStr();
    return `
      <div class="form-group">
        <label class="form-label">Meeting Title</label>
        <input class="form-input" id="f_mtitle" placeholder="e.g. Kickoff — Week 1">
      </div>
      <div class="form-group">
        <label class="form-label">Meeting Date</label>
        <input class="form-input" type="date" id="f_mdate" value="${today}" style="color-scheme:dark">
      </div>
      <div class="form-group">
        <label class="form-label">Decision 1</label>
        <input class="form-input" id="f_d1" placeholder="What was decided?">
      </div>
      <div class="form-group">
        <label class="form-label">Owner / Action 1</label>
        <input class="form-input" id="f_o1" placeholder="→ Who does what">
      </div>
      <div class="form-group">
        <label class="form-label">Decision 2 (optional)</label>
        <input class="form-input" id="f_d2">
      </div>
      <div class="form-group">
        <label class="form-label">Owner / Action 2</label>
        <input class="form-input" id="f_o2">
      </div>
      <div class="form-group">
        <label class="form-label">Decision 3 (optional)</label>
        <input class="form-input" id="f_d3">
      </div>
      <div class="form-group">
        <label class="form-label">Owner / Action 3</label>
        <input class="form-input" id="f_o3">
      </div>
    `;
  }
  return "";
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
      const meetingDate  = document.getElementById("f_mdate").value;
      if (!meetingTitle) { alert("Meeting title is required."); return; }

      const items = [];
      for (let i = 1; i <= 3; i++) {
        const text  = document.getElementById(`f_d${i}`)?.value.trim();
        const owner = document.getElementById(`f_o${i}`)?.value.trim();
        if (text) items.push({ id: uid(), text, owner: owner || "" });
      }
      if (!items.length) { alert("At least one decision is required."); return; }

      await addDoc(collection(db, "decisions"), {
        meetingTitle,
        meetingDate,  // "YYYY-MM-DD" — real date for sorting
        items,
        createdAt: serverTimestamp(),
      });
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

  const titles = { pipeline: "PIPELINE", tasks: "TASKS", decisions: "DECISIONS LOG" };
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