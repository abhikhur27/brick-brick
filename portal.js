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

// ─── COLUMN CONFIG ───────────────────────────────────────────────────────────
// Firestore status value → display label
const COLS = [
  { key: "leads",     label: "Leads"          },
  { key: "contacted", label: "In Conversation" },
  { key: "proposal",  label: "Proposal Sent"  },
  { key: "closed",    label: "Closed"         },
];

// ─── UNSUBSCRIBE HANDLES (prevent listener leaks on re-login) ────────────────
let unsubPipeline   = null;
let unsubTasks      = null;
let unsubDecisions  = null;

// ─── MODAL STATE ─────────────────────────────────────────────────────────────
let modalMode    = "pipeline"; // "pipeline" | "tasks" | "decisions"
let modalContext = "";         // pre-fill status column when clicking "+ Add" per-column

// ─── AUTH ─────────────────────────────────────────────────────────────────────
/**
 * Called by the Login button (and Enter key on password field).
 * Exposed to window so inline onclick attributes in the HTML work.
 */
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
    // onAuthStateChanged handles the UI transition
  } catch (err) {
    errEl.textContent = friendlyAuthError(err.code);
    btn.textContent   = "ENTER WORKSPACE";
    btn.disabled      = false;
  }
};

window.doLogout = async function () {
  // Detach Firestore listeners before signing out
  if (unsubPipeline)  unsubPipeline();
  if (unsubTasks)     unsubTasks();
  if (unsubDecisions) unsubDecisions();
  await signOut(auth);
};

// Support pressing Enter in the password field
document.getElementById("loginPassword")?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") window.doLogin();
});

/**
 * Central auth-state watcher — this is the ONLY place that shows/hides the
 * login screen vs the app.  Both login success and logout flow through here.
 */
onAuthStateChanged(auth, (user) => {
  if (user) {
    // Hide login, show workspace
    document.getElementById("loginScreen").style.display = "none";
    document.getElementById("app").classList.add("visible");

    const signedInEl = document.getElementById("signedInAs");
    if (signedInEl) signedInEl.textContent = user.email;

    // Start real-time Firestore listeners
    startListeners();
  } else {
    // Show login, hide workspace
    document.getElementById("loginScreen").style.display = "flex";
    document.getElementById("app").classList.remove("visible");

    // Reset login button state in case of logout-then-back
    const btn = document.getElementById("loginBtn");
    if (btn) { btn.textContent = "ENTER WORKSPACE"; btn.disabled = false; }
  }
});

function friendlyAuthError(code) {
  const map = {
    "auth/invalid-email":        "Invalid email address.",
    "auth/user-not-found":       "No account found with that email.",
    "auth/wrong-password":       "Incorrect password.",
    "auth/invalid-credential":   "Incorrect email or password.",
    "auth/too-many-requests":    "Too many attempts — try again later.",
    "auth/user-disabled":        "This account has been disabled.",
  };
  return map[code] || "Login failed. Check your credentials.";
}

// ─── REAL-TIME FIRESTORE LISTENERS ───────────────────────────────────────────
function startListeners() {
  // Pipeline — ordered by creation time
  unsubPipeline = onSnapshot(
    query(collection(db, "pipeline"), orderBy("createdAt", "asc")),
    (snap) => {
      const cards = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      renderPipeline(cards);
    },
    (err) => console.error("Pipeline listener error:", err)
  );

  // Tasks — ordered by due date
  unsubTasks = onSnapshot(
    query(collection(db, "tasks"), orderBy("due", "asc")),
    (snap) => {
      const tasks = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      renderTasks(tasks);
    },
    (err) => console.error("Tasks listener error:", err)
  );

  // Decisions — most recent first
  unsubDecisions = onSnapshot(
    query(collection(db, "decisions"), orderBy("createdAt", "desc")),
    (snap) => {
      const decisions = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      renderDecisions(decisions);
    },
    (err) => console.error("Decisions listener error:", err)
  );
}

window.refreshAll = function () {
  // Listeners are real-time, so a manual refresh just re-attaches them
  if (unsubPipeline)  unsubPipeline();
  if (unsubTasks)     unsubTasks();
  if (unsubDecisions) unsubDecisions();
  startListeners();
};

// ─── PIPELINE RENDER ─────────────────────────────────────────────────────────
function renderPipeline(cards) {
  const board = document.getElementById("kanbanBoard");
  if (!board) return;
  board.innerHTML = "";

  COLS.forEach(({ key, label }) => {
    const colCards = cards.filter((c) => c.status === key);

    const colEl = document.createElement("div");
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
        card.service === "AI Workflow"    ? "tag-ai"  :
        card.service === "Website Build"  ? "tag-web" : "tag-other";

      // Move-to buttons (all other columns)
      const moveButtons = COLS
        .filter((c) => c.key !== key)
        .map((c) => `<button class="card-btn" onclick="moveCard('${card.id}','${c.key}')">${c.label}</button>`)
        .join("");

      const cardEl = document.createElement("div");
      cardEl.className = "card";
      cardEl.innerHTML = `
        <div class="card-title">${escHtml(card.title)}</div>
        <div class="card-meta">${escHtml(card.note || "")}</div>
        <span class="card-tag ${tagClass}">${escHtml(card.service || "")}</span>
        <div class="card-actions">
          ${moveButtons}
          <button class="card-btn delete" onclick="deleteCard('${card.id}')">✕</button>
        </div>
      `;
      container.appendChild(cardEl);
    });
  });
}

window.moveCard = async function (id, newStatus) {
  try {
    await updateDoc(doc(db, "pipeline", id), { status: newStatus });
  } catch (err) {
    console.error("moveCard error:", err);
  }
};

window.deleteCard = async function (id) {
  if (!confirm("Delete this lead?")) return;
  try {
    await deleteDoc(doc(db, "pipeline", id));
  } catch (err) {
    console.error("deleteCard error:", err);
  }
};

// ─── TASKS RENDER ────────────────────────────────────────────────────────────
function renderTasks(tasks) {
  const tbody = document.getElementById("tasksBody");
  if (!tbody) return;
  tbody.innerHTML = "";

  // Sort: incomplete first (by due date), completed at bottom
  const sorted = [...tasks].sort((a, b) => {
    if (!!a.done !== !!b.done) return a.done ? 1 : -1;
    return (a.due || "").localeCompare(b.due || "");
  });

  if (!sorted.length) {
    tbody.innerHTML = `<tr><td colspan="6" class="empty-state" style="padding:32px;text-align:center">No tasks yet — add one above.</td></tr>`;
    return;
  }

  sorted.forEach((task) => {
    const tr = document.createElement("tr");
    if (task.done) tr.classList.add("task-done-row");

    const isOverdue = !task.done && task.due && task.due < todayStr();
    const pClass    = task.priority === "High" ? "p-high" : task.priority === "Mid" ? "p-mid" : "p-low";
    const initials  = (task.owner || "?").split(" ").map((w) => w[0]).join("").toUpperCase().slice(0, 2);

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
      <td><span class="priority ${pClass}">${task.priority || "—"}</span></td>
      <td><button class="card-btn delete" onclick="deleteTask('${task.id}')">✕</button></td>
    `;
    tbody.appendChild(tr);
  });
}

window.toggleTask = async function (id, newDoneState) {
  try {
    await updateDoc(doc(db, "tasks", id), { done: newDoneState });
  } catch (err) {
    console.error("toggleTask error:", err);
  }
};

window.deleteTask = async function (id) {
  if (!confirm("Delete this task?")) return;
  try {
    await deleteDoc(doc(db, "tasks", id));
  } catch (err) {
    console.error("deleteTask error:", err);
  }
};

// ─── DECISIONS RENDER ────────────────────────────────────────────────────────
function renderDecisions(decisions) {
  const log = document.getElementById("decisionsLog");
  if (!log) return;
  log.innerHTML = "";

  if (!decisions.length) {
    log.innerHTML = `<div class="empty-state">No meetings logged yet. Add your first one above.</div>`;
    return;
  }

  decisions.forEach((entry) => {
    const el = document.createElement("div");
    el.className = "decision-entry";
    const items = (entry.items || []).map((item) => `
      <div class="decision-item">
        <div class="decision-text">${escHtml(item.text)}</div>
        <div class="decision-owner">${escHtml(item.owner || "")}</div>
      </div>
    `).join("");

    el.innerHTML = `
      <div class="decision-date">
        <span>${escHtml(entry.date)}</span>
        <button class="delete-entry-btn" onclick="deleteDecision('${entry.id}')">Delete</button>
      </div>
      ${items}
    `;
    log.appendChild(el);
  });
}

window.deleteDecision = async function (id) {
  if (!confirm("Delete this meeting log?")) return;
  try {
    await deleteDoc(doc(db, "decisions", id));
  } catch (err) {
    console.error("deleteDecision error:", err);
  }
};

// ─── MODAL ────────────────────────────────────────────────────────────────────
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
  if (e.key === "Escape") window.closeModal();
});

function getModalForm(mode) {
  if (mode === "pipeline") {
    const colOpts = COLS.map((c) =>
      `<option value="${c.key}" ${c.key === (modalContext || "leads") ? "selected" : ""}>${c.label}</option>`
    ).join("");
    return `
      <div class="form-group">
        <label class="form-label">Client / Lead Name</label>
        <input class="form-input" id="f_title" placeholder="e.g. Local coffee shop">
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
    return `
      <div class="form-group">
        <label class="form-label">Meeting Title / Date</label>
        <input class="form-input" id="f_date" placeholder="e.g. March 10, 2026 — Meeting #2">
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
        service:   document.getElementById("f_service").value,
        status:    document.getElementById("f_status").value,
        note:      document.getElementById("f_note").value.trim(),
        createdAt: serverTimestamp(),
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
      const date = document.getElementById("f_date").value.trim();
      if (!date) { alert("Meeting title/date is required."); return; }
      const items = [];
      for (let i = 1; i <= 3; i++) {
        const text  = document.getElementById(`f_d${i}`)?.value.trim();
        const owner = document.getElementById(`f_o${i}`)?.value.trim();
        if (text) items.push({ text, owner: owner || "" });
      }
      if (!items.length) { alert("At least one decision is required."); return; }
      await addDoc(collection(db, "decisions"), {
        date,
        items,
        createdAt: serverTimestamp(),
      });
    }

    window.closeModal();
  } catch (err) {
    console.error("saveModal error:", err);
    alert("Save failed: " + err.message);
  }
};

// ─── PAGE NAVIGATION ─────────────────────────────────────────────────────────
let currentPage = "pipeline";

window.showPage = function (name, el, source) {
  currentPage = name;

  document.querySelectorAll(".page").forEach((p) => p.classList.remove("active"));
  document.querySelectorAll(".nav-item").forEach((n) => n.classList.remove("active"));
  document.querySelectorAll(".top-nav-item").forEach((n) => n.classList.remove("active"));

  document.getElementById("page-" + name)?.classList.add("active");

  // Sync sidebar + top nav
  const sidebarItem = document.querySelector(`.nav-item[data-page="${name}"]`);
  const topNavItem  = document.getElementById("tnav-" + name);
  if (sidebarItem) sidebarItem.classList.add("active");
  if (topNavItem)  topNavItem.classList.add("active");

  const titles = { pipeline: "PIPELINE", tasks: "TASKS", decisions: "DECISIONS LOG" };
  const pageTitleEl = document.getElementById("pageTitle");
  if (pageTitleEl) pageTitleEl.textContent = titles[name] || name.toUpperCase();
};

// ─── UTILITIES ───────────────────────────────────────────────────────────────
function escHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatDate(str) {
  if (!str) return "—";
  const d = new Date(str + "T00:00:00");
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function todayStr() {
  return new Date().toISOString().slice(0, 10); // "YYYY-MM-DD"
}