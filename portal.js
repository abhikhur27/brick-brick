// ─── FIREBASE SETUP ──────────────────────────────────────────────────────────
import { initializeApp }          from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getAuth, signInWithEmailAndPassword, signOut, onAuthStateChanged, sendPasswordResetEmail }
                                   from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { getFirestore, collection, addDoc, updateDoc, deleteDoc, doc, setDoc, getDoc,
         onSnapshot, serverTimestamp, query, orderBy, arrayUnion, where }
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

const CLIENT_FLOW_COLS = [
  { key: "queued",    label: "Queued" },
  { key: "blueprint", label: "Blueprint" },
  { key: "building",  label: "In Build" },
  { key: "delivered", label: "Delivered" },
];

const LEAD_SERVICE_OPTIONS = [
  { value: "AI Workflow", tagClass: "tag-ai" },
  { value: "Website Build", tagClass: "tag-web" },
  { value: "Automation Ops", tagClass: "tag-automation" },
  { value: "SEO / Content", tagClass: "tag-content" },
  { value: "Paid Ads", tagClass: "tag-growth" },
  { value: "Brand / Creative", tagClass: "tag-brand" },
];

// ─── STATE CACHES (updated by real-time listeners) ───────────────────────────
let currentPipeline  = [];
let currentTasks     = [];
let currentDecisions = [];
let currentClients   = [];
let currentClientRequests = [];
let currentClientFlow = [];
let currentTeamUsers = [];
let currentManagedUsers = [];
let currentProvisioning = [];
let managedRecordsByKey = new Map();
let showArchivedRequests = false;

// ─── TASK SORT STATE ─────────────────────────────────────────────────────────
let taskSortField = "due";     // "due" | "createdAt" | "priority" | "owner" | "done"
let taskSortAsc   = true;

// ─── LISTENER HANDLES ────────────────────────────────────────────────────────
let unsubPipeline   = null;
let unsubTasks      = null;
let unsubDecisions  = null;
let unsubClients    = null;
let unsubClientRequests = null;
let unsubClientFlow = null;
let unsubTeamUsers = null;
let unsubUsers = null;
let unsubProvisioning = null;

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
let managedUsersFilter = "client";
let editingManagedRecordKey = null;

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
  stopRealtimeListeners();
  await signOut(auth);
};

document.getElementById("loginPassword")?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") window.doLogin();
});

document.getElementById("managedProvisionForm")?.addEventListener("submit", handleManagedProvisionSubmit);
document.getElementById("managedProvisionRole")?.addEventListener("change", toggleManagedProvisionClientField);
document.getElementById("managedUserOverlay")?.addEventListener("click", function (e) {
  if (e.target === this) window.closeManagedUserModal();
});

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function isTeamRole(role) {
  return role === "admin" || role === "super_admin";
}

function isSuperAdminRole(role) {
  return role === "super_admin";
}

function toggleAccessManagerVisibility(show) {
  const section = document.getElementById("accessManagerSection");
  if (section) section.style.display = show ? "block" : "none";
}

async function getUserRecord(uid) {
  try {
    const snap = await getDoc(doc(db, "users", uid));
    return snap.exists() ? snap.data() : null;
  } catch (err) {
    console.error("getUserRecord:", err);
    return null;
  }
}

async function bootstrapTeamUserFromProvision(user) {
  const email = normalizeEmail(user?.email);
  if (!email) return null;

  try {
    const provisionRef = doc(db, "login_provisioning", email);
    const provisionSnap = await getDoc(provisionRef);
    if (!provisionSnap.exists()) return null;

    const provision = provisionSnap.data() || {};
    const role = String(provision.role || "");
    const status = String(provision.status || "active");
    if (!isTeamRole(role) || status === "disabled") return null;

    await setDoc(doc(db, "users", user.uid), {
      role,
      clientId: "",
      email,
      name: String(provision.name || ""),
      notes: String(provision.notes || ""),
      disabled: false,
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
    }, { merge: true });

    await setDoc(provisionRef, {
      email,
      authUid: user.uid,
      status: "active",
      updatedAt: serverTimestamp(),
      updatedByUid: user.uid,
      linkedAt: serverTimestamp(),
    }, { merge: true });

    return { role, email };
  } catch (err) {
    console.error("bootstrapTeamUserFromProvision:", err);
    return null;
  }
}

async function resolvePortalUser(user) {
  let userData = await getUserRecord(user.uid);
  if (!userData) {
    const bootstrapped = await bootstrapTeamUserFromProvision(user);
    if (!bootstrapped) return null;
    userData = await getUserRecord(user.uid);
  }
  return userData;
}

// Central auth-state observer - the ONLY place that shows/hides login vs workspace.
onAuthStateChanged(auth, async (user) => {
  document.body.style.visibility = "visible";

  if (!user) {
    currentUserRole = null;
    currentClientFlow = [];
    currentClientRequests = [];
    currentTeamUsers = [];
    showArchivedRequests = false;
    currentManagedUsers = [];
    currentProvisioning = [];
    managedRecordsByKey = new Map();
    stopRealtimeListeners();
    toggleAccessManagerVisibility(false);
    document.getElementById("loginScreen").style.display = "flex";
    document.getElementById("app").classList.remove("visible");
    const btn = document.getElementById("loginBtn");
    if (btn) {
      btn.textContent = "ENTER WORKSPACE";
      btn.disabled = false;
    }
    return;
  }

  const userData = await resolvePortalUser(user);
  const role = String(userData?.role || "");
  if (!isTeamRole(role) || userData?.disabled === true) {
    await signOut(auth);
    const errEl = document.getElementById("loginError");
    if (errEl) errEl.textContent = "Internal workspace access is for admin/team accounts only.";
    return;
  }

  currentUserRole = role;
  document.getElementById("loginScreen").style.display = "none";
  document.getElementById("app").classList.add("visible");
  toggleAccessManagerVisibility(isSuperAdminRole(role));

  const el = document.getElementById("signedInAs");
  if (el) {
    const roleLabel = role === "super_admin" ? "Super Admin" : "Admin";
    el.textContent = `${user.email} (${roleLabel})`;
  }

  startListeners();
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
  stopRealtimeListeners();
  startTeamUserListener();
  updateArchivedToggleButton();

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
      populateManagedClientOptions();
      renderManagedUsersTable();
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

  unsubClientFlow = onSnapshot(
    query(collection(db, "client_flow"), orderBy("createdAt", "desc")),
    (snap) => {
      currentClientFlow = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      renderClientFlowBoard(currentClientFlow);
    },
    (err) => console.error("Client flow error:", err)
  );

  if (isSuperAdminRole(currentUserRole)) {
    startManagedUserListeners();
  } else {
    stopManagedUserListeners();
    currentManagedUsers = [];
    currentProvisioning = [];
    managedRecordsByKey = new Map();
  }
}

function stopManagedUserListeners() {
  if (unsubUsers) unsubUsers();
  if (unsubProvisioning) unsubProvisioning();
  unsubUsers = null;
  unsubProvisioning = null;
}

function startTeamUserListener() {
  stopTeamUserListener();
  unsubTeamUsers = onSnapshot(
    query(collection(db, "users"), where("role", "in", ["admin", "super_admin"])),
    (snap) => {
      currentTeamUsers = snap.docs
        .map((d) => ({ id: d.id, ...d.data() }))
        .filter((user) => user.disabled !== true)
        .sort((a, b) => {
          const an = String(a.name || a.email || "").toLowerCase();
          const bn = String(b.name || b.email || "").toLowerCase();
          return an.localeCompare(bn);
        });
    },
    (err) => {
      console.error("Team users listener error:", err);
      currentTeamUsers = [];
    }
  );
}

function stopTeamUserListener() {
  if (unsubTeamUsers) unsubTeamUsers();
  unsubTeamUsers = null;
  currentTeamUsers = [];
}

function stopRealtimeListeners() {
  if (unsubPipeline) unsubPipeline();
  if (unsubTasks) unsubTasks();
  if (unsubDecisions) unsubDecisions();
  if (unsubClients) unsubClients();
  if (unsubClientRequests) unsubClientRequests();
  if (unsubClientFlow) unsubClientFlow();
  stopTeamUserListener();
  stopManagedUserListeners();
  unsubPipeline = null;
  unsubTasks = null;
  unsubDecisions = null;
  unsubClients = null;
  unsubClientRequests = null;
  unsubClientFlow = null;
}

function startManagedUserListeners() {
  stopManagedUserListeners();

  unsubUsers = onSnapshot(
    collection(db, "users"),
    (snap) => {
      currentManagedUsers = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      renderManagedUsersTable();
    },
    (err) => {
      console.error("Users listener error:", err);
      setElementStatus("managedUsersStatus", "Could not load user profiles.", true);
    }
  );

  unsubProvisioning = onSnapshot(
    collection(db, "login_provisioning"),
    (snap) => {
      currentProvisioning = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      renderManagedUsersTable();
    },
    (err) => {
      console.error("Provisioning listener error:", err);
      setElementStatus("managedUsersStatus", "Could not load provisioning records.", true);
    }
  );
}

window.refreshAll = function () {
  startListeners();
};

window.openSalesPlaybook = function () {
  window.location.href = "sales_playbook.html";
};

function leadServiceTagClass(service) {
  const normalized = String(service || "").trim();
  const match = LEAD_SERVICE_OPTIONS.find((item) => item.value === normalized);
  return match ? match.tagClass : "tag-other";
}

function isPresetLeadService(service) {
  const normalized = String(service || "").trim();
  return LEAD_SERVICE_OPTIONS.some((item) => item.value === normalized);
}

function leadServiceSelectValue(service) {
  return isPresetLeadService(service) ? String(service).trim() : "__other__";
}

function leadServiceOptionsHtml(selectedService) {
  const selectedValue = leadServiceSelectValue(selectedService);
  const options = LEAD_SERVICE_OPTIONS
    .map((item) => `<option value="${escHtmlAttr(item.value)}" ${item.value === selectedValue ? "selected" : ""}>${escHtml(item.value)}</option>`)
    .join("");
  return `
    ${options}
    <option value="__other__" ${selectedValue === "__other__" ? "selected" : ""}>Other (custom)</option>
  `;
}

function syncServiceOtherField(selectId, wrapId, inputId) {
  const selectEl = document.getElementById(selectId);
  const wrapEl = document.getElementById(wrapId);
  const inputEl = document.getElementById(inputId);
  if (!selectEl || !wrapEl || !inputEl) return;
  const show = selectEl.value === "__other__";
  wrapEl.style.display = show ? "block" : "none";
  inputEl.required = show;
  if (!show) inputEl.value = "";
}

function resolveLeadServiceValue(selectId, inputId) {
  const selectedValue = String(document.getElementById(selectId)?.value || "");
  if (selectedValue !== "__other__") return selectedValue;
  return String(document.getElementById(inputId)?.value || "").trim();
}

function teamUserLabel(user) {
  return String(user?.name || user?.email || user?.id || "Team");
}

function teamUserInitials(name) {
  return String(name || "T")
    .split(" ")
    .map((part) => part[0] || "")
    .join("")
    .toUpperCase()
    .slice(0, 2) || "T";
}

function resolveTeamUserName(uid, fallbackName = "") {
  const normalizedUid = String(uid || "");
  if (!normalizedUid) return "";
  const found = currentTeamUsers.find((user) => user.id === normalizedUid);
  if (found) return teamUserLabel(found);
  return String(fallbackName || normalizedUid.slice(0, 8));
}

function teamAssigneeOptionsHtml(selectedUid = "", fallbackName = "") {
  const normalizedSelected = String(selectedUid || "");
  const options = [`<option value="">Unassigned</option>`];
  const seen = new Set();
  const users = currentTeamUsers.length
    ? currentTeamUsers
    : (auth.currentUser?.uid
      ? [{
        id: String(auth.currentUser.uid),
        name: String(auth.currentUser.displayName || auth.currentUser.email || "Me"),
        email: auth.currentUser.email || "",
      }]
      : []);

  users.forEach((user) => {
    const uid = String(user.id || "");
    if (!uid) return;
    seen.add(uid);
    options.push(
      `<option value="${escHtmlAttr(uid)}" ${uid === normalizedSelected ? "selected" : ""}>${escHtml(teamUserLabel(user))}</option>`
    );
  });

  if (normalizedSelected && !seen.has(normalizedSelected)) {
    const fallback = resolveTeamUserName(normalizedSelected, fallbackName);
    options.push(`<option value="${escHtmlAttr(normalizedSelected)}" selected>${escHtml(fallback)}</option>`);
  }

  return options.join("");
}

function assigneeChipHtml(uid, fallbackName = "", emptyLabel = "Unassigned") {
  const label = resolveTeamUserName(uid, fallbackName);
  if (!label) {
    return `<span class="date-text">${escHtml(emptyLabel)}</span>`;
  }
  return `
    <span class="owner-chip">
      <span class="avatar">${escHtml(teamUserInitials(label))}</span>
      ${escHtml(label)}
    </span>
  `;
}

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
      const tagClass = leadServiceTagClass(card.service);
      const ownerChip = (card.ownerUid || card.ownerName)
        ? `<div class="pipeline-owner">${assigneeChipHtml(card.ownerUid, card.ownerName)}</div>`
        : "";

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
        ${ownerChip}
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
  const selectedServiceValue = leadServiceSelectValue(card.service);
  const customServiceValue = selectedServiceValue === "__other__" ? String(card.service || "") : "";
  const ownerOptions = teamAssigneeOptionsHtml(card.ownerUid || "", card.ownerName || "");

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
          ${leadServiceOptionsHtml(card.service)}
        </select>
      </div>
      <div class="form-group" id="ld_serviceOtherWrap" style="${selectedServiceValue === "__other__" ? "" : "display:none"}">
        <label class="form-label">Custom Service</label>
        <input class="form-input" id="ld_serviceOther" value="${escHtmlAttr(customServiceValue)}" placeholder="Type service name">
      </div>
      <div class="form-group">
        <label class="form-label">Status</label>
        <select class="form-select" id="ld_status">${colOpts}</select>
      </div>
      <div class="form-group">
        <label class="form-label">Working Owner</label>
        <select class="form-select" id="ld_ownerUid">
          ${ownerOptions}
        </select>
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

  document.getElementById("ld_service")?.addEventListener("change", () => {
    syncServiceOtherField("ld_service", "ld_serviceOtherWrap", "ld_serviceOther");
  });
  syncServiceOtherField("ld_service", "ld_serviceOtherWrap", "ld_serviceOther");

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
  const service = resolveLeadServiceValue("ld_service", "ld_serviceOther");
  if (!service) {
    alert("Please select a service or provide a custom service label.");
    return;
  }
  const ownerUid = String(document.getElementById("ld_ownerUid").value || "");
  const ownerName = ownerUid ? resolveTeamUserName(ownerUid, "") : "";
  try {
    await updateDoc(doc(db, "pipeline", editingLeadId), {
      title:     document.getElementById("ld_title").value.trim(),
      company:   document.getElementById("ld_company").value.trim(),
      contact:   document.getElementById("ld_contact").value.trim(),
      service,
      ownerUid,
      ownerName,
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
    ? history.map((entry, index) => `
        <div class="billing-entry billing-entry-edit">
          <input class="form-input billing-date-input" id="bh_date_${escHtmlAttr(entry.id || ("legacy_" + index))}" type="date" value="${escHtmlAttr(entry.date || todayStr())}">
          <input class="form-input billing-note-input" id="bh_note_${escHtmlAttr(entry.id || ("legacy_" + index))}" value="${escHtmlAttr(entry.note || "")}" placeholder="Billing note">
          <div class="billing-entry-actions">
            <button class="card-btn" type="button" onclick="saveClientBillingEntry('${escHtmlAttr(entry.id || ("legacy_" + index))}')">Save</button>
            <button class="card-btn delete" type="button" onclick="deleteClientBillingEntry('${escHtmlAttr(entry.id || ("legacy_" + index))}')">✕</button>
          </div>
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

async function upsertClientUserLink(authUid, clientId, email) {
  const uid = String(authUid || "").trim();
  if (!uid) return;
  await setDoc(doc(db, "users", uid), {
    role: "client",
    clientId,
    email: normalizeEmail(email),
    disabled: false,
    updatedAt: serverTimestamp(),
  }, { merge: true });
}

async function upsertClientProvisionRecord(clientId, clientData, options = {}) {
  try {
    const nextEmail = normalizeEmail(clientData?.email);
    const previousEmail = normalizeEmail(options.previousEmail);

    if (previousEmail && previousEmail !== nextEmail) {
      const previousRef = doc(db, "login_provisioning", previousEmail);
      const previousSnap = await getDoc(previousRef);
      if (previousSnap.exists()) {
        const previous = previousSnap.data() || {};
        if (String(previous.role || "") === "client" && String(previous.clientId || "") === clientId) {
          await setDoc(previousRef, {
            status: "disabled",
            updatedAt: serverTimestamp(),
            updatedByUid: auth.currentUser?.uid || "",
          }, { merge: true });
        }
      }
    }

    if (!nextEmail) return false;

    await setDoc(doc(db, "login_provisioning", nextEmail), {
      email: nextEmail,
      role: "client",
      clientId,
      name: String(clientData.contactName || clientData.companyName || ""),
      notes: String(clientData.notes || ""),
      status: "active",
      authUid: String(clientData.authUid || ""),
      updatedAt: serverTimestamp(),
      updatedByUid: auth.currentUser?.uid || "",
      createdAt: serverTimestamp(),
      createdByUid: auth.currentUser?.uid || "",
    }, { merge: true });

    return true;
  } catch (err) {
    if (err?.code === "permission-denied") {
      console.warn("Provisioning sync skipped: super admin required.");
      return false;
    }
    throw err;
  }
}

window.saveClientDetail = async function () {
  if (!selectedClientId) return;

  const existingClient = currentClients.find((c) => c.id === selectedClientId);
  const email = normalizeEmail(document.getElementById("cd_email").value);
  const authUid = document.getElementById("cd_authUid").value.trim();

  try {
    const updates = {
      companyName: document.getElementById("cd_companyName").value.trim(),
      contactName: document.getElementById("cd_contactName").value.trim(),
      email,
      emailLower: email,
      authUid,
      planName: document.getElementById("cd_planName").value.trim(),
      status: document.getElementById("cd_status").value,
      recurring: document.getElementById("cd_recurring").value === "true",
      paid: document.getElementById("cd_paid").value === "true",
      updatesPerMonth: Math.max(0, Number(document.getElementById("cd_updatesPerMonth").value || 0)),
      updatesRemaining: Math.max(0, Number(document.getElementById("cd_updatesRemaining").value || 0)),
      lastPaymentNote: document.getElementById("cd_lastPaymentNote").value.trim(),
      billingNotes: document.getElementById("cd_billingNotes").value.trim(),
      notes: document.getElementById("cd_notes").value.trim(),
      updatedAt: serverTimestamp(),
    };
    await updateDoc(doc(db, "clients", selectedClientId), updates);

    if (authUid) {
      await upsertClientUserLink(authUid, selectedClientId, email);
    }

    const provisionSynced = await upsertClientProvisionRecord(
      selectedClientId,
      { ...updates, authUid },
      { previousEmail: existingClient?.email || "" }
    );

    if (provisionSynced) {
      setElementStatus("managedProvisionStatus", "Client saved and provisioning updated.", false);
    } else {
      setElementStatus("managedProvisionStatus", "Client saved. Provisioning sync requires super admin.", false);
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

window.saveClientBillingEntry = async function (entryId) {
  if (!selectedClientId) return;
  const client = currentClients.find((c) => c.id === selectedClientId);
  if (!client) return;

  const history = Array.isArray(client.billingHistory) ? [...client.billingHistory] : [];
  const index = history.findIndex((entry, i) => String(entry.id || ("legacy_" + i)) === String(entryId));
  if (index < 0) return;

  const dateValue = document.getElementById(`bh_date_${entryId}`)?.value || todayStr();
  const noteValue = document.getElementById(`bh_note_${entryId}`)?.value.trim() || "";
  if (!noteValue) {
    alert("Billing note cannot be empty.");
    return;
  }

  history[index] = {
    id: history[index].id || uid(),
    date: dateValue,
    note: noteValue,
  };

  try {
    await updateDoc(doc(db, "clients", selectedClientId), {
      billingHistory: history,
      updatedAt: serverTimestamp(),
    });
  } catch (err) {
    console.error("saveClientBillingEntry:", err);
    alert("Could not save billing entry.");
  }
};

window.deleteClientBillingEntry = async function (entryId) {
  if (!selectedClientId) return;
  const client = currentClients.find((c) => c.id === selectedClientId);
  if (!client) return;
  if (!confirm("Delete this billing history entry?")) return;

  const history = Array.isArray(client.billingHistory) ? client.billingHistory : [];
  const next = history.filter((entry, index) => String(entry.id || ("legacy_" + index)) !== String(entryId));

  try {
    await updateDoc(doc(db, "clients", selectedClientId), {
      billingHistory: next,
      updatedAt: serverTimestamp(),
    });
  } catch (err) {
    console.error("deleteClientBillingEntry:", err);
    alert("Could not delete billing entry.");
  }
};

function setElementStatus(elementId, message, isError) {
  const el = document.getElementById(elementId);
  if (!el) return;
  el.textContent = message || "";
  el.className = `form-status${isError ? " error" : ""}`;
}

function clientNameById(clientId) {
  if (!clientId) return "Not linked";
  const client = currentClients.find((c) => c.id === clientId);
  return client?.companyName || client?.contactName || clientId;
}

function populateManagedClientOptions(selectedClientId = "", selectId = "managedProvisionClientId") {
  const select = document.getElementById(selectId);
  if (!select) return;
  const currentValue = selectedClientId || select.value || "";
  const options = [`<option value="">Select client</option>`];
  currentClients.forEach((client) => {
    const label = `${client.companyName || "Unnamed Client"} (${clientStatusLabel(client.status || "active")})`;
    options.push(
      `<option value="${escHtmlAttr(client.id)}" ${client.id === currentValue ? "selected" : ""}>${escHtml(label)}</option>`
    );
  });
  select.innerHTML = options.join("");
}

function toggleManagedProvisionClientField() {
  const role = document.getElementById("managedProvisionRole")?.value || "client";
  const wrap = document.getElementById("managedProvisionClientWrap");
  if (!wrap) return;
  wrap.style.display = role === "client" ? "" : "none";
}

function toggleManagedModalClientField() {
  const role = document.getElementById("mu_role")?.value || "client";
  const wrap = document.getElementById("mu_clientWrap");
  if (!wrap) return;
  wrap.style.display = role === "client" ? "" : "none";
}

function managedRoleLabel(role) {
  if (role === "super_admin") return "Super Admin";
  if (role === "admin") return "Admin";
  return "Client";
}

function isTeamManagedRole(role) {
  return role === "admin" || role === "super_admin";
}

function managedStatusLabel(record) {
  const status = String(record.status || "active");
  if (status === "disabled") return "Disabled";
  if (record.uid) return "Linked";
  return "Pending";
}

function deriveManagedRecords() {
  const merged = new Map();

  currentProvisioning.forEach((entry) => {
    const email = normalizeEmail(entry.email || entry.id);
    const role = String(entry.role || "");
    const key = email ? `email:${email}` : `provision:${entry.id}`;
    merged.set(key, {
      key,
      email,
      role,
      name: String(entry.name || ""),
      notes: String(entry.notes || ""),
      clientId: String(entry.clientId || ""),
      status: String(entry.status || "active"),
      provisionId: String(entry.id || ""),
      uid: String(entry.authUid || ""),
    });
  });

  currentManagedUsers.forEach((entry) => {
    const email = normalizeEmail(entry.email || "");
    const role = String(entry.role || "");
    const key = email ? `email:${email}` : `uid:${entry.id}`;
    const existing = merged.get(key);
    if (existing) {
      existing.uid = existing.uid || entry.id;
      existing.role = existing.role || role;
      existing.clientId = existing.clientId || String(entry.clientId || "");
      existing.name = existing.name || String(entry.name || "");
      existing.notes = existing.notes || String(entry.notes || "");
      if (entry.disabled === true) existing.status = "disabled";
      return;
    }

    merged.set(key, {
      key,
      email,
      role,
      name: String(entry.name || ""),
      notes: String(entry.notes || ""),
      clientId: String(entry.clientId || ""),
      status: entry.disabled === true ? "disabled" : "active",
      provisionId: email || "",
      uid: entry.id,
    });
  });

  return Array.from(merged.values())
    .filter((record) => {
      if (!record.role) return false;
      if (managedUsersFilter === "admin") return isTeamManagedRole(record.role);
      return record.role === "client";
    })
    .sort((a, b) => (a.email || "").localeCompare(b.email || ""));
}

function renderManagedUsersTable() {
  const body = document.getElementById("managedUsersBody");
  if (!body || !isSuperAdminRole(currentUserRole)) return;

  const records = deriveManagedRecords();
  managedRecordsByKey = new Map(records.map((record) => [record.key, record]));

  body.innerHTML = "";
  if (!records.length) {
    body.innerHTML = `<tr><td colspan="7"><div class="empty-state" style="margin:18px 0">No users in this view yet.</div></td></tr>`;
    return;
  }

  records.forEach((record) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escHtml(record.email || "No email")}</td>
      <td>${escHtml(record.name || "-")}</td>
      <td>${escHtml(managedRoleLabel(record.role))}</td>
      <td>${escHtml(record.role === "client" ? clientNameById(record.clientId) : "—")}</td>
      <td><span class="date-text">${escHtml(record.uid || "—")}</span></td>
      <td>${escHtml(managedStatusLabel(record))}</td>
    `;

    const actionCell = document.createElement("td");
    const manageBtn = document.createElement("button");
    manageBtn.className = "card-btn";
    manageBtn.type = "button";
    manageBtn.textContent = "Manage";
    manageBtn.addEventListener("click", () => window.openManagedUserModal(record.key));
    actionCell.appendChild(manageBtn);
    tr.appendChild(actionCell);
    body.appendChild(tr);
  });
}

window.setManagedUsersFilter = function (mode) {
  managedUsersFilter = mode === "admin" ? "admin" : "client";
  const clientsTab = document.getElementById("managedTabClients");
  const adminsTab = document.getElementById("managedTabAdmins");
  if (clientsTab) clientsTab.classList.toggle("active", managedUsersFilter === "client");
  if (adminsTab) adminsTab.classList.toggle("active", managedUsersFilter === "admin");
  renderManagedUsersTable();
};

async function upsertManagedProvision(data, options = {}) {
  const email = normalizeEmail(data.email);
  if (!email) throw new Error("Email is required.");

  const role = String(data.role || "");
  if (!["client", "admin", "super_admin"].includes(role)) {
    throw new Error("Role must be client, admin, or super_admin.");
  }

  const clientId = role === "client" ? String(data.clientId || "") : "";
  if (role === "client" && !clientId) {
    throw new Error("Client role requires a linked client record.");
  }

  const previousEmail = normalizeEmail(options.previousEmail);
  if (previousEmail && previousEmail !== email) {
    const previousRef = doc(db, "login_provisioning", previousEmail);
    const previousSnap = await getDoc(previousRef);
    if (previousSnap.exists()) {
      await setDoc(previousRef, {
        status: "disabled",
        updatedAt: serverTimestamp(),
        updatedByUid: auth.currentUser?.uid || "",
      }, { merge: true });
    }
  }

  const status = String(data.status || "active");
  await setDoc(doc(db, "login_provisioning", email), {
    email,
    role,
    name: String(data.name || ""),
    notes: String(data.notes || ""),
    clientId,
    status,
    updatedAt: serverTimestamp(),
    updatedByUid: auth.currentUser?.uid || "",
    createdAt: serverTimestamp(),
    createdByUid: auth.currentUser?.uid || "",
  }, { merge: true });

  const linkedUid =
    String(options.linkedUid || "")
    || String(currentManagedUsers.find((u) => normalizeEmail(u.email || "") === email)?.id || "");

  if (linkedUid) {
    await setDoc(doc(db, "users", linkedUid), {
      role,
      clientId,
      email,
      name: String(data.name || ""),
      notes: String(data.notes || ""),
      disabled: status === "disabled",
      updatedAt: serverTimestamp(),
    }, { merge: true });
  }

  if (role === "client" && clientId) {
    const clientUpdate = {
      email,
      emailLower: email,
      updatedAt: serverTimestamp(),
    };
    const nextAuthUid = linkedUid || String(data.authUid || "");
    if (nextAuthUid) clientUpdate.authUid = nextAuthUid;
    await updateDoc(doc(db, "clients", clientId), clientUpdate);
  }
}

async function handleManagedProvisionSubmit(event) {
  event.preventDefault();
  if (!isSuperAdminRole(currentUserRole)) {
    setElementStatus("managedProvisionStatus", "Only super admins can manage provisioning.", true);
    return;
  }

  const email = normalizeEmail(document.getElementById("managedProvisionEmail")?.value || "");
  const role = document.getElementById("managedProvisionRole")?.value || "client";
  const clientId = document.getElementById("managedProvisionClientId")?.value || "";
  const name = document.getElementById("managedProvisionName")?.value.trim() || "";
  const notes = document.getElementById("managedProvisionNotes")?.value.trim() || "";

  try {
    await upsertManagedProvision({ email, role, clientId, name, notes, status: "active" });
    setElementStatus("managedProvisionStatus", "Provision saved. User can sign in once Auth account exists.", false);
    document.getElementById("managedProvisionForm")?.reset();
    document.getElementById("managedProvisionRole").value = "client";
    toggleManagedProvisionClientField();
    populateManagedClientOptions();
  } catch (err) {
    console.error("handleManagedProvisionSubmit:", err);
    setElementStatus("managedProvisionStatus", err.message || "Could not save provisioning.", true);
  }
}

window.openManagedUserModal = function (recordKey) {
  const record = managedRecordsByKey.get(recordKey);
  if (!record) return;
  editingManagedRecordKey = recordKey;

  const body = document.getElementById("managedUserBody");
  if (!body) return;

  body.innerHTML = `
    <div class="detail-grid">
      <div class="form-group">
        <label class="form-label">Email</label>
        <input class="form-input" id="mu_email" type="email" value="${escHtmlAttr(record.email || "")}">
      </div>
      <div class="form-group">
        <label class="form-label">Name</label>
        <input class="form-input" id="mu_name" value="${escHtmlAttr(record.name || "")}">
      </div>
      <div class="form-group">
        <label class="form-label">Role</label>
        <select class="form-select" id="mu_role">
          <option value="client" ${record.role === "client" ? "selected" : ""}>Client</option>
          <option value="admin" ${record.role === "admin" ? "selected" : ""}>Admin</option>
          <option value="super_admin" ${record.role === "super_admin" ? "selected" : ""}>Super Admin</option>
        </select>
      </div>
      <div class="form-group" id="mu_clientWrap">
        <label class="form-label">Linked Client</label>
        <select class="form-select" id="mu_clientId"></select>
      </div>
      <div class="form-group">
        <label class="form-label">Status</label>
        <select class="form-select" id="mu_status">
          <option value="active" ${record.status === "disabled" ? "" : "selected"}>Active</option>
          <option value="disabled" ${record.status === "disabled" ? "selected" : ""}>Disabled</option>
        </select>
      </div>
      <div class="form-group">
        <label class="form-label">Linked UID</label>
        <div class="detail-meta-value">${escHtml(record.uid || "Not linked yet")}</div>
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Notes</label>
        <textarea class="form-input form-textarea" id="mu_notes" rows="4">${escHtml(record.notes || "")}</textarea>
      </div>
    </div>
  `;

  populateManagedClientOptions(record.clientId, "mu_clientId");
  document.getElementById("mu_role")?.addEventListener("change", toggleManagedModalClientField);
  toggleManagedModalClientField();

  document.getElementById("managedUserOverlay")?.classList.add("open");
};

window.closeManagedUserModal = function () {
  document.getElementById("managedUserOverlay")?.classList.remove("open");
  editingManagedRecordKey = null;
};

window.saveManagedUser = async function () {
  if (!editingManagedRecordKey) return;
  const record = managedRecordsByKey.get(editingManagedRecordKey);
  if (!record) return;

  const email = normalizeEmail(document.getElementById("mu_email")?.value || "");
  const role = document.getElementById("mu_role")?.value || "client";
  const clientId = document.getElementById("mu_clientId")?.value || "";
  const name = document.getElementById("mu_name")?.value.trim() || "";
  const notes = document.getElementById("mu_notes")?.value.trim() || "";
  const status = document.getElementById("mu_status")?.value || "active";

  try {
    await upsertManagedProvision(
      { email, role, clientId, name, notes, status, authUid: record.uid || "" },
      { previousEmail: record.email || "", linkedUid: record.uid || "" }
    );
    setElementStatus("managedUsersStatus", "Provision updated.", false);
    window.closeManagedUserModal();
  } catch (err) {
    console.error("saveManagedUser:", err);
    setElementStatus("managedUsersStatus", err.message || "Could not update provisioning.", true);
  }
};

window.deleteManagedUser = async function () {
  if (!editingManagedRecordKey) return;
  const record = managedRecordsByKey.get(editingManagedRecordKey);
  if (!record) return;

  if (!confirm("Revoke this user's access? This disables provisioning and removes linked profile access.")) return;

  try {
    if (record.uid) {
      await deleteDoc(doc(db, "users", record.uid));
    }

    const provisionId = normalizeEmail(record.provisionId || record.email);
    if (provisionId) {
      await setDoc(doc(db, "login_provisioning", provisionId), {
        email: provisionId,
        role: record.role || "client",
        clientId: record.role === "client" ? String(record.clientId || "") : "",
        name: String(record.name || ""),
        notes: String(record.notes || ""),
        status: "disabled",
        authUid: "",
        updatedAt: serverTimestamp(),
        updatedByUid: auth.currentUser?.uid || "",
      }, { merge: true });
    }

    if (record.role === "client" && record.clientId && record.uid) {
      const clientRef = doc(db, "clients", record.clientId);
      const clientSnap = await getDoc(clientRef);
      if (clientSnap.exists() && String(clientSnap.data().authUid || "") === record.uid) {
        await updateDoc(clientRef, {
          authUid: "",
          updatedAt: serverTimestamp(),
        });
      }
    }

    setElementStatus("managedUsersStatus", "Access revoked.", false);
    window.closeManagedUserModal();
  } catch (err) {
    console.error("deleteManagedUser:", err);
    setElementStatus("managedUsersStatus", "Could not revoke access.", true);
  }
};

window.sendManagedReset = async function () {
  const email = normalizeEmail(document.getElementById("mu_email")?.value || "");
  if (!email) {
    setElementStatus("managedUsersStatus", "Enter a valid email before sending reset.", true);
    return;
  }

  try {
    await sendPasswordResetEmail(auth, email);
    setElementStatus("managedUsersStatus", `Password reset sent to ${email}.`, false);
  } catch (err) {
    console.error("sendManagedReset:", err);
    setElementStatus("managedUsersStatus", "Could not send reset email. Verify Auth account exists.", true);
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

function flowStageLabel(stage) {
  const key = String(stage || "queued");
  const found = CLIENT_FLOW_COLS.find((col) => col.key === key);
  return found ? found.label : "Queued";
}

function flowStageToRequestStatus(stage) {
  if (stage === "delivered") return "done";
  if (stage === "building") return "scheduled";
  return "in_review";
}

function requestTimelineMs(value) {
  if (!value) return 0;
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  if (typeof value === "object" && typeof value.toDate === "function") {
    return value.toDate().getTime();
  }
  if (typeof value === "object" && Number.isFinite(value.seconds)) {
    return Number(value.seconds) * 1000;
  }
  return 0;
}

function formatDateTimeLabel(ms) {
  if (!ms) return "—";
  return new Date(ms).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function timelineKindLabel(kind) {
  if (kind === "flow") return "Pipeline";
  if (kind === "status") return "Status";
  if (kind === "submitted") return "Submitted";
  return "Update";
}

function timelineEntryDisplayText(entry) {
  if (entry.text) return entry.text;
  if (entry.status) return `Status changed to ${requestStatusLabel(entry.status)}.`;
  return "Team update posted.";
}

function normalizeTimelineEntry(entry) {
  if (!entry || typeof entry !== "object") return null;
  return {
    id: String(entry.id || ""),
    kind: String(entry.kind || entry.type || "note"),
    status: String(entry.status || ""),
    text: String(entry.text || entry.note || entry.message || ""),
    visibility: String(entry.visibility || "client"),
    actor: String(entry.actor || entry.author || "Team"),
    createdAtMs: requestTimelineMs(entry.createdAtMs)
      || requestTimelineMs(entry.createdAt)
      || requestTimelineMs(entry.timestamp),
  };
}

function buildRequestTimeline(req, includeInternal = true) {
  const timeline = [];
  const raw = Array.isArray(req.timeline) ? req.timeline : [];
  raw.forEach((entry) => {
    const normalized = normalizeTimelineEntry(entry);
    if (!normalized) return;
    if (!includeInternal && normalized.visibility === "internal") return;
    timeline.push(normalized);
  });

  const submittedMs = requestTimelineMs(req.createdAt) || requestTimelineMs(req.updatedAt) || Date.now();
  if (!timeline.some((entry) => entry.kind === "submitted")) {
    timeline.push({
      id: "submitted_fallback",
      kind: "submitted",
      status: "submitted",
      text: "Request submitted from the client workspace.",
      visibility: "client",
      actor: req.clientName || "Client",
      createdAtMs: submittedMs,
    });
  }

  const currentStatus = String(req.status || "submitted");
  if (!timeline.some((entry) => entry.status === currentStatus)) {
    timeline.push({
      id: "status_fallback",
      kind: "status",
      status: currentStatus,
      text: `Current status: ${requestStatusLabel(currentStatus)}.`,
      visibility: "client",
      actor: "Team",
      createdAtMs: requestTimelineMs(req.updatedAt) || submittedMs,
    });
  }

  timeline.sort((a, b) => a.createdAtMs - b.createdAtMs);
  return timeline;
}

function renderRequestTimelineHtml(req, includeInternal = true) {
  const timeline = buildRequestTimeline(req, includeInternal);
  if (!timeline.length) {
    return `<div class="empty-state" style="padding:14px;font-size:10px">No timeline updates yet.</div>`;
  }

  return `
    <div class="request-timeline-list">
      ${timeline.map((entry) => {
        const statusChip = entry.status
          ? `<span class="req-status ${requestStatusClass(entry.status)}">${requestStatusLabel(entry.status)}</span>`
          : `<span class="timeline-chip">${escHtml(timelineKindLabel(entry.kind))}</span>`;
        return `
          <div class="request-timeline-item">
            <div class="request-timeline-head">
              <div class="request-timeline-head-left">
                ${statusChip}
                <span class="request-timeline-time">${escHtml(formatDateTimeLabel(entry.createdAtMs))}</span>
              </div>
              <span class="timeline-chip ${entry.visibility === "internal" ? "internal" : "client"}">
                ${entry.visibility === "internal" ? "Internal" : "Client Visible"}
              </span>
            </div>
            <div class="request-timeline-text">${escHtml(timelineEntryDisplayText(entry))}</div>
            <div class="request-timeline-meta">${escHtml(entry.actor || "Team")}</div>
          </div>
        `;
      }).join("")}
    </div>
  `;
}

function makeRequestTimelineEntry({ kind = "note", status = "", text = "", visibility = "client" }) {
  return {
    id: uid(),
    kind,
    status: String(status || ""),
    text: String(text || "").slice(0, 2000),
    visibility: visibility === "internal" ? "internal" : "client",
    actor: auth.currentUser?.email || "Team",
    createdAtMs: Date.now(),
  };
}

function hasActiveForgeLaneCard(req) {
  const flowId = String(req?.clientPipelineId || req?.pipelineId || "");
  if (!flowId) return false;
  return currentClientFlow.some((card) => card.id === flowId);
}

function updateArchivedToggleButton() {
  const btn = document.getElementById("toggleArchivedRequestsBtn");
  if (!btn) return;
  btn.textContent = showArchivedRequests ? "Hide Archived" : "Show Archived";
  btn.classList.toggle("active", showArchivedRequests);
}

window.toggleArchivedRequests = function () {
  showArchivedRequests = !showArchivedRequests;
  updateArchivedToggleButton();
  renderClientRequestsAdmin(currentClientRequests);
};

function renderClientRequestsAdmin(requests) {
  const body = document.getElementById("clientRequestsBody");
  if (!body) return;
  body.innerHTML = "";
  const visibleRequests = showArchivedRequests
    ? requests
    : requests.filter((req) => req.archived !== true);

  if (!visibleRequests.length) {
    const message = showArchivedRequests
      ? "No client requests yet."
      : "No active client requests. Use \"Show Archived\" to view history.";
    body.innerHTML = `<tr><td colspan="8"><div class="empty-state" style="margin:24px 0">${message}</div></td></tr>`;
    return;
  }

  visibleRequests.forEach((req) => {
    const tr = document.createElement("tr");
    if (req.archived === true) tr.classList.add("request-row-archived");
    const updatedDate = req.updatedAt?.toDate
      ? req.updatedAt.toDate().toLocaleDateString("en-US", { month: "short", day: "numeric" })
      : (req.createdAt?.toDate
        ? req.createdAt.toDate().toLocaleDateString("en-US", { month: "short", day: "numeric" })
        : "—");
    const ownerCell = assigneeChipHtml(req.ownerUid, req.ownerName);
    const statusPill = req.archived === true
      ? `<span class="req-status rs-archived">Archived</span>`
      : `<span class="req-status ${requestStatusClass(req.status)}">${requestStatusLabel(req.status)}</span>`;
    tr.innerHTML = `
      <td>${escHtml(req.clientName || "Unknown")}</td>
      <td>${escHtml(req.title || "Untitled request")}</td>
      <td>${escHtml(req.category || "general")}</td>
      <td>${escHtml(req.priority || "normal")}</td>
      <td>${ownerCell}</td>
      <td>${statusPill}</td>
      <td><span class="date-text">${updatedDate}</span></td>
      <td><button class="card-btn" onclick="openRequestDetail('${req.id}')">Manage</button></td>
    `;
    body.appendChild(tr);
  });
}

function renderClientFlowBoard(cards) {
  const board = document.getElementById("clientForgeBoard");
  if (!board) return;
  board.innerHTML = "";

  CLIENT_FLOW_COLS.forEach(({ key, label }) => {
    const columnCards = cards.filter((card) => String(card.status || "queued") === key);
    const columnEl = document.createElement("div");
    columnEl.className = "kanban-col client-flow-col";
    columnEl.innerHTML = `
      <div class="col-header">
        <span class="col-title">${label}</span>
        <span class="col-count">${columnCards.length}</span>
      </div>
      <div id="flow-col-${key}" class="kanban-dropzone"></div>
    `;
    board.appendChild(columnEl);

    const container = columnEl.querySelector(`#flow-col-${key}`);
    columnCards.forEach((card) => {
      const moveButtons = CLIENT_FLOW_COLS
        .filter((c) => c.key !== key)
        .map((c) => `<button class="card-btn" onclick="moveClientFlowCard('${card.id}','${c.key}')">${c.label}</button>`)
        .join("");

      const cardEl = document.createElement("div");
      cardEl.className = "card client-flow-card";
      cardEl.innerHTML = `
        <div class="card-title">${escHtml(card.title || "Untitled request")}</div>
        <div class="card-company">${escHtml(card.clientName || "Unknown client")}</div>
        <div class="card-meta">${escHtml(card.category || "general")} · ${escHtml(card.priority || "normal")}</div>
        ${card.description ? `<div class="client-flow-note">${escHtml(card.description).slice(0, 180)}</div>` : ""}
        <div class="card-actions">
          ${moveButtons}
          <button class="card-btn delete" onclick="deleteClientFlowCard('${card.id}')">✕</button>
        </div>
      `;
      container.appendChild(cardEl);
    });
  });
}

window.moveClientFlowCard = async function (id, nextStatus) {
  const card = currentClientFlow.find((item) => item.id === id);
  try {
    await updateDoc(doc(db, "client_flow", id), {
      status: nextStatus,
      updatedAt: serverTimestamp(),
    });

    const requestId = String(card?.requestId || "");
    if (requestId) {
      const mappedStatus = flowStageToRequestStatus(nextStatus);
      const linkedRequest = currentClientRequests.find((req) => req.id === requestId);
      const updates = {
        updatedAt: serverTimestamp(),
        timeline: arrayUnion(
          makeRequestTimelineEntry({
            kind: "flow",
            status: mappedStatus,
            text: `Forge Lane update: moved to ${flowStageLabel(nextStatus)}.`,
            visibility: "client",
          })
        ),
      };

      if (!linkedRequest || String(linkedRequest.status || "") !== mappedStatus) {
        updates.status = mappedStatus;
      }

      try {
        await updateDoc(doc(db, "client_requests", requestId), updates);
      } catch (syncErr) {
        console.error("moveClientFlowCard request sync:", syncErr);
      }
    }
  } catch (err) {
    console.error("moveClientFlowCard:", err);
    alert("Could not move request card.");
  }
};

window.deleteClientFlowCard = async function (id) {
  if (!confirm("Remove this request from Forge Lane?")) return;
  const card = currentClientFlow.find((item) => item.id === id);
  try {
    await deleteDoc(doc(db, "client_flow", id));

    const requestId = String(card?.requestId || "");
    if (requestId) {
      const updates = {
        clientPipelineId: "",
        pipelineId: "",
        updatedAt: serverTimestamp(),
        timeline: arrayUnion(
          makeRequestTimelineEntry({
            kind: "flow",
            text: "Removed from Forge Lane board.",
            visibility: "internal",
          })
        ),
      };
      try {
        await updateDoc(doc(db, "client_requests", requestId), updates);
      } catch (syncErr) {
        console.error("deleteClientFlowCard request sync:", syncErr);
      }
    }
  } catch (err) {
    console.error("deleteClientFlowCard:", err);
    alert("Could not delete request card.");
  }
};

window.openRequestDetail = function (id) {
  const req = currentClientRequests.find((r) => r.id === id);
  if (!req) return;
  editingRequestId = id;
  const ownerOptions = teamAssigneeOptionsHtml(req.ownerUid || "", req.ownerName || "");
  const activeInFlow = hasActiveForgeLaneCard(req);
  document.getElementById("requestDetailBody").innerHTML = `
    <div class="detail-grid">
      ${req.archived === true ? `
      <div class="form-group form-group-full">
        <div class="request-helper">This request is archived. Restore it to make it active again.</div>
      </div>
      ` : ""}
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
      <div class="form-group">
        <label class="form-label">Working Owner</label>
        <select class="form-select" id="rd_ownerUid">
          ${ownerOptions}
        </select>
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Client Update (Visible in Client Panel)</label>
        <textarea class="form-input form-textarea" id="rd_clientUpdate" rows="3" placeholder="Add a short progress update for this client."></textarea>
        <div class="request-helper">Saved notes appear in the client's request timeline.</div>
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Internal Notes</label>
        <textarea class="form-input form-textarea" id="rd_internalNotes" rows="4">${escHtml(req.internalNotes || "")}</textarea>
      </div>
      <div class="form-group form-group-full">
        <label class="form-label">Timeline & Conversation</label>
        ${renderRequestTimelineHtml(req, true)}
      </div>
    </div>
  `;

  const pushBtn = document.getElementById("requestPushBtn");
  if (pushBtn) {
    pushBtn.textContent = req.archived === true
      ? "Archived request"
      : (activeInFlow ? "Already in Forge Lane" : "Push to Forge Lane");
    pushBtn.disabled = Boolean(activeInFlow || req.archived === true);
  }

  const archiveBtn = document.getElementById("requestArchiveBtn");
  if (archiveBtn) {
    archiveBtn.textContent = req.archived === true ? "Restore Request" : "Archive Request";
  }

  document.getElementById("requestDetailOverlay").classList.add("open");
};

window.closeRequestDetail = function () {
  document.getElementById("requestDetailOverlay").classList.remove("open");
  editingRequestId = null;
};

window.saveRequestDetail = async function () {
  if (!editingRequestId) return;
  const req = currentClientRequests.find((item) => item.id === editingRequestId);
  if (!req) return;

  const nextPriority = document.getElementById("rd_priority").value;
  const nextStatus = document.getElementById("rd_status").value;
  const nextOwnerUid = String(document.getElementById("rd_ownerUid").value || "");
  const nextOwnerName = nextOwnerUid ? resolveTeamUserName(nextOwnerUid, "") : "";
  const internalNotes = document.getElementById("rd_internalNotes").value.trim();
  const clientUpdate = document.getElementById("rd_clientUpdate").value.trim();

  const updates = {
    priority: nextPriority,
    status: nextStatus,
    ownerUid: nextOwnerUid,
    ownerName: nextOwnerName,
    internalNotes,
    updatedAt: serverTimestamp(),
  };

  const timelineEntries = [];
  if (nextOwnerUid !== String(req.ownerUid || "")) {
    timelineEntries.push(
      makeRequestTimelineEntry({
        kind: "assignment",
        status: nextStatus,
        text: nextOwnerUid ? `Assigned to ${nextOwnerName}.` : "Request unassigned.",
        visibility: "internal",
      })
    );
  }

  if (nextStatus !== String(req.status || "submitted")) {
    timelineEntries.push(
      makeRequestTimelineEntry({
        kind: "status",
        status: nextStatus,
        text: `Status changed to ${requestStatusLabel(nextStatus)}.`,
        visibility: "client",
      })
    );
  }

  if (clientUpdate) {
    timelineEntries.push(
      makeRequestTimelineEntry({
        kind: "note",
        status: nextStatus,
        text: clientUpdate,
        visibility: "client",
      })
    );
  }

  if (timelineEntries.length) {
    updates.timeline = arrayUnion(...timelineEntries);
  }

  try {
    await updateDoc(doc(db, "client_requests", editingRequestId), updates);
    closeRequestDetail();
  } catch (err) {
    console.error("saveRequestDetail:", err);
    alert("Could not save request.");
  }
};

window.toggleArchiveRequest = async function () {
  if (!editingRequestId) return;
  const req = currentClientRequests.find((item) => item.id === editingRequestId);
  if (!req) return;

  const archiveNext = req.archived !== true;
  const prompt = archiveNext
    ? "Archive this request? It will be hidden from active request lists."
    : "Restore this archived request to active lists?";
  if (!confirm(prompt)) return;

  try {
    await updateDoc(doc(db, "client_requests", editingRequestId), {
      archived: archiveNext,
      archivedAt: archiveNext ? serverTimestamp() : null,
      updatedAt: serverTimestamp(),
      timeline: arrayUnion(
        makeRequestTimelineEntry({
          kind: "archive",
          text: archiveNext ? "Request archived by team." : "Request restored by team.",
          visibility: "internal",
        })
      ),
    });
    closeRequestDetail();
  } catch (err) {
    console.error("toggleArchiveRequest:", err);
    alert("Could not update archive state.");
  }
};

window.deleteRequest = async function () {
  if (!editingRequestId) return;
  if (!confirm("Delete this client request permanently? This cannot be undone.")) return;
  try {
    await deleteDoc(doc(db, "client_requests", editingRequestId));
    closeRequestDetail();
  } catch (err) {
    console.error("deleteRequest:", err);
    alert("Could not delete request.");
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
  const activeInFlow = hasActiveForgeLaneCard(req);
  if (!req || activeInFlow || req.archived === true) return;
  try {
    const flowRef = await addDoc(collection(db, "client_flow"), {
      requestId: req.id,
      clientId: req.clientId || "",
      clientName: req.clientName || "Unknown client",
      clientEmail: req.clientEmail || "",
      title: `${req.clientName || "Client"} — ${req.title || "Request"}`,
      requestTitle: req.title || "Request",
      description: req.description || "",
      category: req.category || "general",
      priority: req.priority || "normal",
      status: "queued",
      source: "client-request-push",
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
    });

    await updateDoc(doc(db, "client_requests", editingRequestId), {
      clientPipelineId: flowRef.id,
      status: "in_review",
      timeline: arrayUnion(
        makeRequestTimelineEntry({
          kind: "flow",
          status: "in_review",
          text: "Moved into Forge Lane queue for production planning.",
          visibility: "client",
        })
      ),
      updatedAt: serverTimestamp(),
    });

    const pushBtn = document.getElementById("requestPushBtn");
    if (pushBtn) {
      pushBtn.textContent = "Already in Forge Lane";
      pushBtn.disabled = true;
    }
  } catch (err) {
    console.error("pushRequestToPipeline:", err);
    alert("Could not push request to Forge Lane.");
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

    const items = Array.isArray(entry.items) ? entry.items : [];
    const itemsHtml = items.length
      ? items.map((item) => `
      <div class="decision-item" data-item-id="${item.id}">
        <div class="decision-item-header">
          <div class="decision-item-text-wrap">
            <div class="decision-item-fields">
              <input class="form-input decision-item-input" id="dec_text_${escHtmlAttr(entry.id)}_${escHtmlAttr(item.id)}" value="${escHtmlAttr(item.text || "")}" placeholder="Decision text">
              <input class="form-input decision-item-owner-input" id="dec_owner_${escHtmlAttr(entry.id)}_${escHtmlAttr(item.id)}" value="${escHtmlAttr(item.owner || "")}" placeholder="Owner / action">
            </div>
          </div>
          <div class="decision-item-actions">
            <button
              class="card-btn"
              onclick="saveDecisionItem('${escHtmlAttr(entry.id)}', '${escHtmlAttr(item.id)}')"
              title="Save this item"
            >Save</button>
            <button
              class="delete-entry-btn item-delete"
              onclick="deleteDecisionItem('${escHtmlAttr(entry.id)}', '${escHtmlAttr(item.id)}')"
              title="Delete this item"
            >✕</button>
          </div>
        </div>
      </div>
    `).join("")
      : `<div class="empty-state" style="padding:14px;font-size:10px">No decision items yet.</div>`;

    el.innerHTML = `
      <div class="decision-date">
        <div class="decision-date-left">
          <span class="decision-title">${escHtml(titleStr)}</span>
          ${dateStr ? `<span class="decision-date-tag">${dateStr}</span>` : ""}
        </div>
        <button class="card-btn" onclick="addDecisionItem('${escHtmlAttr(entry.id)}')">+ Add Item</button>
      </div>
      ${itemsHtml}
    `;
    log.appendChild(el);
  });
}

window.saveDecisionItem = async function (docId, itemId) {
  const entry = currentDecisions.find((d) => d.id === docId);
  if (!entry) return;

  const items = Array.isArray(entry.items) ? [...entry.items] : [];
  const index = items.findIndex((item) => item.id === itemId);
  if (index < 0) return;

  const textInput = document.getElementById(`dec_text_${docId}_${itemId}`);
  const ownerInput = document.getElementById(`dec_owner_${docId}_${itemId}`);
  const textValue = textInput?.value.trim() || "";
  const ownerValue = ownerInput?.value.trim() || "";
  if (!textValue) {
    alert("Decision text cannot be empty.");
    return;
  }

  items[index] = {
    ...items[index],
    text: textValue,
    owner: ownerValue,
  };

  try {
    await updateDoc(doc(db, "decisions", docId), { items });
  } catch (err) {
    console.error("saveDecisionItem:", err);
    alert("Could not save decision item.");
  }
};

window.addDecisionItem = async function (docId) {
  const entry = currentDecisions.find((d) => d.id === docId);
  if (!entry) return;

  const items = Array.isArray(entry.items) ? [...entry.items] : [];
  items.push({ id: uid(), text: "New decision", owner: "" });

  try {
    await updateDoc(doc(db, "decisions", docId), { items });
  } catch (err) {
    console.error("addDecisionItem:", err);
    alert("Could not add decision item.");
  }
};

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

  if (modalMode === "pipeline") {
    document.getElementById("f_service")?.addEventListener("change", () => {
      syncServiceOtherField("f_service", "f_serviceOtherWrap", "f_serviceOther");
    });
    syncServiceOtherField("f_service", "f_serviceOtherWrap", "f_serviceOther");
  }

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
    window.closeManagedUserModal();
  }
});

function getModalForm(mode) {
  if (mode === "pipeline") {
    const colOpts = COLS.map((c) =>
      `<option value="${c.key}" ${c.key === (modalContext || "leads") ? "selected" : ""}>${c.label}</option>`
    ).join("");
    const ownerOptions = teamAssigneeOptionsHtml();
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
          ${leadServiceOptionsHtml("AI Workflow")}
        </select>
      </div>
      <div class="form-group form-group-full" id="f_serviceOtherWrap" style="display:none">
        <label class="form-label">Custom Service</label>
        <input class="form-input" id="f_serviceOther" placeholder="Type service name">
      </div>
      <div class="form-group">
        <label class="form-label">Status</label>
        <select class="form-select" id="f_status">${colOpts}</select>
      </div>
      <div class="form-group">
        <label class="form-label">Working Owner</label>
        <select class="form-select" id="f_ownerUid">
          ${ownerOptions}
        </select>
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
      const service = resolveLeadServiceValue("f_service", "f_serviceOther");
      if (!service) {
        alert("Please select a service or provide a custom service label.");
        return;
      }
      const ownerUid = String(document.getElementById("f_ownerUid").value || "");
      const ownerName = ownerUid ? resolveTeamUserName(ownerUid, "") : "";
      await addDoc(collection(db, "pipeline"), {
        title,
        company:   document.getElementById("f_company").value.trim(),
        contact:   document.getElementById("f_contact").value.trim(),
        service,
        ownerUid,
        ownerName,
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
      const email = normalizeEmail(document.getElementById("f_email").value);
      const updatesPerMonth = Math.max(0, Number(document.getElementById("f_updatesPerMonth").value || 0));
      const updatesRemaining = Math.max(0, Number(document.getElementById("f_updatesRemaining").value || 0));
      const contactName = document.getElementById("f_contactName").value.trim();
      const notes = document.getElementById("f_clientNotes").value.trim();

      const clientRef = await addDoc(collection(db, "clients"), {
        companyName,
        contactName,
        email,
        emailLower: email,
        authUid,
        planName: document.getElementById("f_planName").value.trim(),
        recurring: document.getElementById("f_recurring").value === "true",
        paid: document.getElementById("f_paid").value === "true",
        status: document.getElementById("f_clientStatus").value,
        updatesPerMonth,
        updatesRemaining,
        notes,
        lastPaymentNote: document.getElementById("f_lastPaymentNote").value.trim(),
        billingNotes: document.getElementById("f_billingNotes").value.trim(),
        billingHistory: [],
        createdAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
      });

      selectedClientId = clientRef.id;

      if (authUid) {
        await upsertClientUserLink(authUid, clientRef.id, email);
      }

      const provisionSynced = await upsertClientProvisionRecord(clientRef.id, {
        companyName,
        contactName,
        email,
        authUid,
        notes,
      });
      if (provisionSynced) {
        setElementStatus("managedProvisionStatus", "Client created and provisioning synced.", false);
      } else {
        setElementStatus("managedProvisionStatus", "Client created. Provisioning sync requires super admin.", false);
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

toggleManagedProvisionClientField();
populateManagedClientOptions();
window.setManagedUsersFilter("client");

