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

const TEAM_ASSIGNMENT_UID = "__team__";
const OTHER_ASSIGNMENT_UID = "__other__";
const TEAM_RESET_RETURN_URL = new URL("/reset-password.html?portal=team", window.location.origin).toString();
const CLIENT_RESET_RETURN_URL = new URL("/reset-password.html?portal=client", window.location.origin).toString();

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
let showArchivedMyWorkRequests = false;
let showCompletedMyTasks = false;
let clientsSearchTerm = "";
const LEADS_VISIBLE_LIMIT = 10;
const TASK_STALE_DAYS = 7;
const PIPELINE_STALE_DAYS = 3;
const PIPELINE_NEW_WINDOW_DAYS = 1;
const PIPELINE_FOCUS_FILTERS = ["all", "unassigned", "stale", "high_intent"];
const PIPELINE_HIGH_INTENT_SERVICES = new Set(["AI Workflow", "Website Build", "Automation Ops"]);
const LEAD_LIST_DEFAULT_FILTERS = {
  stage: "open",
  source: "all",
  owner: "any",
  timeframe: "30",
  followUp: "all",
  duplicate: "all",
  sort: "priority",
  limit: 25,
};
let showAllLeads = false;
let teamTimelineExpanded = true;
let collapsedPipelineCols = new Set();
let hasTaskBackfillRun = false;
let isTaskBackfillRunning = false;
let pipelineFocusFilter = "all";
let pipelineRadarNoticeText = "";
let pipelineRadarNoticeError = false;
let pipelineRadarNoticeTimer = null;
let leadListFilters = { ...LEAD_LIST_DEFAULT_FILTERS };
let generatedLeadList = [];
let leadListStatusMessage = "";
let leadListStatusError = false;
let leadListStatusTimer = null;
let currentLeadResearchImports = [];
let leadResearchStatusMessage = "";
let leadResearchStatusError = false;
let leadResearchStatusTimer = null;
const LEAD_RESEARCH_ROW_LIMIT = 150;
const LEAD_RESEARCH_BLOCKED_HEADERS = [
  "ssn",
  "socialsecurity",
  "creditcard",
  "cardnumber",
  "dateofbirth",
  "dob",
  "bankaccount",
  "routingnumber",
  "driverslicense",
  "passportnumber",
];

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
let unsubLeadResearchImports = null;

// ─── MODAL STATE ─────────────────────────────────────────────────────────────
let modalMode    = "pipeline";
let modalContext = "";
let editingLeadId = null;   // ID of the lead currently open in detail modal
let editingTaskId = null;   // ID of the task currently open in detail modal
let editingRequestId = null;
let mergePrimaryLeadId = null;
let selectedClientId = null;
let currentUserRole = null;
let draggingPipelineCardId = null;
let suppressCardClickUntil = 0;
let managedUsersFilter = "client";
let editingManagedRecordKey = null;
let editingDecisionItems = new Set();

// ─── AUTH ─────────────────────────────────────────────────────────────────────
window.doLogin = async function () {
  const email    = document.getElementById("loginEmail").value.trim();
  const password = document.getElementById("loginPassword").value;
  const btn      = document.getElementById("loginBtn");

  setPortalLoginMessage("", true);
  btn.textContent   = "SIGNING IN...";
  btn.disabled      = true;

  try {
    await signInWithEmailAndPassword(auth, email, password);
  } catch (err) {
    setPortalLoginMessage(friendlyAuthError(err.code), true);
    btn.textContent   = "ENTER WORKSPACE";
    btn.disabled      = false;
  }
};

window.sendPortalLoginReset = async function () {
  const email = normalizeEmail(document.getElementById("loginEmail")?.value || "");
  const resetBtn = document.getElementById("loginResetBtn");
  if (!email) {
    setPortalLoginMessage("Enter your email first, then click reset.", true);
    return;
  }

  if (resetBtn) {
    resetBtn.disabled = true;
    resetBtn.textContent = "Sending...";
  }
  try {
    await sendPasswordResetEmail(auth, email, resetActionCodeSettingsForRole("admin"));
    setPortalLoginMessage(`Reset email sent to ${email}. After reset you will return to Team Workspace.`, false);
  } catch (err) {
    console.error("sendPortalLoginReset:", err);
    setPortalLoginMessage(friendlyAuthError(err.code), true);
  } finally {
    if (resetBtn) {
      resetBtn.disabled = false;
      resetBtn.textContent = "Forgot password?";
    }
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
document.getElementById("clientsSearchInput")?.addEventListener("input", function (e) {
  clientsSearchTerm = String(e.target?.value || "").trim().toLowerCase();
  renderClientsWorkspace(currentClients);
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

function canEditDecisions() {
  return isSuperAdminRole(currentUserRole);
}

function applyPortalLoginContextMessage() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("reset") === "success") {
    setPortalLoginMessage("Password reset complete. Sign in to continue.", false);
    params.delete("reset");
    const nextQuery = params.toString();
    const nextUrl = `${window.location.pathname}${nextQuery ? `?${nextQuery}` : ""}${window.location.hash || ""}`;
    window.history.replaceState({}, "", nextUrl);
  }
}

function isTeamAssignmentUid(uid) {
  return String(uid || "") === TEAM_ASSIGNMENT_UID;
}

function isOtherAssignmentUid(uid) {
  return String(uid || "") === OTHER_ASSIGNMENT_UID;
}

function findTeamUserByLooseLabel(label) {
  const needle = String(label || "").trim().toLowerCase();
  if (!needle) return null;
  return currentTeamUsers.find((user) => {
    const uid = String(user.id || "").toLowerCase();
    const name = String(user.name || "").trim().toLowerCase();
    const email = normalizeEmail(user.email);
    return needle === uid || needle === name || needle === email;
  }) || null;
}

function normalizeTaskOwnerFields(task = {}) {
  const rawUid = String(task.ownerUid || "");
  const rawOwnerName = String(task.ownerName || "");
  const rawOwner = String(task.owner || rawOwnerName || "").trim();
  const rawOwnerType = String(task.ownerType || "");

  if (isTeamAssignmentUid(rawUid) || rawOwnerType === "team") {
    return {
      ownerUid: TEAM_ASSIGNMENT_UID,
      ownerName: "Entire Team",
      ownerType: "team",
      owner: "Entire Team",
    };
  }

  if (rawUid) {
    const resolved = resolveTeamUserName(rawUid, rawOwnerName || rawOwner);
    return {
      ownerUid: rawUid,
      ownerName: resolved,
      ownerType: "user",
      owner: resolved,
    };
  }

  const legacyLower = rawOwner.toLowerCase();
  if (!rawOwner || legacyLower === "team" || legacyLower === "entire team" || legacyLower === "all team") {
    return {
      ownerUid: TEAM_ASSIGNMENT_UID,
      ownerName: "Entire Team",
      ownerType: "team",
      owner: "Entire Team",
    };
  }

  const matchedTeamUser = findTeamUserByLooseLabel(rawOwner);
  if (matchedTeamUser) {
    const label = teamUserLabel(matchedTeamUser);
    return {
      ownerUid: String(matchedTeamUser.id || ""),
      ownerName: label,
      ownerType: "user",
      owner: label,
    };
  }

  return {
    ownerUid: "",
    ownerName: rawOwner,
    ownerType: "other",
    owner: rawOwner,
  };
}

function taskOwnerDisplayName(task = {}) {
  const normalized = normalizeTaskOwnerFields(task);
  return normalized.ownerName || normalized.owner || "Entire Team";
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
    currentPipeline = [];
    mergePrimaryLeadId = null;
    currentClientFlow = [];
    currentClientRequests = [];
    currentTeamUsers = [];
    showArchivedRequests = false;
    showArchivedMyWorkRequests = false;
    showCompletedMyTasks = false;
    showAllLeads = false;
    teamTimelineExpanded = true;
    clientsSearchTerm = "";
    hasTaskBackfillRun = false;
    isTaskBackfillRunning = false;
    pipelineFocusFilter = "all";
    pipelineRadarNoticeText = "";
    pipelineRadarNoticeError = false;
    if (pipelineRadarNoticeTimer) {
      clearTimeout(pipelineRadarNoticeTimer);
      pipelineRadarNoticeTimer = null;
    }
    leadListFilters = { ...LEAD_LIST_DEFAULT_FILTERS };
    generatedLeadList = [];
    leadListStatusMessage = "";
    leadListStatusError = false;
    if (leadListStatusTimer) {
      clearTimeout(leadListStatusTimer);
      leadListStatusTimer = null;
    }
    currentLeadResearchImports = [];
    leadResearchStatusMessage = "";
    leadResearchStatusError = false;
    if (leadResearchStatusTimer) {
      clearTimeout(leadResearchStatusTimer);
      leadResearchStatusTimer = null;
    }
    currentManagedUsers = [];
    currentProvisioning = [];
    managedRecordsByKey = new Map();
    editingDecisionItems = new Set();
    stopRealtimeListeners();
    toggleAccessManagerVisibility(false);
    document.getElementById("loginScreen").style.display = "flex";
    document.getElementById("app").classList.remove("visible");
    const btn = document.getElementById("loginBtn");
    if (btn) {
      btn.textContent = "ENTER WORKSPACE";
      btn.disabled = false;
    }
    setPortalLoginMessage("", true);
    applyPortalLoginContextMessage();
    const searchEl = document.getElementById("clientsSearchInput");
    if (searchEl) searchEl.value = "";
    return;
  }

  const userData = await resolvePortalUser(user);
  const role = String(userData?.role || "");
  if (!isTeamRole(role) || userData?.disabled === true) {
    await signOut(auth);
    setPortalLoginMessage("Internal workspace access is for admin/team accounts only.", true);
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

function setPortalLoginMessage(message, isError = true) {
  const errEl = document.getElementById("loginError");
  if (!errEl) return;
  errEl.textContent = String(message || "");
  errEl.classList.toggle("is-success", !isError && Boolean(message));
}

function resetActionCodeSettingsForRole(role) {
  const normalizedRole = String(role || "").toLowerCase();
  const targetUrl = normalizedRole === "client" ? CLIENT_RESET_RETURN_URL : TEAM_RESET_RETURN_URL;
  return {
    url: targetUrl,
    handleCodeInApp: false,
  };
}

function randomProvisionPassword(length = 24) {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$%^&*()-_=+";
  const size = Math.max(12, Number(length || 24));
  const values = new Uint32Array(size);
  crypto.getRandomValues(values);
  let output = "";
  for (let i = 0; i < values.length; i += 1) {
    output += chars[values[i] % chars.length];
  }
  return output;
}

function normalizeIdentityToolkitError(err) {
  const raw = String(err?.message || err || "").trim().toUpperCase();
  if (!raw) return "UNKNOWN";
  if (raw.includes("EMAIL_EXISTS")) return "EMAIL_EXISTS";
  if (raw.includes("INVALID_EMAIL")) return "INVALID_EMAIL";
  if (raw.includes("TOO_MANY_ATTEMPTS_TRY_LATER")) return "TOO_MANY_ATTEMPTS_TRY_LATER";
  if (raw.includes("OPERATION_NOT_ALLOWED")) return "OPERATION_NOT_ALLOWED";
  return raw;
}

function friendlyProvisionAuthError(code) {
  const normalized = String(code || "").toUpperCase();
  const map = {
    EMAIL_EXISTS: "Auth account already exists for this email.",
    INVALID_EMAIL: "Invalid email address.",
    TOO_MANY_ATTEMPTS_TRY_LATER: "Too many attempts. Try again shortly.",
    OPERATION_NOT_ALLOWED: "Email/password auth is not enabled in Firebase.",
  };
  return map[normalized] || "Could not create Auth account automatically.";
}

async function identityToolkitRequest(path, payload) {
  const apiKey = String(firebaseConfig?.apiKey || "").trim();
  if (!apiKey) throw new Error("MISSING_API_KEY");
  const endpoint = `https://identitytoolkit.googleapis.com/v1/${path}?key=${encodeURIComponent(apiKey)}`;
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(String(data?.error?.message || `HTTP_${response.status}`));
  }
  return data || {};
}

function knownManagedUidByEmail(email) {
  const normalized = normalizeEmail(email);
  if (!normalized) return "";
  const linked = currentManagedUsers.find((entry) => normalizeEmail(entry.email || "") === normalized);
  return String(linked?.id || "");
}

async function sendProvisionResetEmail(email, role) {
  await sendPasswordResetEmail(auth, email, resetActionCodeSettingsForRole(role));
}

async function ensureProvisionAuthCredential(email, role, options = {}) {
  const normalizedEmail = normalizeEmail(email);
  const normalizedRole = String(role || "").toLowerCase();
  const existingUid = String(options.existingUid || knownManagedUidByEmail(normalizedEmail));
  const result = {
    uid: existingUid,
    created: false,
    resetSent: false,
    accountExists: Boolean(existingUid),
    requiresManualUid: false,
  };

  if (!normalizedEmail) return result;
  if (result.uid) return result;

  try {
    const tempPassword = randomProvisionPassword();
    const created = await identityToolkitRequest("accounts:signUp", {
      email: normalizedEmail,
      password: tempPassword,
      returnSecureToken: true,
    });
    result.uid = String(created?.localId || "");
    result.created = Boolean(result.uid);
    if (result.created) {
      try {
        await sendProvisionResetEmail(normalizedEmail, normalizedRole);
        result.resetSent = true;
      } catch (resetErr) {
        console.error("sendProvisionResetEmail:", resetErr);
        result.resetSent = false;
      }
      return result;
    }
    throw new Error("NO_UID_RETURNED");
  } catch (err) {
    const code = normalizeIdentityToolkitError(err);
    if (code === "EMAIL_EXISTS") {
      result.accountExists = true;
      const knownUid = knownManagedUidByEmail(normalizedEmail);
      result.uid = knownUid || "";
      result.requiresManualUid = !result.uid;
      return result;
    }
    throw new Error(friendlyProvisionAuthError(code));
  }
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
      renderMyWorkDashboard();
    },
    (err) => console.error("Pipeline error:", err)
  );

  // Fetch all tasks; sort client-side so we can switch sort fields without re-querying
  unsubTasks = onSnapshot(
    query(collection(db, "tasks"), orderBy("createdAt", "asc")),
    (snap) => {
      currentTasks = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      renderTasks(currentTasks);
      renderMyWorkDashboard();
      backfillTaskAssignments(currentTasks);
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
      renderMyWorkDashboard();
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

  unsubLeadResearchImports = onSnapshot(
    collection(db, "lead_research_imports"),
    (snap) => {
      currentLeadResearchImports = snap.docs
        .map((d) => ({ id: d.id, ...d.data() }))
        .sort((a, b) => {
          const ams = requestTimelineMs(a.createdAt) || requestTimelineMs(a.updatedAt);
          const bms = requestTimelineMs(b.createdAt) || requestTimelineMs(b.updatedAt);
          return bms - ams;
        });
      renderLeadListBuilder(currentPipeline);
    },
    (err) => {
      console.error("Lead research imports error:", err);
      currentLeadResearchImports = [];
      setLeadResearchStatus("Could not load lead research queue.", true);
    }
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
      renderPipeline(currentPipeline);
      renderClientRequestsAdmin(currentClientRequests);
      renderMyWorkDashboard();
      backfillTaskAssignments(currentTasks);
    },
    (err) => {
      console.error("Team users listener error:", err);
      currentTeamUsers = [];
      renderMyWorkDashboard();
    }
  );
}

function stopTeamUserListener() {
  if (unsubTeamUsers) unsubTeamUsers();
  unsubTeamUsers = null;
  currentTeamUsers = [];
  renderMyWorkDashboard();
}

function stopRealtimeListeners() {
  if (unsubPipeline) unsubPipeline();
  if (unsubTasks) unsubTasks();
  if (unsubDecisions) unsubDecisions();
  if (unsubClients) unsubClients();
  if (unsubClientRequests) unsubClientRequests();
  if (unsubClientFlow) unsubClientFlow();
  if (unsubLeadResearchImports) unsubLeadResearchImports();
  stopTeamUserListener();
  stopManagedUserListeners();
  unsubPipeline = null;
  unsubTasks = null;
  unsubDecisions = null;
  unsubClients = null;
  unsubClientRequests = null;
  unsubClientFlow = null;
  unsubLeadResearchImports = null;
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
  if (isTeamAssignmentUid(normalizedUid)) return "Entire Team";
  const found = currentTeamUsers.find((user) => user.id === normalizedUid);
  if (found) return teamUserLabel(found);
  return String(fallbackName || normalizedUid.slice(0, 8));
}

function teamAssigneeOptionsHtml(selectedUid = "", fallbackName = "", options = {}) {
  const normalizedSelected = String(selectedUid || "");
  const includeUnassigned = options.includeUnassigned !== false;
  const includeTeam = options.includeTeam !== false;
  const selectOptions = [];
  const seen = new Set();
  if (includeUnassigned) {
    selectOptions.push(`<option value="">Unassigned</option>`);
  }
  if (includeTeam) {
    selectOptions.push(
      `<option value="${TEAM_ASSIGNMENT_UID}" ${normalizedSelected === TEAM_ASSIGNMENT_UID ? "selected" : ""}>Entire Team</option>`
    );
    seen.add(TEAM_ASSIGNMENT_UID);
  }
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
    selectOptions.push(
      `<option value="${escHtmlAttr(uid)}" ${uid === normalizedSelected ? "selected" : ""}>${escHtml(teamUserLabel(user))}</option>`
    );
  });

  if (normalizedSelected && !seen.has(normalizedSelected)) {
    const fallback = resolveTeamUserName(normalizedSelected, fallbackName);
    selectOptions.push(`<option value="${escHtmlAttr(normalizedSelected)}" selected>${escHtml(fallback)}</option>`);
  }

  return selectOptions.join("");
}

function taskOwnerSelectValue(task = {}) {
  const normalized = normalizeTaskOwnerFields(task);
  if (isTeamAssignmentUid(normalized.ownerUid)) return TEAM_ASSIGNMENT_UID;
  if (normalized.ownerUid) return normalized.ownerUid;
  if (normalized.ownerType === "other") return OTHER_ASSIGNMENT_UID;
  return TEAM_ASSIGNMENT_UID;
}

function taskAssigneeOptionsHtml(task = {}) {
  const selected = taskOwnerSelectValue(task);
  const baseOptions = teamAssigneeOptionsHtml(
    selected,
    taskOwnerDisplayName(task),
    { includeUnassigned: false, includeTeam: true }
  );
  return `${baseOptions}<option value="${OTHER_ASSIGNMENT_UID}" ${selected === OTHER_ASSIGNMENT_UID ? "selected" : ""}>Other (custom)</option>`;
}

function syncTaskOwnerOtherField(selectId, wrapId, inputId) {
  const selectEl = document.getElementById(selectId);
  const wrapEl = document.getElementById(wrapId);
  const inputEl = document.getElementById(inputId);
  if (!selectEl || !wrapEl || !inputEl) return;
  const show = selectEl.value === OTHER_ASSIGNMENT_UID;
  wrapEl.style.display = show ? "block" : "none";
  inputEl.required = show;
  if (!show) inputEl.value = "";
}

function resolveTaskOwnerFromInputs(selectId, otherInputId) {
  const selected = String(document.getElementById(selectId)?.value || TEAM_ASSIGNMENT_UID);
  if (selected === TEAM_ASSIGNMENT_UID || !selected) {
    return {
      ownerUid: TEAM_ASSIGNMENT_UID,
      ownerName: "Entire Team",
      ownerType: "team",
      owner: "Entire Team",
    };
  }
  if (selected === OTHER_ASSIGNMENT_UID) {
    const custom = String(document.getElementById(otherInputId)?.value || "").trim();
    if (!custom) {
      return null;
    }
    return {
      ownerUid: "",
      ownerName: custom,
      ownerType: "other",
      owner: custom,
    };
  }
  const resolved = resolveTeamUserName(selected, "");
  return {
    ownerUid: selected,
    ownerName: resolved,
    ownerType: "user",
    owner: resolved,
  };
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

function pipelineStatusLabel(status) {
  const found = COLS.find((col) => col.key === String(status || ""));
  return found ? found.label : "Lead";
}

function pipelineLeadUpdatedMs(card) {
  return requestTimelineMs(card?.updatedAt) || requestTimelineMs(card?.createdAt);
}

function isPipelineLeadClosed(card) {
  return String(card?.status || "") === "closed";
}

function isPipelineLeadUnassigned(card) {
  const ownerUid = String(card?.ownerUid || "");
  const ownerName = String(card?.ownerName || "").trim().toLowerCase();
  if (isTeamAssignmentUid(ownerUid)) return true;
  if (!ownerUid && !ownerName) return true;
  if (!ownerUid && (ownerName === "team" || ownerName === "entire team" || ownerName === "all team")) return true;
  return false;
}

function isPipelineLeadStale(card) {
  if (isPipelineLeadClosed(card)) return false;
  const updatedMs = pipelineLeadUpdatedMs(card);
  if (!updatedMs) return false;
  const staleCutoffMs = Date.now() - (PIPELINE_STALE_DAYS * 24 * 60 * 60 * 1000);
  return updatedMs < staleCutoffMs;
}

function isPipelineLeadNew(card) {
  if (isPipelineLeadClosed(card)) return false;
  const createdMs = requestTimelineMs(card?.createdAt) || pipelineLeadUpdatedMs(card);
  if (!createdMs) return false;
  const freshCutoffMs = Date.now() - (PIPELINE_NEW_WINDOW_DAYS * 24 * 60 * 60 * 1000);
  return createdMs >= freshCutoffMs;
}

function isPipelineLeadHighIntent(card) {
  if (isPipelineLeadClosed(card)) return false;
  const status = String(card?.status || "");
  if (status === "contacted" || status === "proposal") return true;
  const service = String(card?.service || "").trim();
  return PIPELINE_HIGH_INTENT_SERVICES.has(service);
}

function pipelineLeadMatchesFocus(card, focus = pipelineFocusFilter) {
  const normalizedFocus = String(focus || "all");
  if (normalizedFocus === "unassigned") return !isPipelineLeadClosed(card) && isPipelineLeadUnassigned(card);
  if (normalizedFocus === "stale") return isPipelineLeadStale(card);
  if (normalizedFocus === "high_intent") return isPipelineLeadHighIntent(card);
  return true;
}

function oldestUnassignedPipelineLead(cards) {
  return (Array.isArray(cards) ? cards : [])
    .filter((card) => !isPipelineLeadClosed(card) && isPipelineLeadUnassigned(card))
    .sort((a, b) => {
      const ams = requestTimelineMs(a.createdAt) || pipelineLeadUpdatedMs(a);
      const bms = requestTimelineMs(b.createdAt) || pipelineLeadUpdatedMs(b);
      return ams - bms;
    })[0] || null;
}

function pipelineFocusLabel(focus) {
  if (focus === "unassigned") return "Unassigned";
  if (focus === "stale") return `Stale (${PIPELINE_STALE_DAYS}+ days)`;
  if (focus === "high_intent") return "High Intent";
  return "All Leads";
}

function setPipelineRadarNotice(message, isError = false) {
  pipelineRadarNoticeText = String(message || "").trim();
  pipelineRadarNoticeError = Boolean(isError);
  if (pipelineRadarNoticeTimer) {
    clearTimeout(pipelineRadarNoticeTimer);
    pipelineRadarNoticeTimer = null;
  }
  if (pipelineRadarNoticeText) {
    pipelineRadarNoticeTimer = setTimeout(() => {
      pipelineRadarNoticeText = "";
      pipelineRadarNoticeError = false;
      pipelineRadarNoticeTimer = null;
      renderPipeline(currentPipeline);
    }, 4500);
  }
}

function setLeadListStatus(message, isError = false) {
  leadListStatusMessage = String(message || "").trim();
  leadListStatusError = Boolean(isError);
  if (leadListStatusTimer) {
    clearTimeout(leadListStatusTimer);
    leadListStatusTimer = null;
  }
  if (leadListStatusMessage) {
    leadListStatusTimer = setTimeout(() => {
      leadListStatusMessage = "";
      leadListStatusError = false;
      leadListStatusTimer = null;
      renderLeadListBuilder(currentPipeline);
    }, 4200);
  }
  renderLeadListBuilder(currentPipeline);
}

function setLeadResearchStatus(message, isError = false) {
  leadResearchStatusMessage = String(message || "").trim();
  leadResearchStatusError = Boolean(isError);
  if (leadResearchStatusTimer) {
    clearTimeout(leadResearchStatusTimer);
    leadResearchStatusTimer = null;
  }
  if (leadResearchStatusMessage) {
    leadResearchStatusTimer = setTimeout(() => {
      leadResearchStatusMessage = "";
      leadResearchStatusError = false;
      leadResearchStatusTimer = null;
      renderLeadListBuilder(currentPipeline);
    }, 4600);
  }
  renderLeadListBuilder(currentPipeline);
}

function textHasAnyKeywords(text, keywords = []) {
  const hay = String(text || "").toLowerCase();
  if (!hay) return false;
  return keywords.some((keyword) => hay.includes(String(keyword || "").toLowerCase()));
}

function normalizeSalesBriefService(service, text) {
  const direct = String(service || "").trim();
  if (direct && direct.toLowerCase() !== "other") return direct;

  if (textHasAnyKeywords(text, ["chatbot", "assistant", "gpt", "ai", "llm"])) {
    return "AI Workflow";
  }
  if (textHasAnyKeywords(text, ["automation", "workflow", "zapier", "integration", "crm", "manual process"])) {
    return "Automation Ops";
  }
  if (textHasAnyKeywords(text, ["website", "landing", "redesign", "seo", "wordpress", "shopify"])) {
    return "Website Build";
  }
  if (textHasAnyKeywords(text, ["ads", "ppc", "google ads", "meta ads"])) {
    return "Paid Ads";
  }
  if (textHasAnyKeywords(text, ["content", "blog", "copywriting", "social"])) {
    return "SEO / Content";
  }
  return "Website Build";
}

function salesBriefNeedsByService(service) {
  const map = {
    "AI Workflow": [
      "AI assistant use-case mapping",
      "workflow automation blueprint",
      "handoff documentation and owner training",
    ],
    "Website Build": [
      "conversion-focused page structure",
      "faster mobile experience",
      "lead capture and follow-up hooks",
    ],
    "Automation Ops": [
      "manual-step audit",
      "workflow automations with fail-safes",
      "operations visibility dashboard",
    ],
    "SEO / Content": [
      "high-intent content plan",
      "technical SEO baseline cleanup",
      "monthly ranking and lead reporting",
    ],
    "Paid Ads": [
      "campaign tracking setup",
      "landing page and offer alignment",
      "budget pacing and lead quality monitoring",
    ],
    "Brand / Creative": [
      "brand positioning refresh",
      "conversion-aligned messaging",
      "creative consistency across channels",
    ],
  };
  return map[service] || [
    "discovery call and current-state audit",
    "service-fit recommendation",
    "implementation plan with clear handoff",
  ];
}

function salesBriefOpportunityClass(tier) {
  if (tier === "high") return "p-high";
  if (tier === "mid") return "p-mid";
  return "p-low";
}

function salesBriefOpportunityLabel(tier) {
  if (tier === "high") return "High";
  if (tier === "mid") return "Mid";
  return "Low";
}

function buildLeadSalesBrief(card = {}) {
  const title = String(card?.title || "").trim();
  const company = String(card?.company || "").trim();
  const contact = String(card?.contact || "").trim();
  const note = String(card?.note || "").trim();
  const source = String(card?.source || "").trim().toLowerCase();
  const status = String(card?.status || "leads").trim().toLowerCase();
  const combinedText = `${title}\n${company}\n${contact}\n${note}`;
  const recommendedService = normalizeSalesBriefService(card?.service, combinedText);
  const likelyNeeds = salesBriefNeedsByService(recommendedService);

  let score = 18;
  if (source.includes("website")) score += 16;
  if (status === "contacted") score += 12;
  if (status === "proposal") score += 18;
  if (textHasAnyKeywords(combinedText, ["urgent", "asap", "immediately", "this week"])) score += 22;
  if (textHasAnyKeywords(combinedText, ["quote", "budget", "pricing", "proposal"])) score += 12;
  if (textHasAnyKeywords(combinedText, ["manual", "bottleneck", "slow", "messy process"])) score += 10;
  if (company) score += 5;
  if (findEmailInText(contact) || findEmailInText(note)) score += 7;
  if (/\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b/.test(`${contact} ${note}`)) score += 5;
  if (note.length >= 140) score += 5;
  score = Math.min(100, Math.max(0, score));

  const opportunityTier = score >= 68 ? "high" : score >= 42 ? "mid" : "low";
  const outreachAngle = opportunityTier === "high"
    ? "Reach out same day with two clear options and propose a 20-minute scoping call."
    : opportunityTier === "mid"
      ? "Send a focused discovery checklist and a short call booking link."
      : "Lead with a quick audit offer and add to nurture follow-up if no reply.";
  const summaryEntity = company || title || "this lead";
  const summary = `${summaryEntity} is a fit for ${recommendedService}. Prioritize ${likelyNeeds[0]}.`;

  return {
    summary,
    recommendedService,
    likelyNeeds,
    opportunityTier,
    opportunityLabel: salesBriefOpportunityLabel(opportunityTier),
    opportunityClass: salesBriefOpportunityClass(opportunityTier),
    outreachAngle,
    score,
  };
}

function formatSalesBriefText(card, brief) {
  const entity = String(card?.company || card?.title || "Lead").trim();
  return [
    `Sales Brief: ${entity}`,
    `Recommended Service: ${brief.recommendedService}`,
    `Opportunity: ${brief.opportunityLabel} (${brief.score}/100)`,
    `Likely Needs: ${brief.likelyNeeds.join("; ")}`,
    `Outreach Angle: ${brief.outreachAngle}`,
    `Summary: ${brief.summary}`,
  ].join("\n");
}

function normalizeLeadResearchStatus(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "approved") return "approved";
  if (normalized === "rejected") return "rejected";
  if (normalized === "imported") return "imported";
  return "pending";
}

function leadResearchStatusLabel(status) {
  const normalized = normalizeLeadResearchStatus(status);
  if (normalized === "approved") return "Approved";
  if (normalized === "rejected") return "Rejected";
  if (normalized === "imported") return "Imported";
  return "Pending Review";
}

function leadResearchStatusClass(status) {
  return `research-status-${normalizeLeadResearchStatus(status)}`;
}

function toAbsoluteWebsiteUrl(rawValue) {
  const value = String(rawValue || "").trim();
  if (!value) return "";
  if (value.toLowerCase() === "n/a" || value.toLowerCase() === "na" || value.toLowerCase() === "none") return "";
  if (/^https?:\/\//i.test(value)) return value;
  if (!value.includes(".")) return "";
  return `https://${value.replace(/^\/+/, "")}`;
}

function normalizeCsvHeader(value) {
  return String(value || "")
    .replace(/^\uFEFF/, "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function parseCsvText(text) {
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === "\"") {
        if (text[i + 1] === "\"") {
          cell += "\"";
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        cell += ch;
      }
      continue;
    }
    if (ch === "\"") {
      inQuotes = true;
      continue;
    }
    if (ch === ",") {
      row.push(cell);
      cell = "";
      continue;
    }
    if (ch === "\n") {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
      continue;
    }
    if (ch === "\r") continue;
    cell += ch;
  }

  if (cell.length || row.length) {
    row.push(cell);
    rows.push(row);
  }

  const cleaned = rows
    .map((cells) => cells.map((v) => String(v || "").trim()))
    .filter((cells) => cells.some((v) => v.length));
  if (!cleaned.length) {
    return { headers: [], records: [] };
  }

  const rawHeaders = cleaned[0];
  const headers = rawHeaders.map((value, idx) => normalizeCsvHeader(value) || `col${idx + 1}`);
  const records = cleaned.slice(1).map((cells) => {
    const entry = {};
    headers.forEach((header, index) => {
      entry[header] = String(cells[index] || "").trim();
    });
    return entry;
  });
  return { headers, records };
}

function pickCsvValue(entry, keys = []) {
  for (const key of keys) {
    const normalized = normalizeCsvHeader(key);
    const value = String(entry?.[normalized] || "").trim();
    if (value) return value;
  }
  return "";
}

function coerceConfidenceValue(rawValue) {
  const raw = String(rawValue || "").trim().replace("%", "");
  const numeric = Number(raw);
  if (!Number.isFinite(numeric)) return 50;
  return Math.max(0, Math.min(100, Math.round(numeric)));
}

function sourceKeyFromLabelOrUrl(label, url) {
  const normalizedLabel = String(label || "").trim().toLowerCase();
  const normalizedUrl = String(url || "").trim().toLowerCase();
  let base = normalizedLabel;

  if (!base && normalizedUrl) {
    try {
      const parsed = new URL(normalizedUrl);
      base = parsed.hostname || "";
    } catch (_) {
      base = normalizedUrl;
    }
  }

  if (!base) return "public-directory";
  return base
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/.*$/, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40) || "public-directory";
}

function sourceLabelFromUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  try {
    const parsed = new URL(raw);
    return String(parsed.hostname || "")
      .replace(/^www\./i, "")
      .trim();
  } catch (_) {
    return "";
  }
}

function leadResearchSourceMeta(record = {}) {
  const sourceUrl = toAbsoluteWebsiteUrl(
    record.publicSourceUrl
    || record.sourceUrl
    || record.researchSourceUrl
    || ""
  );
  const sourceLabel = String(
    record.publicSourceLabel
    || record.sourceLabel
    || record.researchSourceLabel
    || sourceLabelFromUrl(sourceUrl)
    || "Public directory"
  ).trim();
  const sourceKey = String(
    record.researchSourceKey
    || sourceKeyFromLabelOrUrl(sourceLabel, sourceUrl)
  ).trim() || "public-directory";
  return { sourceKey, sourceLabel, sourceUrl };
}

function pipelineResearchSourceMeta(card = {}) {
  const sourceRaw = String(card?.source || "").toLowerCase();
  const hasResearchSource = sourceRaw.includes("research");
  if (!hasResearchSource && !card?.researchSourceKey && !card?.researchSourceLabel && !card?.researchSourceUrl) {
    return null;
  }

  let sourceLabel = String(card?.researchSourceLabel || "").trim();
  let sourceUrl = toAbsoluteWebsiteUrl(card?.researchSourceUrl || "");
  if (!sourceLabel || !sourceUrl) {
    const note = String(card?.note || "");
    const sourceMatch = note.match(/Research import source:\s*([^\n(]+?)(?:\s*\((https?:\/\/[^)]+)\))?(?:\n|$)/i);
    if (sourceMatch) {
      if (!sourceLabel) sourceLabel = String(sourceMatch[1] || "").trim();
      if (!sourceUrl) sourceUrl = toAbsoluteWebsiteUrl(sourceMatch[2] || "");
    }
  }

  if (!sourceLabel) sourceLabel = sourceLabelFromUrl(sourceUrl) || "Research import";
  const sourceKey = String(card?.researchSourceKey || sourceKeyFromLabelOrUrl(sourceLabel, sourceUrl)).trim() || "research-import";
  return { sourceKey, sourceLabel, sourceUrl };
}

function leadOutcomeKey(card = {}) {
  const raw = String(card?.outcome || "").trim().toLowerCase();
  if (raw === "won" || raw === "lost") return raw;
  if (String(card?.status || "").trim().toLowerCase() === "closed") return "closed_unspecified";
  return "open";
}

function leadOutcomeLabel(outcomeKey) {
  if (outcomeKey === "won") return "Won";
  if (outcomeKey === "lost") return "Lost";
  if (outcomeKey === "closed_unspecified") return "Closed (Needs Outcome)";
  return "Open";
}

function leadResearchCompanyKey(record = {}) {
  return normalizeLeadEntityKey(record.company || record.title || "");
}

function leadResearchEmailKey(record = {}) {
  return normalizeEmail(record.email || "");
}

function leadSourceKey(card) {
  const source = String(card?.source || "").trim().toLowerCase();
  if (!source) return "manual";
  if (source === "website-contact-form") return "website";
  if (source.includes("website")) return "website";
  if (source.includes("research")) return "research";
  if (card?.researchSourceKey || card?.researchSourceLabel || card?.researchSourceUrl) return "research";
  return "manual";
}

function leadSourceLabel(card) {
  const source = String(card?.source || "").trim();
  if (!source) return "Manual";
  if (source === "website-contact-form") return "Website Form";
  if (leadSourceKey(card) === "research") {
    const meta = pipelineResearchSourceMeta(card) || leadResearchSourceMeta(card);
    return meta?.sourceLabel ? `Research · ${meta.sourceLabel}` : "Research Import";
  }
  return source.replace(/[-_]+/g, " ");
}

function findEmailInText(value) {
  const match = String(value || "").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return match ? String(match[0]).toLowerCase() : "";
}

function leadPrimaryEmail(card) {
  const contact = String(card?.contact || "").trim();
  const fromContact = findEmailInText(contact);
  if (fromContact) return fromContact;
  return findEmailInText(card?.note || "");
}

function leadOwnerLabel(card) {
  return resolveTeamUserName(card?.ownerUid, card?.ownerName) || "Unassigned";
}

function normalizeLeadEntityKey(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "")
    .trim();
}

function startOfLocalDayMs(ms = Date.now()) {
  const d = new Date(ms);
  d.setHours(0, 0, 0, 0);
  return d.getTime();
}

function endOfLocalDayMs(ms = Date.now()) {
  const d = new Date(ms);
  d.setHours(23, 59, 59, 999);
  return d.getTime();
}

function leadNextFollowUpMs(card) {
  if (isPipelineLeadClosed(card)) return 0;
  const baseMs = pipelineLeadUpdatedMs(card) || requestTimelineMs(card?.createdAt);
  if (!baseMs) return 0;
  const status = String(card?.status || "leads");
  const daysByStatus = {
    leads: 1,
    contacted: 2,
    proposal: 3,
  };
  const waitDays = daysByStatus[status] ?? 2;
  return baseMs + (waitDays * 24 * 60 * 60 * 1000);
}

function followUpBucketForMs(nextFollowUpMs) {
  if (!nextFollowUpMs) return "none";
  const startToday = startOfLocalDayMs();
  const endToday = endOfLocalDayMs();
  const endNext7 = endOfLocalDayMs(Date.now() + (6 * 24 * 60 * 60 * 1000));
  if (nextFollowUpMs < startToday) return "overdue";
  if (nextFollowUpMs <= endToday) return "due_today";
  if (nextFollowUpMs <= endNext7) return "next_7";
  return "later";
}

function followUpLabelForBucket(bucket, nextFollowUpMs) {
  if (bucket === "overdue") return "Overdue";
  if (bucket === "due_today") return "Today";
  if (bucket === "next_7") return "Next 7d";
  if (bucket === "later") return "Later";
  return nextFollowUpMs ? formatDateTimeLabel(nextFollowUpMs) : "—";
}

function followUpClassForBucket(bucket) {
  if (bucket === "overdue") return "lead-followup-overdue";
  if (bucket === "due_today") return "lead-followup-today";
  if (bucket === "next_7") return "lead-followup-next";
  return "lead-followup-later";
}

function mergeLeadNoteText(primaryNote, secondaryNote, secondaryId) {
  const left = String(primaryNote || "").trim();
  const right = String(secondaryNote || "").trim();
  if (!right) return left;
  if (left && left.includes(right)) return left;
  const stamp = formatDateTimeLabel(Date.now());
  const block = `[Merged from lead #${String(secondaryId || "").slice(0, 6)} on ${stamp}]\n${right}`;
  if (!left) return block;
  return `${left}\n\n${block}`;
}

function mergeArchivedNoteText(existingNote, primaryId, primaryTitle) {
  const left = String(existingNote || "").trim();
  const stamp = formatDateTimeLabel(Date.now());
  const block = `Merged into lead #${String(primaryId || "").slice(0, 6)} (${String(primaryTitle || "Primary Lead").trim()}) on ${stamp}.`;
  if (!left) return block;
  if (left.includes(block)) return left;
  return `${left}\n\n${block}`;
}

function leadFollowUpPriorityScore(card) {
  if (isPipelineLeadClosed(card)) return 0;
  let score = 0;
  const followUpBucket = followUpBucketForMs(leadNextFollowUpMs(card));
  if (followUpBucket === "overdue") score += 25;
  if (followUpBucket === "due_today") score += 15;
  if (isPipelineLeadStale(card)) score += 35;
  if (isPipelineLeadUnassigned(card)) score += 25;
  if (isPipelineLeadHighIntent(card)) score += 20;
  if (isPipelineLeadNew(card)) score += 10;
  if (String(card?.status || "") === "proposal") score += 10;
  if (String(card?.status || "") === "contacted") score += 5;
  return Math.min(score, 100);
}

function leadPriorityClass(score) {
  if (score >= 70) return "p-high";
  if (score >= 40) return "p-mid";
  return "p-low";
}

function leadPriorityLabel(score) {
  if (score >= 70) return "High";
  if (score >= 40) return "Mid";
  return "Low";
}

function buildGeneratedLeadList(cards) {
  const allCards = Array.isArray(cards) ? cards : [];
  const currentUid = String(auth.currentUser?.uid || "");
  const stageFilter = String(leadListFilters.stage || "open");
  const sourceFilter = String(leadListFilters.source || "all");
  const ownerFilter = String(leadListFilters.owner || "any");
  const timeframeDays = Number(leadListFilters.timeframe || 0);
  const followUpFilter = String(leadListFilters.followUp || "all");
  const duplicateFilter = String(leadListFilters.duplicate || "all");
  const sortFilter = String(leadListFilters.sort || "priority");
  const limit = Math.max(1, Math.min(200, Number(leadListFilters.limit || LEAD_LIST_DEFAULT_FILTERS.limit)));
  const cutoffMs = timeframeDays > 0
    ? Date.now() - (timeframeDays * 24 * 60 * 60 * 1000)
    : 0;
  const emailGroups = new Map();
  const companyGroups = new Map();

  allCards.forEach((card) => {
    if (isPipelineLeadClosed(card)) return;
    const cardId = String(card?.id || "");
    if (!cardId) return;

    const email = leadPrimaryEmail(card);
    if (email) {
      if (!emailGroups.has(email)) emailGroups.set(email, []);
      emailGroups.get(email).push(cardId);
    }

    const companyKey = normalizeLeadEntityKey(card?.company || card?.title || "");
    if (companyKey.length >= 5) {
      if (!companyGroups.has(companyKey)) companyGroups.set(companyKey, []);
      companyGroups.get(companyKey).push(cardId);
    }
  });

  const filtered = allCards.filter((card) => {
    const cardId = String(card?.id || "");
    const status = String(card?.status || "");
    if (stageFilter === "open" && status === "closed") return false;
    if (stageFilter !== "open" && stageFilter !== "all" && status !== stageFilter) return false;

    const sourceKey = leadSourceKey(card);
    if (sourceFilter === "website" && sourceKey !== "website") return false;
    if (sourceFilter === "research" && sourceKey !== "research") return false;
    if (sourceFilter === "manual" && sourceKey !== "manual") return false;

    const ownerUid = String(card?.ownerUid || "");
    if (ownerFilter === "mine" && ownerUid !== currentUid) return false;
    if (ownerFilter === "unassigned" && !isPipelineLeadUnassigned(card)) return false;
    if (ownerFilter === "team" && !isTeamAssignmentUid(ownerUid)) return false;

    if (cutoffMs) {
      const createdMs = requestTimelineMs(card?.createdAt) || pipelineLeadUpdatedMs(card);
      if (!createdMs || createdMs < cutoffMs) return false;
    }

    const nextFollowUpMs = leadNextFollowUpMs(card);
    const followUpBucket = followUpBucketForMs(nextFollowUpMs);
    if (followUpFilter === "call_now" && followUpBucket !== "overdue" && followUpBucket !== "due_today") return false;
    if (followUpFilter === "overdue" && followUpBucket !== "overdue") return false;
    if (followUpFilter === "due_today" && followUpBucket !== "due_today") return false;
    if (followUpFilter === "next_7" && followUpBucket !== "due_today" && followUpBucket !== "next_7") return false;

    const email = leadPrimaryEmail(card);
    const companyKey = normalizeLeadEntityKey(card?.company || card?.title || "");
    const emailDupCount = email ? (emailGroups.get(email)?.length || 0) : 0;
    const companyDupCount = companyKey ? (companyGroups.get(companyKey)?.length || 0) : 0;
    const hasDuplicate = emailDupCount > 1 || companyDupCount > 1;
    if (duplicateFilter === "duplicates" && !hasDuplicate) return false;
    if (duplicateFilter === "unique" && hasDuplicate) return false;

    if (!cardId) return false;
    return true;
  });

  const list = filtered.map((card) => {
    const cardId = String(card?.id || "");
    const email = leadPrimaryEmail(card);
    const companyKey = normalizeLeadEntityKey(card?.company || card?.title || "");
    const emailGroup = email ? (emailGroups.get(email) || []) : [];
    const companyGroup = companyKey ? (companyGroups.get(companyKey) || []) : [];
    const relatedMatches = [...new Set([...emailGroup, ...companyGroup])]
      .filter((candidateId) => candidateId !== cardId);
    const duplicateType = emailGroup.length > 1
      ? "email"
      : (companyGroup.length > 1 ? "company" : "");
    const duplicateCount = relatedMatches.length ? relatedMatches.length + 1 : 0;
    const duplicateHint = relatedMatches.length
      ? `${duplicateType === "email" ? "Email match" : "Company match"}: ${relatedMatches.slice(0, 3).map((id) => `#${id.slice(0, 6)}`).join(", ")}${relatedMatches.length > 3 ? " ..." : ""}`
      : "";
    const nextFollowUpMs = leadNextFollowUpMs(card);
    const followUpBucket = followUpBucketForMs(nextFollowUpMs);
    const createdMs = requestTimelineMs(card?.createdAt) || pipelineLeadUpdatedMs(card);
    const updatedMs = pipelineLeadUpdatedMs(card);
    const priorityScore = leadFollowUpPriorityScore(card);
    const sourceKey = leadSourceKey(card);
    const outcomeKey = leadOutcomeKey(card);
    const ownerLabel = leadOwnerLabel(card);
    return {
      id: cardId,
      title: String(card?.title || "Untitled lead"),
      company: String(card?.company || ""),
      contactLabel: String(card?.contact || "").trim() || "—",
      email,
      stage: String(card?.status || "leads"),
      stageLabel: pipelineStatusLabel(card?.status),
      outcomeKey,
      outcomeLabel: leadOutcomeLabel(outcomeKey),
      sourceKey,
      sourceLabel: leadSourceLabel(card),
      ownerUid: String(card?.ownerUid || ""),
      ownerName: String(card?.ownerName || ""),
      ownerLabel,
      unassigned: isPipelineLeadUnassigned(card),
      priorityScore,
      priorityClass: leadPriorityClass(priorityScore),
      priorityLabel: leadPriorityLabel(priorityScore),
      nextFollowUpMs,
      followUpBucket,
      followUpLabel: followUpLabelForBucket(followUpBucket, nextFollowUpMs),
      followUpClass: followUpClassForBucket(followUpBucket),
      duplicateCount,
      duplicateType,
      duplicateHint,
      mergeMatches: relatedMatches,
      createdMs,
      updatedMs,
    };
  });

  list.sort((a, b) => {
    if (sortFilter === "oldest") return a.createdMs - b.createdMs;
    if (sortFilter === "newest") return b.createdMs - a.createdMs;
    if (sortFilter === "updated") return b.updatedMs - a.updatedMs;
    if (sortFilter === "followup") {
      const bucketWeight = {
        overdue: 0,
        due_today: 1,
        next_7: 2,
        later: 3,
        none: 4,
      };
      const aw = bucketWeight[a.followUpBucket] ?? 5;
      const bw = bucketWeight[b.followUpBucket] ?? 5;
      if (aw !== bw) return aw - bw;
      const aFollowUp = a.nextFollowUpMs || Number.MAX_SAFE_INTEGER;
      const bFollowUp = b.nextFollowUpMs || Number.MAX_SAFE_INTEGER;
      if (aFollowUp !== bFollowUp) return aFollowUp - bFollowUp;
    }
    if (b.priorityScore !== a.priorityScore) return b.priorityScore - a.priorityScore;
    return a.createdMs - b.createdMs;
  });

  return list.slice(0, limit);
}

function leadResearchCandidateFromRecord(record = {}) {
  return {
    id: String(record.id || ""),
    title: String(record.title || record.company || "Untitled lead"),
    company: String(record.company || ""),
    contact: [record.contact, record.email, record.phone].filter(Boolean).join(" | "),
    service: String(record.serviceHint || ""),
    source: "research-import",
    status: "leads",
    note: String(record.note || ""),
  };
}

function buildResearchSourceScoreRows() {
  const staged = Array.isArray(currentLeadResearchImports) ? currentLeadResearchImports : [];
  const scoreMap = new Map();

  const ensure = (key, label, url) => {
    const safeKey = String(key || "public-directory").trim() || "public-directory";
    if (!scoreMap.has(safeKey)) {
      scoreMap.set(safeKey, {
        key: safeKey,
        label: String(label || "Public directory").trim() || "Public directory",
        url: String(url || "").trim(),
        staged: 0,
        approved: 0,
        imported: 0,
        rejected: 0,
        pipeline: 0,
        won: 0,
        lost: 0,
        closedUnspecified: 0,
        confidenceTotal: 0,
        confidenceCount: 0,
      });
    }
    const row = scoreMap.get(safeKey);
    if (label && !row.label) row.label = String(label).trim();
    if (url && !row.url) row.url = String(url).trim();
    return row;
  };

  staged.forEach((record) => {
    const meta = leadResearchSourceMeta(record);
    const row = ensure(meta.sourceKey, meta.sourceLabel, meta.sourceUrl);
    row.staged += 1;
    const status = normalizeLeadResearchStatus(record.status);
    if (status === "approved") row.approved += 1;
    if (status === "imported") row.imported += 1;
    if (status === "rejected") row.rejected += 1;
    const confidence = coerceConfidenceValue(record.confidence);
    if (Number.isFinite(confidence)) {
      row.confidenceTotal += confidence;
      row.confidenceCount += 1;
    }
  });

  currentPipeline.forEach((card) => {
    const meta = pipelineResearchSourceMeta(card);
    if (!meta) return;
    const row = ensure(meta.sourceKey, meta.sourceLabel, meta.sourceUrl);
    row.pipeline += 1;
    const outcomeKey = leadOutcomeKey(card);
    if (outcomeKey === "won") row.won += 1;
    if (outcomeKey === "lost") row.lost += 1;
    if (outcomeKey === "closed_unspecified") row.closedUnspecified += 1;
    if (String(card.researchConfidence || "").trim()) {
      const confidence = coerceConfidenceValue(card.researchConfidence);
      row.confidenceTotal += confidence;
      row.confidenceCount += 1;
    }
  });

  const rows = [...scoreMap.values()].map((row) => {
    const avgConfidence = row.confidenceCount
      ? Math.round(row.confidenceTotal / row.confidenceCount)
      : 0;
    const conversionRate = row.pipeline
      ? Math.round((row.won / row.pipeline) * 100)
      : 0;
    const sampleWeight = Math.min(row.pipeline, 20) / 20;
    const qualityScore = Math.round(
      (conversionRate * 0.7)
      + (avgConfidence * 0.2)
      + (sampleWeight * 100 * 0.1)
    );

    return {
      ...row,
      avgConfidence,
      conversionRate,
      qualityScore,
      pipelineOpen: Math.max(0, row.pipeline - row.won - row.lost - row.closedUnspecified),
    };
  });

  rows.sort((a, b) => {
    if (b.qualityScore !== a.qualityScore) return b.qualityScore - a.qualityScore;
    if (b.conversionRate !== a.conversionRate) return b.conversionRate - a.conversionRate;
    if (b.pipeline !== a.pipeline) return b.pipeline - a.pipeline;
    return a.label.localeCompare(b.label);
  });
  return rows.slice(0, 12);
}

function renderResearchSourceScoreSection() {
  const rows = buildResearchSourceScoreRows();
  if (!rows.length) {
    return `
      <section class="research-source-score">
        <div class="lead-list-title">Research Source Quality Score</div>
        <div class="lead-list-subtitle">No source performance data yet. Import staged leads to start tracking.</div>
      </section>
    `;
  }

  return `
    <section class="research-source-score">
      <div class="lead-list-head">
        <div>
          <div class="lead-list-title">Research Source Quality Score</div>
          <div class="lead-list-subtitle">Ranks directories by won conversion and confidence.</div>
        </div>
        <div class="lead-list-subtext">Set lead outcomes to Won or Lost for accurate conversion rankings.</div>
      </div>
      <div class="lead-list-wrap">
        <table class="lead-list-table lead-source-score-table">
          <thead>
            <tr>
              <th>Source</th>
              <th>Score</th>
              <th>Won Conv.</th>
              <th>Outcomes</th>
              <th>Pipeline</th>
              <th>Avg Conf.</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((row) => `
              <tr>
                <td>
                  <div class="lead-list-name">${escHtml(row.label)}</div>
                  ${
                    row.url
                      ? `<a class="lead-list-email" href="${escHtmlAttr(row.url)}" target="_blank" rel="noopener noreferrer">${escHtml(sourceLabelFromUrl(row.url) || row.url)}</a>`
                      : `<div class="lead-list-subtext">No source URL</div>`
                  }
                </td>
                <td><span class="priority ${row.qualityScore >= 70 ? "p-high" : row.qualityScore >= 40 ? "p-mid" : "p-low"}">${row.qualityScore}</span></td>
                <td>
                  <span class="lead-signal-chip">${row.conversionRate}%</span>
                  <div class="lead-list-subtext">${row.won} won / ${row.pipeline} total</div>
                </td>
                <td>
                  <div class="lead-list-subtext">Won ${row.won}</div>
                  <div class="lead-list-subtext">Lost ${row.lost}</div>
                  ${
                    row.closedUnspecified
                      ? `<div class="lead-list-subtext">Needs outcome ${row.closedUnspecified}</div>`
                      : ""
                  }
                </td>
                <td>
                  <div>${row.pipeline}</div>
                  <div class="lead-list-subtext">Open ${row.pipelineOpen}</div>
                </td>
                <td>${row.avgConfidence ? `${row.avgConfidence}/100` : "-"}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function renderLeadResearchQueueSection() {
  const imports = Array.isArray(currentLeadResearchImports) ? currentLeadResearchImports : [];
  const pendingCount = imports.filter((entry) => normalizeLeadResearchStatus(entry.status) === "pending").length;
  const approvedCount = imports.filter((entry) => normalizeLeadResearchStatus(entry.status) === "approved").length;
  const importedCount = imports.filter((entry) => normalizeLeadResearchStatus(entry.status) === "imported").length;
  const rejectedCount = imports.filter((entry) => normalizeLeadResearchStatus(entry.status) === "rejected").length;
  const rows = imports.slice(0, 40);

  return `
    <section class="lead-research-panel" id="leadResearchQueue">
      <div class="lead-list-head">
        <div>
          <div class="lead-list-title">Public-Source Lead Research Intake</div>
          <div class="lead-list-subtitle">CSV imports are staged for review before any lead is pushed into the sales pipeline.</div>
        </div>
        <div class="lead-list-actions">
          <input type="file" id="leadResearchCsvInput" accept=".csv,text/csv" style="display:none" onchange="ingestLeadResearchCsvFile(this)">
          <button class="card-btn" type="button" onclick="downloadLeadResearchCsvTemplate()">Template</button>
          <button class="card-btn" type="button" onclick="copyLeadResearchWorkflowCommand()">Copy Script Cmd</button>
          <button class="card-btn" type="button" onclick="triggerLeadResearchCsvPicker()">Import CSV</button>
          <button class="card-btn" type="button" onclick="importApprovedResearchLeads()" ${approvedCount ? "" : "disabled"}>Import Approved (${approvedCount})</button>
          <button class="card-btn" type="button" onclick="clearRejectedResearchLeads()" ${rejectedCount ? "" : "disabled"}>Clear Rejected (${rejectedCount})</button>
        </div>
      </div>

      <div class="lead-list-metrics">
        <span class="lead-list-pill">Staged <strong>${imports.length}</strong></span>
        <span class="lead-list-pill">Pending <strong>${pendingCount}</strong></span>
        <span class="lead-list-pill">Approved <strong>${approvedCount}</strong></span>
        <span class="lead-list-pill">Imported <strong>${importedCount}</strong></span>
        <span class="lead-list-pill">Rejected <strong>${rejectedCount}</strong></span>
      </div>

      <div class="lead-research-guardrail">
        Guardrails: import only publicly available business data with a source URL or source label. Do not upload personal sensitive data.
      </div>

      ${renderResearchSourceScoreSection()}

      ${
        leadResearchStatusMessage
          ? `<div class="lead-list-status${leadResearchStatusError ? " is-error" : ""}">${escHtml(leadResearchStatusMessage)}</div>`
          : ""
      }

      <div class="lead-list-wrap">
        ${
          rows.length
            ? `<table class="lead-list-table lead-research-table">
                 <thead>
                   <tr>
                     <th>Lead</th>
                     <th>Source</th>
                     <th>Confidence</th>
                     <th>Status</th>
                     <th>Sales Brief</th>
                     <th></th>
                   </tr>
                 </thead>
                 <tbody>
                   ${rows.map((entry) => {
                     const status = normalizeLeadResearchStatus(entry.status);
                     const candidate = leadResearchCandidateFromRecord(entry);
                     const brief = buildLeadSalesBrief(candidate);
                     const sourceMeta = leadResearchSourceMeta(entry);
                     const sourceUrl = sourceMeta.sourceUrl;
                     const sourceLabel = sourceMeta.sourceLabel;
                     const confidence = coerceConfidenceValue(entry.confidence);
                     const confidenceClass = confidence >= 70 ? "p-high" : confidence >= 40 ? "p-mid" : "p-low";
                     return `
                       <tr>
                         <td>
                           <div class="lead-list-name">${escHtml(candidate.title)}</div>
                           <div class="lead-list-subtext">${escHtml(candidate.company || "No company")} | ${escHtml(entry.email || "No email")}</div>
                           ${
                             [entry.city, entry.state, entry.country].filter(Boolean).length
                               ? `<div class="lead-list-subtext">${escHtml([entry.city, entry.state, entry.country].filter(Boolean).join(", "))}</div>`
                               : ""
                           }
                           <div class="lead-list-subtext">${escHtml(entry.note || "No qualification notes")}</div>
                         </td>
                         <td>
                           <div class="lead-list-subtext">${escHtml(sourceLabel)}</div>
                           ${
                             sourceUrl
                               ? `<a class="lead-list-email" href="${escHtmlAttr(sourceUrl)}" target="_blank" rel="noopener noreferrer">Open source</a>`
                               : `<div class="lead-list-subtext">No source URL</div>`
                           }
                         </td>
                         <td><span class="priority ${confidenceClass}">${confidence}/100</span></td>
                         <td><span class="research-status ${leadResearchStatusClass(status)}">${leadResearchStatusLabel(status)}</span></td>
                         <td>
                           <div class="lead-list-subtext"><strong>${escHtml(brief.recommendedService)}</strong> | ${escHtml(brief.opportunityLabel)} (${brief.score}/100)</div>
                           <div class="lead-list-subtext">${escHtml(brief.summary)}</div>
                         </td>
                         <td>
                           <div class="merge-assist-actions">
                             <button class="card-btn" type="button" onclick="copyResearchSalesBrief('${escHtmlAttr(entry.id)}')">Copy Brief</button>
                             ${
                               status === "pending"
                                 ? `<button class="card-btn" type="button" onclick="approveResearchLead('${escHtmlAttr(entry.id)}')">Approve</button>`
                                 : ""
                             }
                             ${
                               status === "approved"
                                 ? `<button class="card-btn" type="button" onclick="importResearchLead('${escHtmlAttr(entry.id)}')">Import</button>`
                                 : ""
                             }
                             ${
                               status === "pending" || status === "approved"
                                 ? `<button class="card-btn delete" type="button" onclick="rejectResearchLead('${escHtmlAttr(entry.id)}')">Reject</button>`
                                 : ""
                             }
                           </div>
                         </td>
                       </tr>
                     `;
                   }).join("")}
                 </tbody>
               </table>`
            : `<div class="empty-state" style="padding:14px;font-size:10px">No staged research leads yet. Import a CSV to start a review queue.</div>`
        }
      </div>
    </section>
  `;
}

function renderLeadListBuilder(cards) {
  const host = document.getElementById("leadListBuilder");
  if (!host) return;

  const leadList = buildGeneratedLeadList(cards);
  generatedLeadList = leadList;
  const uniqueEmails = [...new Set(leadList.map((lead) => lead.email).filter(Boolean))];
  const websiteCount = leadList.filter((lead) => lead.sourceKey === "website").length;
  const researchCount = leadList.filter((lead) => lead.sourceKey === "research").length;
  const highPriorityCount = leadList.filter((lead) => lead.priorityScore >= 70).length;
  const unassignedCount = leadList.filter((lead) => lead.unassigned).length;
  const callNowCount = leadList.filter((lead) =>
    lead.followUpBucket === "overdue" || lead.followUpBucket === "due_today"
  ).length;
  const duplicateLeadCount = leadList.filter((lead) => lead.duplicateCount > 1).length;

  host.innerHTML = `
    <div class="lead-list-head">
      <div>
        <div class="lead-list-title">Lead List Generator</div>
        <div class="lead-list-subtitle">Build outreach-ready lead lists from inbound and manual pipeline data.</div>
      </div>
      <div class="lead-list-actions">
        <button class="card-btn" type="button" onclick="generateLeadList()">Generate</button>
        <button class="card-btn" type="button" onclick="copyLeadListEmails()" ${uniqueEmails.length ? "" : "disabled"}>Copy Emails (${uniqueEmails.length})</button>
        <button class="card-btn" type="button" onclick="downloadLeadListCsv()" ${leadList.length ? "" : "disabled"}>Download CSV</button>
        <button class="card-btn" type="button" onclick="resetLeadListFilters()">Reset</button>
      </div>
    </div>

    <div class="lead-list-filter-grid">
      <div class="lead-list-filter-group">
        <label class="lead-list-filter-label">Stage</label>
        <select class="form-select" onchange="updateLeadListFilter('stage', this.value)">
          <option value="open" ${leadListFilters.stage === "open" ? "selected" : ""}>Open Stages</option>
          <option value="all" ${leadListFilters.stage === "all" ? "selected" : ""}>All Stages</option>
          <option value="leads" ${leadListFilters.stage === "leads" ? "selected" : ""}>Leads</option>
          <option value="contacted" ${leadListFilters.stage === "contacted" ? "selected" : ""}>In Conversation</option>
          <option value="proposal" ${leadListFilters.stage === "proposal" ? "selected" : ""}>Proposal Sent</option>
          <option value="closed" ${leadListFilters.stage === "closed" ? "selected" : ""}>Closed</option>
        </select>
      </div>
      <div class="lead-list-filter-group">
        <label class="lead-list-filter-label">Source</label>
        <select class="form-select" onchange="updateLeadListFilter('source', this.value)">
          <option value="all" ${leadListFilters.source === "all" ? "selected" : ""}>All Sources</option>
          <option value="website" ${leadListFilters.source === "website" ? "selected" : ""}>Website Form</option>
          <option value="research" ${leadListFilters.source === "research" ? "selected" : ""}>Research Imports</option>
          <option value="manual" ${leadListFilters.source === "manual" ? "selected" : ""}>Manual / Team Added</option>
        </select>
      </div>
      <div class="lead-list-filter-group">
        <label class="lead-list-filter-label">Owner</label>
        <select class="form-select" onchange="updateLeadListFilter('owner', this.value)">
          <option value="any" ${leadListFilters.owner === "any" ? "selected" : ""}>Any Owner</option>
          <option value="mine" ${leadListFilters.owner === "mine" ? "selected" : ""}>Assigned to Me</option>
          <option value="unassigned" ${leadListFilters.owner === "unassigned" ? "selected" : ""}>Unassigned</option>
          <option value="team" ${leadListFilters.owner === "team" ? "selected" : ""}>Entire Team</option>
        </select>
      </div>
      <div class="lead-list-filter-group">
        <label class="lead-list-filter-label">Timeframe</label>
        <select class="form-select" onchange="updateLeadListFilter('timeframe', this.value)">
          <option value="0" ${String(leadListFilters.timeframe) === "0" ? "selected" : ""}>All Time</option>
          <option value="7" ${String(leadListFilters.timeframe) === "7" ? "selected" : ""}>Last 7 Days</option>
          <option value="30" ${String(leadListFilters.timeframe) === "30" ? "selected" : ""}>Last 30 Days</option>
          <option value="90" ${String(leadListFilters.timeframe) === "90" ? "selected" : ""}>Last 90 Days</option>
        </select>
      </div>
      <div class="lead-list-filter-group">
        <label class="lead-list-filter-label">Follow-up</label>
        <select class="form-select" onchange="updateLeadListFilter('followUp', this.value)">
          <option value="all" ${leadListFilters.followUp === "all" ? "selected" : ""}>All</option>
          <option value="call_now" ${leadListFilters.followUp === "call_now" ? "selected" : ""}>Call Now (Today + Overdue)</option>
          <option value="overdue" ${leadListFilters.followUp === "overdue" ? "selected" : ""}>Overdue</option>
          <option value="due_today" ${leadListFilters.followUp === "due_today" ? "selected" : ""}>Due Today</option>
          <option value="next_7" ${leadListFilters.followUp === "next_7" ? "selected" : ""}>Next 7 Days</option>
        </select>
      </div>
      <div class="lead-list-filter-group">
        <label class="lead-list-filter-label">Duplicates</label>
        <select class="form-select" onchange="updateLeadListFilter('duplicate', this.value)">
          <option value="all" ${leadListFilters.duplicate === "all" ? "selected" : ""}>All</option>
          <option value="duplicates" ${leadListFilters.duplicate === "duplicates" ? "selected" : ""}>Duplicates Only</option>
          <option value="unique" ${leadListFilters.duplicate === "unique" ? "selected" : ""}>Unique Only</option>
        </select>
      </div>
      <div class="lead-list-filter-group">
        <label class="lead-list-filter-label">Sort</label>
        <select class="form-select" onchange="updateLeadListFilter('sort', this.value)">
          <option value="priority" ${leadListFilters.sort === "priority" ? "selected" : ""}>Follow-up Priority</option>
          <option value="followup" ${leadListFilters.sort === "followup" ? "selected" : ""}>Follow-up Due First</option>
          <option value="newest" ${leadListFilters.sort === "newest" ? "selected" : ""}>Newest First</option>
          <option value="oldest" ${leadListFilters.sort === "oldest" ? "selected" : ""}>Oldest First</option>
          <option value="updated" ${leadListFilters.sort === "updated" ? "selected" : ""}>Recently Updated</option>
        </select>
      </div>
      <div class="lead-list-filter-group">
        <label class="lead-list-filter-label">Limit</label>
        <input class="form-input" type="number" min="1" max="200" value="${escHtmlAttr(leadListFilters.limit)}" onchange="updateLeadListFilter('limit', this.value)">
      </div>
    </div>

    <div class="lead-list-metrics">
      <span class="lead-list-pill">Results <strong>${leadList.length}</strong></span>
      <span class="lead-list-pill">Website <strong>${websiteCount}</strong></span>
      <span class="lead-list-pill">Research <strong>${researchCount}</strong></span>
      <span class="lead-list-pill">Priority High <strong>${highPriorityCount}</strong></span>
      <span class="lead-list-pill">Unassigned <strong>${unassignedCount}</strong></span>
      <span class="lead-list-pill">Call Now <strong>${callNowCount}</strong></span>
      <span class="lead-list-pill">Duplicates <strong>${duplicateLeadCount}</strong></span>
    </div>

    ${
      leadListStatusMessage
        ? `<div class="lead-list-status${leadListStatusError ? " is-error" : ""}">${escHtml(leadListStatusMessage)}</div>`
        : ""
    }

    <div class="lead-list-wrap">
      ${
        leadList.length
          ? `<table class="lead-list-table">
               <thead>
                 <tr>
                   <th>#</th>
                   <th>Lead</th>
                   <th>Contact</th>
                   <th>Stage</th>
                   <th>Source</th>
                   <th>Owner</th>
                   <th>Priority</th>
                   <th>Follow-up</th>
                   <th>Signals</th>
                   <th>Created</th>
                   <th></th>
                 </tr>
               </thead>
               <tbody>
                 ${leadList.map((lead, index) => `
                   <tr>
                     <td class="lead-list-rank">${index + 1}</td>
                     <td>
                       <div class="lead-list-name">${escHtml(lead.title)}</div>
                       ${lead.company ? `<div class="lead-list-subtext">${escHtml(lead.company)}</div>` : ""}
                     </td>
                     <td>
                       <div>${escHtml(lead.contactLabel)}</div>
                       ${
                         lead.email
                           ? `<div class="lead-list-email">${escHtml(lead.email)}</div>`
                           : `<div class="lead-list-subtext">No email detected</div>`
                       }
                     </td>
                     <td>
                       <span class="timeline-chip">${escHtml(lead.stageLabel)}</span>
                       ${
                         lead.outcomeKey !== "open"
                           ? `<div class="lead-list-subtext">${escHtml(lead.outcomeLabel)}</div>`
                           : ""
                       }
                     </td>
                     <td><div class="lead-list-subtext">${escHtml(lead.sourceLabel)}</div></td>
                     <td>${assigneeChipHtml(lead.ownerUid, lead.ownerName, "Unassigned")}</td>
                     <td><span class="priority ${lead.priorityClass}">${escHtml(lead.priorityLabel)}</span></td>
                     <td>
                       <span class="lead-followup-chip ${lead.followUpClass}">${escHtml(lead.followUpLabel)}</span>
                       ${
                         lead.nextFollowUpMs
                           ? `<div class="lead-list-subtext">${escHtml(formatDateTimeLabel(lead.nextFollowUpMs))}</div>`
                           : ""
                       }
                     </td>
                     <td>
                       ${
                         lead.duplicateCount > 1
                           ? `<span class="lead-signal-chip">Dupes x${lead.duplicateCount}</span>`
                           : `<span class="lead-list-subtext">—</span>`
                       }
                       ${
                         lead.duplicateHint
                           ? `<div class="lead-list-subtext">${escHtml(lead.duplicateHint)}</div>`
                           : ""
                       }
                       ${
                         lead.duplicateCount > 1
                           ? `<div class="merge-assist-actions"><button class="card-btn lead-list-open-btn" type="button" onclick="openLeadMergeAssistant('${escHtmlAttr(lead.id)}')">Review</button></div>`
                           : ""
                       }
                     </td>
                     <td class="date-text">${escHtml(formatDateTimeLabel(lead.createdMs))}</td>
                     <td><button class="card-btn lead-list-open-btn" type="button" onclick="openLeadDetail('${escHtmlAttr(lead.id)}')">Open</button></td>
                   </tr>
                 `).join("")}
               </tbody>
             </table>`
          : `<div class="empty-state" style="padding:14px;font-size:10px">No leads match current filters. Adjust filters and generate again.</div>`
      }
    </div>

    ${renderLeadResearchQueueSection()}
  `;
}

function renderInboundSalesBriefQueue(cards) {
  const inboundCards = (Array.isArray(cards) ? cards : [])
    .filter((card) => !isPipelineLeadClosed(card))
    .filter((card) => String(leadSourceKey(card)) === "website")
    .sort((a, b) => {
      const ams = requestTimelineMs(a.createdAt) || pipelineLeadUpdatedMs(a);
      const bms = requestTimelineMs(b.createdAt) || pipelineLeadUpdatedMs(b);
      return bms - ams;
    })
    .slice(0, 4);

  if (!inboundCards.length) {
    return `
      <section class="sales-brief-queue">
        <div class="sales-brief-head">
          <div class="sales-brief-title">Inbound Sales Brief Queue</div>
          <div class="sales-brief-subtitle">No recent website-form leads yet.</div>
        </div>
      </section>
    `;
  }

  return `
    <section class="sales-brief-queue">
      <div class="sales-brief-head">
        <div class="sales-brief-title">Inbound Sales Brief Queue</div>
        <div class="sales-brief-subtitle">Deterministic brief cards for fast first-response consistency.</div>
      </div>
      <div class="sales-brief-grid">
        ${inboundCards.map((card) => {
          const brief = buildLeadSalesBrief(card);
          return `
            <article class="sales-brief-card">
              <div class="sales-brief-card-head">
                <div class="sales-brief-card-title">${escHtml(card.title || "Untitled lead")}</div>
                <span class="priority ${brief.opportunityClass}">${escHtml(brief.opportunityLabel)}</span>
              </div>
              <div class="sales-brief-meta">${escHtml(card.company || "No company")} | ${escHtml(brief.recommendedService)}</div>
              <div class="sales-brief-summary">${escHtml(brief.summary)}</div>
              <div class="sales-brief-angle">${escHtml(brief.outreachAngle)}</div>
              <div class="sales-brief-actions">
                <button class="card-btn" type="button" onclick="copyLeadSalesBrief('${escHtmlAttr(card.id)}')">Copy Brief</button>
                <button class="card-btn" type="button" onclick="attachSalesBriefToLead('${escHtmlAttr(card.id)}')">Attach to Note</button>
                <button class="card-btn" type="button" onclick="openLeadDetail('${escHtmlAttr(card.id)}')">Open Lead</button>
              </div>
            </article>
          `;
        }).join("")}
      </div>
    </section>
  `;
}

function renderPipelineRadar(cards, visibleCards) {
  const radar = document.getElementById("pipelineRadar");
  if (!radar) return;

  const allCards = Array.isArray(cards) ? cards : [];
  const scopedCards = Array.isArray(visibleCards) ? visibleCards : allCards;
  const openCards = allCards.filter((card) => !isPipelineLeadClosed(card));
  const oldestUnassigned = oldestUnassignedPipelineLead(allCards);
  const filterButtons = [
    { key: "all", label: "All", count: allCards.length },
    { key: "unassigned", label: "Unassigned", count: openCards.filter(isPipelineLeadUnassigned).length },
    { key: "stale", label: "Stale", count: openCards.filter(isPipelineLeadStale).length },
    { key: "high_intent", label: "High Intent", count: openCards.filter(isPipelineLeadHighIntent).length },
  ];
  const newLeadCount = openCards
    .filter((card) => String(card.status || "") === "leads")
    .filter(isPipelineLeadNew)
    .length;
  const inboundCount = openCards.filter((card) => leadSourceKey(card) === "website").length;
  const attentionCount = openCards.filter((card) =>
    isPipelineLeadUnassigned(card) || isPipelineLeadStale(card)
  ).length;

  radar.innerHTML = `
    <div class="pipeline-radar-head">
      <div>
        <div class="pipeline-radar-title">Pipeline Follow-Up Radar</div>
        <div class="pipeline-radar-subtitle">Prioritize outreach and keep high-value leads moving.</div>
      </div>
      <div class="pipeline-radar-metrics">
        <div class="pipeline-radar-metric">
          <span class="pipeline-radar-metric-label">Needs Attention</span>
          <span class="pipeline-radar-metric-value">${attentionCount}</span>
        </div>
        <div class="pipeline-radar-metric">
          <span class="pipeline-radar-metric-label">New (${PIPELINE_NEW_WINDOW_DAYS}d)</span>
          <span class="pipeline-radar-metric-value">${newLeadCount}</span>
        </div>
        <div class="pipeline-radar-metric">
          <span class="pipeline-radar-metric-label">Inbound</span>
          <span class="pipeline-radar-metric-value">${inboundCount}</span>
        </div>
      </div>
    </div>
    <div class="pipeline-radar-filters">
      ${filterButtons.map((filter) => `
        <button
          type="button"
          class="pipeline-focus-chip${pipelineFocusFilter === filter.key ? " active" : ""}"
          onclick="setPipelineFocusFilter('${filter.key}')"
          aria-pressed="${pipelineFocusFilter === filter.key ? "true" : "false"}"
        >
          <span>${filter.label}</span>
          <strong>${filter.count}</strong>
        </button>
      `).join("")}
    </div>
    <div class="pipeline-radar-actions">
      <button
        type="button"
        class="pipeline-radar-action-btn"
        onclick="claimOldestUnassignedLead()"
        ${oldestUnassigned ? "" : "disabled"}
      >
        Claim Oldest Unassigned
      </button>
      <span class="pipeline-radar-action-context">
        ${
          oldestUnassigned
            ? `Next lead: ${escHtml(oldestUnassigned.title || "Untitled lead")}`
            : "No unassigned open leads right now."
        }
      </span>
    </div>
    ${
      pipelineFocusFilter !== "all"
        ? `<div class="pipeline-radar-active">
             Showing ${scopedCards.length} / ${allCards.length} leads for: <strong>${escHtml(pipelineFocusLabel(pipelineFocusFilter))}</strong>
             <button type="button" class="pipeline-radar-clear" onclick="setPipelineFocusFilter('all')">Clear filter</button>
           </div>`
        : ""
    }
    ${
      pipelineRadarNoticeText
        ? `<div class="pipeline-radar-notice${pipelineRadarNoticeError ? " is-error" : ""}">${escHtml(pipelineRadarNoticeText)}</div>`
        : ""
    }
    ${renderInboundSalesBriefQueue(allCards)}
  `;
}

function renderMyWorkDashboard() {
  const summaryEl = document.getElementById("myWorkSummary");
  const callsEl = document.getElementById("myCallsList");
  const leadsEl = document.getElementById("myLeadsList");
  const requestsEl = document.getElementById("myRequestsList");
  const tasksEl = document.getElementById("myTasksList");
  if (!summaryEl || !callsEl || !leadsEl || !requestsEl || !tasksEl) return;

  const currentUid = String(auth.currentUser?.uid || "");
  if (!currentUid) {
    summaryEl.innerHTML = "";
    callsEl.innerHTML = `<div class="empty-state" style="padding:14px;font-size:10px">Sign in to view your ownership dashboard.</div>`;
    leadsEl.innerHTML = `<div class="empty-state" style="padding:14px;font-size:10px">Sign in to view your ownership dashboard.</div>`;
    requestsEl.innerHTML = `<div class="empty-state" style="padding:14px;font-size:10px">Sign in to view your ownership dashboard.</div>`;
    tasksEl.innerHTML = `<div class="empty-state" style="padding:14px;font-size:10px">Sign in to view your ownership dashboard.</div>`;
    return;
  }

  const myLeads = currentPipeline
    .filter((card) => {
      const ownerUid = String(card.ownerUid || "");
      return ownerUid === currentUid || isTeamAssignmentUid(ownerUid);
    })
    .sort((a, b) => {
      const ams = requestTimelineMs(a.updatedAt) || requestTimelineMs(a.createdAt);
      const bms = requestTimelineMs(b.updatedAt) || requestTimelineMs(b.createdAt);
      return bms - ams;
    });

  const myRequests = currentClientRequests
    .filter((req) => {
      const ownerUid = String(req.ownerUid || "");
      return ownerUid === currentUid || isTeamAssignmentUid(ownerUid);
    })
    .sort((a, b) => {
      const ams = requestTimelineMs(a.updatedAt) || requestTimelineMs(a.createdAt);
      const bms = requestTimelineMs(b.updatedAt) || requestTimelineMs(b.createdAt);
      return bms - ams;
    });
  const activeMyRequests = myRequests.filter((req) => req.archived !== true);
  const archivedMyRequests = myRequests.filter((req) => req.archived === true);
  const visibleMyRequests = showArchivedMyWorkRequests ? myRequests : activeMyRequests;
  const me = currentTeamUsers.find((user) => String(user.id || "") === currentUid);
  const meLabel = String(me?.name || auth.currentUser?.displayName || "").trim().toLowerCase();
  const meEmail = normalizeEmail(me?.email || auth.currentUser?.email || "");

  const allMyTasks = currentTasks
    .filter((task) => {
      const normalized = normalizeTaskOwnerFields(task);
      if (isTeamAssignmentUid(normalized.ownerUid)) return true;
      if (normalized.ownerUid) return normalized.ownerUid === currentUid;
      const ownerLower = String(normalized.ownerName || normalized.owner || "").trim().toLowerCase();
      return ownerLower && (ownerLower === meLabel || ownerLower === meEmail);
    })
    .sort((a, b) => {
      if (Boolean(a.done) !== Boolean(b.done)) return a.done ? 1 : -1;
      const adue = a.due || "9999-99-99";
      const bdue = b.due || "9999-99-99";
      if (adue !== bdue) return adue.localeCompare(bdue);
      const ams = requestTimelineMs(a.updatedAt) || requestTimelineMs(a.createdAt);
      const bms = requestTimelineMs(b.updatedAt) || requestTimelineMs(b.createdAt);
      return bms - ams;
    });
  const openMyTasks = allMyTasks.filter((task) => !task.done);
  const completedMyTasks = allMyTasks.filter((task) => task.done);
  const visibleMyTasks = showCompletedMyTasks ? allMyTasks : openMyTasks;

  const leadsNeedingFollowUp = myLeads.filter((card) => String(card.status || "") === "leads").length;
  const myCallLeads = myLeads
    .map((card) => {
      const nextFollowUpMs = leadNextFollowUpMs(card);
      const followUpBucket = followUpBucketForMs(nextFollowUpMs);
      return { ...card, nextFollowUpMs, followUpBucket };
    })
    .filter((card) => card.followUpBucket === "overdue" || card.followUpBucket === "due_today")
    .sort((a, b) => {
      const aw = a.followUpBucket === "overdue" ? 0 : 1;
      const bw = b.followUpBucket === "overdue" ? 0 : 1;
      if (aw !== bw) return aw - bw;
      return (a.nextFollowUpMs || Number.MAX_SAFE_INTEGER) - (b.nextFollowUpMs || Number.MAX_SAFE_INTEGER);
    });
  const highPriorityRequests = activeMyRequests.filter((req) => String(req.priority || "normal") === "high").length;
  summaryEl.innerHTML = `
    <div class="my-work-pill">My Leads: <strong>${myLeads.length}</strong></div>
    <div class="my-work-pill">Need follow-up: <strong>${leadsNeedingFollowUp}</strong></div>
    <div class="my-work-pill">Calls Today: <strong>${myCallLeads.length}</strong></div>
    <div class="my-work-pill">My Requests (Active): <strong>${activeMyRequests.length}</strong></div>
    <div class="my-work-pill">My Requests (Archived): <strong>${archivedMyRequests.length}</strong></div>
    <div class="my-work-pill">My Tasks (Open): <strong>${openMyTasks.length}</strong></div>
    <div class="my-work-pill">High Priority: <strong>${highPriorityRequests}</strong></div>
  `;

  if (!myCallLeads.length) {
    callsEl.innerHTML = `<div class="empty-state" style="padding:14px;font-size:10px">No calls due right now. Great job staying on top of follow-ups.</div>`;
  } else {
    callsEl.innerHTML = myCallLeads.slice(0, 12).map((card) => {
      const tagClass = leadServiceTagClass(card.service);
      const chipClass = card.followUpBucket === "overdue" ? "overdue" : "today";
      const chipLabel = card.followUpBucket === "overdue" ? "Overdue" : "Today";
      const dueLabel = card.nextFollowUpMs ? formatDateTimeLabel(card.nextFollowUpMs) : "Now";
      return `
        <button class="my-work-item" type="button" onclick="openLeadDetail('${escHtmlAttr(card.id)}')">
          <div class="my-work-item-row">
            <span class="my-work-item-title">${escHtml(card.title || "Untitled lead")}</span>
            <span class="my-call-chip ${chipClass}">${chipLabel}</span>
          </div>
          <div class="my-work-item-row">
            <span class="card-tag ${tagClass}">${escHtml(card.service || "Service")}</span>
            <span class="date-text">${escHtml(dueLabel)}</span>
          </div>
          ${card.note ? `<div class="my-work-item-copy">${escHtml(card.note).slice(0, 120)}</div>` : ""}
        </button>
      `;
    }).join("");
  }

  if (!myLeads.length) {
    leadsEl.innerHTML = `<div class="empty-state" style="padding:14px;font-size:10px">No assigned leads yet.</div>`;
  } else {
    leadsEl.innerHTML = myLeads.map((card) => {
      const statusLabel = pipelineStatusLabel(card.status);
      const tagClass = leadServiceTagClass(card.service);
      const updatedMs = requestTimelineMs(card.updatedAt) || requestTimelineMs(card.createdAt);
      return `
        <button class="my-work-item" type="button" onclick="openLeadDetail('${escHtmlAttr(card.id)}')">
          <div class="my-work-item-row">
            <span class="my-work-item-title">${escHtml(card.title || "Untitled lead")}</span>
            <span class="timeline-chip">${escHtml(statusLabel)}</span>
          </div>
          <div class="my-work-item-row">
            <span class="card-tag ${tagClass}">${escHtml(card.service || "Service")}</span>
            <span class="date-text">${escHtml(formatDateTimeLabel(updatedMs))}</span>
          </div>
          ${card.note ? `<div class="my-work-item-copy">${escHtml(card.note).slice(0, 160)}</div>` : ""}
        </button>
      `;
    }).join("");
  }

  const archivedToggleLabel = showArchivedMyWorkRequests
    ? "Hide Archived"
    : `Show Archived (${archivedMyRequests.length})`;
  const requestsTools = `
    <div class="my-work-inline-tools">
      <span class="date-text">${showArchivedMyWorkRequests ? "Showing active + archived assignments" : "Showing active assignments only"}</span>
      <button class="sort-pill${showArchivedMyWorkRequests ? " active" : ""}" type="button" onclick="toggleMyWorkArchivedRequests()" ${archivedMyRequests.length ? "" : "disabled"}>
        ${archivedToggleLabel}
      </button>
    </div>
  `;

  if (!visibleMyRequests.length) {
    const emptyMessage = showArchivedMyWorkRequests
      ? "No assigned client requests yet."
      : "No active assigned requests. Use archived toggle to review completed work.";
    requestsEl.innerHTML = `${requestsTools}<div class="empty-state" style="padding:14px;font-size:10px">${emptyMessage}</div>`;
  } else {
    requestsEl.innerHTML = requestsTools + visibleMyRequests.map((req) => {
      const status = String(req.status || "submitted");
      const updatedMs = requestTimelineMs(req.updatedAt) || requestTimelineMs(req.createdAt);
      const archivedBadge = req.archived === true
        ? `<span class="timeline-chip internal">Archived</span>`
        : "";
      return `
        <button class="my-work-item" type="button" onclick="openRequestDetail('${escHtmlAttr(req.id)}')">
          <div class="my-work-item-row">
            <span class="my-work-item-title">${escHtml(req.title || "Untitled request")}</span>
            <span>
              <span class="req-status ${requestStatusClass(status)}">${escHtml(requestStatusLabel(status))}</span>
              ${archivedBadge}
            </span>
          </div>
          <div class="my-work-item-row">
            <span class="date-text">${escHtml(req.clientName || "Unknown client")}</span>
            <span class="date-text">${escHtml(formatDateTimeLabel(updatedMs))}</span>
          </div>
          <div class="my-work-item-copy">${escHtml(req.category || "general")} - ${escHtml(req.priority || "normal")}</div>
        </button>
      `;
    }).join("");
  }

  const completedToggleLabel = showCompletedMyTasks
    ? "Open Only"
    : `Include Completed (${completedMyTasks.length})`;
  const tasksTools = `
    <div class="my-work-inline-tools">
      <span class="date-text">${showCompletedMyTasks ? "Showing open + completed assignments" : "Showing open assignments only"}</span>
      <button class="sort-pill${showCompletedMyTasks ? " active" : ""}" type="button" onclick="toggleMyWorkCompletedTasks()" ${(completedMyTasks.length || showCompletedMyTasks) ? "" : "disabled"}>
        ${completedToggleLabel}
      </button>
    </div>
  `;

  if (!allMyTasks.length) {
    tasksEl.innerHTML = `${tasksTools}<div class="empty-state" style="padding:14px;font-size:10px">No assigned tasks yet.</div>`;
  } else if (!visibleMyTasks.length) {
    tasksEl.innerHTML = `${tasksTools}<div class="empty-state" style="padding:14px;font-size:10px">No open assigned tasks. Use the completed toggle to review finished work.</div>`;
  } else {
    tasksEl.innerHTML = tasksTools + visibleMyTasks.map((task) => {
      const owner = normalizeTaskOwnerFields(task);
      const dueLabel = task.due ? formatDate(task.due) : "No due date";
      const priority = String(task.priority || "Mid");
      const pClass = priority === "High" ? "p-high" : priority === "Mid" ? "p-mid" : "p-low";
      const updatedLabel = formatDateTimeLabel(taskUpdatedMs(task));
      const stale = isTaskStale(task);
      const sharedBadge = isTeamAssignmentUid(owner.ownerUid)
        ? `<span class="timeline-chip client">Team Shared</span>`
        : "";
      const staleBadge = stale ? `<span class="timeline-chip internal">Stale</span>` : "";
      const doneToggleTitle = task.done ? "Mark open" : "Mark completed";
      return `
        <button class="my-work-item${task.done ? " is-done" : ""}${stale ? " is-stale" : ""}" type="button" onclick="openTaskDetail('${escHtmlAttr(task.id)}')">
          <div class="my-work-item-row">
            <span class="my-work-item-title">${escHtml(task.name || "Untitled task")}</span>
            <span>
              <span class="checkbox ${task.done ? "checked" : ""} my-work-task-check" onclick="toggleMyWorkTaskDone(event, '${escHtmlAttr(task.id)}', ${!task.done})" title="${doneToggleTitle}"></span>
              ${sharedBadge}
              ${staleBadge}
              <span class="timeline-chip ${task.done ? "internal" : "client"}">${task.done ? "Completed" : "Open"}</span>
            </span>
          </div>
          <div class="my-work-item-row">
            <span class="owner-chip"><span class="avatar">${escHtml(teamUserInitials(owner.ownerName || owner.owner || "Team"))}</span>${escHtml(owner.ownerName || owner.owner || "Entire Team")}</span>
            <span class="priority ${pClass}">${escHtml(priority)}</span>
          </div>
          <div class="my-work-item-copy">Due: ${escHtml(dueLabel)} | Updated: ${escHtml(updatedLabel)}</div>
        </button>
      `;
    }).join("");
  }
}

window.toggleMyWorkArchivedRequests = function () {
  showArchivedMyWorkRequests = !showArchivedMyWorkRequests;
  renderMyWorkDashboard();
};

window.toggleMyWorkCompletedTasks = function () {
  showCompletedMyTasks = !showCompletedMyTasks;
  renderMyWorkDashboard();
};

window.toggleMyWorkTaskDone = async function (evt, id, newDone) {
  if (evt?.stopPropagation) evt.stopPropagation();
  if (evt?.preventDefault) evt.preventDefault();
  await toggleTask(id, newDone);
};

// ─── PIPELINE RENDER ──────────────────────────────────────────────────────────
function renderPipeline(cards) {
  const board = document.getElementById("kanbanBoard");
  if (!board) return;
  const allCards = Array.isArray(cards) ? cards : [];
  const scopedCards = pipelineFocusFilter === "all"
    ? allCards
    : allCards.filter((card) => pipelineLeadMatchesFocus(card, pipelineFocusFilter));

  renderLeadListBuilder(allCards);
  renderPipelineRadar(allCards, scopedCards);
  board.innerHTML = "";

  COLS.forEach(({ key, label }) => {
    let colCards = scopedCards.filter((c) => c.status === key);
    if (key === "leads") {
      colCards = [...colCards].sort((a, b) => {
        const ams = requestTimelineMs(a.createdAt) || requestTimelineMs(a.updatedAt);
        const bms = requestTimelineMs(b.createdAt) || requestTimelineMs(b.updatedAt);
        return ams - bms;
      });
    }

    const isCollapsed = collapsedPipelineCols.has(key);
    const shouldLimitLeads =
      key === "leads"
      && !showAllLeads
      && colCards.length > LEADS_VISIBLE_LIMIT;
    const visibleCards = shouldLimitLeads ? colCards.slice(0, LEADS_VISIBLE_LIMIT) : colCards;
    const hiddenLeadCount = shouldLimitLeads ? colCards.length - visibleCards.length : 0;

    const colEl    = document.createElement("div");
    colEl.className = `kanban-col${isCollapsed ? " is-collapsed" : ""}`;
    colEl.innerHTML = `
      <div class="col-header">
        <div class="col-header-left">
          <span class="col-title">${label}</span>
          ${key === "leads" ? `<span class="col-mode-pill">FIFO</span>` : ""}
        </div>
        <div class="col-header-right">
          <span class="col-count">${colCards.length}</span>
          <button class="col-toggle-btn" type="button" onclick="togglePipelineColumnCollapse('${key}')">
            ${isCollapsed ? "Expand" : "Collapse"}
          </button>
        </div>
      </div>
      ${
        key === "leads" && hiddenLeadCount > 0
          ? `<div class="kanban-note">Showing first ${LEADS_VISIBLE_LIMIT} oldest leads. ${hiddenLeadCount} hidden.</div>`
          : ""
      }
      <div id="col-${key}" class="kanban-dropzone${isCollapsed ? " is-collapsed" : ""}"></div>
      ${
        key === "leads" && colCards.length > LEADS_VISIBLE_LIMIT
          ? `<button class="card-btn leads-visible-toggle" type="button" onclick="toggleLeadOverflow()">
               ${showAllLeads ? `Show top ${LEADS_VISIBLE_LIMIT} (FIFO)` : `Show all (${colCards.length})`}
             </button>`
          : ""
      }
      <button class="add-card" onclick="openAddModal('pipeline','${key}')">+ Add</button>
    `;
    board.appendChild(colEl);

    const container = colEl.querySelector(`#col-${key}`);
    if (!container || isCollapsed) return;

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

    visibleCards.forEach((card) => {
      const tagClass = leadServiceTagClass(card.service);
      const outcomeKey = leadOutcomeKey(card);
      const outcomeBadge = outcomeKey !== "open"
        ? `<span class="lead-outcome-chip outcome-${outcomeKey}">${escHtml(leadOutcomeLabel(outcomeKey))}</span>`
        : "";
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
        ${outcomeBadge}
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
  const current = currentPipeline.find((entry) => entry.id === id);
  const nextStatus = String(newStatus || "");
  const updates = { status: nextStatus, updatedAt: serverTimestamp() };
  if (nextStatus !== "closed" && current?.outcome) {
    updates.outcome = "";
  }
  try {
    await updateDoc(doc(db, "pipeline", id), updates);
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
  const currentOutcome = leadOutcomeKey(card);

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
        <label class="form-label">Lead Outcome</label>
        <select class="form-select" id="ld_outcome">
          <option value="" ${currentOutcome === "open" || currentOutcome === "closed_unspecified" ? "selected" : ""}>Open / Not decided</option>
          <option value="won" ${currentOutcome === "won" ? "selected" : ""}>Won</option>
          <option value="lost" ${currentOutcome === "lost" ? "selected" : ""}>Lost</option>
        </select>
        ${
          currentOutcome === "closed_unspecified"
            ? `<div class="request-helper">This lead is closed but missing an outcome. Set Won or Lost for conversion reporting.</div>`
            : ""
        }
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
  document.getElementById("ld_status")?.addEventListener("change", syncLeadOutcomeField);
  syncLeadOutcomeField();

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
  const nextStatus = String(document.getElementById("ld_status").value || "leads");
  const rawOutcome = String(document.getElementById("ld_outcome")?.value || "").trim().toLowerCase();
  const nextOutcome = nextStatus === "closed" && (rawOutcome === "won" || rawOutcome === "lost")
    ? rawOutcome
    : "";
  const ownerUid = String(document.getElementById("ld_ownerUid").value || "");
  const ownerName = isTeamAssignmentUid(ownerUid)
    ? "Entire Team"
    : (ownerUid ? resolveTeamUserName(ownerUid, "") : "");
  try {
    await updateDoc(doc(db, "pipeline", editingLeadId), {
      title:     document.getElementById("ld_title").value.trim(),
      company:   document.getElementById("ld_company").value.trim(),
      contact:   document.getElementById("ld_contact").value.trim(),
      service,
      ownerUid,
      ownerName,
      status: nextStatus,
      outcome: nextOutcome,
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

document.getElementById("leadMergeOverlay")?.addEventListener("click", function (e) {
  if (e.target === this) window.closeLeadMergeAssistant();
});

function syncLeadOutcomeField() {
  const statusEl = document.getElementById("ld_status");
  const outcomeEl = document.getElementById("ld_outcome");
  if (!statusEl || !outcomeEl) return;
  const isClosed = String(statusEl.value || "") === "closed";
  outcomeEl.disabled = !isClosed;
  if (!isClosed) {
    outcomeEl.value = "";
  }
}

// ─── TASKS ────────────────────────────────────────────────────────────────────
// Priority sort order helper
const PRIORITY_ORDER = { High: 0, Mid: 1, Low: 2 };

async function backfillTaskAssignments(tasks) {
  if (hasTaskBackfillRun || isTaskBackfillRunning || !Array.isArray(tasks)) return;
  if (!currentTeamUsers.length) return;

  const needsBackfill = tasks.filter((task) =>
    !Object.prototype.hasOwnProperty.call(task, "ownerUid")
    || !Object.prototype.hasOwnProperty.call(task, "ownerName")
    || !Object.prototype.hasOwnProperty.call(task, "ownerType")
  );

  if (!needsBackfill.length) {
    hasTaskBackfillRun = true;
    return;
  }

  isTaskBackfillRunning = true;
  try {
    for (const task of needsBackfill) {
      const normalized = normalizeTaskOwnerFields(task);
      await updateDoc(doc(db, "tasks", task.id), {
        ownerUid: normalized.ownerUid,
        ownerName: normalized.ownerName,
        ownerType: normalized.ownerType,
        owner: normalized.owner,
        updatedAt: serverTimestamp(),
      });
    }
    hasTaskBackfillRun = true;
  } catch (err) {
    console.error("backfillTaskAssignments:", err);
  } finally {
    isTaskBackfillRunning = false;
  }
}

function taskUpdatedMs(task) {
  return requestTimelineMs(task?.updatedAt) || requestTimelineMs(task?.createdAt);
}

function isTaskStale(task) {
  if (!task || task.done) return false;
  const updatedMs = taskUpdatedMs(task);
  if (!updatedMs) return false;
  const staleCutoffMs = Date.now() - (TASK_STALE_DAYS * 24 * 60 * 60 * 1000);
  return updatedMs < staleCutoffMs;
}

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
        va = taskOwnerDisplayName(a).toLowerCase();
        vb = taskOwnerDisplayName(b).toLowerCase();
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

    const stale = isTaskStale(task);
    if (stale) tr.classList.add("task-stale-row");

    const pClass = task.priority === "High" ? "p-high" : task.priority === "Mid" ? "p-mid" : "p-low";
    const normalizedOwner = normalizeTaskOwnerFields(task);
    const ownerLabel = taskOwnerDisplayName(task) || "Entire Team";
    const initials = ownerLabel.split(" ").map((w) => w[0]).join("").toUpperCase().slice(0, 2);

    const ownerKind = isTeamAssignmentUid(normalizedOwner.ownerUid)
      ? "team"
      : (normalizedOwner.ownerUid ? "personal" : "custom");
    const ownerKindLabel = ownerKind === "team"
      ? "Team Shared"
      : (ownerKind === "personal" ? "Personal" : "Custom");
    const taskName = String(task.name || "");

    const addedStr = task.createdAt?.toDate
      ? task.createdAt.toDate().toLocaleDateString("en-US", { month: "short", day: "numeric" })
      : "-";

    const staleBadge = stale
      ? `<span class="task-stale-flag" title="No task activity for ${TASK_STALE_DAYS}+ days">Stale</span>`
      : "";

    tr.innerHTML = `
      <td><div class="checkbox ${task.done ? "checked" : ""}" onclick="toggleTask('${task.id}', ${!task.done})"></div></td>
      <td class="task-name-cell">
        <div class="task-name-wrap">
          <span class="task-name" title="${escHtmlAttr(taskName)}">${escHtml(taskName)}</span>
          ${staleBadge}
        </div>
      </td>
      <td>
        <div class="task-owner-stack">
          <span class="owner-chip owner-chip-${ownerKind}">
            <span class="avatar">${initials}</span>
            ${escHtml(ownerLabel)}
          </span>
          <span class="task-owner-kind kind-${ownerKind}">${ownerKindLabel}</span>
        </div>
      </td>
      <td><span class="date-text ${isOverdue ? "date-overdue" : ""}">${formatDate(task.due)}</span></td>
      <td><span class="date-text">${addedStr}</span></td>
      <td><span class="priority ${pClass}">${task.priority || "-"}</span></td>
      <td>
        <div class="task-actions">
          <button class="card-btn" onclick="openTaskDetail('${task.id}')">Edit</button>
          <button class="card-btn delete" onclick="deleteTask('${task.id}')">&times;</button>
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
    document.querySelectorAll(".sort-pill[data-sort]").forEach((p) => p.classList.remove("active"));
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
  try {
    await updateDoc(doc(db, "tasks", id), {
      done: newDone,
      updatedAt: serverTimestamp(),
    });
  }
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
  const selectedOwner = taskOwnerSelectValue(task);
  const normalizedOwner = normalizeTaskOwnerFields(task);
  const customOwnerValue = selectedOwner === OTHER_ASSIGNMENT_UID
    ? String(normalizedOwner.ownerName || normalizedOwner.owner || "")
    : "";
  const ownerOptions = taskAssigneeOptionsHtml(task);

  document.getElementById("taskDetailBody").innerHTML = `
    <div class="detail-grid">
      <div class="form-group form-group-full">
        <label class="form-label">Task</label>
        <input class="form-input" id="td_name" value="${escHtmlAttr(task.name || "")}" placeholder="What needs to get done?">
      </div>
      <div class="form-group">
        <label class="form-label">Assigned To</label>
        <select class="form-select" id="td_ownerUid">
          ${ownerOptions}
        </select>
      </div>
      <div class="form-group" id="td_ownerOtherWrap" style="${selectedOwner === OTHER_ASSIGNMENT_UID ? "" : "display:none"}">
        <label class="form-label">Other Assignee</label>
        <input class="form-input" id="td_ownerOther" value="${escHtmlAttr(customOwnerValue)}" placeholder="Type custom assignee">
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
  document.getElementById("td_ownerUid")?.addEventListener("change", () => {
    syncTaskOwnerOtherField("td_ownerUid", "td_ownerOtherWrap", "td_ownerOther");
  });
  syncTaskOwnerOtherField("td_ownerUid", "td_ownerOtherWrap", "td_ownerOther");

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
  const owner = resolveTaskOwnerFromInputs("td_ownerUid", "td_ownerOther");
  if (!owner) {
    alert("Please type a custom assignee name or choose a team assignee.");
    return;
  }

  try {
    await updateDoc(doc(db, "tasks", editingTaskId), {
      name,
      owner:     owner.owner,
      ownerUid:  owner.ownerUid,
      ownerName: owner.ownerName,
      ownerType: owner.ownerType,
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

function clientMatchesSearch(client) {
  const needle = String(clientsSearchTerm || "").trim().toLowerCase();
  if (!needle) return true;
  const haystack = [
    client.companyName,
    client.contactName,
    client.email,
    client.planName,
    client.notes,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return haystack.includes(needle);
}

window.clearClientsSearch = function () {
  clientsSearchTerm = "";
  const input = document.getElementById("clientsSearchInput");
  if (input) input.value = "";
  renderClientsWorkspace(currentClients);
};

function renderClientsWorkspace(clients) {
  const activeList = document.getElementById("clientsActiveList");
  const oneTimeList = document.getElementById("clientsOneTimeList");
  const inactiveList = document.getElementById("clientsInactiveList");
  if (!activeList || !oneTimeList || !inactiveList) return;

  const scoped = clients.filter((client) => clientMatchesSearch(client));
  const inactive = scoped.filter((c) => c.status === "inactive" || c.status === "completed");
  const oneTime = scoped.filter((c) =>
    !(c.status === "inactive" || c.status === "completed")
    && (c.status === "one-time" || !c.recurring)
  );
  const activeRecurring = scoped.filter((c) =>
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
        <label class="form-label">Client Auth UID (Optional Fallback)</label>
        <input class="form-input" id="cd_authUid" value="${escHtmlAttr(client.authUid || "")}" placeholder="Auto-linked from provisioning">
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
  const manualAuthUid = String(document.getElementById("cd_authUid").value || "").trim();

  try {
    let authSync = { uid: manualAuthUid, created: false, resetSent: false, accountExists: Boolean(manualAuthUid), requiresManualUid: false };
    if (!manualAuthUid && email && isSuperAdminRole(currentUserRole)) {
      authSync = await ensureProvisionAuthCredential(email, "client", { existingUid: String(existingClient?.authUid || "") });
    }
    const resolvedAuthUid = String(authSync.uid || "");

    const updates = {
      companyName: document.getElementById("cd_companyName").value.trim(),
      contactName: document.getElementById("cd_contactName").value.trim(),
      email,
      emailLower: email,
      authUid: resolvedAuthUid,
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

    if (resolvedAuthUid) {
      await upsertClientUserLink(resolvedAuthUid, selectedClientId, email);
    }

    const provisionSynced = await upsertClientProvisionRecord(
      selectedClientId,
      { ...updates, authUid: resolvedAuthUid },
      { previousEmail: existingClient?.email || "" }
    );

    if (provisionSynced) {
    let message = "Client saved and provisioning updated.";
    if (authSync.created && authSync.resetSent) {
      message = `Client saved. Auth created and reset link sent to ${email}.`;
    } else if (authSync.created && !authSync.resetSent) {
      message = "Client saved. Auth created, but reset email failed. Use managed user reset.";
    } else if (!manualAuthUid && authSync.accountExists && authSync.requiresManualUid) {
      message = "Client saved. Auth account exists but UID is unresolved; user can sign in to auto-link.";
    }
      setElementStatus("managedProvisionStatus", message, false);
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
  const requestedAuthUid = String(data.authUid || "").trim();
  const linkedUid =
    String(options.linkedUid || "").trim()
    || requestedAuthUid
    || String(currentManagedUsers.find((u) => normalizeEmail(u.email || "") === email)?.id || "");

  await setDoc(doc(db, "login_provisioning", email), {
    email,
    role,
    name: String(data.name || ""),
    notes: String(data.notes || ""),
    clientId,
    status,
    authUid: linkedUid,
    updatedAt: serverTimestamp(),
    updatedByUid: auth.currentUser?.uid || "",
    createdAt: serverTimestamp(),
    createdByUid: auth.currentUser?.uid || "",
  }, { merge: true });

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
    const nextAuthUid = linkedUid || requestedAuthUid;
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
  const existingProvision = currentProvisioning.find((entry) => normalizeEmail(entry.email || "") === email);

  try {
    const authSync = existingProvision
      ? { uid: String(existingProvision.authUid || ""), created: false, resetSent: false, accountExists: Boolean(existingProvision.authUid), requiresManualUid: false }
      : await ensureProvisionAuthCredential(email, role, { existingUid: "" });

    await upsertManagedProvision(
      { email, role, clientId, name, notes, status: "active", authUid: authSync.uid || "" },
      { linkedUid: authSync.uid || "" }
    );

    let message = "Provision saved.";
    if (authSync.created && authSync.resetSent) {
      message = `Provision saved. Auth created and reset link sent to ${email}.`;
    } else if (authSync.created && !authSync.resetSent) {
      message = "Provision saved. Auth created, but reset email failed. Use 'Send Reset Link' from the user row.";
    } else if (!existingProvision && authSync.accountExists && authSync.requiresManualUid) {
      message = "Provision saved. Auth account already exists but UID could not be resolved automatically; user can sign in to auto-link.";
    } else if (!existingProvision && authSync.accountExists) {
      message = "Provision saved. Existing Auth account detected and linked.";
    }
    setElementStatus("managedProvisionStatus", message, false);

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
    let authSync = { uid: String(record.uid || ""), created: false, resetSent: false, accountExists: Boolean(record.uid), requiresManualUid: false };
    if (!record.uid && status !== "disabled") {
      authSync = await ensureProvisionAuthCredential(email, role, { existingUid: "" });
    }

    await upsertManagedProvision(
      { email, role, clientId, name, notes, status, authUid: authSync.uid || "" },
      { previousEmail: record.email || "", linkedUid: authSync.uid || "" }
    );
    let message = "Provision updated.";
    if (authSync.created && authSync.resetSent) {
      message = `Provision updated. Auth created and reset link sent to ${email}.`;
    } else if (authSync.created && !authSync.resetSent) {
      message = "Provision updated. Auth created, but reset email failed. Send reset manually from this modal.";
    } else if (!record.uid && authSync.accountExists && authSync.requiresManualUid) {
      message = "Provision updated. Existing Auth account detected but UID could not be resolved automatically.";
    } else if (!record.uid && authSync.accountExists) {
      message = "Provision updated. Existing Auth account linked.";
    }
    setElementStatus("managedUsersStatus", message, false);
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

  if (!confirm("Revoke this user's access? This disables provisioning and removes linked profile access. (Spark mode does not auto-delete Firebase Auth credentials.)")) return;

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

    setElementStatus("managedUsersStatus", "Access revoked in app. Firebase Auth credential remains unless removed in backend/console.", false);
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

  const record = editingManagedRecordKey ? managedRecordsByKey.get(editingManagedRecordKey) : null;
  const role = String(
    record?.role
      || document.getElementById("mu_role")?.value
      || "client"
  ).toLowerCase();
  const returnLabel = role === "client" ? "Client Panel" : "Team Workspace";

  try {
    await sendPasswordResetEmail(auth, email, resetActionCodeSettingsForRole(role));
    setElementStatus("managedUsersStatus", `Password reset sent to ${email}. Return path: ${returnLabel}.`, false);
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

function requestBillingState(req = {}) {
  const direct = String(req.billingState || "").toLowerCase();
  if (direct === "paid" || direct === "unpaid") return direct;
  if (req.unpaidAtSubmission === true) return "unpaid";
  if (req.unpaidAtSubmission === false) return "paid";
  return "";
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

  timeline.sort((a, b) => b.createdAtMs - a.createdAtMs);
  return timeline;
}

function renderRequestTimelineHtml(req, includeInternal = true) {
  const timeline = buildRequestTimeline(req, includeInternal);
  const timelineContent = timeline.length
    ? `
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
    `
    : `<div class="empty-state" style="padding:14px;font-size:10px">No timeline updates yet.</div>`;

  return `
    <div class="timeline-toolbar">
      <span class="date-text">Newest first</span>
      <button class="sort-pill" type="button" onclick="toggleTeamTimeline()">
        ${teamTimelineExpanded ? "Collapse" : "Expand"}
      </button>
    </div>
    <div class="request-timeline-shell${teamTimelineExpanded ? "" : " collapsed"}">
      ${timelineContent}
    </div>
  `;
}

window.toggleTeamTimeline = function () {
  teamTimelineExpanded = !teamTimelineExpanded;
  if (editingRequestId) {
    const req = currentClientRequests.find((item) => item.id === editingRequestId);
    if (req) window.openRequestDetail(req.id);
  }
};

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
    body.innerHTML = `<tr><td colspan="7"><div class="empty-state" style="margin:24px 0">${message}</div></td></tr>`;
    return;
  }

  visibleRequests.forEach((req) => {
    const tr = document.createElement("tr");
    tr.classList.add("request-row");
    if (req.archived === true) tr.classList.add("request-row-archived");
    tr.tabIndex = 0;
    tr.setAttribute("role", "button");
    tr.setAttribute("aria-label", `Open request ${String(req.title || "Untitled request")}`);
    tr.addEventListener("click", () => openRequestDetail(req.id));
    tr.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        openRequestDetail(req.id);
      }
    });
    const updatedDate = req.updatedAt?.toDate
      ? req.updatedAt.toDate().toLocaleDateString("en-US", { month: "short", day: "numeric" })
      : (req.createdAt?.toDate
        ? req.createdAt.toDate().toLocaleDateString("en-US", { month: "short", day: "numeric" })
        : "—");
    const ownerCell = assigneeChipHtml(req.ownerUid, req.ownerName);
    const statusPill = req.archived === true
      ? `<span class="req-status rs-archived">Archived</span>`
      : `<span class="req-status ${requestStatusClass(req.status)}">${requestStatusLabel(req.status)}</span>`;
    const billingState = requestBillingState(req);
    const unpaidBadge = billingState === "unpaid"
      ? `<span class="timeline-chip unpaid">Unpaid</span>`
      : "";
    tr.innerHTML = `
      <td>${escHtml(req.clientName || "Unknown")}</td>
      <td class="request-title-cell">
        <div class="request-title-text">${escHtml(req.title || "Untitled request")}</div>
        ${unpaidBadge ? `<div class="request-title-meta">${unpaidBadge}</div>` : ""}
      </td>
      <td>${escHtml(req.category || "general")}</td>
      <td>${escHtml(req.priority || "normal")}</td>
      <td>${ownerCell}</td>
      <td>${statusPill}</td>
      <td><span class="date-text">${updatedDate}</span></td>
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

window.togglePipelineColumnCollapse = function (key) {
  if (!key) return;
  if (collapsedPipelineCols.has(key)) {
    collapsedPipelineCols.delete(key);
  } else {
    collapsedPipelineCols.add(key);
  }
  renderPipeline(currentPipeline);
};

window.toggleLeadOverflow = function () {
  showAllLeads = !showAllLeads;
  renderPipeline(currentPipeline);
};

window.setPipelineFocusFilter = function (nextFilter) {
  const normalized = PIPELINE_FOCUS_FILTERS.includes(String(nextFilter || ""))
    ? String(nextFilter || "")
    : "all";
  if (pipelineFocusFilter === normalized && normalized !== "all") {
    pipelineFocusFilter = "all";
  } else {
    pipelineFocusFilter = normalized;
  }
  renderPipeline(currentPipeline);
};

window.copyLeadSalesBrief = async function (leadId) {
  const id = String(leadId || "");
  const card = currentPipeline.find((entry) => entry.id === id);
  if (!card) {
    setPipelineRadarNotice("Could not copy brief: lead not found.", true);
    return;
  }
  const brief = buildLeadSalesBrief(card);
  try {
    await navigator.clipboard.writeText(formatSalesBriefText(card, brief));
    setPipelineRadarNotice(`Sales brief copied for ${card.title || "lead"}.`, false);
  } catch (err) {
    console.error("copyLeadSalesBrief:", err);
    setPipelineRadarNotice("Clipboard access was blocked. Open lead detail and copy manually.", true);
  }
};

window.attachSalesBriefToLead = async function (leadId) {
  const id = String(leadId || "");
  const card = currentPipeline.find((entry) => entry.id === id);
  if (!card) {
    setPipelineRadarNotice("Could not attach brief: lead not found.", true);
    return;
  }
  const brief = buildLeadSalesBrief(card);
  const stamp = formatDateTimeLabel(Date.now());
  const briefBlock = [
    `[Sales Brief ${stamp}]`,
    `Recommended Service: ${brief.recommendedService}`,
    `Opportunity: ${brief.opportunityLabel} (${brief.score}/100)`,
    `Likely Needs: ${brief.likelyNeeds.join("; ")}`,
    `Outreach Angle: ${brief.outreachAngle}`,
    `Summary: ${brief.summary}`,
  ].join("\n");
  const currentNote = String(card.note || "").trim();
  const nextNote = currentNote.includes(brief.summary)
    ? currentNote
    : (currentNote ? `${currentNote}\n\n${briefBlock}` : briefBlock);

  try {
    await updateDoc(doc(db, "pipeline", id), {
      note: nextNote,
      updatedAt: serverTimestamp(),
    });
    currentPipeline = currentPipeline.map((entry) => (
      entry.id === id
        ? { ...entry, note: nextNote, updatedAt: Date.now() }
        : entry
    ));
    renderPipeline(currentPipeline);
    setPipelineRadarNotice("Sales brief attached to lead notes.", false);
  } catch (err) {
    console.error("attachSalesBriefToLead:", err);
    setPipelineRadarNotice("Could not attach sales brief to lead note.", true);
  }
};

window.downloadLeadResearchCsvTemplate = function () {
  const headers = [
    "title",
    "company",
    "contact",
    "email",
    "phone",
    "website",
    "source",
    "sourceUrl",
    "serviceHint",
    "notes",
    "confidence",
    "city",
    "state",
    "country",
  ];
  const sample = [
    "Northside Dental",
    "Northside Dental",
    "Practice Manager",
    "hello@northsidedental.example",
    "(555) 010-2040",
    "https://northsidedental.example",
    "OpenStreetMap",
    "https://www.openstreetmap.org/node/123456789",
    "Website Build",
    "No booking automation on website; likely fit for website + workflow upgrade.",
    "72",
    "Austin",
    "TX",
    "USA",
  ];
  const csv = [headers.join(","), sample.map((value) => `"${String(value).replace(/"/g, "\"\"")}"`).join(",")].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const href = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = href;
  a.download = "lead-research-template.csv";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(href);
  setLeadResearchStatus("Lead research CSV template downloaded.", false);
};

window.copyLeadResearchWorkflowCommand = async function () {
  const command = [
    "python tools/osm_lead_research.py",
    "--city \"Austin\"",
    "--state \"TX\"",
    "--categories dentist,plumber,hvac,roofing",
    "--max-results-per-category 40",
    "--output-csv exports/austin-research.csv",
  ].join(" ");
  try {
    await navigator.clipboard.writeText(command);
    setLeadResearchStatus("Script command copied. Run it in terminal, then import the generated CSV.", false);
  } catch (err) {
    console.error("copyLeadResearchWorkflowCommand:", err);
    setLeadResearchStatus(`Run this command: ${command}`, false);
  }
};

window.triggerLeadResearchCsvPicker = function () {
  const input = document.getElementById("leadResearchCsvInput");
  if (!input) {
    setLeadResearchStatus("Lead research CSV picker is unavailable.", true);
    return;
  }
  input.click();
};

window.ingestLeadResearchCsvFile = async function (inputEl) {
  const file = inputEl?.files?.[0];
  if (!file) return;

  try {
    const text = await file.text();
    const { headers, records } = parseCsvText(text);
    if (!headers.length || !records.length) {
      setLeadResearchStatus("CSV appears empty. Include a header row and at least one lead row.", true);
      return;
    }

    const blockedHeaders = headers.filter((header) =>
      LEAD_RESEARCH_BLOCKED_HEADERS.some((blocked) => header.includes(blocked))
    );
    if (blockedHeaders.length) {
      setLeadResearchStatus(`Blocked sensitive column(s): ${blockedHeaders.join(", ")}. Remove these and retry.`, true);
      return;
    }

    const existingPipelineEmail = new Set(
      currentPipeline
        .map((card) => normalizeEmail(leadPrimaryEmail(card)))
        .filter(Boolean)
    );
    const existingPipelineCompany = new Set(
      currentPipeline
        .map((card) => normalizeLeadEntityKey(card.company || card.title || ""))
        .filter((key) => key.length >= 5)
    );
    const existingResearchEmail = new Set(
      currentLeadResearchImports
        .map((record) => leadResearchEmailKey(record))
        .filter(Boolean)
    );
    const existingResearchCompany = new Set(
      currentLeadResearchImports
        .map((record) => leadResearchCompanyKey(record))
        .filter((key) => key.length >= 5)
    );

    let created = 0;
    let skippedDuplicate = 0;
    let skippedInvalid = 0;
    let skippedCompliance = 0;

    const rows = records.slice(0, LEAD_RESEARCH_ROW_LIMIT);
    for (const entry of rows) {
      const titleValue = pickCsvValue(entry, ["title", "leadname", "lead", "businessname", "business", "company", "name"]);
      const companyValue = pickCsvValue(entry, ["company", "businessname", "business", "organization", "name"]) || titleValue;
      const title = titleValue || companyValue;
      const contact = pickCsvValue(entry, ["contact", "contactname", "owner", "decisionmaker", "person", "fullname"]);
      const email = normalizeEmail(pickCsvValue(entry, ["email", "businessemail", "contactemail", "emailaddress"]));
      const phone = pickCsvValue(entry, ["phone", "telephone", "mobile", "phonenumber"]);
      const website = toAbsoluteWebsiteUrl(pickCsvValue(entry, ["website", "domain", "site", "web", "companywebsite"]));
      const sourceLabel = pickCsvValue(entry, ["source", "directory", "platform", "sourcelabel", "list"]);
      const sourceUrl = toAbsoluteWebsiteUrl(pickCsvValue(entry, ["sourceurl", "listingurl", "profileurl", "link"]));
      const sourceKey = sourceKeyFromLabelOrUrl(sourceLabel, sourceUrl);
      const serviceHint = pickCsvValue(entry, ["service", "servicehint", "need", "needs", "category", "interest"]);
      const note = pickCsvValue(entry, ["notes", "note", "qualification", "qualificationnotes", "context", "painpoint", "details", "summary"]);
      const confidence = coerceConfidenceValue(pickCsvValue(entry, ["confidence", "score", "fit", "opportunityscore", "leadscore"]));
      const city = pickCsvValue(entry, ["city", "locationcity", "town"]);
      const state = pickCsvValue(entry, ["state", "province", "region"]);
      const country = pickCsvValue(entry, ["country", "nation"]);

      if (!title) {
        skippedInvalid += 1;
        continue;
      }
      if (!sourceLabel && !sourceUrl) {
        skippedCompliance += 1;
        continue;
      }

      const emailKey = normalizeEmail(email);
      const companyKey = normalizeLeadEntityKey(companyValue || title);
      const isDuplicate = (emailKey && (existingPipelineEmail.has(emailKey) || existingResearchEmail.has(emailKey)))
        || (companyKey.length >= 5 && (existingPipelineCompany.has(companyKey) || existingResearchCompany.has(companyKey)));
      if (isDuplicate) {
        skippedDuplicate += 1;
        continue;
      }

      await addDoc(collection(db, "lead_research_imports"), {
        title,
        company: companyValue,
        contact,
        email,
        phone,
        website,
        serviceHint,
        note,
        confidence,
        publicSourceLabel: sourceLabel,
        publicSourceUrl: sourceUrl,
        researchSourceKey: sourceKey,
        city,
        state,
        country,
        sourceFile: file.name,
        status: "pending",
        createdAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
        createdByUid: auth.currentUser?.uid || "",
        createdByEmail: auth.currentUser?.email || "",
      });
      created += 1;
      if (emailKey) existingResearchEmail.add(emailKey);
      if (companyKey.length >= 5) existingResearchCompany.add(companyKey);
    }

    const limitSuffix = records.length > LEAD_RESEARCH_ROW_LIMIT
      ? ` Limited to first ${LEAD_RESEARCH_ROW_LIMIT} rows.`
      : "";
    setLeadResearchStatus(
      `Imported ${created} staged leads.${limitSuffix} Skipped: duplicates ${skippedDuplicate}, invalid ${skippedInvalid}, missing source attribution ${skippedCompliance}.`,
      created === 0
    );
  } catch (err) {
    console.error("ingestLeadResearchCsvFile:", err);
    setLeadResearchStatus("Could not parse/import CSV. Check encoding and header format.", true);
  } finally {
    if (inputEl) inputEl.value = "";
  }
};

window.copyResearchSalesBrief = async function (recordId) {
  const id = String(recordId || "");
  const record = currentLeadResearchImports.find((entry) => entry.id === id);
  if (!record) {
    setLeadResearchStatus("Could not copy brief: staged lead not found.", true);
    return;
  }
  const candidate = leadResearchCandidateFromRecord(record);
  const brief = buildLeadSalesBrief(candidate);
  try {
    await navigator.clipboard.writeText(formatSalesBriefText(candidate, brief));
    setLeadResearchStatus(`Sales brief copied for ${candidate.title}.`, false);
  } catch (err) {
    console.error("copyResearchSalesBrief:", err);
    setLeadResearchStatus("Clipboard access was blocked. Open lead row and copy manually.", true);
  }
};

window.approveResearchLead = async function (recordId) {
  const id = String(recordId || "");
  if (!id) return;
  try {
    await updateDoc(doc(db, "lead_research_imports", id), {
      status: "approved",
      reviewedAt: serverTimestamp(),
      reviewedByUid: auth.currentUser?.uid || "",
      reviewedByName: auth.currentUser?.email || "",
      updatedAt: serverTimestamp(),
    });
    setLeadResearchStatus("Lead approved for pipeline import.", false);
  } catch (err) {
    console.error("approveResearchLead:", err);
    setLeadResearchStatus("Could not approve staged lead.", true);
  }
};

window.rejectResearchLead = async function (recordId) {
  const id = String(recordId || "");
  if (!id) return;
  const reason = prompt("Optional rejection reason (shown internally):", "");
  if (reason === null) return;
  try {
    await updateDoc(doc(db, "lead_research_imports", id), {
      status: "rejected",
      rejectionReason: String(reason || "").trim(),
      reviewedAt: serverTimestamp(),
      reviewedByUid: auth.currentUser?.uid || "",
      reviewedByName: auth.currentUser?.email || "",
      updatedAt: serverTimestamp(),
    });
    setLeadResearchStatus("Lead marked as rejected.", false);
  } catch (err) {
    console.error("rejectResearchLead:", err);
    setLeadResearchStatus("Could not reject staged lead.", true);
  }
};

async function importResearchLeadRecord(record) {
  const id = String(record?.id || "");
  if (!id) return { ok: false, reason: "missing-id" };
  const status = normalizeLeadResearchStatus(record?.status);
  if (status !== "approved") return { ok: false, reason: "not-approved" };

  const emailKey = leadResearchEmailKey(record);
  const companyKey = leadResearchCompanyKey(record);
  const duplicateInPipeline = currentPipeline.some((card) => {
    const pipelineEmail = normalizeEmail(leadPrimaryEmail(card));
    const pipelineCompany = normalizeLeadEntityKey(card.company || card.title || "");
    return (emailKey && pipelineEmail && emailKey === pipelineEmail)
      || (companyKey.length >= 5 && pipelineCompany === companyKey);
  });
  if (duplicateInPipeline) {
    return { ok: false, reason: "duplicate" };
  }

  const candidate = leadResearchCandidateFromRecord(record);
  const brief = buildLeadSalesBrief(candidate);
  const sourceMeta = leadResearchSourceMeta(record);
  const sourceLabel = sourceMeta.sourceLabel;
  const sourceUrl = sourceMeta.sourceUrl;
  const sourceKey = sourceMeta.sourceKey;
  const confidence = coerceConfidenceValue(record.confidence);
  const noteParts = [
    `Research import source: ${sourceLabel}${sourceUrl ? ` (${sourceUrl})` : ""}`,
    `Confidence: ${confidence}/100`,
    [record.city, record.state, record.country].filter(Boolean).join(", ")
      ? `Location: ${[record.city, record.state, record.country].filter(Boolean).join(", ")}`
      : "",
    record.note ? `Research notes: ${record.note}` : "",
    `Sales brief:\n- Recommended Service: ${brief.recommendedService}\n- Opportunity: ${brief.opportunityLabel} (${brief.score}/100)\n- Outreach Angle: ${brief.outreachAngle}\n- Summary: ${brief.summary}`,
  ].filter(Boolean);
  const contactLabel = [record.contact, record.email, record.phone].filter(Boolean).join(" | ").slice(0, 200);

  const pipelineRef = await addDoc(collection(db, "pipeline"), {
    title: candidate.title,
    company: candidate.company,
    contact: contactLabel,
    service: brief.recommendedService,
    ownerUid: TEAM_ASSIGNMENT_UID,
    ownerName: "Entire Team",
    status: "leads",
    source: `research-${sourceKey}`,
    researchSourceKey: sourceKey,
    researchSourceLabel: sourceLabel,
    researchSourceUrl: sourceUrl,
    researchConfidence: confidence,
    researchImportId: id,
    researchImportedAt: serverTimestamp(),
    note: noteParts.join("\n\n"),
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
  });

  await updateDoc(doc(db, "lead_research_imports", id), {
    status: "imported",
    importedAt: serverTimestamp(),
    importedPipelineId: pipelineRef.id,
    reviewedAt: serverTimestamp(),
    reviewedByUid: auth.currentUser?.uid || "",
    reviewedByName: auth.currentUser?.email || "",
    updatedAt: serverTimestamp(),
  });
  return { ok: true, pipelineId: pipelineRef.id };
}

window.importResearchLead = async function (recordId) {
  const id = String(recordId || "");
  const record = currentLeadResearchImports.find((entry) => entry.id === id);
  if (!record) {
    setLeadResearchStatus("Could not import: staged lead not found.", true);
    return;
  }
  try {
    const result = await importResearchLeadRecord(record);
    if (result.ok) {
      setLeadResearchStatus("Approved lead imported into pipeline.", false);
      return;
    }
    if (result.reason === "not-approved") {
      setLeadResearchStatus("Approve the staged lead before importing.", true);
      return;
    }
    if (result.reason === "duplicate") {
      setLeadResearchStatus("Skipped import: duplicate already exists in pipeline.", true);
      return;
    }
    setLeadResearchStatus("Could not import staged lead.", true);
  } catch (err) {
    console.error("importResearchLead:", err);
    setLeadResearchStatus("Could not import staged lead.", true);
  }
};

window.importApprovedResearchLeads = async function () {
  const approved = currentLeadResearchImports
    .filter((entry) => normalizeLeadResearchStatus(entry.status) === "approved")
    .slice(0, LEAD_RESEARCH_ROW_LIMIT);
  if (!approved.length) {
    setLeadResearchStatus("No approved staged leads available for import.", true);
    return;
  }

  let imported = 0;
  let skippedDuplicate = 0;
  let failed = 0;

  for (const record of approved) {
    try {
      const result = await importResearchLeadRecord(record);
      if (result.ok) imported += 1;
      else if (result.reason === "duplicate") skippedDuplicate += 1;
      else failed += 1;
    } catch (err) {
      console.error("importApprovedResearchLeads item:", err);
      failed += 1;
    }
  }

  setLeadResearchStatus(
    `Bulk import complete. Imported ${imported}, duplicates skipped ${skippedDuplicate}, failed ${failed}.`,
    failed > 0
  );
};

window.clearRejectedResearchLeads = async function () {
  const rejected = currentLeadResearchImports.filter((entry) => normalizeLeadResearchStatus(entry.status) === "rejected");
  if (!rejected.length) {
    setLeadResearchStatus("No rejected rows to clear.", true);
    return;
  }
  if (!confirm(`Delete ${rejected.length} rejected staged leads?`)) return;
  try {
    for (const row of rejected) {
      await deleteDoc(doc(db, "lead_research_imports", row.id));
    }
    setLeadResearchStatus(`Cleared ${rejected.length} rejected staged leads.`, false);
  } catch (err) {
    console.error("clearRejectedResearchLeads:", err);
    setLeadResearchStatus("Could not clear rejected staged leads.", true);
  }
};

window.updateLeadListFilter = function (field, rawValue) {
  const key = String(field || "").trim();
  if (!key) return;
  if (key === "limit") {
    const nextLimit = Math.max(1, Math.min(200, Number(rawValue || LEAD_LIST_DEFAULT_FILTERS.limit)));
    leadListFilters.limit = Number.isFinite(nextLimit) ? nextLimit : LEAD_LIST_DEFAULT_FILTERS.limit;
  } else {
    leadListFilters[key] = String(rawValue || "");
  }
  renderLeadListBuilder(currentPipeline);
};

window.resetLeadListFilters = function () {
  leadListFilters = { ...LEAD_LIST_DEFAULT_FILTERS };
  setLeadListStatus("Lead list filters reset.", false);
};

window.generateLeadList = function () {
  renderLeadListBuilder(currentPipeline);
  const callNowCount = generatedLeadList.filter((lead) =>
    lead.followUpBucket === "overdue" || lead.followUpBucket === "due_today"
  ).length;
  const duplicateCount = generatedLeadList.filter((lead) => lead.duplicateCount > 1).length;
  setLeadListStatus(`Generated ${generatedLeadList.length} leads | Call now: ${callNowCount} | Duplicates: ${duplicateCount}.`, false);
};

window.applyLeadListPreset = function (presetKey) {
  const key = String(presetKey || "");
  if (key === "website_inbound") {
    leadListFilters = {
      ...LEAD_LIST_DEFAULT_FILTERS,
      stage: "open",
      source: "website",
      owner: "any",
      timeframe: "30",
      followUp: "all",
      duplicate: "all",
      sort: "newest",
      limit: 50,
    };
    setLeadListStatus("Preset loaded: website inbound leads (30 days).", false);
    return;
  }
  if (key === "follow_up_queue") {
    leadListFilters = {
      ...LEAD_LIST_DEFAULT_FILTERS,
      stage: "open",
      source: "all",
      owner: "unassigned",
      timeframe: "30",
      followUp: "call_now",
      duplicate: "all",
      sort: "priority",
      limit: 25,
    };
    setLeadListStatus("Preset loaded: unassigned follow-up queue.", false);
    return;
  }
  if (key === "daily_call_list") {
    leadListFilters = {
      ...LEAD_LIST_DEFAULT_FILTERS,
      stage: "open",
      source: "all",
      owner: "any",
      timeframe: "30",
      followUp: "call_now",
      duplicate: "all",
      sort: "followup",
      limit: 40,
    };
    setLeadListStatus("Preset loaded: daily call list (due today + overdue).", false);
    return;
  }
  if (key === "duplicates_review") {
    leadListFilters = {
      ...LEAD_LIST_DEFAULT_FILTERS,
      stage: "open",
      source: "all",
      owner: "any",
      timeframe: "90",
      followUp: "all",
      duplicate: "duplicates",
      sort: "priority",
      limit: 60,
    };
    setLeadListStatus("Preset loaded: duplicates review queue.", false);
    return;
  }
  setLeadListStatus("Unknown lead-list preset.", true);
};

window.copyLeadListEmails = async function () {
  const emails = [...new Set(generatedLeadList.map((lead) => lead.email).filter(Boolean))];
  if (!emails.length) {
    setLeadListStatus("No emails found in the current lead list.", true);
    return;
  }
  try {
    await navigator.clipboard.writeText(emails.join(", "));
    setLeadListStatus(`Copied ${emails.length} emails to clipboard.`, false);
  } catch (err) {
    console.error("copyLeadListEmails:", err);
    setLeadListStatus("Clipboard access was blocked. Use CSV export instead.", true);
  }
};

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

window.downloadLeadListCsv = function () {
  if (!generatedLeadList.length) {
    setLeadListStatus("No leads available to export.", true);
    return;
  }
  const header = [
    "Rank",
    "Lead",
    "Company",
    "Contact",
    "Email",
    "Stage",
    "Outcome",
    "Source",
    "Owner",
    "Priority",
    "PriorityScore",
    "FollowUpDate",
    "FollowUpBucket",
    "DuplicateCount",
    "DuplicateType",
    "MergeMatches",
    "CreatedAt",
    "UpdatedAt",
    "LeadId",
  ];
  const rows = generatedLeadList.map((lead, index) => [
    index + 1,
    lead.title,
    lead.company,
    lead.contactLabel,
    lead.email,
    lead.stageLabel,
    lead.outcomeLabel,
    lead.sourceLabel,
    lead.ownerLabel,
    lead.priorityLabel,
    lead.priorityScore,
    lead.nextFollowUpMs ? formatDateTimeLabel(lead.nextFollowUpMs) : "",
    lead.followUpBucket,
    lead.duplicateCount,
    lead.duplicateType,
    lead.mergeMatches.join("|"),
    formatDateTimeLabel(lead.createdMs),
    formatDateTimeLabel(lead.updatedMs),
    lead.id,
  ]);
  const csv = [header, ...rows].map((row) => row.map(csvEscape).join(",")).join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  const stamp = new Date().toISOString().slice(0, 10);
  a.href = url;
  a.download = `lead-list-${stamp}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
  setLeadListStatus(`Downloaded CSV with ${generatedLeadList.length} leads.`, false);
};

function findMergeCandidateIds(primaryLeadId) {
  const primary = currentPipeline.find((card) => card.id === primaryLeadId);
  if (!primary) return [];
  const primaryEmail = leadPrimaryEmail(primary);
  const primaryCompanyKey = normalizeLeadEntityKey(primary?.company || primary?.title || "");
  return currentPipeline
    .filter((card) => card.id !== primaryLeadId)
    .filter((card) => !isPipelineLeadClosed(card))
    .filter((card) => {
      const emailMatch = primaryEmail && leadPrimaryEmail(card) === primaryEmail;
      const companyMatch = primaryCompanyKey.length >= 5
        && normalizeLeadEntityKey(card?.company || card?.title || "") === primaryCompanyKey;
      return Boolean(emailMatch || companyMatch);
    })
    .map((card) => card.id);
}

function mergeAssistantLeadRowHtml(card, options = {}) {
  const roleLabel = options.primary ? "Primary lead" : "Duplicate candidate";
  const createdMs = requestTimelineMs(card?.createdAt) || pipelineLeadUpdatedMs(card);
  const followUpMs = leadNextFollowUpMs(card);
  const followUpBucket = followUpBucketForMs(followUpMs);
  const followUpLabel = followUpLabelForBucket(followUpBucket, followUpMs);
  const followClass = followUpBucket === "overdue"
    ? "lead-followup-overdue"
    : followUpBucket === "due_today"
      ? "lead-followup-today"
      : followUpBucket === "next_7"
        ? "lead-followup-next"
        : "lead-followup-later";
  return `
    <div class="merge-assist-row${options.primary ? " primary" : ""}">
      <div class="merge-assist-head">
        <div>
          <div class="merge-assist-title">${escHtml(card?.title || "Untitled lead")}</div>
          <div class="merge-assist-note">${escHtml(card?.company || "No company")} | ${escHtml(card?.contact || "No contact")}</div>
        </div>
        <span class="merge-assist-badge">${escHtml(roleLabel)}</span>
      </div>
      <div class="merge-assist-note">
        Stage: ${escHtml(pipelineStatusLabel(card?.status))} | Created: ${escHtml(formatDateTimeLabel(createdMs))} | Follow-up: <span class="lead-followup-chip ${followClass}">${escHtml(followUpLabel)}</span>
      </div>
      <div class="merge-assist-note">${assigneeChipHtml(card?.ownerUid, card?.ownerName, "Unassigned")}</div>
      ${
        options.primary
          ? ""
          : `<div class="merge-assist-actions">
               <button class="card-btn" type="button" onclick="openLeadDetail('${escHtmlAttr(card?.id || "")}')">Open Lead</button>
               <button class="card-btn" type="button" onclick="mergeLeadIntoPrimary('${escHtmlAttr(card?.id || "")}')">Merge Into Primary</button>
             </div>`
      }
    </div>
  `;
}

window.openLeadMergeAssistant = function (leadId) {
  const primaryId = String(leadId || "");
  if (!primaryId) return;
  const primary = currentPipeline.find((card) => card.id === primaryId);
  if (!primary) {
    setLeadListStatus("Could not open merge assistant: lead not found.", true);
    return;
  }
  mergePrimaryLeadId = primaryId;
  const leadListEntry = generatedLeadList.find((lead) => lead.id === primaryId);
  const candidateIds = leadListEntry?.mergeMatches?.length
    ? [...leadListEntry.mergeMatches]
    : findMergeCandidateIds(primaryId);
  const candidates = candidateIds
    .map((id) => currentPipeline.find((card) => card.id === id))
    .filter(Boolean);

  const body = document.getElementById("leadMergeBody");
  if (!body) return;
  body.innerHTML = `
    <div class="merge-assist-list">
      ${mergeAssistantLeadRowHtml(primary, { primary: true })}
      ${
        candidates.length
          ? candidates.map((card) => mergeAssistantLeadRowHtml(card, { primary: false })).join("")
          : `<div class="empty-state" style="padding:14px;font-size:10px">No active duplicate candidates found for this lead.</div>`
      }
    </div>
  `;
  document.getElementById("leadMergeOverlay")?.classList.add("open");
};

window.closeLeadMergeAssistant = function () {
  document.getElementById("leadMergeOverlay")?.classList.remove("open");
  mergePrimaryLeadId = null;
};

window.mergeLeadIntoPrimary = async function (secondaryId) {
  const primaryId = String(mergePrimaryLeadId || "");
  const dupId = String(secondaryId || "");
  if (!primaryId || !dupId || primaryId === dupId) return;

  const primary = currentPipeline.find((card) => card.id === primaryId);
  const secondary = currentPipeline.find((card) => card.id === dupId);
  if (!primary || !secondary) {
    setLeadListStatus("Merge failed: lead records could not be found.", true);
    return;
  }

  const primaryWasUnassigned = isPipelineLeadUnassigned(primary);
  const nextOwnerUid = primaryWasUnassigned ? String(secondary.ownerUid || "") : String(primary.ownerUid || "");
  const nextOwnerName = nextOwnerUid
    ? resolveTeamUserName(nextOwnerUid, primary.ownerName || secondary.ownerName || "")
    : String(primary.ownerName || secondary.ownerName || "");
  const primaryStatus = String(primary.status || "leads");
  const secondaryStatus = String(secondary.status || "leads");
  const nextStatus = primaryStatus === "closed" && secondaryStatus !== "closed"
    ? secondaryStatus
    : primaryStatus;

  const primaryUpdates = {
    title: String(primary.title || secondary.title || "Untitled lead"),
    company: String(primary.company || secondary.company || ""),
    contact: String(primary.contact || secondary.contact || ""),
    service: String(primary.service || secondary.service || ""),
    ownerUid: nextOwnerUid,
    ownerName: nextOwnerName,
    status: nextStatus || "leads",
    note: mergeLeadNoteText(primary.note, secondary.note, secondary.id),
    updatedAt: serverTimestamp(),
  };

  const secondaryUpdates = {
    status: "closed",
    note: mergeArchivedNoteText(secondary.note, primaryId, primaryUpdates.title),
    updatedAt: serverTimestamp(),
  };

  try {
    await updateDoc(doc(db, "pipeline", primaryId), primaryUpdates);
    await updateDoc(doc(db, "pipeline", dupId), secondaryUpdates);

    const nowMs = Date.now();
    currentPipeline = currentPipeline.map((card) => {
      if (card.id === primaryId) {
        return { ...card, ...primaryUpdates, updatedAt: nowMs };
      }
      if (card.id === dupId) {
        return { ...card, ...secondaryUpdates, updatedAt: nowMs };
      }
      return card;
    });

    renderPipeline(currentPipeline);
    setLeadListStatus(`Merged lead #${dupId.slice(0, 6)} into #${primaryId.slice(0, 6)}.`, false);
    setPipelineRadarNotice("Duplicate lead merged and archived.", false);

    const remainingMatches = findMergeCandidateIds(primaryId);
    if (remainingMatches.length) {
      window.openLeadMergeAssistant(primaryId);
    } else {
      window.closeLeadMergeAssistant();
    }
  } catch (err) {
    console.error("mergeLeadIntoPrimary:", err);
    setLeadListStatus("Could not merge leads. Please try again.", true);
  }
};

window.claimOldestUnassignedLead = async function () {
  const currentUid = String(auth.currentUser?.uid || "");
  if (!currentUid) {
    setPipelineRadarNotice("Sign in again to claim leads.", true);
    renderPipeline(currentPipeline);
    return;
  }

  const target = oldestUnassignedPipelineLead(currentPipeline);
  if (!target) {
    setPipelineRadarNotice("No unassigned open leads to claim.", false);
    renderPipeline(currentPipeline);
    return;
  }

  const fallbackLabel = String(auth.currentUser?.displayName || auth.currentUser?.email || "Current User");
  const ownerName = resolveTeamUserName(currentUid, fallbackLabel);

  try {
    await updateDoc(doc(db, "pipeline", target.id), {
      ownerUid: currentUid,
      ownerName,
      updatedAt: serverTimestamp(),
    });
    currentPipeline = currentPipeline.map((card) => (
      card.id === target.id
        ? { ...card, ownerUid: currentUid, ownerName, updatedAt: Date.now() }
        : card
    ));
    setPipelineRadarNotice(`Claimed ${target.title || "lead"} as ${ownerName}.`, false);
  } catch (err) {
    console.error("claimOldestUnassignedLead:", err);
    setPipelineRadarNotice("Could not claim lead. Please try again.", true);
  }
  renderPipeline(currentPipeline);
};

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
  const billingState = requestBillingState(req);
  const billingLabel = billingState === "unpaid"
    ? `<span class="timeline-chip unpaid">Unpaid at Submission</span>`
    : (billingState === "paid"
      ? `<span class="timeline-chip paid">Paid at Submission</span>`
      : `<span class="timeline-chip internal">Billing Snapshot Unknown</span>`);
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
      <div class="form-group">
        <label class="form-label">Billing Snapshot</label>
        <div class="detail-meta-value">${billingLabel}</div>
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
  const nextOwnerName = isTeamAssignmentUid(nextOwnerUid)
    ? "Entire Team"
    : (nextOwnerUid ? resolveTeamUserName(nextOwnerUid, "") : "");
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

    const canEdit = canEditDecisions();
    const items = Array.isArray(entry.items) ? entry.items : [];
    const itemsHtml = items.length
      ? items.map((item) => {
        const itemText = String(item.text || "");
        const itemOwner = String(item.owner || "");
        const editing = canEdit && editingDecisionItems.has(decisionItemEditKey(entry.id, item.id));
        const subtitle = itemOwner || "No subtitle";
        return `
      <div class="decision-item${editing ? " is-editing" : ""}" data-item-id="${item.id}">
        <div class="decision-item-header">
          <div class="decision-item-text-wrap">
            ${
              editing
                ? `<div class="decision-item-fields">
                     <input class="form-input decision-item-input" id="dec_text_${escHtmlAttr(entry.id)}_${escHtmlAttr(item.id)}" value="${escHtmlAttr(itemText)}" placeholder="Decision text">
                     <input class="form-input decision-item-owner-input" id="dec_owner_${escHtmlAttr(entry.id)}_${escHtmlAttr(item.id)}" value="${escHtmlAttr(itemOwner)}" placeholder="Subtitle (optional)">
                   </div>`
                : `<div class="decision-text">${escHtml(itemText || "Untitled decision")}</div>
                   <div class="decision-subtitle">${escHtml(subtitle)}</div>`
            }
          </div>
          <div class="decision-item-actions">
            ${
              canEdit && editing
                ? `<button
                     class="card-btn"
                     onclick="saveDecisionItem('${escHtmlAttr(entry.id)}', '${escHtmlAttr(item.id)}')"
                     title="Save this item"
                   >Save</button>
                   <button
                     class="card-btn"
                     onclick="cancelDecisionItemEdit('${escHtmlAttr(entry.id)}', '${escHtmlAttr(item.id)}')"
                     title="Cancel editing"
                   >Cancel</button>`
                : canEdit
                  ? `<button
                     class="card-btn"
                     onclick="startDecisionItemEdit('${escHtmlAttr(entry.id)}', '${escHtmlAttr(item.id)}')"
                     title="Edit this item"
                   >Edit</button>`
                  : ""
            }
            ${
              canEdit
                ? `<button
              class="delete-entry-btn item-delete"
              onclick="deleteDecisionItem('${escHtmlAttr(entry.id)}', '${escHtmlAttr(item.id)}')"
              title="Delete this item"
            >&times;</button>`
                : ""
            }
          </div>
        </div>
      </div>
    `;
      }).join("")
      : `<div class="empty-state" style="padding:14px;font-size:10px">No decision items yet.</div>`;

    el.innerHTML = `
      <div class="decision-date">
        <div class="decision-date-left">
          <span class="decision-title">${escHtml(titleStr)}</span>
          ${dateStr ? `<span class="decision-date-tag">${dateStr}</span>` : ""}
        </div>
        ${
          canEdit
            ? `<button class="card-btn" onclick="addDecisionItem('${escHtmlAttr(entry.id)}')">+ Add Item</button>`
            : `<span class="decision-date-tag">View only (super admin edits)</span>`
        }
      </div>
      ${itemsHtml}
    `;
    log.appendChild(el);
  });
}

function decisionItemEditKey(docId, itemId) {
  return `${String(docId || "")}::${String(itemId || "")}`;
}

window.startDecisionItemEdit = function (docId, itemId) {
  if (!canEditDecisions()) return;
  editingDecisionItems.add(decisionItemEditKey(docId, itemId));
  renderDecisions(currentDecisions);
};

window.cancelDecisionItemEdit = function (docId, itemId) {
  if (!canEditDecisions()) return;
  editingDecisionItems.delete(decisionItemEditKey(docId, itemId));
  renderDecisions(currentDecisions);
};

window.saveDecisionItem = async function (docId, itemId) {
  if (!canEditDecisions()) {
    alert("Only super admins can edit decisions.");
    return;
  }
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
    editingDecisionItems.delete(decisionItemEditKey(docId, itemId));
    currentDecisions = currentDecisions.map((decision) =>
      decision.id === docId ? { ...decision, items } : decision
    );
    renderDecisions(currentDecisions);
  } catch (err) {
    console.error("saveDecisionItem:", err);
    alert("Could not save decision item.");
  }
};

window.addDecisionItem = async function (docId) {
  if (!canEditDecisions()) {
    alert("Only super admins can edit decisions.");
    return;
  }
  const entry = currentDecisions.find((d) => d.id === docId);
  if (!entry) return;

  const items = Array.isArray(entry.items) ? [...entry.items] : [];
  const newItem = { id: uid(), text: "New decision", owner: "" };
  items.push(newItem);

  try {
    await updateDoc(doc(db, "decisions", docId), { items });
    editingDecisionItems.add(decisionItemEditKey(docId, newItem.id));
    currentDecisions = currentDecisions.map((decision) =>
      decision.id === docId ? { ...decision, items } : decision
    );
    renderDecisions(currentDecisions);
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
  if (!canEditDecisions()) {
    alert("Only super admins can edit decisions.");
    return;
  }
  const entry = currentDecisions.find((d) => d.id === docId);
  if (!entry) return;

  editingDecisionItems.delete(decisionItemEditKey(docId, itemId));
  const remaining = (entry.items || []).filter((i) => i.id !== itemId);

  if (remaining.length === 0) {
    // Last item removed - ask before deleting the whole meeting entry
    if (!confirm("This is the last item in this meeting entry. Delete the entire entry?")) return;
    try {
      await deleteDoc(doc(db, "decisions", docId));
      currentDecisions = currentDecisions.filter((decision) => decision.id !== docId);
      renderDecisions(currentDecisions);
    }
    catch (err) { console.error("deleteDecisionItem (doc):", err); }
  } else {
    try {
      await updateDoc(doc(db, "decisions", docId), { items: remaining });
      currentDecisions = currentDecisions.map((decision) =>
        decision.id === docId ? { ...decision, items: remaining } : decision
      );
      renderDecisions(currentDecisions);
    }
    catch (err) { console.error("deleteDecisionItem (update):", err); }
  }
};

// ─── ADD MODAL ────────────────────────────────────────────────────────────────
window.openAddModal = function (type, context) {
  const supportedModes = new Set(["pipeline", "tasks", "decisions", "clients"]);
  const fallbackMode = supportedModes.has(currentPage) ? currentPage : "pipeline";
  modalMode    = type || fallbackMode;
  modalContext = context || "";

  if (modalMode === "decisions" && !canEditDecisions()) {
    alert("Only super admins can log or edit decisions.");
    return;
  }

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

  if (modalMode === "tasks") {
    document.getElementById("f_taskOwnerUid")?.addEventListener("change", () => {
      syncTaskOwnerOtherField("f_taskOwnerUid", "f_taskOwnerOtherWrap", "f_taskOwnerOther");
    });
    syncTaskOwnerOtherField("f_taskOwnerUid", "f_taskOwnerOtherWrap", "f_taskOwnerOther");
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
    const ownerOptions = taskAssigneeOptionsHtml({ ownerUid: TEAM_ASSIGNMENT_UID, ownerName: "Entire Team", ownerType: "team", owner: "Entire Team" });
    return `
      <div class="form-group">
        <label class="form-label">Task</label>
        <input class="form-input" id="f_name" placeholder="What needs to get done?">
      </div>
      <div class="form-group">
        <label class="form-label">Assigned To</label>
        <select class="form-select" id="f_taskOwnerUid">
          ${ownerOptions}
        </select>
      </div>
      <div class="form-group" id="f_taskOwnerOtherWrap" style="display:none">
        <label class="form-label">Other Assignee</label>
        <input class="form-input" id="f_taskOwnerOther" placeholder="Type custom assignee">
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
        <label class="form-label">Client Auth UID (Optional Fallback)</label>
        <input class="form-input" id="f_authUid" placeholder="Leave blank to auto-create and send reset link">
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
  if (!canEditDecisions()) return;
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
        <input class="form-input decision-row-owner" placeholder="Subtitle (optional)">
      </div>
    </div>
  `;
}

window.addDecisionRow = function () {
  if (!canEditDecisions()) return;
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
  if (!canEditDecisions()) return;
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
      const ownerName = isTeamAssignmentUid(ownerUid)
        ? "Entire Team"
        : (ownerUid ? resolveTeamUserName(ownerUid, "") : "");
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
      const owner = resolveTaskOwnerFromInputs("f_taskOwnerUid", "f_taskOwnerOther");
      if (!owner) {
        alert("Please type a custom assignee name or choose a team assignee.");
        return;
      }
      await addDoc(collection(db, "tasks"), {
        name,
        owner:     owner.owner,
        ownerUid:  owner.ownerUid,
        ownerName: owner.ownerName,
        ownerType: owner.ownerType,
        due:       document.getElementById("f_due").value || "",
        priority:  document.getElementById("f_priority").value,
        done:      false,
        createdAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
      });
    }

    else if (modalMode === "decisions") {
      if (!canEditDecisions()) {
        alert("Only super admins can log or edit decisions.");
        return;
      }
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
      let authSync = { uid: authUid, created: false, resetSent: false, accountExists: Boolean(authUid), requiresManualUid: false };
      if (!authUid && email && isSuperAdminRole(currentUserRole)) {
        authSync = await ensureProvisionAuthCredential(email, "client", { existingUid: "" });
      }
      const resolvedAuthUid = String(authSync.uid || "");

      const clientRef = await addDoc(collection(db, "clients"), {
        companyName,
        contactName,
        email,
        emailLower: email,
        authUid: resolvedAuthUid,
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

      if (resolvedAuthUid) {
        await upsertClientUserLink(resolvedAuthUid, clientRef.id, email);
      }

      const provisionSynced = await upsertClientProvisionRecord(clientRef.id, {
        companyName,
        contactName,
        email,
        authUid: resolvedAuthUid,
        notes,
      });
      if (provisionSynced) {
        let message = "Client created and provisioning synced.";
        if (authSync.created && authSync.resetSent) {
          message = `Client created. Auth created and reset link sent to ${email}.`;
        } else if (authSync.created && !authSync.resetSent) {
          message = "Client created. Auth created, but reset email failed. Send reset from managed users.";
        } else if (!authUid && authSync.accountExists && authSync.requiresManualUid) {
          message = "Client created. Auth exists but UID could not be resolved automatically.";
        }
        setElementStatus("managedProvisionStatus", message, false);
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
    mywork: "MY WORK",
    tasks: "TASKS",
    decisions: "DECISIONS LOG",
    clients: "CLIENT OPS",
  };
  const el2    = document.getElementById("pageTitle");
  if (el2) el2.textContent = titles[name] || name.toUpperCase();

  if (name === "mywork") {
    renderMyWorkDashboard();
  }
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
renderMyWorkDashboard();



