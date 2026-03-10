import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import {
  getAuth,
  signInWithEmailAndPassword,
  signOut,
  onAuthStateChanged,
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import {
  getFirestore,
  doc,
  getDoc,
  setDoc,
  getDocs,
  onSnapshot,
  collection,
  query,
  where,
  limit,
  addDoc,
  serverTimestamp,
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import { firebaseConfig } from "./firebase-config.js";

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);

let currentClientId = null;
let currentClient = null;
let unsubClientDoc = null;
let unsubRequests = null;
let currentRequests = [];
let activeRequestId = null;
let clientTimelineExpanded = true;

document.body.style.visibility = "hidden";

const loginForm = document.getElementById("clientLoginForm");
const loginEmail = document.getElementById("clientLoginEmail");
const loginPassword = document.getElementById("clientLoginPassword");
const loginBtn = document.getElementById("clientLoginBtn");
const loginErr = document.getElementById("clientLoginError");

const loginScreen = document.getElementById("clientLoginScreen");
const appScreen = document.getElementById("clientApp");
const logoutBtn = document.getElementById("clientLogoutBtn");

const requestForm = document.getElementById("clientRequestForm");
const requestBtn = document.getElementById("crSubmitBtn");
const requestStatus = document.getElementById("crStatus");
const requestDetailOverlay = document.getElementById("clientRequestDetailOverlay");
const requestDetailBody = document.getElementById("clientRequestDetailBody");
const requestCloseBtn = document.getElementById("clientRequestCloseBtn");

loginForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  loginErr.textContent = "";
  const email = loginEmail.value.trim();
  const password = loginPassword.value;
  if (!email || !password) {
    loginErr.textContent = "Enter email and password.";
    return;
  }

  loginBtn.disabled = true;
  loginBtn.textContent = "SIGNING IN...";
  try {
    await signInWithEmailAndPassword(auth, email, password);
  } catch (err) {
    loginErr.textContent = friendlyAuthError(err.code);
    loginBtn.disabled = false;
    loginBtn.textContent = "ENTER CLIENT PANEL";
  }
});

logoutBtn?.addEventListener("click", async () => {
  stopClientListeners();
  await signOut(auth);
});

requestForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  clearRequestStatus();

  if (!currentClientId || !currentClient) {
    setRequestStatus("Client record is not loaded yet. Please refresh.", true);
    return;
  }

  const title = document.getElementById("crTitle")?.value.trim() || "";
  const description = document.getElementById("crDescription")?.value.trim() || "";
  const category = document.getElementById("crCategory")?.value || "general_support";
  const priority = document.getElementById("crPriority")?.value || "normal";

  if (!title || !description) {
    setRequestStatus("Please include a title and description.", true);
    return;
  }

  requestBtn.disabled = true;
  requestBtn.textContent = "Submitting...";

  try {
    await addDoc(collection(db, "client_requests"), {
      clientId: currentClientId,
      clientName: currentClient.companyName || currentClient.contactName || "Client",
      clientEmail: currentClient.email || auth.currentUser?.email || "",
      title,
      description,
      category,
      priority,
      status: "submitted",
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
      source: "client-dashboard",
    });

    requestForm.reset();
    const priorityEl = document.getElementById("crPriority");
    if (priorityEl) priorityEl.value = "normal";
    setRequestStatus("Request submitted. We will review it shortly.", false);
  } catch (err) {
    console.error("client request submit:", err);
    setRequestStatus("Could not submit your request. Please try again.", true);
  } finally {
    requestBtn.disabled = false;
    requestBtn.textContent = "Submit request";
  }
});

requestCloseBtn?.addEventListener("click", closeClientRequestDetail);
requestDetailOverlay?.addEventListener("click", (event) => {
  if (event.target === requestDetailOverlay) closeClientRequestDetail();
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && requestDetailOverlay?.classList.contains("open")) {
    closeClientRequestDetail();
  }
});

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

async function findClientByIdentity(uid, email, preferredClientId) {
  if (preferredClientId) {
    const preferredSnap = await getDoc(doc(db, "clients", preferredClientId));
    if (preferredSnap.exists()) {
      const preferred = { id: preferredSnap.id, ...preferredSnap.data() };
      if (
        String(preferred.authUid || "") === uid
        || normalizeEmail(preferred.email) === email
      ) {
        return { ok: true, client: preferred };
      }
      return {
        ok: false,
        error: "This login is not linked to the provisioned client record.",
      };
    }
  }

  const byUid = await getDocs(
    query(collection(db, "clients"), where("authUid", "==", uid), limit(2))
  );
  if (byUid.size > 1) {
    return { ok: false, error: "Multiple client records share this auth UID. Contact support." };
  }
  if (byUid.size === 1) {
    const clientDoc = byUid.docs[0];
    return { ok: true, client: { id: clientDoc.id, ...clientDoc.data() } };
  }

  const byEmail = await getDocs(
    query(collection(db, "clients"), where("email", "==", email), limit(2))
  );
  if (byEmail.size > 1) {
    return { ok: false, error: "Multiple client records share this email. Contact support." };
  }
  if (byEmail.size === 1) {
    const clientDoc = byEmail.docs[0];
    return { ok: true, client: { id: clientDoc.id, ...clientDoc.data() } };
  }

  const byEmailLower = await getDocs(
    query(collection(db, "clients"), where("emailLower", "==", email), limit(2))
  );
  if (byEmailLower.size > 1) {
    return { ok: false, error: "Multiple client records share this email. Contact support." };
  }
  if (byEmailLower.size === 1) {
    const clientDoc = byEmailLower.docs[0];
    return { ok: true, client: { id: clientDoc.id, ...clientDoc.data() } };
  }

  return { ok: false, error: "No client profile is linked to this account yet." };
}

async function bootstrapClientUserRecord(user) {
  const email = normalizeEmail(user?.email);
  if (!email) {
    return { ok: false, error: "Account email is missing. Contact support." };
  }

  let provisionedClientId = "";
  try {
    const provisionSnap = await getDoc(doc(db, "login_provisioning", email));
    if (provisionSnap.exists()) {
      const provision = provisionSnap.data() || {};
      if (String(provision.status || "active") === "disabled") {
        return { ok: false, error: "This client login is disabled. Contact support." };
      }
      const provisionRole = String(provision.role || "client");
      if (provisionRole !== "client") {
        return { ok: false, error: "This login is not assigned to the client dashboard." };
      }
      provisionedClientId = String(provision.clientId || "");
    }
  } catch (err) {
    console.error("client provisioning lookup:", err);
  }

  const match = await findClientByIdentity(user.uid, email, provisionedClientId);
  if (!match.ok) return match;

  const client = match.client;
  await setDoc(doc(db, "users", user.uid), {
    role: "client",
    clientId: client.id,
    email,
    name: String(client.contactName || client.companyName || ""),
    disabled: false,
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp(),
  }, { merge: true });

  return {
    ok: true,
    userData: {
      role: "client",
      clientId: client.id,
      email,
    },
  };
}

async function resolveClientAccess(user) {
  const userSnap = await getDoc(doc(db, "users", user.uid));
  if (userSnap.exists()) {
    const userData = userSnap.data() || {};
    if (String(userData.role || "") !== "client") {
      return { ok: false, error: "This login is not assigned to the client dashboard." };
    }
    if (userData.disabled === true) {
      return { ok: false, error: "This client login is disabled. Contact support." };
    }
    const clientId = String(userData.clientId || "");
    if (!clientId) {
      return { ok: false, error: "Client ID is missing on this account. Contact support." };
    }
    return { ok: true, userData: { ...userData, clientId } };
  }

  return bootstrapClientUserRecord(user);
}

onAuthStateChanged(auth, async (user) => {
  document.body.style.visibility = "visible";

  if (!user) {
    stopClientListeners();
    currentClientId = null;
    currentClient = null;
    clientTimelineExpanded = true;
    setLoggedOutState();
    return;
  }

  try {
    const access = await resolveClientAccess(user);
    if (!access.ok) {
      await signOut(auth);
      loginErr.textContent = access.error || "No client profile is linked to this account yet.";
      return;
    }

    const clientId = String(access.userData.clientId || "");
    if (!clientId) {
      await signOut(auth);
      loginErr.textContent = "Client profile is not linked correctly. Contact support.";
      return;
    }

    currentClientId = clientId;
    setLoggedInState(user.email || "");
    startClientListeners(clientId);
  } catch (err) {
    console.error("client auth state:", err);
    await signOut(auth);
    loginErr.textContent = "Could not complete login. Please try again.";
  }
});

function setLoggedOutState() {
  if (loginScreen) loginScreen.style.display = "flex";
  if (appScreen) appScreen.classList.remove("visible");
  if (loginBtn) {
    loginBtn.disabled = false;
    loginBtn.textContent = "ENTER CLIENT PANEL";
  }
}

function setLoggedInState(email) {
  if (loginScreen) loginScreen.style.display = "none";
  if (appScreen) appScreen.classList.add("visible");
  const signedIn = document.getElementById("cpSignedInAs");
  if (signedIn) signedIn.textContent = `Signed in as ${email}`;
  if (loginErr) loginErr.textContent = "";
}

function startClientListeners(clientId) {
  stopClientListeners();

  unsubClientDoc = onSnapshot(
    doc(db, "clients", clientId),
    (snap) => {
      if (!snap.exists()) {
        currentClient = null;
        renderMissingClient();
        return;
      }
      currentClient = { id: snap.id, ...snap.data() };
      renderClientSummary(currentClient);
    },
    (err) => {
      console.error("client doc listener:", err);
      renderMissingClient();
    }
  );

  unsubRequests = onSnapshot(
    query(collection(db, "client_requests"), where("clientId", "==", clientId)),
    (snap) => {
      const requests = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      requests.sort((a, b) => {
        const as = a.createdAt?.seconds || 0;
        const bs = b.createdAt?.seconds || 0;
        return bs - as;
      });
      currentRequests = requests;
      renderRequestRows(requests);
      if (activeRequestId) {
        const activeReq = currentRequests.find((req) => req.id === activeRequestId);
        if (activeReq) {
          renderClientRequestDetail(activeReq);
        } else {
          closeClientRequestDetail();
        }
      }
    },
    (err) => {
      console.error("client requests listener:", err);
      currentRequests = [];
      renderRequestRows([]);
      closeClientRequestDetail();
    }
  );
}

function stopClientListeners() {
  if (unsubClientDoc) unsubClientDoc();
  if (unsubRequests) unsubRequests();
  unsubClientDoc = null;
  unsubRequests = null;
  currentRequests = [];
  closeClientRequestDetail();
}

function renderMissingClient() {
  const company = document.getElementById("cpCompanyName");
  if (company) company.textContent = "Client record not found";
  renderRequestRows([]);
  closeClientRequestDetail();
}

function renderClientSummary(client) {
  const companyName = client.companyName || client.contactName || "Client Account";
  text("cpCompanyName", companyName);
  text("cpPlanName", client.planName || "Not set");
  text("cpRecurring", client.recurring ? "Recurring" : "One-Time");
  text("cpUpdatesRemaining", Number.isFinite(client.updatesRemaining) ? String(client.updatesRemaining) : "-");
  text("cpPaymentStatus", client.paid ? "Paid" : "Unpaid");
  text("cpLastPaymentNote", client.lastPaymentNote || "-");
  text("cpBillingNotes", client.billingNotes || "-");

  const status = String(client.status || "active");
  const statusEl = document.getElementById("cpAccountStatus");
  if (statusEl) {
    statusEl.textContent = statusLabel(status);
    statusEl.className = `status-pill ${statusClass(status)}`;
  }

  const hero = document.getElementById("clientHeroCard");
  if (hero) hero.classList.toggle("muted", status !== "active" || !client.recurring);

  const historyEl = document.getElementById("cpBillingHistory");
  if (historyEl) {
    const history = Array.isArray(client.billingHistory) ? client.billingHistory : [];
    if (!history.length) {
      historyEl.innerHTML = `<div class="empty-state">No billing history entries yet.</div>`;
    } else {
      historyEl.innerHTML = history
        .map((entry) => `<div class="history-item">${escHtml(entry.date || "-")} | ${escHtml(entry.note || "")}</div>`)
        .join("");
    }
  }
}

function renderRequestRows(requests) {
  const tbody = document.getElementById("clientRequestsBody");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (!requests.length) {
    tbody.innerHTML = `<tr><td colspan="5"><div class="empty-state">No requests submitted yet.</div></td></tr>`;
    return;
  }

  requests.forEach((req) => {
    const tr = document.createElement("tr");
    tr.className = "request-row";
    tr.setAttribute("tabindex", "0");
    tr.title = "Click to view full request details and timeline updates.";
    tr.innerHTML = `
      <td>${escHtml(formatTimestamp(req.createdAt))}</td>
      <td>${escHtml(req.title || "Untitled request")}</td>
      <td>${escHtml(categoryLabel(req.category))}</td>
      <td>${escHtml(priorityLabel(req.priority || "normal"))}</td>
      <td><span class="req-status ${requestStatusClass(req.status)}">${escHtml(statusLabel(req.status || "submitted"))}</span></td>
    `;
    tr.addEventListener("click", () => openClientRequestDetail(req.id));
    tr.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        openClientRequestDetail(req.id);
      }
    });
    tbody.appendChild(tr);
  });
}

function openClientRequestDetail(requestId) {
  const req = currentRequests.find((item) => item.id === requestId);
  if (!req || !requestDetailOverlay || !requestDetailBody) return;
  activeRequestId = requestId;
  renderClientRequestDetail(req);
  requestDetailOverlay.classList.add("open");
}

function closeClientRequestDetail() {
  requestDetailOverlay?.classList.remove("open");
  activeRequestId = null;
}

function renderClientRequestDetail(req) {
  if (!requestDetailBody) return;
  const timeline = buildClientTimeline(req);
  const timelineHtml = timeline.length
    ? timeline
      .map((entry) => {
        const statusChip = entry.status
          ? `<span class="req-status ${requestStatusClass(entry.status)}">${escHtml(statusLabel(entry.status))}</span>`
          : `<span class="timeline-chip">${escHtml(timelineKindLabel(entry.kind))}</span>`;
        return `
          <div class="timeline-item">
            <div class="timeline-item-head">
              <div class="timeline-item-top">
                ${statusChip}
                <span class="timeline-item-time">${escHtml(formatDateTime(entry.createdAtMs))}</span>
              </div>
              <div class="timeline-item-meta">${escHtml(entry.actor || "Team")}</div>
            </div>
            <div class="timeline-item-text">${escHtml(timelineEntryText(entry))}</div>
          </div>
        `;
      })
      .join("")
    : `<div class="empty-state">No timeline updates yet.</div>`;

  requestDetailBody.innerHTML = `
    <div class="client-detail-grid">
      <div class="client-detail-field">
        <div class="client-detail-label">Title</div>
        <div class="client-detail-value">${escHtml(req.title || "Untitled request")}</div>
      </div>
      <div class="client-detail-field">
        <div class="client-detail-label">Status</div>
        <div class="client-detail-value">
          <span class="req-status ${requestStatusClass(req.status)}">${escHtml(statusLabel(req.status || "submitted"))}</span>
        </div>
      </div>
      <div class="client-detail-field">
        <div class="client-detail-label">Category</div>
        <div class="client-detail-value">${escHtml(categoryLabel(req.category))}</div>
      </div>
      <div class="client-detail-field">
        <div class="client-detail-label">Priority</div>
        <div class="client-detail-value">${escHtml(priorityLabel(req.priority || "normal"))}</div>
      </div>
      <div class="client-detail-field">
        <div class="client-detail-label">Created</div>
        <div class="client-detail-value">${escHtml(formatDateTime(toMillis(req.createdAt)))}</div>
      </div>
      <div class="client-detail-field">
        <div class="client-detail-label">Last Updated</div>
        <div class="client-detail-value">${escHtml(formatDateTime(toMillis(req.updatedAt) || toMillis(req.createdAt)))}</div>
      </div>
    </div>

    <div class="client-detail-section">
      <div class="client-detail-label">Description</div>
      <div class="client-detail-description">${escHtml(req.description || "-")}</div>
    </div>

    <div class="client-detail-section">
      <div class="timeline-toolbar">
        <div class="client-detail-label">Timeline & Team Updates</div>
        <div class="timeline-toolbar-right">
          <span class="timeline-order-hint">Newest first</span>
          <button class="btn btn-ghost" type="button" onclick="toggleClientTimeline()">
            ${clientTimelineExpanded ? "Collapse" : "Expand"}
          </button>
        </div>
      </div>
      <div class="timeline-shell${clientTimelineExpanded ? "" : " collapsed"}">
        <div class="timeline-list">${timelineHtml}</div>
      </div>
    </div>
  `;
}

window.toggleClientTimeline = function () {
  clientTimelineExpanded = !clientTimelineExpanded;
  if (!activeRequestId) return;
  const req = currentRequests.find((item) => item.id === activeRequestId);
  if (req) renderClientRequestDetail(req);
};

function buildClientTimeline(req) {
  const timeline = [];
  const raw = Array.isArray(req.timeline) ? req.timeline : [];
  raw.forEach((entry) => {
    if (!entry || typeof entry !== "object") return;
    const visibility = String(entry.visibility || "client");
    if (visibility === "internal") return;
    timeline.push({
      id: String(entry.id || ""),
      kind: String(entry.kind || entry.type || "note"),
      status: String(entry.status || ""),
      text: String(entry.text || entry.note || entry.message || ""),
      actor: String(entry.actor || entry.author || "Team"),
      createdAtMs: toMillis(entry.createdAtMs)
        || toMillis(entry.createdAt)
        || toMillis(entry.timestamp),
    });
  });

  const createdMs = toMillis(req.createdAt) || toMillis(req.updatedAt) || Date.now();
  if (!timeline.some((entry) => entry.kind === "submitted")) {
    timeline.push({
      id: "submitted_fallback",
      kind: "submitted",
      status: "submitted",
      text: "Request submitted.",
      actor: req.clientName || "You",
      createdAtMs: createdMs,
    });
  }

  const currentStatus = String(req.status || "submitted");
  if (!timeline.some((entry) => entry.status === currentStatus)) {
    timeline.push({
      id: "status_fallback",
      kind: "status",
      status: currentStatus,
      text: `Current status: ${statusLabel(currentStatus)}.`,
      actor: "Team",
      createdAtMs: toMillis(req.updatedAt) || createdMs,
    });
  }

  timeline.sort((a, b) => b.createdAtMs - a.createdAtMs);
  return timeline;
}

function timelineEntryText(entry) {
  if (entry.text) return entry.text;
  if (entry.status) return `Status changed to ${statusLabel(entry.status)}.`;
  return "Team update posted.";
}

function timelineKindLabel(kind) {
  if (kind === "flow") return "Pipeline";
  if (kind === "status") return "Status";
  if (kind === "submitted") return "Submitted";
  return "Update";
}

function requestStatusClass(status) {
  if (status === "in_review") return "rs-in_review";
  if (status === "scheduled") return "rs-scheduled";
  if (status === "done") return "rs-done";
  return "rs-submitted";
}

function statusLabel(status) {
  if (status === "submitted") return "Submitted";
  if (status === "in_review") return "In Review";
  if (status === "scheduled") return "Scheduled";
  if (status === "done") return "Done";
  if (status === "inactive") return "Inactive";
  if (status === "one-time") return "One-Time";
  if (status === "completed") return "Completed";
  return "Active";
}

function statusClass(status) {
  if (status === "inactive") return "sp-inactive";
  if (status === "one-time") return "sp-one-time";
  if (status === "completed") return "sp-completed";
  return "sp-active";
}

function categoryLabel(category) {
  const map = {
    issue: "Issue",
    website_change: "Website Change",
    workflow_tweak: "Workflow Tweak",
    automation_adjustment: "Automation Adjustment",
    content_update: "Content Update",
    general_support: "General Support",
  };
  return map[category] || "General Support";
}

function priorityLabel(priority) {
  if (priority === "high") return "High";
  if (priority === "low") return "Low";
  return "Normal";
}

function formatTimestamp(ts) {
  if (!ts?.toDate) return "-";
  return ts.toDate().toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function formatDateTime(ms) {
  if (!ms) return "—";
  return new Date(ms).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function toMillis(value) {
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

function friendlyAuthError(code) {
  const map = {
    "auth/invalid-email": "Invalid email address.",
    "auth/user-not-found": "No account found with that email.",
    "auth/wrong-password": "Incorrect password.",
    "auth/invalid-credential": "Incorrect email or password.",
    "auth/too-many-requests": "Too many attempts. Try again later.",
    "auth/user-disabled": "This account has been disabled.",
  };
  return map[code] || "Login failed. Check your credentials.";
}

function setRequestStatus(message, isError) {
  if (!requestStatus) return;
  requestStatus.textContent = message;
  requestStatus.className = `form-status${isError ? " error" : ""}`;
}

function clearRequestStatus() {
  if (!requestStatus) return;
  requestStatus.textContent = "";
  requestStatus.className = "form-status";
}

function text(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function escHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
