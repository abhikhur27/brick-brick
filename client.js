import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import {
  getAuth,
  signInWithEmailAndPassword,
  signOut,
  onAuthStateChanged,
  sendPasswordResetEmail,
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
import {
  getStorage,
  ref as storageRef,
  uploadBytes,
  listAll,
  getDownloadURL,
  getMetadata,
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-storage.js";
import { firebaseConfig } from "./firebase-config.js";

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const storage = getStorage(app);
const CLIENT_RESET_RETURN_URL = new URL("/reset-password.html?portal=client", window.location.origin).toString();
const REQUEST_ATTACHMENT_MAX_FILES = 6;
const REQUEST_ATTACHMENT_MAX_BYTES = 10 * 1024 * 1024;

let currentClientId = null;
let currentClient = null;
let unsubClientDoc = null;
let unsubRequests = null;
let currentRequests = [];
let activeRequestId = null;
let clientTimelineExpanded = true;
let pendingRequestAttachments = [];
const requestAttachmentCache = new Map();

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
const requestAttachmentInput = document.getElementById("crAttachmentsInput");
const requestAttachmentBrowseBtn = document.getElementById("crAttachBrowseBtn");
const requestAttachmentDropzone = document.getElementById("crAttachmentDropzone");
const requestAttachmentList = document.getElementById("crAttachmentsList");
const requestDetailOverlay = document.getElementById("clientRequestDetailOverlay");
const requestDetailBody = document.getElementById("clientRequestDetailBody");
const requestCloseBtn = document.getElementById("clientRequestCloseBtn");

applyClientLoginContextMessage();

loginForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  setClientLoginMessage("", true);
  const email = loginEmail.value.trim();
  const password = loginPassword.value;
  if (!email || !password) {
    setClientLoginMessage("Enter email and password.", true);
    return;
  }

  loginBtn.disabled = true;
  loginBtn.textContent = "SIGNING IN...";
  try {
    await signInWithEmailAndPassword(auth, email, password);
  } catch (err) {
    setClientLoginMessage(friendlyAuthError(err.code), true);
    loginBtn.disabled = false;
    loginBtn.textContent = "ENTER CLIENT PANEL";
  }
});

window.sendClientReset = async function () {
  const email = normalizeEmail(loginEmail?.value || "");
  const resetBtn = document.getElementById("clientResetBtn");
  if (!email) {
    setClientLoginMessage("Enter your email first, then click reset.", true);
    return;
  }

  if (resetBtn) {
    resetBtn.disabled = true;
    resetBtn.textContent = "Sending...";
  }
  try {
    await sendPasswordResetEmail(auth, email, {
      url: CLIENT_RESET_RETURN_URL,
      handleCodeInApp: false,
    });
    setClientLoginMessage(`Reset email sent to ${email}. After reset you will return to Client Panel.`, false);
  } catch (err) {
    console.error("sendClientReset:", err);
    setClientLoginMessage(friendlyAuthError(err.code), true);
  } finally {
    if (resetBtn) {
      resetBtn.disabled = false;
      resetBtn.textContent = "Forgot password?";
    }
  }
};

logoutBtn?.addEventListener("click", async () => {
  stopClientListeners();
  await signOut(auth);
});

requestAttachmentBrowseBtn?.addEventListener("click", () => {
  requestAttachmentInput?.click();
});

requestAttachmentInput?.addEventListener("change", (event) => {
  const files = Array.from(event.target?.files || []);
  addPendingRequestAttachments(files, "Selected");
  if (requestAttachmentInput) requestAttachmentInput.value = "";
});

requestAttachmentDropzone?.addEventListener("click", () => {
  requestAttachmentInput?.click();
});

requestAttachmentDropzone?.addEventListener("keydown", (event) => {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    requestAttachmentInput?.click();
  }
});

requestAttachmentDropzone?.addEventListener("dragover", (event) => {
  event.preventDefault();
  requestAttachmentDropzone.classList.add("is-active");
});

requestAttachmentDropzone?.addEventListener("dragleave", () => {
  requestAttachmentDropzone.classList.remove("is-active");
});

requestAttachmentDropzone?.addEventListener("drop", (event) => {
  event.preventDefault();
  requestAttachmentDropzone.classList.remove("is-active");
  const files = Array.from(event.dataTransfer?.files || []);
  addPendingRequestAttachments(files, "Added");
});

requestForm?.addEventListener("paste", (event) => {
  const files = filesFromClipboardEvent(event);
  if (!files.length) return;
  event.preventDefault();
  addPendingRequestAttachments(files, "Pasted");
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
  const billingState = currentClient?.paid === false ? "unpaid" : "paid";
  const unpaidAtSubmission = billingState === "unpaid";

  if (!title || !description) {
    setRequestStatus("Please include a title and description.", true);
    return;
  }

  requestBtn.disabled = true;
  requestBtn.textContent = "Submitting...";

  try {
    const requestRef = await addDoc(collection(db, "client_requests"), {
      clientId: currentClientId,
      clientName: currentClient.companyName || currentClient.contactName || "Client",
      clientEmail: currentClient.email || auth.currentUser?.email || "",
      title,
      description,
      category,
      priority,
      status: "submitted",
      billingState,
      unpaidAtSubmission,
      createdAt: serverTimestamp(),
      updatedAt: serverTimestamp(),
      source: "client-dashboard",
    });

    let uploadedCount = 0;
    let failedCount = 0;
    if (pendingRequestAttachments.length) {
      const uploadResult = await uploadRequestAttachmentsForClient({
        clientId: currentClientId,
        requestId: requestRef.id,
        files: pendingRequestAttachments,
      });
      uploadedCount = uploadResult.uploaded;
      failedCount = uploadResult.failed;
    }

    requestForm.reset();
    const priorityEl = document.getElementById("crPriority");
    if (priorityEl) priorityEl.value = "normal";
    clearPendingRequestAttachments();

    const attachmentNote = uploadedCount
      ? `${uploadedCount} file${uploadedCount === 1 ? "" : "s"} attached.`
      : "";
    const attachmentWarn = failedCount
      ? ` ${failedCount} file${failedCount === 1 ? "" : "s"} failed to upload.`
      : "";
    if (unpaidAtSubmission) {
      setRequestStatus(
        `Request submitted and marked unpaid for billing follow-up. We will still review it shortly. ${attachmentNote}${attachmentWarn}`.trim(),
        false
      );
    } else {
      setRequestStatus(`Request submitted. We will review it shortly. ${attachmentNote}${attachmentWarn}`.trim(), false);
    }
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
      setClientLoginMessage(access.error || "No client profile is linked to this account yet.", true);
      return;
    }

    const clientId = String(access.userData.clientId || "");
    if (!clientId) {
      await signOut(auth);
      setClientLoginMessage("Client profile is not linked correctly. Contact support.", true);
      return;
    }

    currentClientId = clientId;
    setLoggedInState(user.email || "");
    startClientListeners(clientId);
  } catch (err) {
    console.error("client auth state:", err);
    await signOut(auth);
    setClientLoginMessage("Could not complete login. Please try again.", true);
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
  setClientLoginMessage("", true);
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
  requestAttachmentCache.clear();
  clearPendingRequestAttachments();
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
  text("cpPhone", client.phone || "-");
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
  void ensureClientRequestAttachments(req);
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
      <div class="client-detail-label">Attachments</div>
      ${renderClientAttachmentSection(req)}
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

function requestAttachmentPrefix(clientId, requestId) {
  return `client_request_uploads/${String(clientId || "").trim()}/${String(requestId || "").trim()}`;
}

function sanitizeAttachmentName(name) {
  return String(name || "attachment")
    .replace(/[\\/:*?"<>|]/g, "-")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 120) || "attachment";
}

function requestAttachmentFingerprint(file) {
  return `${String(file?.name || "")}:${Number(file?.size || 0)}:${Number(file?.lastModified || 0)}`;
}

function formatBytes(bytes) {
  const value = Number(bytes || 0);
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function filesFromClipboardEvent(event) {
  const seen = new Set();
  const files = [];
  const clipboardFiles = Array.from(event.clipboardData?.files || []);
  clipboardFiles.forEach((file) => {
    const key = requestAttachmentFingerprint(file);
    if (!seen.has(key)) {
      seen.add(key);
      files.push(file);
    }
  });
  const items = Array.from(event.clipboardData?.items || []);
  items.forEach((item) => {
    if (item.kind !== "file") return;
    const file = item.getAsFile();
    if (!file) return;
    const key = requestAttachmentFingerprint(file);
    if (!seen.has(key)) {
      seen.add(key);
      files.push(file);
    }
  });
  return files;
}

function addPendingRequestAttachments(files, sourceLabel = "Added") {
  const nextFiles = Array.isArray(files) ? files : [];
  if (!nextFiles.length) return;
  const existingKeys = new Set(pendingRequestAttachments.map((file) => requestAttachmentFingerprint(file)));
  let added = 0;
  let skipped = 0;

  nextFiles.forEach((file) => {
    if (!(file instanceof File)) {
      skipped += 1;
      return;
    }
    if (file.size > REQUEST_ATTACHMENT_MAX_BYTES) {
      skipped += 1;
      return;
    }
    if (pendingRequestAttachments.length >= REQUEST_ATTACHMENT_MAX_FILES) {
      skipped += 1;
      return;
    }
    const key = requestAttachmentFingerprint(file);
    if (existingKeys.has(key)) {
      skipped += 1;
      return;
    }
    existingKeys.add(key);
    pendingRequestAttachments.push(file);
    added += 1;
  });

  renderPendingRequestAttachments();
  if (added > 0) {
    setRequestStatus(`${sourceLabel} ${added} attachment${added === 1 ? "" : "s"}.`, false);
  } else if (skipped > 0) {
    setRequestStatus(
      `No new attachments added. Limit is ${REQUEST_ATTACHMENT_MAX_FILES} files, ${formatBytes(REQUEST_ATTACHMENT_MAX_BYTES)} each.`,
      true
    );
  }
}

function clearPendingRequestAttachments() {
  pendingRequestAttachments = [];
  renderPendingRequestAttachments();
}

function renderPendingRequestAttachments() {
  if (!requestAttachmentList) return;
  if (!pendingRequestAttachments.length) {
    requestAttachmentList.innerHTML = "";
    return;
  }
  requestAttachmentList.innerHTML = pendingRequestAttachments
    .map((file, index) => `
      <div class="request-attachment-row">
        <div class="request-attachment-main">
          <div class="request-attachment-name">${escHtml(file.name || "Attachment")}</div>
          <div class="request-attachment-meta">${escHtml(formatBytes(file.size))}</div>
        </div>
        <button class="request-attachment-remove" type="button" data-index="${index}" aria-label="Remove attachment">&times;</button>
      </div>
    `)
    .join("");
  requestAttachmentList.querySelectorAll(".request-attachment-remove").forEach((btn) => {
    btn.addEventListener("click", () => {
      const index = Number(btn.getAttribute("data-index"));
      if (!Number.isFinite(index) || index < 0 || index >= pendingRequestAttachments.length) return;
      pendingRequestAttachments.splice(index, 1);
      renderPendingRequestAttachments();
    });
  });
}

async function uploadRequestAttachmentsForClient({ clientId, requestId, files }) {
  const scopedFiles = Array.isArray(files) ? files.slice(0, REQUEST_ATTACHMENT_MAX_FILES) : [];
  if (!scopedFiles.length) return { uploaded: 0, failed: 0 };
  const prefix = requestAttachmentPrefix(clientId, requestId);
  let uploaded = 0;
  let failed = 0;

  for (const file of scopedFiles) {
    const safeName = sanitizeAttachmentName(file.name);
    const randomId = (typeof window !== "undefined" && window.crypto && typeof window.crypto.randomUUID === "function")
      ? window.crypto.randomUUID()
      : Math.random().toString(36).slice(2);
    const storagePath = `${prefix}/${Date.now()}_${randomId}_${safeName}`;
    try {
      await uploadBytes(storageRef(storage, storagePath), file, {
        contentType: file.type || "application/octet-stream",
        customMetadata: {
          originalName: String(file.name || safeName).slice(0, 150),
          requestId: String(requestId || ""),
          clientId: String(clientId || ""),
          uploadedBy: String(auth.currentUser?.uid || ""),
        },
      });
      uploaded += 1;
    } catch (err) {
      console.error("uploadRequestAttachmentsForClient:", err);
      failed += 1;
    }
  }

  return { uploaded, failed };
}

function attachmentDisplayName(storageName, metadata) {
  const fromMetadata = String(metadata?.customMetadata?.originalName || "").trim();
  if (fromMetadata) return fromMetadata;
  return String(storageName || "Attachment");
}

async function listRequestAttachments(clientId, requestId) {
  if (!clientId || !requestId) return [];
  const listing = await listAll(storageRef(storage, requestAttachmentPrefix(clientId, requestId)));
  const attachments = await Promise.all(
    listing.items.map(async (item) => {
      let metadata = null;
      try {
        metadata = await getMetadata(item);
      } catch (err) {
        console.warn("attachment metadata fetch skipped:", err);
      }
      const url = await getDownloadURL(item);
      return {
        name: attachmentDisplayName(item.name, metadata),
        size: Number(metadata?.size || 0),
        contentType: String(metadata?.contentType || ""),
        url,
      };
    })
  );
  attachments.sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
  return attachments;
}

async function ensureClientRequestAttachments(req) {
  if (!req?.id || !req?.clientId) return;
  const cached = requestAttachmentCache.get(req.id);
  if (cached?.loaded || cached?.loading) return;
  requestAttachmentCache.set(req.id, { loading: true, loaded: false, items: [], error: "" });
  if (activeRequestId === req.id) renderClientRequestDetail(req);
  try {
    const items = await listRequestAttachments(req.clientId, req.id);
    requestAttachmentCache.set(req.id, { loading: false, loaded: true, items, error: "" });
  } catch (err) {
    console.error("ensureClientRequestAttachments:", err);
    requestAttachmentCache.set(req.id, {
      loading: false,
      loaded: true,
      items: [],
      error: "Could not load attachments right now.",
    });
  }
  if (activeRequestId === req.id) {
    const fresh = currentRequests.find((item) => item.id === req.id) || req;
    renderClientRequestDetail(fresh);
  }
}

function renderClientAttachmentSection(req) {
  const state = requestAttachmentCache.get(req.id);
  if (!state || state.loading) {
    return `<div class="empty-state">Loading attachments...</div>`;
  }
  if (state.error) {
    return `<div class="empty-state">${escHtml(state.error)}</div>`;
  }
  if (!state.items.length) {
    return `<div class="empty-state">No attachments uploaded for this request.</div>`;
  }
  return `
    <div class="client-attachments-list">
      ${state.items.map((item) => `
        <a class="client-attachment-link" href="${escHtmlAttr(item.url)}" target="_blank" rel="noopener" download="${escHtmlAttr(item.name)}">
          <div>
            <div>${escHtml(item.name)}</div>
            <div class="client-attachment-meta">${escHtml(item.contentType || "File")} ${item.size ? `| ${escHtml(formatBytes(item.size))}` : ""}</div>
          </div>
          <span class="timeline-chip">Download</span>
        </a>
      `).join("")}
    </div>
  `;
}

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
  const deduped = [];
  const seen = new Set();
  timeline.forEach((entry) => {
    const minuteBucket = Math.floor(Number(entry.createdAtMs || 0) / 60000);
    const signature = [
      String(entry.kind || ""),
      String(entry.status || ""),
      String(entry.text || "").trim(),
      String(minuteBucket),
    ].join("|");
    if (seen.has(signature)) return;
    seen.add(signature);
    deduped.push(entry);
  });
  return deduped;
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

function setClientLoginMessage(message, isError = true) {
  if (!loginErr) return;
  loginErr.textContent = String(message || "");
  loginErr.classList.toggle("is-success", !isError && Boolean(message));
}

function applyClientLoginContextMessage() {
  const params = new URLSearchParams(window.location.search);
  if (params.get("reset") === "success") {
    setClientLoginMessage("Password reset complete. Sign in to continue.", false);
    params.delete("reset");
    const nextQuery = params.toString();
    const nextUrl = `${window.location.pathname}${nextQuery ? `?${nextQuery}` : ""}${window.location.hash || ""}`;
    window.history.replaceState({}, "", nextUrl);
  }
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
