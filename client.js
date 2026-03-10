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
  onSnapshot,
  collection,
  query,
  where,
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

onAuthStateChanged(auth, async (user) => {
  document.body.style.visibility = "visible";

  if (!user) {
    stopClientListeners();
    currentClientId = null;
    currentClient = null;
    setLoggedOutState();
    return;
  }

  const userSnap = await getDoc(doc(db, "users", user.uid));
  if (!userSnap.exists()) {
    await signOut(auth);
    loginErr.textContent = "No client profile is linked to this account yet.";
    return;
  }

  const userData = userSnap.data();
  if (String(userData.role || "") !== "client") {
    await signOut(auth);
    loginErr.textContent = "This login is not assigned to the client dashboard.";
    return;
  }

  const clientId = String(userData.clientId || "");
  if (!clientId) {
    await signOut(auth);
    loginErr.textContent = "Client ID is missing on this account. Contact support.";
    return;
  }

  currentClientId = clientId;
  setLoggedInState(user.email || "");
  startClientListeners(clientId);
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
      renderRequestRows(requests);
    },
    (err) => {
      console.error("client requests listener:", err);
      renderRequestRows([]);
    }
  );
}

function stopClientListeners() {
  if (unsubClientDoc) unsubClientDoc();
  if (unsubRequests) unsubRequests();
  unsubClientDoc = null;
  unsubRequests = null;
}

function renderMissingClient() {
  const company = document.getElementById("cpCompanyName");
  if (company) company.textContent = "Client record not found";
  renderRequestRows([]);
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
    tr.innerHTML = `
      <td>${escHtml(formatTimestamp(req.createdAt))}</td>
      <td>${escHtml(req.title || "Untitled request")}</td>
      <td>${escHtml(categoryLabel(req.category))}</td>
      <td>${escHtml(priorityLabel(req.priority || "normal"))}</td>
      <td><span class="req-status ${requestStatusClass(req.status)}">${escHtml(statusLabel(req.status || "submitted"))}</span></td>
    `;
    tbody.appendChild(tr);
  });
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
