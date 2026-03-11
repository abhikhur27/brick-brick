import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import {
  getAuth,
  verifyPasswordResetCode,
  confirmPasswordReset,
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { firebaseConfig } from "./firebase-config.js";

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);

const params = new URLSearchParams(window.location.search);
const mode = String(params.get("mode") || "");
const oobCode = String(params.get("oobCode") || "").trim();
const continueUrlRaw = String(params.get("continueUrl") || "").trim();
const portalFromUrl = String(params.get("portal") || "").toLowerCase();

const titleEl = document.getElementById("resetTitle");
const copyEl = document.getElementById("resetCopy");
const statusEl = document.getElementById("resetStatus");
const portalLabelEl = document.getElementById("resetPortalLabel");
const formEl = document.getElementById("resetForm");
const newPasswordEl = document.getElementById("newPassword");
const confirmPasswordEl = document.getElementById("confirmPassword");
const submitBtnEl = document.getElementById("resetSubmitBtn");
const returnLinkEl = document.getElementById("resetReturnLink");

const portal = resolvePortalTarget();
const destinationPath = portal === "client" ? "/client.html?reset=success" : "/portal.html?reset=success";
const destinationUrl = new URL(destinationPath, window.location.origin).toString();

configureReturnLink();

if (mode === "resetPassword" && oobCode) {
  runDirectResetFlow();
} else {
  runPostResetLanding();
}

function resolvePortalTarget() {
  if (portalFromUrl === "client") return "client";
  if (portalFromUrl === "team") return "team";

  if (continueUrlRaw) {
    try {
      const continueUrl = new URL(continueUrlRaw, window.location.origin);
      const continuePortal = String(continueUrl.searchParams.get("portal") || "").toLowerCase();
      if (continuePortal === "client") return "client";
      if (continuePortal === "team") return "team";
      if (continueUrl.pathname.toLowerCase().includes("client.html")) return "client";
      if (continueUrl.pathname.toLowerCase().includes("portal.html")) return "team";
    } catch (_err) {
      // Ignore parse errors and use default target below.
    }
  }

  return "team";
}

function configureReturnLink() {
  if (!returnLinkEl || !portalLabelEl) return;
  if (portal === "client") {
    portalLabelEl.textContent = "Client Access";
    returnLinkEl.textContent = "Back to Client Panel";
  } else {
    portalLabelEl.textContent = "Team Access";
    returnLinkEl.textContent = "Back to Team Workspace";
  }
  returnLinkEl.href = destinationUrl;
}

function setStatus(message, { error = false, success = false } = {}) {
  if (!statusEl) return;
  statusEl.textContent = String(message || "");
  statusEl.className = `reset-status${error ? " error" : ""}${success ? " success" : ""}`;
}

function startRedirectCountdown(seconds = 3) {
  let remaining = Math.max(1, Number(seconds) || 3);
  const label = portal === "client" ? "Client Panel" : "Team Workspace";

  const tick = () => {
    setStatus(`Redirecting to ${label} in ${remaining}s...`, { success: true });
    remaining -= 1;
    if (remaining < 0) {
      window.location.assign(destinationUrl);
      return;
    }
    window.setTimeout(tick, 1000);
  };

  tick();
}

async function runDirectResetFlow() {
  if (!titleEl || !copyEl || !formEl || !newPasswordEl || !confirmPasswordEl || !submitBtnEl) return;

  titleEl.textContent = "Reset Password";
  copyEl.textContent = "Validate your code and choose a new password.";
  setStatus("Checking reset link...");

  try {
    const email = await verifyPasswordResetCode(auth, oobCode);
    copyEl.textContent = `Set a new password for ${email}.`;
    formEl.style.display = "grid";
    setStatus("");
  } catch (err) {
    console.error("verifyPasswordResetCode:", err);
    titleEl.textContent = "Reset Link Expired";
    copyEl.textContent = "This reset link is invalid or expired. Request a new one from the login page.";
    setStatus("Could not verify reset link.", { error: true });
    return;
  }

  formEl.addEventListener("submit", async (event) => {
    event.preventDefault();
    const password = String(newPasswordEl.value || "");
    const confirmation = String(confirmPasswordEl.value || "");

    if (password.length < 8) {
      setStatus("Use at least 8 characters.", { error: true });
      return;
    }
    if (password !== confirmation) {
      setStatus("Passwords do not match.", { error: true });
      return;
    }

    submitBtnEl.disabled = true;
    submitBtnEl.textContent = "Saving...";
    setStatus("");

    try {
      await confirmPasswordReset(auth, oobCode, password);
      formEl.style.display = "none";
      titleEl.textContent = "Password Updated";
      copyEl.textContent = "Your password was updated successfully.";
      setStatus("Reset complete.", { success: true });
      startRedirectCountdown(3);
    } catch (err) {
      console.error("confirmPasswordReset:", err);
      setStatus("Could not reset password. Request a new reset link.", { error: true });
      submitBtnEl.disabled = false;
      submitBtnEl.textContent = "Save New Password";
    }
  }, { once: true });
}

function runPostResetLanding() {
  if (!titleEl || !copyEl) return;

  titleEl.textContent = "Password Updated";
  copyEl.textContent = "Your reset flow is complete. We are sending you back to your login portal.";
  setStatus("Reset complete.", { success: true });
  startRedirectCountdown(2);
}
