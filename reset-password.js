(function () {
  const DEFAULT_API_KEY = "AIzaSyD1BYQMWLvzbW8deoBhtSSd_qHPspMr4Yg";

  const params = new URLSearchParams(window.location.search);
  const mode = String(params.get("mode") || "");
  const oobCode = String(params.get("oobCode") || "").trim();
  const apiKey = String(params.get("apiKey") || DEFAULT_API_KEY).trim();
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

  if (!apiKey) {
    titleEl.textContent = "Reset Unavailable";
    copyEl.textContent = "Missing API key in reset link.";
    setStatus("Could not initialize reset flow.", { error: true });
    return;
  }

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

  function setStatus(message, opts) {
    const options = opts || {};
    if (!statusEl) return;
    statusEl.textContent = String(message || "");
    statusEl.className = `reset-status${options.error ? " error" : ""}${options.success ? " success" : ""}`;
  }

  function friendlyResetError(code) {
    const normalized = String(code || "");
    const map = {
      EXPIRED_OOB_CODE: "This reset link expired. Request a new one.",
      INVALID_OOB_CODE: "This reset link is invalid. Request a new one.",
      OPERATION_NOT_ALLOWED: "Password reset is not enabled for this project.",
      USER_DISABLED: "This account is disabled.",
      WEAK_PASSWORD: "Password is too weak. Use a stronger password.",
      MISSING_OOB_CODE: "Reset code is missing from this link.",
      INVALID_API_KEY: "Reset link is invalid (API key mismatch).",
      TOO_MANY_ATTEMPTS_TRY_LATER: "Too many attempts. Try again later.",
      NETWORK_REQUEST_FAILED: "Network issue. Check connection and retry.",
    };
    return map[normalized] || "Could not complete password reset.";
  }

  async function callResetPasswordApi(payload) {
    const endpoint = `https://identitytoolkit.googleapis.com/v1/accounts:resetPassword?key=${encodeURIComponent(apiKey)}`;
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload || {}),
    });

    let data = {};
    try {
      data = await response.json();
    } catch (_err) {
      data = {};
    }

    if (!response.ok || data.error) {
      const code = String(data?.error?.message || "");
      const error = new Error(friendlyResetError(code));
      error.code = code;
      throw error;
    }

    return data;
  }

  function startRedirectCountdown(seconds) {
    let remaining = Math.max(1, Number(seconds) || 3);
    const label = portal === "client" ? "Client Panel" : "Team Workspace";

    const tick = function () {
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
    titleEl.textContent = "Reset Password";
    copyEl.textContent = "Validate your code and choose a new password.";
    setStatus("Checking reset link...");

    try {
      const data = await callResetPasswordApi({ oobCode });
      const email = String(data.email || "this account");
      copyEl.textContent = `Set a new password for ${email}.`;
      formEl.style.display = "grid";
      setStatus("");
    } catch (err) {
      console.error("verify reset link:", err);
      titleEl.textContent = "Reset Link Expired";
      copyEl.textContent = "This reset link is invalid or expired. Request a new one from the login page.";
      setStatus(String(err.message || "Could not verify reset link."), { error: true });
      return;
    }

    formEl.addEventListener("submit", async function (event) {
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
        await callResetPasswordApi({ oobCode, newPassword: password });
        formEl.style.display = "none";
        titleEl.textContent = "Password Updated";
        copyEl.textContent = "Your password was updated successfully.";
        setStatus("Reset complete.", { success: true });
        startRedirectCountdown(3);
      } catch (err) {
        console.error("confirm reset:", err);
        setStatus(String(err.message || "Could not reset password."), { error: true });
        submitBtnEl.disabled = false;
        submitBtnEl.textContent = "Save New Password";
      }
    }, { once: true });
  }

  function runPostResetLanding() {
    titleEl.textContent = "Password Updated";
    copyEl.textContent = "Your reset flow is complete. We are sending you back to your login portal.";
    setStatus("Reset complete.", { success: true });
    startRedirectCountdown(2);
  }
})();