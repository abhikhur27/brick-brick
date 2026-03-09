// ─── FIREBASE SETUP ──────────────────────────────────────────────────────────
// Only used for saving contact submissions to Firestore for internal tracking.
// No Cloud Functions. No Secret Manager. Works on Spark plan.
import { initializeApp }  from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getFirestore, collection, addDoc, serverTimestamp }
                           from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import { firebaseConfig }  from "./firebase-config.js";

const app = initializeApp(firebaseConfig);
const db  = getFirestore(app);

// ─── Web3Forms access key ─────────────────────────────────────────────────────
// This key is PUBLIC by design — it only allows submission to YOUR configured
// destination email. It cannot read submissions, change your account, or be
// used to send to other addresses. Safe to embed in client-side JS.
// Replace the placeholder below with your real key from web3forms.com/access.
const WEB3FORMS_KEY = "945ebcbf-b856-41d0-919e-f5d39418ea06";

// ─── NAV MENU ─────────────────────────────────────────────────────────────────
const menuToggle = document.getElementById("menuToggle");
const mobileMenu = document.getElementById("mobileMenu");

if (menuToggle && mobileMenu) {
  menuToggle.addEventListener("click", () => mobileMenu.classList.toggle("open"));
  mobileMenu.querySelectorAll("a").forEach((link) =>
    link.addEventListener("click", () => mobileMenu.classList.remove("open"))
  );
}

// ─── FOOTER YEAR ──────────────────────────────────────────────────────────────
const yearEl = document.getElementById("year");
if (yearEl) yearEl.textContent = new Date().getFullYear();

// ─── CONTACT FORM ─────────────────────────────────────────────────────────────
const contactForm = document.getElementById("contactForm");
const cfStatus    = document.getElementById("cfStatus");
const cfSubmitBtn = document.getElementById("cfSubmitBtn");

if (contactForm && cfSubmitBtn) {
  cfSubmitBtn.addEventListener("click", async () => {

    // ── Read fields ────────────────────────────────────────────────────────────
    const name     = document.getElementById("cfName").value.trim();
    const email    = document.getElementById("cfEmail").value.trim();
    const company  = document.getElementById("cfCompany").value.trim();
    const phone    = document.getElementById("cfPhone").value.trim();
    const message  = document.getElementById("cfMessage").value.trim();
    // Honeypot: the input has NO name attribute so it can never appear in a URL.
    // If it has a value, a browser auto-fill or bot filled it — silently reject.
    const honeypot = document.getElementById("cfHoneypot").value;

    if (honeypot) {
      showStatus("Message sent. We'll be in touch within one business day.", false);
      contactForm.reset();
      return;
    }

    if (!name || !email) {
      showStatus("Please include your name and email.", true);
      return;
    }

    setLoading(true);

    // ── Step 1: Send email via Web3Forms ──────────────────────────────────────
    //
    // Critical fields:
    //   access_key  — your Web3Forms key (routes to your inbox)
    //   subject     — includes name AND email so sender is always visible
    //   from_name   — display name shown in the From header
    //   replyto     — sets Reply-To so hitting Reply goes to the sender
    //   redirect    — "false" (string) prevents ANY browser redirect
    //   botcheck    — empty string confirms this is a real, non-bot submission
    //   message     — body text includes all fields explicitly so they're
    //                 readable regardless of how the email client renders headers
    //
    // The submitter's email is in BOTH the subject line AND the message body.
    // This is the fix for the "protected" sender issue — some email clients
    // suppress or obfuscate the Reply-To header, so embedding it in the body
    // ensures you always see who sent the inquiry.

    let emailSent = false;
    try {
      const bodyText = [
        `From:    ${name} <${email}>`,
        `Phone:   ${phone   || "—"}`,
        `Company: ${company || "—"}`,
        ``,
        `Message:`,
        message || "(no message provided)",
      ].join("\n");

      const payload = {
        access_key: WEB3FORMS_KEY,
        subject:    `New inquiry from ${name} <${email}>`,
        from_name:  "BrickBrick Website",
        replyto:    email,
        redirect:   "false",    // must be the STRING "false", not boolean
        botcheck:   "",          // empty string = confirmed non-bot to Web3Forms
        name,
        email,
        phone:      phone   || "—",
        company:    company || "—",
        message:    bodyText,
      };

      const res = await fetch("https://api.web3forms.com/submit", {
        method:  "POST",
        headers: {
          "Content-Type": "application/json",
          "Accept":        "application/json",
        },
        body: JSON.stringify(payload),
      });

      const data = await res.json();
      emailSent = data.success === true;
      if (!emailSent) console.warn("Web3Forms non-success response:", data);
    } catch (err) {
      console.warn("Web3Forms fetch failed:", err);
    }

    // ── Step 2: Save to Firestore for internal records ────────────────────────
    // Direct client write — no Cloud Functions, works on Spark plan.
    // contact_submissions is write-open (no auth required) per firestore.rules.
    let firestoreSaved = false;
    try {
      await addDoc(collection(db, "contact_submissions"), {
        name,
        email,
        company:      company || "",
        phone:        phone   || "",
        message:      message || "",
        submittedAt:  serverTimestamp(),
        emailRelayed: emailSent,
      });
      firestoreSaved = true;
    } catch (err) {
      console.warn("Firestore save failed (non-fatal):", err);
    }

    setLoading(false);

    if (emailSent || firestoreSaved) {
      contactForm.reset();
      showStatus("Message received — we'll follow up within one business day.", false);
    } else {
      showStatus(
        "Submission failed. Please email us directly at contact@brick-brick.org.",
        true
      );
    }
  });
}

// ─── UI HELPERS ───────────────────────────────────────────────────────────────
function setLoading(on) {
  if (!cfSubmitBtn) return;
  cfSubmitBtn.disabled    = on;
  cfSubmitBtn.textContent = on ? "Sending…" : "Send inquiry";
}

function showStatus(msg, isError) {
  if (!cfStatus) return;
  cfStatus.textContent = msg;
  cfStatus.className   = "form-status" + (isError ? " error" : "");
}

function escHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;").replace(/</g, "&lt;")
    .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}