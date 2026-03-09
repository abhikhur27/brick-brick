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
const WEB3FORMS_KEY = "REPLACE_WITH_YOUR_WEB3FORMS_ACCESS_KEY";

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

    // Read field values — IDs match the updated index.html
    const name    = document.getElementById("cfName").value.trim();
    const email   = document.getElementById("cfEmail").value.trim();
    const company = document.getElementById("cfCompany").value.trim();
    const phone   = document.getElementById("cfPhone").value.trim();
    const message = document.getElementById("cfMessage").value.trim();
    // Honeypot — if filled, a bot submitted the form; silently reject
    const honeypot = document.getElementById("cfHoneypot").value;

    if (honeypot) {
      // Bot detected: fake success so bots don't retry
      showStatus("Message sent. We'll be in touch within one business day.", false);
      contactForm.reset();
      return;
    }

    if (!name || !email) {
      showStatus("Please include your name and email.", true);
      return;
    }

    setLoading(true);

    // ── Step 1: Send email via Web3Forms ─────────────────────────────────────
    // Web3Forms accepts a plain JSON POST. The access key routes the submission
    // to the email address configured in your Web3Forms dashboard.
    let emailSent = false;
    try {
      const payload = {
        access_key:   WEB3FORMS_KEY,
        subject:      `New inquiry from ${name}`,
        from_name:    "BrickBrick Contact Form",
        // reply-to lets you hit Reply in Gmail and it goes to the sender
        replyto:      email,
        name,
        email,
        company:      company || "—",
        phone:        phone   || "—",
        message:      message || "No message provided.",
        // Tell Web3Forms not to send their default confirmation email to sender
        // (we're not promising that to users)
        botcheck:     "",
      };

      const res = await fetch("https://api.web3forms.com/submit", {
        method:  "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body:    JSON.stringify(payload),
      });
      const data = await res.json();
      emailSent = data.success === true;
    } catch (err) {
      console.warn("Web3Forms request failed:", err);
    }

    // ── Step 2: Save to Firestore for internal records ────────────────────────
    // This is a direct client write to Firestore — no Cloud Functions, no Blaze.
    // The contact_submissions collection is write-open (no auth required) per rules.
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
      // Both paths failed — give a real fallback so the person isn't stuck
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