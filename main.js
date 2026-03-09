// ─── FIREBASE SETUP ──────────────────────────────────────────────────────────
import { initializeApp }       from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getFirestore, collection, addDoc, serverTimestamp }
                                from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import { firebaseConfig }       from "./firebase-config.js";

const app = initializeApp(firebaseConfig);
const db  = getFirestore(app);

// ─── NAV MENU ────────────────────────────────────────────────────────────────
const menuToggle = document.getElementById("menuToggle");
const mobileMenu = document.getElementById("mobileMenu");

if (menuToggle && mobileMenu) {
  menuToggle.addEventListener("click", () => {
    mobileMenu.classList.toggle("open");
  });
  mobileMenu.querySelectorAll("a").forEach((link) => {
    link.addEventListener("click", () => mobileMenu.classList.remove("open"));
  });
}

// ─── FOOTER YEAR ─────────────────────────────────────────────────────────────
const yearEl = document.getElementById("year");
if (yearEl) yearEl.textContent = new Date().getFullYear();

// ─── CONTACT FORM ─────────────────────────────────────────────────────────────
// Architecture:
//   1. Write the submission to `contact_submissions` (team can view it in portal later).
//   2. Write a `mail` document — the Firebase "Trigger Email from Firestore" extension
//      watches this collection and sends the email automatically.
//   Both writes happen in the same async block.  If Firebase isn't available, fall back
//   to mailto so the form is never a dead end.

const contactForm  = document.getElementById("contactForm");
const cfStatus     = document.getElementById("cfStatus");
const cfSubmitBtn  = document.getElementById("cfSubmitBtn");

if (contactForm) {
  contactForm.addEventListener("submit", async (e) => {
    e.preventDefault();

    const name    = document.getElementById("cfName").value.trim();
    const email   = document.getElementById("cfEmail").value.trim();
    const company = document.getElementById("cfCompany").value.trim();
    const message = document.getElementById("cfMessage").value.trim();

    if (!name || !email) {
      showStatus("Please fill in your name and email.", true);
      return;
    }

    cfSubmitBtn.disabled    = true;
    cfSubmitBtn.textContent = "Sending…";
    cfStatus.className      = "form-status";
    cfStatus.textContent    = "";

    const submissionData = { name, email, company, message, submittedAt: serverTimestamp() };

    try {
      // 1. Save to contact_submissions (no auth needed — Firestore rule allows public writes)
      await addDoc(collection(db, "contact_submissions"), submissionData);

      // 2. Write to `mail` collection for Firebase "Trigger Email" extension
      await addDoc(collection(db, "mail"), {
        to: "contact@brick-brick.org",
        message: {
          subject: `New inquiry from ${name || "visitor"}`,
          html: `
            <p><strong>Name:</strong> ${escHtml(name)}</p>
            <p><strong>Email:</strong> ${escHtml(email)}</p>
            <p><strong>Company:</strong> ${escHtml(company || "—")}</p>
            <hr>
            <p><strong>Message:</strong></p>
            <p>${escHtml(message).replace(/\n/g, "<br>")}</p>
          `,
          text: `Name: ${name}\nEmail: ${email}\nCompany: ${company || "—"}\n\nMessage:\n${message}`,
        },
      });

      contactForm.reset();
      cfSubmitBtn.textContent = "Sent ✓";
      showStatus("Message sent. We'll be in touch within one business day.", false);
    } catch (err) {
      console.error("Contact form error:", err);
      // Graceful fallback: open mailto
      const subject = encodeURIComponent(`BrickBrick inquiry from ${name || "visitor"}`);
      const body    = encodeURIComponent(`Name: ${name}\nEmail: ${email}\nCompany: ${company}\n\n${message}`);
      window.location.href = `mailto:contact@brick-brick.org?subject=${subject}&body=${body}`;
    }
  });
}

function showStatus(msg, isError) {
  cfStatus.textContent  = msg;
  cfStatus.className    = "form-status" + (isError ? " error" : "");
  cfSubmitBtn.disabled  = false;
  cfSubmitBtn.textContent = "Send inquiry";
}

function escHtml(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}