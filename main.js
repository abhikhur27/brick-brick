import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getFirestore, collection, addDoc, serverTimestamp }
  from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import { firebaseConfig } from "./firebase-config.js";

const MAKE_WEBHOOK_URL = "https://hook.us2.make.com/6r8c9mwbh7yfej1erffgpidbwsfy8mmw";
const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

const menuToggle = document.getElementById("menuToggle");
const mobileMenu = document.getElementById("mobileMenu");

if (menuToggle && mobileMenu) {
  menuToggle.addEventListener("click", () => mobileMenu.classList.toggle("open"));
  mobileMenu.querySelectorAll("a").forEach((link) => {
    link.addEventListener("click", () => mobileMenu.classList.remove("open"));
  });
}

const yearEl = document.getElementById("year");
if (yearEl) yearEl.textContent = new Date().getFullYear();

const contactForm = document.getElementById("contactForm");
const cfStatus = document.getElementById("cfStatus");
const cfSubmitBtn = document.getElementById("cfSubmitBtn");

if (contactForm && cfSubmitBtn) {
  contactForm.addEventListener("submit", async (event) => {
    event.preventDefault();

    const name = document.getElementById("cfName")?.value.trim() || "";
    const email = document.getElementById("cfEmail")?.value.trim() || "";
    const company = document.getElementById("cfCompany")?.value.trim() || "";
    const phone = document.getElementById("cfPhone")?.value.trim() || "";
    const message = document.getElementById("cfMessage")?.value.trim() || "";
    const honeypot = document.getElementById("cfHoneypot")?.value || "";

    if (honeypot) {
      showStatus("Message received - we'll follow up within one business day.", false);
      contactForm.reset();
      return;
    }

    if (!name || !email || !message) {
      showStatus("Please include your name, email, and message.", true);
      return;
    }

    setLoading(true);

    try {
      const payload = {
        name,
        email,
        phone,
        company,
        message,
        timestamp: new Date().toISOString(),
      };

      const response = await fetch(MAKE_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        throw new Error(`Webhook request failed with status ${response.status}`);
      }

      // Queue the lead for automatic import into the portal pipeline.
      await addDoc(collection(db, "contact_submissions"), {
        name,
        email,
        phone,
        company,
        message,
        submittedAt: serverTimestamp(),
        source: "website-contact-form",
      });

      contactForm.reset();
      showStatus("Message received - we'll follow up within one business day.", false);
    } catch (error) {
      console.warn("Webhook submission failed:", error);
      showStatus("Submission failed. Please email us directly at contact@brick-brick.org.", true);
    } finally {
      setLoading(false);
    }
  });
}

function setLoading(isLoading) {
  if (!cfSubmitBtn) return;
  cfSubmitBtn.disabled = isLoading;
  cfSubmitBtn.textContent = isLoading ? "Sending..." : "Send inquiry";
}

function showStatus(message, isError) {
  if (!cfStatus) return;
  cfStatus.textContent = message;
  cfStatus.className = `form-status${isError ? " error" : ""}`;
}
