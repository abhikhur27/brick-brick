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

function inferServiceType(message) {
  const text = String(message || "").toLowerCase();
  if (text.includes("workflow") || text.includes("automation") || text.includes("ai")) {
    return "AI Workflow";
  }
  if (text.includes("website") || text.includes("site")) {
    return "Website Build";
  }
  return "Other";
}

function buildLeadNote({ name, email, phone, message }) {
  return [
    "Inbound website contact form.",
    `Name: ${name || "-"}`,
    `Email: ${email || "-"}`,
    `Phone: ${phone || "-"}`,
    "",
    "Message:",
    message || "(no message provided)",
  ].join("\n");
}

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

    if (!name || !email) {
      showStatus("Please include your name and email.", true);
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

      const leadRef = await addDoc(collection(db, "pipeline"), {
        title: (company || name || "Website Inquiry").trim(),
        company,
        contact: email || name,
        service: inferServiceType(message),
        status: "leads",
        note: buildLeadNote({ name, email, phone, message }),
        source: "website-contact-form",
        createdAt: serverTimestamp(),
        updatedAt: serverTimestamp(),
      });

      const [submissionResult, webhookResult] = await Promise.allSettled([
        addDoc(collection(db, "contact_submissions"), {
          name,
          email,
          phone,
          company,
          message,
          submittedAt: serverTimestamp(),
          source: "website-contact-form",
          pipelineId: leadRef.id,
          importedToPipelineAt: serverTimestamp(),
        }),
        fetch(MAKE_WEBHOOK_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        }).then((response) => {
          if (!response.ok) {
            throw new Error(`Webhook request failed with status ${response.status}`);
          }
          return response;
        }),
      ]);

      if (submissionResult.status === "rejected") {
        console.warn("contact_submissions write failed:", submissionResult.reason);
      }
      if (webhookResult.status === "rejected") {
        console.warn("Webhook submission failed:", webhookResult.reason);
      }

      contactForm.reset();
      showStatus("Message received - we'll follow up within one business day.", false);
    } catch (error) {
      console.warn("Lead creation failed:", error);
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
