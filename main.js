import { initializeApp } from "https://www.gstatic.com/firebasejs/12.7.0/firebase-app.js";
import { getFirestore, collection, addDoc, serverTimestamp }
  from "https://www.gstatic.com/firebasejs/12.7.0/firebase-firestore.js";
import { getAI, getGenerativeModel, GoogleAIBackend }
  from "https://www.gstatic.com/firebasejs/12.7.0/firebase-ai.js";
import { initializeAppCheck, ReCaptchaV3Provider }
  from "https://www.gstatic.com/firebasejs/12.7.0/firebase-app-check.js";
import { firebaseConfig, appCheckSiteKey } from "./firebase-config.js";

const MAKE_WEBHOOK_URL = "https://hook.us2.make.com/6r8c9mwbh7yfej1erffgpidbwsfy8mmw";
const AI_MODEL_NAME = "gemini-2.5-flash";
const AI_DAILY_LIMIT = 20;
const AI_MAX_INPUT_CHARS = 500;
const AI_USAGE_STORAGE_KEY = "bb_ai_chat_usage_v1";

const AI_SYSTEM_PROMPT = [
  "You are BrickBrick's website assistant.",
  "Keep answers short, practical, and friendly.",
  "Always reply in complete sentences.",
  "Focus on BrickBrick services: business websites, founder sites, AI workflow automation, and internal portals.",
  "If asked about pricing or custom scope, advise users to submit the contact form.",
  "Do not invent team members, customer results, or guarantees.",
].join(" ");

const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

if (appCheckSiteKey) {
  try {
    initializeAppCheck(app, {
      provider: new ReCaptchaV3Provider(appCheckSiteKey),
      isTokenAutoRefreshEnabled: true,
    });
  } catch (err) {
    console.warn("App Check initialization failed:", err);
  }
}

let aiModel = null;
let aiInitError = null;
let aiUsesLimitedTokens = false;

function buildAiModel(useLimitedUseAppCheckTokens) {
  const ai = getAI(app, {
    backend: new GoogleAIBackend(),
    useLimitedUseAppCheckTokens,
  });
  return getGenerativeModel(ai, {
    model: AI_MODEL_NAME,
    systemInstruction: AI_SYSTEM_PROMPT,
    generationConfig: {
      temperature: 0.4,
      maxOutputTokens: 420,
    },
  });
}

try {
  aiModel = buildAiModel(Boolean(appCheckSiteKey));
  aiUsesLimitedTokens = Boolean(appCheckSiteKey);
} catch (err) {
  // If App Check is not fully registered yet, temporarily fall back so chat still works.
  if (appCheckSiteKey) {
    try {
      aiModel = buildAiModel(false);
      aiUsesLimitedTokens = false;
      console.warn("AI initialized without limited-use App Check tokens:", err);
    } catch (fallbackErr) {
      aiInitError = fallbackErr;
      console.warn("AI initialization failed:", fallbackErr);
    }
  } else {
    aiInitError = err;
    console.warn("AI initialization failed:", err);
  }
}

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

const aiChatToggle = document.getElementById("aiChatToggle");
const aiChatPanel = document.getElementById("aiChatPanel");
const aiChatClose = document.getElementById("aiChatClose");
const aiChatForm = document.getElementById("aiChatForm");
const aiChatInput = document.getElementById("aiChatInput");
const aiChatSend = document.getElementById("aiChatSend");
const aiChatMessages = document.getElementById("aiChatMessages");
const aiChatStatus = document.getElementById("aiChatStatus");

let aiSession = null;
let aiBusy = false;
let aiTypingElement = null;

initFrontPageAssistant();

function initFrontPageAssistant() {
  if (!aiChatToggle || !aiChatPanel || !aiChatForm || !aiChatInput || !aiChatMessages || !aiChatStatus || !aiChatSend) {
    return;
  }

  aiChatToggle.addEventListener("click", () => {
    const nextOpen = !aiChatPanel.classList.contains("open");
    setAssistantOpen(nextOpen);
  });

  aiChatClose?.addEventListener("click", () => setAssistantOpen(false));

  aiChatForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const prompt = aiChatInput.value.trim();
    if (!prompt || aiBusy) return;
    if (prompt.length > AI_MAX_INPUT_CHARS) {
      setAssistantStatus(`Please keep messages under ${AI_MAX_INPUT_CHARS} characters.`, true);
      return;
    }

    if (!aiModel) {
      setAssistantStatus("Assistant is not configured yet. Use the contact form for now.", true);
      if (aiInitError) console.warn("AI unavailable:", aiInitError);
      return;
    }

    const usage = getAssistantUsage();
    if (usage.count >= AI_DAILY_LIMIT) {
      setAssistantStatus(`Daily limit reached (${AI_DAILY_LIMIT} messages). Please use the contact form.`, true);
      return;
    }
    incrementAssistantUsage();

    appendAssistantMessage("user", prompt);
    aiChatInput.value = "";
    setAssistantBusy(true);
    setAssistantTyping(true);

    try {
      const result = await sendAssistantPrompt(prompt);
      const reply = normalizeAssistantReply(result?.response?.text?.());
      appendAssistantMessage("bot", reply || "I could not generate a response just now. Please use the contact form.");
      setAssistantStatus(`${getAssistantRemaining()} prompts left today in this browser.`, false);
    } catch (err) {
      console.warn("AI send failed:", err);
      decrementAssistantUsage();
      appendAssistantMessage(
        "bot",
        "I hit an issue answering right now. Please submit the contact form and we will follow up."
      );
      setAssistantStatus(getAssistantErrorMessage(err), true);
    } finally {
      setAssistantTyping(false);
      setAssistantBusy(false);
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") setAssistantOpen(false);
  });

  setAssistantStatus(`${getAssistantRemaining()} prompts left today in this browser.`, false);
}

function setAssistantOpen(isOpen) {
  if (!aiChatPanel || !aiChatToggle) return;
  aiChatPanel.classList.toggle("open", isOpen);
  aiChatPanel.setAttribute("aria-hidden", isOpen ? "false" : "true");
  aiChatToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
  if (isOpen) aiChatInput?.focus();
}

function setAssistantBusy(isBusy) {
  aiBusy = isBusy;
  if (aiChatInput) aiChatInput.disabled = isBusy;
  if (aiChatSend) {
    aiChatSend.disabled = isBusy;
    aiChatSend.textContent = isBusy ? "..." : "Send";
  }
}

function setAssistantTyping(isTyping) {
  if (!aiChatMessages) return;
  if (isTyping) {
    if (aiTypingElement) return;
    aiTypingElement = document.createElement("div");
    aiTypingElement.className = "ai-msg ai-msg-bot ai-msg-typing";
    aiTypingElement.innerHTML = `
      <div class="ai-msg-bubble">
        <span></span><span></span><span></span>
      </div>
    `;
    aiChatMessages.appendChild(aiTypingElement);
    scrollAssistantToBottom();
    return;
  }

  if (aiTypingElement) {
    aiTypingElement.remove();
    aiTypingElement = null;
  }
}

function appendAssistantMessage(role, text) {
  if (!aiChatMessages) return;
  const row = document.createElement("div");
  row.className = `ai-msg ${role === "user" ? "ai-msg-user" : "ai-msg-bot"}`;

  const bubble = document.createElement("div");
  bubble.className = "ai-msg-bubble";
  bubble.textContent = text;

  row.appendChild(bubble);
  aiChatMessages.appendChild(row);
  scrollAssistantToBottom();
}

function scrollAssistantToBottom() {
  if (!aiChatMessages) return;
  aiChatMessages.scrollTop = aiChatMessages.scrollHeight;
}

function setAssistantStatus(message, isError) {
  if (!aiChatStatus) return;
  aiChatStatus.textContent = message;
  aiChatStatus.className = `ai-chat-status${isError ? " error" : ""}`;
}

function getAssistantSession() {
  if (!aiModel) {
    throw new Error("AI model is unavailable.");
  }
  if (!aiSession) {
    aiSession = aiModel.startChat();
  }
  return aiSession;
}

async function sendAssistantPrompt(prompt) {
  const chat = getAssistantSession();
  try {
    return await chat.sendMessage(prompt);
  } catch (err) {
    // If App Check is still propagating/registration is incomplete, retry once without limited-use tokens.
    if (aiUsesLimitedTokens && shouldRetryWithoutLimitedTokens(err)) {
      console.warn("Retrying AI request without limited-use App Check tokens.", err);
      aiModel = buildAiModel(false);
      aiSession = null;
      aiUsesLimitedTokens = false;
      const fallbackChat = getAssistantSession();
      return await fallbackChat.sendMessage(prompt);
    }
    throw err;
  }
}

function shouldRetryWithoutLimitedTokens(err) {
  const msg = String(err?.message || err || "").toLowerCase();
  return (
    msg.includes("app check")
    || msg.includes("appcheck")
    || msg.includes("recaptcha")
    || msg.includes("attestation")
    || msg.includes("not registered")
    || msg.includes("permission denied")
    || msg.includes("403")
  );
}

function getAssistantErrorMessage(err) {
  if (shouldRetryWithoutLimitedTokens(err)) {
    return "App Check setup is still propagating. Try again in about 1 minute.";
  }
  return "Temporary error from the AI backend.";
}

function normalizeAssistantReply(text) {
  return String(text || "")
    .replace(/\r\n/g, "\n")
    .trim();
}

function getAssistantRemaining() {
  const usage = getAssistantUsage();
  return Math.max(0, AI_DAILY_LIMIT - usage.count);
}

function incrementAssistantUsage() {
  const usage = getAssistantUsage();
  usage.count += 1;
  saveAssistantUsage(usage);
}

function decrementAssistantUsage() {
  const usage = getAssistantUsage();
  usage.count = Math.max(0, usage.count - 1);
  saveAssistantUsage(usage);
}

function getAssistantUsage() {
  const today = new Date().toISOString().slice(0, 10);
  try {
    const raw = localStorage.getItem(AI_USAGE_STORAGE_KEY);
    if (!raw) return { day: today, count: 0 };
    const parsed = JSON.parse(raw);
    if (parsed.day !== today) return { day: today, count: 0 };
    return {
      day: today,
      count: Number.isFinite(parsed.count) ? Math.max(0, parsed.count) : 0,
    };
  } catch {
    return { day: today, count: 0 };
  }
}

function saveAssistantUsage(usage) {
  try {
    localStorage.setItem(AI_USAGE_STORAGE_KEY, JSON.stringify(usage));
  } catch {
    // Ignore localStorage write issues and continue without local quota persistence.
  }
}
