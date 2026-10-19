"use strict";

const MAX_FILE_BYTES = 25 * 1024 * 1024;
const HEALTH_POLL_INTERVAL_MS = 10000;
const allowedExtensions = new Set(["pdf", "md", "markdown", "html", "htm"]);
let csrfToken = "";
let healthTimer = 0;
let healthCheckInFlight = false;

const signInView = document.querySelector("#sign-in-view");
const workspaceContent = document.querySelector("#workspace-content");
const signInForm = document.querySelector("#sign-in-form");
const signInMessage = document.querySelector("#sign-in-message");
const signInButton = document.querySelector("#sign-in-submit");
const signOutButton = document.querySelector("#sign-out-button");
const workspacePasswordInput = document.querySelector("#workspace-password");
const connectionDot = document.querySelector("#connection-dot");
const connectionLabel = document.querySelector("#connection-label");
const connectionDetail = document.querySelector("#connection-detail");
const chatForm = document.querySelector("#chat-form");
const chatQuery = document.querySelector("#chat-query");
const chatMessage = document.querySelector("#chat-message");
const chatSubmit = document.querySelector("#chat-submit");
const messageList = document.querySelector("#message-list");
const emptyChat = document.querySelector("#empty-chat");
const uploadForm = document.querySelector("#upload-form");
const uploadFile = document.querySelector("#report-file");
const selectedFileLabel = document.querySelector("#selected-file-label");
const uploadButton = document.querySelector("#upload-submit");
const uploadProgress = document.querySelector("#upload-progress");
const uploadMessage = document.querySelector("#upload-message");
const ingestResult = document.querySelector("#ingest-result");
const reportSelect = document.querySelector("#report-select");
const reportListMessage = document.querySelector("#report-list-message");
const pageBreadcrumb = document.querySelector("#page-breadcrumb");

function setConnection(online, detail) {
  connectionDot.classList.toggle("online", online);
  connectionDot.classList.toggle("offline", !online);
  connectionLabel.textContent = online ? "API connected" : "API unavailable";
  connectionDetail.textContent = detail;
}

function timeoutSignal(timeoutMs) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  return { signal: controller.signal, cancel: () => window.clearTimeout(timer) };
}

async function parseResponse(response) {
  let payload;
  try {
    payload = await response.json();
  } catch {
    payload = {};
  }
  if (!response.ok) {
    const detail = typeof payload.detail === "string"
      ? payload.detail
      : `Request failed with status ${response.status}.`;
    const error = new Error(detail);
    error.status = response.status;
    throw error;
  }
  return payload;
}

async function requestJson(path, options = {}, timeoutMs = 45000) {
  const timeout = timeoutSignal(timeoutMs);
  try {
    const response = await fetch(path, {
      credentials: "same-origin",
      cache: "no-store",
      ...options,
      signal: timeout.signal,
    });
    return await parseResponse(response);
  } catch (error) {
    if (error.name === "AbortError") {
      throw new Error("The request timed out. Check the connection and try again.");
    }
    if (error instanceof TypeError) {
      throw new Error("Could not reach the backend. Check that FastAPI is running.");
    }
    throw error;
  } finally {
    timeout.cancel();
  }
}

function csrfHeaders() {
  return csrfToken ? { "X-CSRF-Token": csrfToken } : {};
}

function jsonOptions(method, body) {
  return {
    method,
    headers: {
      "Content-Type": "application/json",
      ...csrfHeaders(),
    },
    body: JSON.stringify(body),
  };
}

function showSignIn(message = "") {
  csrfToken = "";
  workspaceContent.hidden = true;
  signInView.hidden = false;
  signOutButton.hidden = true;
  signInMessage.textContent = message;
}

function showWorkspace() {
  signInView.hidden = true;
  workspaceContent.hidden = false;
  signOutButton.hidden = false;
  signInMessage.textContent = "";
}

function explainError(error) {
  if (error.status === 401) {
    showSignIn("Your session is no longer valid. Sign in again to continue.");
  }
  return error.message;
}

async function checkHealth() {
  if (healthCheckInFlight) {
    return false;
  }
  healthCheckInFlight = true;
  try {
    const health = await requestJson("/health", { method: "GET" }, 6000);
    const online = health.status === "healthy";
    setConnection(online, online ? "FastAPI health check passed" : "Unexpected health response");
    return online;
  } catch {
    setConnection(false, "Could not reach FastAPI");
    return false;
  } finally {
    healthCheckInFlight = false;
  }
}

async function restoreSession() {
  startHealthChecks();
  await checkHealth();
  try {
    const session = await requestJson("/ui/api/session", { method: "GET" }, 6000);
    csrfToken = session.csrf_token;
    showWorkspace();
    startHealthChecks();
    await loadReports();
  } catch (error) {
    showSignIn(error.status === 401 ? "" : error.message);
  }
}

function startHealthChecks() {
  if (!healthTimer) {
    healthTimer = window.setInterval(() => {
      void checkHealth();
    }, HEALTH_POLL_INTERVAL_MS);
  }
}

signInForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  signInMessage.textContent = "";
  signInButton.disabled = true;
  let submittedPassword = workspacePasswordInput.value;
  workspacePasswordInput.value = "";
  try {
    const session = await requestJson(
      "/ui/api/session",
      jsonOptions("POST", { password: submittedPassword }),
      10000,
    );
    submittedPassword = "";
    csrfToken = session.csrf_token;
    showWorkspace();
    await loadReports();
  } catch (error) {
    submittedPassword = "";
    signInMessage.textContent = explainError(error);
  } finally {
    workspacePasswordInput.value = "";
    signInButton.disabled = false;
  }
});

signOutButton.addEventListener("click", async () => {
  signOutButton.disabled = true;
  try {
    await requestJson("/ui/api/session", {
      method: "DELETE",
      headers: csrfHeaders(),
    }, 8000);
  } catch {
    // Clear local UI state even if the server session has already expired.
  } finally {
    signOutButton.disabled = false;
    showSignIn();
  }
});

document.querySelectorAll(".nav-link").forEach((button) => {
  button.addEventListener("click", () => {
    document.querySelectorAll(".nav-link").forEach((navButton) => {
      navButton.classList.toggle("active", navButton === button);
    });
    document.querySelectorAll(".panel").forEach((panel) => {
      const active = panel.id === button.dataset.panel;
      panel.classList.toggle("active", active);
      panel.hidden = !active;
    });
    pageBreadcrumb.textContent = button.dataset.panel === "upload-panel"
      ? "Ingest a report"
      : "Ask reports";
  });
});

function addMessage(role, text) {
  emptyChat.hidden = true;
  const message = document.createElement("article");
  message.className = `chat-message ${role}`;
  message.textContent = text;
  messageList.append(message);
  messageList.scrollTop = messageList.scrollHeight;
  return message;
}

function addEvidenceSection(parent, heading) {
  const section = document.createElement("section");
  section.className = "evidence-block";
  const title = document.createElement("strong");
  title.textContent = heading;
  section.append(title);
  parent.append(section);
  return section;
}

function renderChatResponse(response) {
  const article = document.createElement("article");
  article.className = "chat-message assistant";

  const confidence = Number(response.confidence);
  const meta = document.createElement("p");
  meta.className = "answer-meta";
  meta.textContent = Number.isFinite(confidence)
    ? `Confidence ${(confidence * 100).toFixed(0)}%${response.escalated ? " · Sent for human review" : ""}`
    : response.escalated ? "Sent for human review" : "Grounded response";
  article.append(meta);

  const answer = document.createElement("div");
  answer.textContent = typeof response.answer === "string"
    ? response.answer
    : "The backend response did not include an answer.";
  article.append(answer);

  if (Array.isArray(response.citations) && response.citations.length) {
    const section = addEvidenceSection(article, `Sources (${response.citations.length})`);
    response.citations.forEach((citation) => {
      const item = document.createElement("div");
      item.className = "citation";
      const label = document.createElement("span");
      label.className = "citation-label";
      const reportTitle = typeof citation.report_title === "string" ? citation.report_title : null;
      const reportId = citation.report_id == null ? "Report" : `Report ${citation.report_id}`;
      const pageLabel = citation.page == null ? "page not recorded" : `page ${citation.page}`;
      label.textContent = `${reportTitle || reportId} · ${pageLabel}`;
      const quote = document.createElement("div");
      quote.className = "citation-quote";
      quote.textContent = typeof citation.quote === "string" ? citation.quote : "Source excerpt unavailable.";
      item.append(label, quote);
      section.append(item);
    });
  }

  if (Array.isArray(response.derived_values) && response.derived_values.length) {
    const section = addEvidenceSection(article, "Derived values and assumptions");
    const result = document.createElement("pre");
    result.className = "structured-result";
    result.textContent = JSON.stringify(response.derived_values, null, 2);
    section.append(result);
  }

  if (Array.isArray(response.conflicts) && response.conflicts.length) {
    const section = addEvidenceSection(article, "Source conflicts");
    const result = document.createElement("pre");
    result.className = "structured-result";
    result.textContent = JSON.stringify(response.conflicts, null, 2);
    section.append(result);
  }

  messageList.append(article);
  messageList.scrollTop = messageList.scrollHeight;
}

document.querySelectorAll(".suggestion").forEach((button) => {
  button.addEventListener("click", () => {
    chatQuery.value = button.dataset.query;
    chatQuery.focus();
  });
});

chatForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  chatMessage.textContent = "";
  const clientId = Number(document.querySelector("#client-id").value);
  const query = chatQuery.value.trim();
  if (!Number.isSafeInteger(clientId) || clientId <= 0) {
    chatMessage.textContent = "Enter a valid positive client ID.";
    return;
  }
  if (!query) {
    chatMessage.textContent = "Enter a question before sending.";
    return;
  }

  addMessage("user", query);
  const loading = addMessage("assistant", "Checking authorized reports and source evidence…");
  chatSubmit.disabled = true;
  try {
    const response = await requestJson(
      "/ui/api/chat",
      jsonOptions("POST", { client_id: clientId, query }),
      60000,
    );
    loading.remove();
    renderChatResponse(response);
    chatQuery.value = "";
  } catch (error) {
    loading.remove();
    const message = explainError(error);
    addMessage("error", `Unable to complete the request: ${message} You can retry without re-entering your question.`);
    chatMessage.textContent = message;
  } finally {
    chatSubmit.disabled = false;
  }
});

function renderReportOptions(reports) {
  reportSelect.replaceChildren();
  const prompt = document.createElement("option");
  prompt.value = "";
  prompt.textContent = reports.length ? "Select a registered report…" : "No reports registered";
  reportSelect.append(prompt);
  reports.forEach((report) => {
    const option = document.createElement("option");
    option.value = String(report.id);
    option.textContent = `${report.title} · #${report.id} · ${report.ingestion_status}`;
    option.dataset.title = report.title;
    reportSelect.append(option);
  });
  reportSelect.disabled = reports.length === 0;
  uploadButton.disabled = reports.length === 0;
  if (!reports.length) {
    reportListMessage.textContent = "No report records were returned. Register a report in MySQL before uploading its source.";
  } else {
    reportListMessage.textContent = "Reports are loaded from the backend. Uploading replaces the source file for the selected record.";
  }
}

async function loadReports() {
  reportListMessage.textContent = "Loading registered reports…";
  try {
    const response = await requestJson("/ui/api/reports", { method: "GET" }, 15000);
    renderReportOptions(Array.isArray(response.reports) ? response.reports : []);
  } catch (error) {
    reportListMessage.textContent = explainError(error);
    if (error.status === 401) {
      return;
    }
    reportSelect.replaceChildren();
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "Could not load reports";
    reportSelect.append(option);
    reportSelect.disabled = true;
  }
}

uploadFile.addEventListener("change", () => {
  const file = uploadFile.files[0];
  selectedFileLabel.textContent = file ? file.name : "No file selected";
  uploadMessage.textContent = "";
  ingestResult.hidden = true;
});

uploadForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  uploadMessage.textContent = "";
  ingestResult.hidden = true;
  const file = uploadFile.files[0];
  if (!file) {
    uploadMessage.textContent = "Choose a report file to upload.";
    return;
  }
  const extension = file.name.split(".").pop().toLowerCase();
  if (!allowedExtensions.has(extension)) {
    uploadMessage.textContent = "Choose a PDF, Markdown, or HTML file.";
    return;
  }
  if (file.size === 0 || file.size > MAX_FILE_BYTES) {
    uploadMessage.textContent = "The file must be non-empty and no larger than 25 MiB.";
    return;
  }

  const form = new FormData(uploadForm);
  form.set("report_id", reportSelect.value);
  form.set("sector", document.querySelector("#sector").value.trim());
  form.set("region", document.querySelector("#region").value.trim());
  uploadButton.disabled = true;
  uploadProgress.hidden = false;

  try {
    const response = await requestJson(
      "/ui/api/ingest",
      {
        method: "POST",
        headers: csrfHeaders(),
        body: form,
      },
      180000,
    );
    if (response.status !== "success") {
      throw new Error("The backend did not confirm successful ingestion.");
    }
    ingestResult.classList.remove("error");
    ingestResult.textContent = `Ingestion confirmed for report ${response.report_id}: ${response.chunks_indexed} chunks indexed from ${response.source_filename}.`;
    ingestResult.hidden = false;
    uploadMessage.textContent = "";
    await loadReports();
  } catch (error) {
    const message = explainError(error);
    uploadMessage.textContent = message;
    ingestResult.classList.add("error");
    ingestResult.textContent = `Ingestion was not confirmed. ${message}`;
    ingestResult.hidden = false;
  } finally {
    uploadButton.disabled = reportSelect.disabled;
    uploadProgress.hidden = true;
  }
});

restoreSession();
