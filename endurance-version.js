const IDLE_THRESHOLD_MS = 10000;
const CHOICE_COUNTS = [2, 4, 6];
const CLOUD_BATCH_SIZE = 20;
const CLOUD_FLUSH_INTERVAL_MS = 3000;
const CLOUD_RETRY_DELAYS_MS = [1000, 3000, 5000];
const CLOUD_OUTBOX_KEY = "learning-log-cloud-outbox-v4";
const CLOUD_ACK_POLL_ATTEMPTS = 5;
const CLOUD_ACK_POLL_DELAY_MS = 700;
const APP_VERSION = "endurance-github-pages-6";
const LOG_HEADERS = [
  "record_type",
  "event_id",
  "session_id",
  "participant_id",
  "question_index",
  "event_type",
  "event_at",
  "response_time_ms",
  "is_correct",
  "idle_total_ms",
  "choice_count",
  "dot_difference",
  "session_elapsed_ms",
  "question_elapsed_ms",
  "end_reason",
  "answered_count"
];

let sessionId = "";
let participantId = "P001";
let serverLoggingAvailable = null;
let sessionLogs = [];
let sessionBackupKey = "";
let collectorUrl = "";
let cloudCollectionFailed = false;
let cloudOutbox = [];
let cloudFlushPromise = null;
let cloudFlushTimer = null;
let choiceCount = 2;
let choiceCountQueue = [];
let sessionStart = 0;
let questionStart = 0;
let lastActionAt = 0;
let idleStart = null;
let idleStoredMs = 0;
let answeredCount = 0;
let questionIndex = 0;
let currentTask = null;
let isLocked = false;
let isFinished = false;
let stressEventCount = 0;
let timer = null;
let endEventType = "";
let selfReportSubmitted = false;

const startScreen = document.getElementById("startScreen");
const workspace = document.getElementById("workspace");
const doneScreen = document.getElementById("doneScreen");
const participantInput = document.getElementById("participantInput");
const startBtn = document.getElementById("startBtn");
const restartBtn = document.getElementById("restartBtn");
const choices = document.getElementById("choices");
const stressBtn = document.getElementById("stressBtn");
const stressFeedback = document.getElementById("stressFeedback");
const giveUpBtn = document.getElementById("giveUpBtn");
const answeredText = document.getElementById("answeredText");
const doneTitle = document.getElementById("doneTitle");
const doneAnsweredText = document.getElementById("doneAnsweredText");
const deliveryStatus = document.getElementById("deliveryStatus");
const deliveryIcon = document.getElementById("deliveryIcon");
const deliveryTitle = document.getElementById("deliveryTitle");
const doneNoteText = document.getElementById("doneNoteText");
const downloadCsvBtn = document.getElementById("downloadCsvBtn");
const selfReportPanel = document.getElementById("selfReportPanel");
const selfReportStatus = document.getElementById("selfReportStatus");
const submitSelfReportBtn = document.getElementById("submitSelfReportBtn");
const skipSelfReportBtn = document.getElementById("skipSelfReportBtn");
const otherReasonInput = document.getElementById("otherReason");

function createLocalSessionId() {
  const timestamp = new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
  const random = Math.random().toString(16).slice(2, 8);
  return `LOCAL-${timestamp}-${random}`;
}

function csvCell(value) {
  if (value === undefined || value === null) return "";
  return `"${String(value).replaceAll('"', '""')}"`;
}

function logsToCsv(logs) {
  const rows = [
    LOG_HEADERS.join(","),
    ...logs.map((log) => LOG_HEADERS.map((header) => csvCell(log[header])).join(","))
  ];
  return `\uFEFF${rows.join("\n")}\n`;
}

function safeFilePart(value) {
  return String(value || "")
    .trim()
    .replace(/[\\/:*?"<>|]/g, "-")
    .replace(/\s+/g, "_")
    .slice(0, 40) || "participant";
}

function makeCsvFilename() {
  const timestamp = new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
  return `learning-log_${safeFilePart(participantId)}_${timestamp}.csv`;
}

function downloadLogArray(logs, filename) {
  const blob = new Blob([logsToCsv(logs)], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function backupLocalLogs() {
  if (!sessionBackupKey) return;
  try {
    localStorage.setItem(sessionBackupKey, JSON.stringify(sessionLogs));
  } catch (error) {
    console.warn("local backup failed", error);
  }
}

function downloadCsv() {
  if (sessionLogs.length === 0) {
    alert("保存できるログがまだありません。");
    return;
  }

  downloadLogArray(sessionLogs, makeCsvFilename());
  setDeliveryStatus(
    "error",
    "CSVを保存しました",
    "通常は端末の「ダウンロード」に保存されています。研究者に渡してください。"
  );
}

function getCollectorUrl() {
  const value = window.LEARNING_LOG_COLLECTOR_URL || "";
  return String(value).trim();
}

function makeEventId(type, data) {
  if (data.event_id) return String(data.event_id);
  if (type === "session") return `${data.session_id}:session`;
  return [
    data.session_id,
    data.event_type || type,
    data.question_index ?? "",
    data.event_at || data.received_at || new Date().toISOString()
  ].join(":");
}

function persistCloudOutbox() {
  try {
    localStorage.setItem(CLOUD_OUTBOX_KEY, JSON.stringify(cloudOutbox));
  } catch (error) {
    console.warn("cloud outbox backup failed", error);
  }
}

function restoreCloudOutbox() {
  try {
    const stored = JSON.parse(localStorage.getItem(CLOUD_OUTBOX_KEY) || "[]");
    cloudOutbox = Array.isArray(stored) ? stored : [];
  } catch (error) {
    cloudOutbox = [];
    console.warn("cloud outbox restore failed", error);
  }
}

function cloudPayload(type, data) {
  return {
    type,
    sent_at: new Date().toISOString(),
    app_version: APP_VERSION,
    data
  };
}

function setDeliveryStatus(state, title, message) {
  deliveryStatus.className = `delivery-status ${state}`;
  deliveryIcon.textContent = state === "success" ? "✓" : "!";
  deliveryTitle.textContent = title;
  doneNoteText.textContent = message;
}

async function postToCollector(type, data) {
  if (!collectorUrl) return false;

  try {
    await fetch(collectorUrl, {
      method: "POST",
      mode: "no-cors",
      cache: "no-store",
      headers: {
        "content-type": "text/plain;charset=utf-8"
      },
      body: JSON.stringify(cloudPayload(type, data)),
      keepalive: true
    });
    return true;
  } catch (error) {
    console.warn("cloud collection failed; CSV backup remains available", error);
    return false;
  }
}

function stableBatchId(records) {
  const text = records.map((item) => item.event_id).join("|");
  let hashA = 2166136261;
  let hashB = 2654435761;
  for (let index = 0; index < text.length; index += 1) {
    const code = text.charCodeAt(index);
    hashA = Math.imul(hashA ^ code, 16777619);
    hashB = Math.imul(hashB ^ code, 2246822519);
  }
  return `batch-${(hashA >>> 0).toString(16)}-${(hashB >>> 0).toString(16)}-${records.length}`;
}

function checkBatchAcknowledged(batchId, expectedCount) {
  return new Promise((resolve) => {
    const callbackName = `__learningLogAck_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    const script = document.createElement("script");
    let settled = false;

    const cleanup = (result) => {
      if (settled) return;
      settled = true;
      window.clearTimeout(timeoutId);
      script.remove();
      delete window[callbackName];
      resolve(result);
    };

    window[callbackName] = (result) => {
      const processedCount = Number(result?.accepted || 0) + Number(result?.duplicates || 0);
      cleanup(Boolean(result && result.ok && processedCount === expectedCount));
    };
    script.onerror = () => cleanup(false);
    const separator = collectorUrl.includes("?") ? "&" : "?";
    script.src = `${collectorUrl}${separator}action=status&batch_id=${encodeURIComponent(batchId)}&callback=${encodeURIComponent(callbackName)}&_=${Date.now()}`;
    const timeoutId = window.setTimeout(() => cleanup(false), 6000);
    document.head.appendChild(script);
  });
}

async function waitForBatchAcknowledgement(batchId, expectedCount) {
  for (let attempt = 0; attempt < CLOUD_ACK_POLL_ATTEMPTS; attempt += 1) {
    if (await checkBatchAcknowledged(batchId, expectedCount)) return true;
    if (attempt < CLOUD_ACK_POLL_ATTEMPTS - 1) await wait(CLOUD_ACK_POLL_DELAY_MS);
  }
  return false;
}

function scheduleCloudFlush(delayMs = CLOUD_FLUSH_INTERVAL_MS) {
  if (cloudFlushTimer || cloudOutbox.length === 0) return;
  cloudFlushTimer = window.setTimeout(() => {
    cloudFlushTimer = null;
    flushCloudOutbox().catch((error) => {
      console.warn("scheduled cloud flush failed", error);
    });
  }, delayMs);
}

function enqueueCloudRecord(type, data) {
  const eventId = makeEventId(type, data);
  const recordData = { ...data, event_id: eventId };
  if (!cloudOutbox.some((item) => item.event_id === eventId)) {
    cloudOutbox.push({ event_id: eventId, type, data: recordData });
    persistCloudOutbox();
  }

  if (cloudOutbox.length >= CLOUD_BATCH_SIZE) {
    scheduleCloudFlush(0);
  } else {
    scheduleCloudFlush();
  }
  return eventId;
}

async function flushCloudOutbox() {
  if (cloudFlushPromise) return cloudFlushPromise;
  if (!collectorUrl || cloudOutbox.length === 0) return cloudOutbox.length === 0;

  cloudFlushPromise = (async () => {
    const batch = cloudOutbox.slice(0, CLOUD_BATCH_SIZE);
    const batchId = stableBatchId(batch);
    const dispatched = await postToCollector("batch", { batch_id: batchId, records: batch });
    const acknowledged = dispatched && await waitForBatchAcknowledgement(batchId, batch.length);
    if (!acknowledged) {
      cloudCollectionFailed = true;
      scheduleCloudFlush(CLOUD_RETRY_DELAYS_MS[1]);
      return false;
    }

    const deliveredIds = new Set(batch.map((item) => item.event_id));
    cloudOutbox = cloudOutbox.filter((item) => !deliveredIds.has(item.event_id));
    persistCloudOutbox();
    if (cloudOutbox.length > 0) scheduleCloudFlush(0);
    return true;
  })().finally(() => {
    cloudFlushPromise = null;
  });

  return cloudFlushPromise;
}

function wait(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

async function flushAllCloudRecords() {
  if (cloudFlushTimer) {
    window.clearTimeout(cloudFlushTimer);
    cloudFlushTimer = null;
  }

  for (let attempt = 0; attempt <= CLOUD_RETRY_DELAYS_MS.length; attempt += 1) {
    while (cloudOutbox.length > 0) {
      const before = cloudOutbox.length;
      const delivered = await flushCloudOutbox();
      if (!delivered || cloudOutbox.length >= before) break;
    }
    if (cloudOutbox.length === 0) return true;
    if (attempt < CLOUD_RETRY_DELAYS_MS.length) {
      await wait(CLOUD_RETRY_DELAYS_MS[attempt]);
    }
  }
  return false;
}

function activeSessionElapsed(now = Date.now()) {
  return Math.max(0, now - sessionStart);
}

function activeQuestionElapsed(now = Date.now()) {
  return Math.max(0, now - questionStart);
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function shuffle(items) {
  const list = [...items];
  for (let index = list.length - 1; index > 0; index -= 1) {
    const swapIndex = randomInt(0, index);
    [list[index], list[swapIndex]] = [list[swapIndex], list[index]];
  }
  return list;
}

function nextChoiceCount() {
  if (choiceCountQueue.length === 0) {
    choiceCountQueue = shuffle(CHOICE_COUNTS);
  }
  return choiceCountQueue.shift();
}

function makeTask() {
  questionIndex += 1;
  choiceCount = nextChoiceCount();
  const counts = shuffle([2, 3, 4, 5, 6, 7, 8, 9]).slice(0, choiceCount);
  const maxCount = Math.max(...counts);
  const minCount = Math.min(...counts);
  const targetIndex = counts.indexOf(maxCount);
  const options = counts.map((count, index) => ({
    key: `choice_${index + 1}`,
    count
  }));

  return {
    options,
    counts,
    difference: maxCount - minCount,
    targetKey: options[targetIndex].key
  };
}

function dotBoard(count) {
  const board = document.createElement("span");
  board.className = "dot-board";
  const filledIndexes = new Set(shuffle([0, 1, 2, 3, 4, 5, 6, 7, 8]).slice(0, count));

  for (let index = 0; index < 9; index += 1) {
    const cell = document.createElement("span");
    cell.className = "dot-cell";
    if (filledIndexes.has(index)) {
      const dot = document.createElement("span");
      dot.className = "dot";
      cell.appendChild(dot);
    }
    board.appendChild(cell);
  }

  return board;
}

function renderChoice(option) {
  const button = document.createElement("button");
  button.className = "choice";
  button.type = "button";
  button.setAttribute("aria-label", `点が${option.count}個の選択肢`);
  button.appendChild(dotBoard(option.count));

  button.addEventListener("click", () => answer(option.key));
  return button;
}

function renderTask() {
  currentTask = makeTask();
  questionStart = Date.now();
  lastActionAt = questionStart;
  idleStart = null;
  idleStoredMs = 0;
  isLocked = false;

  choices.innerHTML = "";
  choices.dataset.count = String(choiceCount);
  currentTask.options.forEach((option) => {
    choices.appendChild(renderChoice(option));
  });
}

async function postJson(url, data) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(data)
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}

function shouldTryServerApi() {
  const host = window.location.hostname;
  return host === "localhost"
    || host === "127.0.0.1"
    || host.startsWith("192.168.")
    || host.startsWith("10.")
    || host.startsWith("172.16.")
    || host.startsWith("172.17.")
    || host.startsWith("172.18.")
    || host.startsWith("172.19.")
    || host.startsWith("172.2")
    || host.startsWith("172.30.")
    || host.startsWith("172.31.");
}

async function createSession() {
  const payload = {
    participant_id: participantId,
    app_version: APP_VERSION
  };

  if (shouldTryServerApi() && serverLoggingAvailable !== false) {
    try {
      const result = await postJson("/api/session", payload);
      serverLoggingAvailable = true;
      return result.session_id;
    } catch (error) {
      console.warn("server session failed, using local CSV mode", error);
      serverLoggingAvailable = false;
    }
  }

  serverLoggingAvailable = false;
  return createLocalSessionId();
}

function resetIdleClock() {
  const now = Date.now();
  if (idleStart !== null) {
    idleStoredMs += Math.max(0, now - idleStart);
    idleStart = null;
  }
  lastActionAt = now;
}

function currentIdleTotal(now = Date.now()) {
  return idleStoredMs + (idleStart !== null ? Math.max(0, now - idleStart) : 0);
}

function buildAnswerLog(choiceKey) {
  const now = Date.now();
  const isCorrect = choiceKey === currentTask.targetKey;

  return {
    record_type: "answer",
    session_id: sessionId,
    participant_id: participantId,
    question_index: questionIndex,
    event_type: "answer",
    event_at: new Date(now).toISOString(),
    response_time_ms: Math.round(activeQuestionElapsed(now)),
    is_correct: isCorrect,
    idle_total_ms: Math.round(currentIdleTotal(now)),
    choice_count: choiceCount,
    dot_difference: currentTask.difference
  };
}

function buildStressEvent(now = Date.now()) {
  stressEventCount += 1;
  return {
    record_type: "stress_event",
    event_id: `${sessionId}:stress:${stressEventCount}`,
    session_id: sessionId,
    participant_id: participantId,
    question_index: questionIndex,
    event_type: "stress",
    session_elapsed_ms: Math.round(activeSessionElapsed(now)),
    question_elapsed_ms: Math.round(activeQuestionElapsed(now))
  };
}

function buildEndEvent(reason, now = Date.now()) {
  return {
    record_type: "end_event",
    event_id: `${sessionId}:end`,
    session_id: sessionId,
    participant_id: participantId,
    question_index: questionIndex,
    event_type: "end",
    event_at: new Date(now).toISOString(),
    session_elapsed_ms: Math.round(activeSessionElapsed(now)),
    end_reason: reason,
    answered_count: answeredCount
  };
}

function selectedReportValue(name) {
  return document.querySelector(`input[name="${name}"]:checked`)?.value || "";
}

function selectedReportValues(name) {
  return Array.from(document.querySelectorAll(`input[name="${name}"]:checked`))
    .map((input) => input.value)
    .join("|");
}

function resetSelfReport() {
  selfReportSubmitted = false;
  endEventType = "";
  document.querySelectorAll('#selfReportPanel input[type="radio"], #selfReportPanel input[type="checkbox"]').forEach((input) => {
    input.checked = false;
  });
  otherReasonInput.value = "";
  selfReportStatus.textContent = "";
  submitSelfReportBtn.disabled = false;
  skipSelfReportBtn.disabled = false;
  selfReportPanel.hidden = false;
}

function buildSelfReport(responseStatus) {
  const submittedAt = new Date().toISOString();
  return {
    report_id: `${sessionId}:self_report`,
    session_id: sessionId,
    participant_id: participantId,
    submitted_at: submittedAt,
    end_event_type: endEventType,
    end_reason: responseStatus === "skipped" ? "" : selectedReportValue("endReason"),
    concentration_rating: responseStatus === "skipped" ? "" : selectedReportValue("concentrationRating"),
    stress_feelings: responseStatus === "skipped" ? "" : selectedReportValues("stressFeelings"),
    difficulty_rating: responseStatus === "skipped" ? "" : selectedReportValue("difficultyRating"),
    external_interruption: responseStatus === "skipped" ? "" : selectedReportValue("externalInterruption"),
    other_reason: responseStatus === "skipped" ? "" : otherReasonInput.value.trim(),
    response_status: responseStatus
  };
}

async function submitSelfReport(responseStatus) {
  if (selfReportSubmitted || !sessionId) return;

  selfReportSubmitted = true;
  submitSelfReportBtn.disabled = true;
  skipSelfReportBtn.disabled = true;
  selfReportStatus.textContent = "ふりかえりを送信しています。画面を閉じずにお待ちください。";

  enqueueCloudRecord("self_report", buildSelfReport(responseStatus));
  const delivered = await flushAllCloudRecords();
  const succeeded = Boolean(collectorUrl) && delivered && cloudOutbox.length === 0;
  selfReportStatus.textContent = succeeded
    ? responseStatus === "skipped"
      ? "回答しないことを記録しました。これで終了です。"
      : "ふりかえりを送信しました。これで終了です。画面を閉じても大丈夫です。"
    : "ふりかえりを送信できませんでした。接続が回復すると再送します。";
  restartBtn.disabled = false;
}

async function saveLog(log, cloudType) {
  const localLog = { ...log };
  localLog.event_id = makeEventId(cloudType, localLog);
  sessionLogs.push(localLog);
  backupLocalLogs();
  if (downloadCsvBtn) {
    downloadCsvBtn.disabled = sessionLogs.length === 0;
  }

  enqueueCloudRecord(cloudType, localLog);

  if (serverLoggingAvailable === false) {
    return true;
  }

  try {
    await postJson("/api/logs", log);
  } catch (error) {
    serverLoggingAvailable = false;
    console.warn("server log failed, local CSV mode continues", error);
  }
  return true;
}

function tick() {
  if (!sessionId || isFinished) return;

  const now = Date.now();
  if (idleStart === null && now - lastActionAt >= IDLE_THRESHOLD_MS) {
    idleStart = lastActionAt + IDLE_THRESHOLD_MS;
  }
}

function answer(choiceKey) {
  if (isLocked || isFinished) return;

  isLocked = true;
  resetIdleClock();
  const log = buildAnswerLog(choiceKey);
  answeredCount += 1;
  answeredText.textContent = answeredCount;
  saveLog(log, "answer");

  window.requestAnimationFrame(() => {
    if (isFinished) return;
    renderTask();
  });
}

function recordStress() {
  if (!sessionId || isLocked || isFinished) return;

  resetIdleClock();
  const stressEvent = buildStressEvent();
  saveLog(stressEvent, "stress_event");
  stressFeedback.textContent = `ストレスを記録しました（${stressEventCount}回目）`;
}

async function finish(reason) {
  if (isFinished) return;

  isFinished = true;
  window.clearInterval(timer);
  timer = null;

  endEventType = "end";
  if (doneTitle) {
    doneTitle.textContent = "終了しました";
  }
  doneAnsweredText.textContent = answeredCount;
  setDeliveryStatus(
    "sending",
    "データを送信中です",
    "この画面を閉じずに、送信が終わるまでお待ちください。"
  );
  if (downloadCsvBtn) {
    downloadCsvBtn.hidden = true;
    downloadCsvBtn.disabled = true;
  }
  restartBtn.disabled = true;
  workspace.classList.remove("active");
  doneScreen.classList.add("active");

  if (sessionId && currentTask) {
    await saveLog(buildEndEvent(reason), "end_event");
  }
  const allDelivered = await flushAllCloudRecords();

  const automaticCollectionSucceeded =
    Boolean(collectorUrl) && allDelivered && cloudOutbox.length === 0;
  if (automaticCollectionSucceeded) {
    setDeliveryStatus(
      "success",
      "データを送信しました",
      "操作ログの送信が終わりました。続けて下のふりかえりを回答し、「ふりかえりを送信して終了する」を押してください。送信完了が表示されるまで、画面を閉じないでください。"
    );
  } else {
    setDeliveryStatus(
      "error",
      "データを送信できませんでした",
      "CSVを保存して、研究者に渡してください。続けて下のふりかえりも回答し、「ふりかえりを送信して終了する」を押してください。"
    );
  }
  
  if (downloadCsvBtn) {
    downloadCsvBtn.hidden = automaticCollectionSucceeded;
    downloadCsvBtn.disabled = sessionLogs.length === 0;
  }
  if (automaticCollectionSucceeded && sessionBackupKey) {
    localStorage.removeItem(sessionBackupKey);
  }
}

async function start() {
  const enteredParticipantId = participantInput.value.trim();
  if (!enteredParticipantId) {
    alert("個人IDを入力してください。");
    participantInput.focus();
    return;
  }
  participantId = enteredParticipantId;
  collectorUrl = getCollectorUrl();

  sessionId = await createSession();
  sessionLogs = [];
  sessionBackupKey = `learning-log-${sessionId}`;
  sessionStart = Date.now();
  questionIndex = 0;
  choiceCountQueue = [];
  answeredCount = 0;
  stressEventCount = 0;
  isLocked = false;
  isFinished = false;
  resetSelfReport();
  cloudCollectionFailed = false;
  answeredText.textContent = "0";
  stressFeedback.textContent = "ストレスや疲れを感じたときに押してください。問題はそのまま続きます。";

  if (downloadCsvBtn) {
    downloadCsvBtn.hidden = true;
    downloadCsvBtn.disabled = true;
  }

  enqueueCloudRecord("session", {
    created_at: new Date(sessionStart).toISOString(),
    session_id: sessionId,
    participant_id: participantId,
    app_version: APP_VERSION
  });

  startScreen.style.display = "none";
  doneScreen.classList.remove("active");
  workspace.classList.add("active");
  renderTask();
  timer = window.setInterval(tick, 250);
}


collectorUrl = getCollectorUrl();
restoreCloudOutbox();
if (cloudOutbox.length > 0) scheduleCloudFlush(0);
window.addEventListener("online", () => scheduleCloudFlush(0));

startBtn.addEventListener("click", () => {
  start().catch((error) => {
    console.error(error);
    alert("始められませんでした。サーバが動いているか確認してください。");
  });
});



stressBtn.addEventListener("click", recordStress);
giveUpBtn.addEventListener("click", () => finish("give_up"));
document.querySelectorAll('input[name="stressFeelings"]').forEach((input) => {
  input.addEventListener("change", () => {
    const stressFeelingInputs = Array.from(document.querySelectorAll('input[name="stressFeelings"]'));
    const notPressedInput = stressFeelingInputs.find((item) => item.value === "not_pressed");
    if (input.value === "not_pressed" && input.checked) {
      stressFeelingInputs.forEach((item) => {
        if (item !== input) item.checked = false;
      });
    } else if (input.checked && notPressedInput) {
      notPressedInput.checked = false;
    }
  });
});
downloadCsvBtn.addEventListener("click", downloadCsv);
submitSelfReportBtn.addEventListener("click", () => submitSelfReport("completed"));
skipSelfReportBtn.addEventListener("click", () => submitSelfReport("skipped"));
restartBtn.addEventListener("click", () => {
  doneScreen.classList.remove("active");
  startScreen.style.display = "grid";
});
