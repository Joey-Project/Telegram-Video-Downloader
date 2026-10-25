const PROTOCOL = "miniapp-updates-v1";
const SESSION_PATH = "/updates/session";
const WEBSOCKET_PATH = "/api/updates/ws";
const MAX_TICKET_LENGTH = 2_048;
const MAX_MESSAGE_LENGTH = 16 * 1_024;
const CONNECT_TIMEOUT_MS = 10_000;
const READY_TIMEOUT_MS = 10_000;
const REFRESH_TIMEOUT_MS = 125_000;
const REFRESH_QUEUE_TIMEOUT_MS = 125_000;
const MAX_RETRY_MS = 60_000;
const MAX_PENDING_REFRESHES = 32;
const DATABASE_EPOCH_PATTERN = /^[0-9a-f]{32}$/u;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;
const REFRESH_REASONS = new Set(["open", "manual", "reference"]);
const REFRESH_SCOPES = new Set(["library", "settings", "operations", "download"]);

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validEpoch(value) {
  return typeof value === "string" && DATABASE_EPOCH_PATTERN.test(value);
}

function responseStatus(error) {
  return Number.isInteger(error?.status) ? error.status : 0;
}

function validateSession(value, now) {
  if (!isRecord(value)
      || typeof value.ticket !== "string"
      || value.ticket.length === 0 || value.ticket.length > MAX_TICKET_LENGTH
      || /[\u0000-\u001f\u007f]/u.test(value.ticket)
      || value.websocket_path !== WEBSOCKET_PATH
      || value.protocol !== PROTOCOL
      || !Number.isSafeInteger(value.expires_at) || value.expires_at <= now
      || !validEpoch(value.database_epoch)) {
    throw new Error("The Mini App update session response is invalid.");
  }
  return value;
}

function parseMessage(data) {
  if (typeof data !== "string" || data.length === 0 || data.length > MAX_MESSAGE_LENGTH) return null;
  let value;
  try { value = JSON.parse(data); } catch { return null; }
  if (!isRecord(value)) return null;
  if (value.type === "ready") {
    if (!validEpoch(value.database_epoch)
        || Object.hasOwn(value, "snapshot_control") && value.snapshot_control !== "v1") return null;
    return value;
  }
  if (value.type === "invalidate") {
    if (!validEpoch(value.database_epoch)
        || !["state", "inbox", "epoch"].includes(value.reason)
        || Object.hasOwn(value, "state_version")
          && (!Number.isSafeInteger(value.state_version) || value.state_version < 0)) return null;
    return value;
  }
  if (value.type === "refresh_result") {
    if (!validEpoch(value.database_epoch)
        || typeof value.request_id !== "string" || !UUID_PATTERN.test(value.request_id)
        || !["published", "failed", "unavailable"].includes(value.status)
        || Object.hasOwn(value, "state_version")
          && (!Number.isSafeInteger(value.state_version) || value.state_version < 1)
        || Object.hasOwn(value, "reported_at")
          && (!Number.isSafeInteger(value.reported_at) || value.reported_at < 1)
        || Object.hasOwn(value, "error_code")
          && (typeof value.error_code !== "string" || !/^[a-z0-9_-]{1,64}$/iu.test(value.error_code))) return null;
    if (value.status === "published"
        && (!Number.isSafeInteger(value.state_version) || value.state_version < 1
          || !Number.isSafeInteger(value.reported_at) || value.reported_at < 1)) return null;
    return value;
  }
  return null;
}

function websocketUrl(path, location) {
  if (!location || typeof location.origin !== "string") throw new Error("The Mini App origin is unavailable.");
  const url = new URL(path, location.origin);
  if (url.origin !== location.origin || url.pathname !== WEBSOCKET_PATH || url.search || url.hash) {
    throw new Error("The Mini App update session returned an unsafe WebSocket URL.");
  }
  if (location.protocol === "https:") url.protocol = "wss:";
  else if (location.protocol === "http:") url.protocol = "ws:";
  else throw new Error("The Mini App origin does not support WebSocket updates.");
  return url.href;
}

function unavailableResult(errorCode = "unavailable") {
  return { status: "unavailable", error_code: errorCode };
}

/**
 * Own the authenticated notification socket, viewer interest, and on-demand
 * refresh requests. `readSnapshot` must only read the published Cloudflare cache.
 */
export function createSnapshotUpdates({
  api,
  readSnapshot,
  surface,
  onAuthorizationExpired,
  onReady,
  onEpochChanged,
  onStatus,
  environment = {},
}) {
  if (typeof api !== "function" || typeof readSnapshot !== "function") {
    throw new TypeError("Snapshot updates require API and published-snapshot callbacks.");
  }
  if (surface !== "file" && surface !== "settings") {
    throw new TypeError("Snapshot updates require a Mini App surface.");
  }

  const win = environment.window ?? globalThis.window ?? null;
  const doc = environment.document ?? globalThis.document ?? null;
  const nav = environment.navigator ?? globalThis.navigator ?? null;
  const location = environment.location ?? win?.location ?? globalThis.location ?? null;
  const WebSocketConstructor = environment.WebSocket ?? win?.WebSocket ?? globalThis.WebSocket;
  const AbortControllerConstructor = environment.AbortController ?? globalThis.AbortController;
  const telegram = environment.telegram ?? win?.Telegram?.WebApp ?? null;
  const now = environment.now ?? Date.now;
  const random = environment.random ?? Math.random;
  const randomUUID = environment.randomUUID ?? (() => globalThis.crypto.randomUUID());
  const setTimer = environment.setTimeout ?? win?.setTimeout?.bind(win) ?? globalThis.setTimeout.bind(globalThis);
  const clearTimerFn = environment.clearTimeout ?? win?.clearTimeout?.bind(win) ?? globalThis.clearTimeout.bind(globalThis);

  let started = false;
  let expired = false;
  let unsupported = false;
  let telegramActive = true;
  let lifecyclePaused = false;
  let status = "paused";
  let socket = null;
  let connecting = false;
  let connectionSequence = 0;
  let sessionController = null;
  let authExpiresAt = null;
  let databaseEpoch = null;
  let epochVerificationPending = false;
  let snapshotControlV1 = false;
  let lastInterestKey = null;
  let epochReverification = false;
  let retryDelay = 1_000;
  let desiredWatch = false;
  let snapshotReadPromise = null;
  let snapshotReadAgain = false;
  let snapshotReadMinimumVersion = 0;
  let snapshotReadEpochChanged = false;
  let snapshotReadExpectedEpoch = null;
  let connectionTimer = 0;
  let readyTimer = 0;
  let expiryTimer = 0;
  let retryTimer = 0;
  let listenersAttached = false;
  let pendingRefreshes = new Map();
  let coalescedRefreshes = new Map();
  const intentionallyClosedSockets = new WeakSet();

  function emitStatus(next) {
    if (status === next) return;
    status = next;
    try { onStatus?.(next); } catch { /* UI callbacks must not interrupt transport recovery. */ }
  }

  function isOnline() {
    return nav?.onLine !== false;
  }

  function isVisible() {
    return isOnline()
      && doc?.visibilityState !== "hidden"
      && doc?.hidden !== true
      && telegramActive !== false;
  }

  function isPaused() {
    return !isVisible();
  }

  function clearTimer(timer) {
    if (timer) clearTimerFn(timer);
  }

  function clearConnectionTimers() {
    clearTimer(connectionTimer);
    clearTimer(readyTimer);
    connectionTimer = 0;
    readyTimer = 0;
  }

  function clearReconnectTimer() {
    clearTimer(retryTimer);
    retryTimer = 0;
  }

  function settleRefresh(entry, result) {
    if (!pendingRefreshes.has(entry.requestId)) return;
    pendingRefreshes.delete(entry.requestId);
    if (coalescedRefreshes.get(entry.coalesceKey) === entry) coalescedRefreshes.delete(entry.coalesceKey);
    clearTimer(entry.queueTimeout);
    clearTimer(entry.responseTimeout);
    entry.resolve(result);
  }

  function settleAllRefreshes(result) {
    for (const entry of pendingRefreshes.values()) settleRefresh(entry, result);
  }

  function sendFrame(value) {
    if (!socket || socket.readyState !== 1) return false;
    try {
      socket.send(JSON.stringify(value));
      return true;
    } catch {
      return false;
    }
  }

  function sendInterest(visible = isVisible(), force = false) {
    if (!snapshotControlV1 || !databaseEpoch) return;
    const watch = visible && desiredWatch;
    const key = `${databaseEpoch}:${visible ? "1" : "0"}:${surface}:${watch ? "1" : "0"}`;
    if (!force && key === lastInterestKey) return;
    if (!sendFrame({
      type: "interest",
      database_epoch: databaseEpoch,
      visible,
      surface,
      watch,
    })) return;
    lastInterestKey = key;
  }

  function refreshFrame(entry) {
    return {
      type: "refresh_request",
      database_epoch: databaseEpoch,
      request_id: entry.requestId,
      reason: entry.reason,
      scope: entry.scope,
      ...(entry.libraryId ? { library_id: entry.libraryId } : {}),
    };
  }

  function flushUnsentRefreshes() {
    if (!snapshotControlV1 || !databaseEpoch || isPaused()) return;
    for (const entry of pendingRefreshes.values()) {
      if (entry.sent || entry.epoch !== null && entry.epoch !== databaseEpoch) continue;
      if (!sendFrame(refreshFrame(entry))) continue;
      entry.epoch = databaseEpoch;
      entry.sent = true;
      clearTimer(entry.queueTimeout);
      entry.queueTimeout = 0;
      entry.responseTimeout = setTimer(
        () => settleRefresh(entry, { status: "failed", error_code: "timeout" }),
        REFRESH_TIMEOUT_MS,
      );
    }
  }

  function settleSentRefreshesOnDisconnect() {
    for (const entry of pendingRefreshes.values()) {
      if (entry.sent && !entry.resultReceived) {
        settleRefresh(entry, unavailableResult("transport_disconnected"));
      }
    }
  }

  function requestSnapshotRead(minimumVersion = 0, { epochChanged = false, expectedEpoch = databaseEpoch } = {}) {
    const requestedEpoch = typeof expectedEpoch === "string" ? expectedEpoch : null;
    if (snapshotReadExpectedEpoch !== requestedEpoch) {
      snapshotReadExpectedEpoch = requestedEpoch;
      snapshotReadMinimumVersion = minimumVersion;
    } else {
      snapshotReadMinimumVersion = Math.max(snapshotReadMinimumVersion, minimumVersion);
    }
    snapshotReadAgain = true;
    snapshotReadEpochChanged ||= epochChanged;
    if (snapshotReadPromise) return snapshotReadPromise;

    snapshotReadPromise = (async () => {
      let snapshot;
      let attempts = 0;
      while (snapshotReadAgain && started && !expired && !isPaused()) {
        snapshotReadAgain = false;
        const readEpoch = snapshotReadExpectedEpoch;
        const minimum = snapshotReadMinimumVersion;
        snapshotReadMinimumVersion = 0;
        const reverifyEpoch = snapshotReadEpochChanged;
        snapshotReadEpochChanged = false;
        snapshot = await readSnapshot({
          quiet: true,
          minimumVersion: minimum,
          ...(readEpoch ? { expectedDatabaseEpoch: readEpoch } : {}),
          ...(reverifyEpoch ? { epochChanged: true } : {}),
        });
        const latestExpectedEpoch = snapshotReadExpectedEpoch;
        if (readEpoch !== latestExpectedEpoch && snapshot?.database_epoch !== latestExpectedEpoch) {
          snapshotReadAgain = true;
          continue;
        }
        if (latestExpectedEpoch && snapshot?.database_epoch !== latestExpectedEpoch) {
          snapshotReadAgain = false;
          snapshotReadMinimumVersion = 0;
          throw new Error("The owner snapshot belongs to a different database epoch.");
        }
        if (reverifyEpoch || snapshotReadEpochChanged) {
          snapshotReadEpochChanged = false;
          epochVerificationPending = false;
        }
        attempts += 1;
        const version = Number.isSafeInteger(snapshot?.state_version) ? snapshot.state_version : 0;
        const epochMatchesRead = readEpoch === latestExpectedEpoch;
        const requiredVersion = Math.max(epochMatchesRead ? minimum : 0, snapshotReadMinimumVersion);
        if (version < requiredVersion && attempts < 2) {
          snapshotReadAgain = true;
          snapshotReadMinimumVersion = requiredVersion;
          continue;
        }
        if (version < requiredVersion) {
          snapshotReadAgain = false;
          snapshotReadMinimumVersion = 0;
          throw new Error("The published snapshot is behind the desktop refresh result.");
        }
        if (snapshotReadAgain && snapshotReadMinimumVersion <= version && !snapshotReadEpochChanged) {
          snapshotReadAgain = false;
          snapshotReadMinimumVersion = 0;
        }
      }
      return snapshot;
    })().finally(() => {
      snapshotReadPromise = null;
    });
    return snapshotReadPromise;
  }

  async function finishPublishedRefresh(entry, message) {
    try {
      const snapshot = await requestSnapshotRead(message.state_version);
      settleRefresh(entry, { ...message, snapshot });
    } catch {
      // Publication is independently confirmed by the correlated Mac result.
      settleRefresh(entry, { ...message, cache_read_failed: true });
    }
  }

  function handleRefreshResult(message) {
    if (!snapshotControlV1 || message.database_epoch !== databaseEpoch) return;
    const entry = pendingRefreshes.get(message.request_id);
    if (!entry || entry.epoch !== message.database_epoch || !entry.sent) return;
    if (message.status === "published") {
      entry.resultReceived = true;
      clearTimer(entry.responseTimeout);
      entry.responseTimeout = 0;
      void finishPublishedRefresh(entry, message);
      return;
    }
    settleRefresh(entry, message);
  }

  async function reverifyEpoch() {
    if (epochReverification || !epochVerificationPending || !started || expired || isPaused()) return;
    epochReverification = true;
    try {
      const snapshot = await requestSnapshotRead(0, { epochChanged: true, expectedEpoch: databaseEpoch });
      if (snapshot !== undefined) epochVerificationPending = false;
    } catch {
      epochVerificationPending = true;
    } finally {
      epochReverification = false;
      if (started && !expired && !isPaused()) {
        if (!socket && !connecting) connect();
        else if (!epochVerificationPending) flushUnsentRefreshes();
      }
    }
  }

  function handleInvalidate(message) {
    if (message.reason === "epoch") {
      if (message.database_epoch === databaseEpoch && epochVerificationPending) return;
      epochVerificationPending = true;
      databaseEpoch = message.database_epoch;
      try { onEpochChanged?.({ databaseEpoch: message.database_epoch }); } catch { /* Epoch fencing must not break transport recovery. */ }
      cancelPendingRefreshes("database_epoch_changed");
      snapshotControlV1 = false;
      snapshotReadMinimumVersion = 0;
      snapshotReadExpectedEpoch = databaseEpoch;
      snapshotReadEpochChanged = true;
      snapshotReadAgain = true;
      lastInterestKey = null;
      stopTransport();
      void reverifyEpoch();
      return;
    }
    if (message.database_epoch !== databaseEpoch) return;
    if (!isVisible() || !desiredWatch && pendingRefreshes.size === 0) return;
    const minimumVersion = Number.isSafeInteger(message.state_version) ? message.state_version : 0;
    void requestSnapshotRead(minimumVersion).catch(() => {});
  }

  function scheduleReconnect() {
    if (!started || expired || unsupported || isPaused() || retryTimer || connecting || socket) return;
    const jitteredDelay = Math.max(1_000, Math.min(MAX_RETRY_MS, Math.round(retryDelay * (0.75 + random() * 0.5))));
    retryDelay = Math.min(MAX_RETRY_MS, retryDelay * 2);
    retryTimer = setTimer(() => {
      retryTimer = 0;
      connect();
    }, jitteredDelay);
  }

  function closeSocket(code = 1000, reason = "Mini App paused") {
    const current = socket;
    if (!current) return;
    sendInterest(false);
    socket = null;
    intentionallyClosedSockets.add(current);
    try { current.close(code, reason); } catch { /* The socket may already be closed. */ }
  }

  function stopTransport() {
    connectionSequence += 1;
    sessionController?.abort();
    sessionController = null;
    connecting = false;
    clearConnectionTimers();
    clearReconnectTimer();
    closeSocket();
  }

  function clearExpiryTimer() {
    clearTimer(expiryTimer);
    expiryTimer = 0;
  }

  function removeLifecycleListeners() {
    if (!listenersAttached) return;
    listenersAttached = false;
    win?.removeEventListener?.("online", handleOnline);
    win?.removeEventListener?.("offline", handleOffline);
    doc?.removeEventListener?.("visibilitychange", handleVisibilityChange);
    telegram?.offEvent?.("activated", handleTelegramActivated);
    telegram?.offEvent?.("deactivated", handleTelegramDeactivated);
  }

  function cancelPendingRefreshes(errorCode = "unavailable") {
    settleAllRefreshes(unavailableResult(errorCode));
  }

  function stop() {
    if (!started) return;
    sendInterest(false);
    started = false;
    stopTransport();
    clearExpiryTimer();
    cancelPendingRefreshes("stopped");
    removeLifecycleListeners();
    if (!expired) emitStatus("paused");
  }

  function expireAuthorization() {
    if (expired) return;
    sendInterest(false);
    expired = true;
    started = false;
    stopTransport();
    clearExpiryTimer();
    cancelPendingRefreshes("authorization_expired");
    removeLifecycleListeners();
    emitStatus("expired");
    try { onAuthorizationExpired?.(); } catch { /* Access gating must not restart transport. */ }
  }

  function scheduleAuthorizationExpiry() {
    clearExpiryTimer();
    if (!Number.isSafeInteger(authExpiresAt)) return;
    const remaining = authExpiresAt - now();
    if (remaining <= 0) {
      expireAuthorization();
      return;
    }
    expiryTimer = setTimer(() => {
      expiryTimer = 0;
      if (now() >= authExpiresAt) expireAuthorization();
      else scheduleAuthorizationExpiry();
    }, remaining);
  }

  function handleLifecyclePause() {
    if (!started || expired) return;
    lifecyclePaused = true;
    sendInterest(false);
    stopTransport();
    cancelPendingRefreshes(isOnline() ? "hidden" : "offline");
    emitStatus(!isOnline() ? "offline" : "paused");
  }

  function resumeFromLifecycle() {
    if (!started || expired) return;
    if (isPaused()) {
      handleLifecyclePause();
      return;
    }
    lifecyclePaused = false;
    clearReconnectTimer();
    if (unsupported || typeof WebSocketConstructor !== "function") {
      emitStatus("unavailable");
      return;
    }
    connect();
  }

  function handleOnline() {
    resumeFromLifecycle();
  }

  function handleOffline() {
    handleLifecyclePause();
  }

  function handleVisibilityChange() {
    if (isPaused()) handleLifecyclePause();
    else resumeFromLifecycle();
  }

  function handleTelegramActivated() {
    telegramActive = true;
    resumeFromLifecycle();
  }

  function handleTelegramDeactivated() {
    telegramActive = false;
    handleLifecyclePause();
  }

  function attachLifecycleListeners() {
    if (listenersAttached) return;
    listenersAttached = true;
    win?.addEventListener?.("online", handleOnline);
    win?.addEventListener?.("offline", handleOffline);
    doc?.addEventListener?.("visibilitychange", handleVisibilityChange);
    telegram?.onEvent?.("activated", handleTelegramActivated);
    telegram?.onEvent?.("deactivated", handleTelegramDeactivated);
  }

  function finishConnectionFailure(sequence, { unsupportedRoute = false, authExpired = false } = {}) {
    if (sequence !== connectionSequence || !started || expired) return;
    if (authExpired) {
      expireAuthorization();
      return;
    }
    connectionSequence += 1;
    sessionController?.abort();
    sessionController = null;
    connecting = false;
    clearConnectionTimers();
    settleSentRefreshesOnDisconnect();
    closeSocket();
    if (unsupportedRoute) {
      unsupported = true;
      cancelPendingRefreshes("transport_unavailable");
      emitStatus("unavailable");
      return;
    }
    emitStatus("fallback");
    scheduleReconnect();
  }

  function handleSocketFailure(current, { authExpired = false } = {}) {
    const wasCurrent = socket === current;
    const intentionallyClosed = intentionallyClosedSockets.has(current);
    if (authExpired) {
      expireAuthorization();
      return;
    }
    if (intentionallyClosed || !wasCurrent) return;
    settleSentRefreshesOnDisconnect();
    socket = null;
    connecting = false;
    clearConnectionTimers();
    snapshotControlV1 = false;
    emitStatus("fallback");
    scheduleReconnect();
  }

  function attachSocketHandlers(current, sequence) {
    current.addEventListener("open", () => {
      if (!started || sequence !== connectionSequence || socket !== current) return;
      clearTimer(connectionTimer);
      connectionTimer = 0;
      if (current.protocol !== PROTOCOL) {
        finishConnectionFailure(sequence, { unsupportedRoute: true });
        return;
      }
      readyTimer = setTimer(() => {
        readyTimer = 0;
        finishConnectionFailure(sequence);
      }, READY_TIMEOUT_MS);
    });
    current.addEventListener("message", (event) => {
      if (!started || sequence !== connectionSequence || socket !== current) return;
      const message = parseMessage(event.data);
      if (!message) {
        finishConnectionFailure(sequence, { unsupportedRoute: true });
        return;
      }
      if (message.type === "ready") {
        clearTimer(readyTimer);
        readyTimer = 0;
        connecting = false;
        retryDelay = 1_000;
        clearReconnectTimer();
        const epochChanged = databaseEpoch !== null && databaseEpoch !== message.database_epoch;
        databaseEpoch = message.database_epoch;
        snapshotControlV1 = message.snapshot_control === "v1";
        try { onReady?.({ databaseEpoch, snapshotControl: snapshotControlV1 ? "v1" : null }); } catch { /* Readiness observers must not interrupt transport recovery. */ }
        lastInterestKey = null;
        if (epochChanged) {
          epochVerificationPending = true;
          cancelPendingRefreshes("database_epoch_changed");
          try { onEpochChanged?.({ databaseEpoch: message.database_epoch }); } catch { /* Epoch fencing must not break transport recovery. */ }
          snapshotReadMinimumVersion = 0;
          snapshotReadExpectedEpoch = databaseEpoch;
          snapshotReadEpochChanged = true;
          snapshotReadAgain = true;
        }
        emitStatus("live");
        sendInterest(isVisible(), true);
        if (!snapshotControlV1) cancelPendingRefreshes("unsupported");
        if (epochVerificationPending) void reverifyEpoch();
        flushUnsentRefreshes();
        return;
      }
      if (message.type === "invalidate") {
        handleInvalidate(message);
        return;
      }
      handleRefreshResult(message);
    });
    current.addEventListener("error", () => {
      if (socket === current) finishConnectionFailure(sequence);
    });
    current.addEventListener("close", (event) => {
      handleSocketFailure(current, { authExpired: event.code === 4401 });
    });
  }

  async function connect() {
    if (!started || expired || unsupported || isPaused() || connecting || socket) return;
    if (typeof WebSocketConstructor !== "function") {
      unsupported = true;
      emitStatus("unavailable");
      cancelPendingRefreshes("transport_unavailable");
      return;
    }

    connecting = true;
    const sequence = ++connectionSequence;
    emitStatus("connecting");
    const controller = typeof AbortControllerConstructor === "function" ? new AbortControllerConstructor() : null;
    sessionController = controller;
    connectionTimer = setTimer(() => {
      connectionTimer = 0;
      if (sequence !== connectionSequence) return;
      controller?.abort();
      finishConnectionFailure(sequence);
    }, CONNECT_TIMEOUT_MS);

    try {
      const session = validateSession(
        await api(SESSION_PATH, { method: "POST", ...(controller ? { signal: controller.signal } : {}) }),
        now(),
      );
      if (!started || expired || sequence !== connectionSequence) return;
      authExpiresAt = authExpiresAt === null ? session.expires_at : Math.min(authExpiresAt, session.expires_at);
      scheduleAuthorizationExpiry();
      if (expired) return;
      const url = websocketUrl(session.websocket_path, location);
      const current = new WebSocketConstructor(url, [PROTOCOL, session.ticket]);
      socket = current;
      attachSocketHandlers(current, sequence);
    } catch (error) {
      if (!started || expired || sequence !== connectionSequence) return;
      const statusCode = responseStatus(error);
      if (statusCode === 401 || statusCode === 403) {
        expireAuthorization();
        return;
      }
      const unsupportedRoute = [404, 405, 501].includes(statusCode)
        || /session response is invalid|unsafe WebSocket URL|does not support WebSocket/u.test(String(error?.message ?? ""));
      finishConnectionFailure(sequence, { unsupportedRoute });
    }
  }

  function refreshCoalesceKey({ scope, libraryId }) {
    return `${scope}:${libraryId ?? ""}`;
  }

  function requestMacRefresh({ reason, scope, libraryId } = {}) {
    if (!REFRESH_REASONS.has(reason) || !REFRESH_SCOPES.has(scope)
        || libraryId !== undefined && (typeof libraryId !== "string" || new TextEncoder().encode(libraryId).byteLength > 256
          || /[\u0000-\u001f\u007f]/u.test(libraryId))
        || libraryId !== undefined && !["library", "download"].includes(scope)) {
      return Promise.reject(new TypeError("The Mini App snapshot refresh request is invalid."));
    }
    if (!started || expired || isPaused()) return Promise.resolve(unavailableResult(isOnline() ? "not_visible" : "offline"));
    if (unsupported || typeof WebSocketConstructor !== "function") return Promise.resolve(unavailableResult("transport_unavailable"));
    if (socket && status === "live" && !snapshotControlV1) return Promise.resolve(unavailableResult("unsupported"));

    const coalesceKey = refreshCoalesceKey({ scope, libraryId });
    const existing = coalescedRefreshes.get(coalesceKey);
    if (existing) return existing.promise;
    if (pendingRefreshes.size >= MAX_PENDING_REFRESHES) return Promise.resolve(unavailableResult("too_many_requests"));

    const requestId = randomUUID();
    if (typeof requestId !== "string" || !UUID_PATTERN.test(requestId)) {
      return Promise.reject(new Error("The Mini App could not create a valid refresh request ID."));
    }
    let resolve;
    const promise = new Promise((settle) => { resolve = settle; });
    const entry = {
      requestId,
      reason,
      scope,
      libraryId,
      coalesceKey,
      epoch: null,
      sent: false,
      resultReceived: false,
      resolve,
      promise,
      queueTimeout: 0,
      responseTimeout: 0,
    };
    entry.queueTimeout = setTimer(
      () => settleRefresh(entry, { status: "failed", error_code: "timeout" }),
      REFRESH_QUEUE_TIMEOUT_MS,
    );
    pendingRefreshes.set(requestId, entry);
    coalescedRefreshes.set(coalesceKey, entry);
    if (status === "live" && snapshotControlV1) flushUnsentRefreshes();
    else if (!connecting && !socket) connect();
    return promise;
  }

  function setWatch(watch) {
    desiredWatch = watch === true;
    sendInterest();
  }

  function start() {
    if (started || expired) return;
    started = true;
    attachLifecycleListeners();
    if (isPaused()) {
      handleLifecyclePause();
      return;
    }
    connect();
  }

  return { start, stop, setWatch, requestMacRefresh };
}
