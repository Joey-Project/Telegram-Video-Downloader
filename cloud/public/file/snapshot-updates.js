const PROTOCOL = "miniapp-updates-v1";
const SESSION_PATH = "/updates/session";
const WEBSOCKET_PATH = "/api/updates/ws";
const MAX_TICKET_LENGTH = 2_048;
const MAX_MESSAGE_LENGTH = 16 * 1_024;
const CONNECT_TIMEOUT_MS = 10_000;
const READY_TIMEOUT_MS = 10_000;
const FALLBACK_INTERVAL_MS = 60_000;
const RECONCILE_INTERVAL_MS = 5 * 60_000;
const MIN_REFRESH_INTERVAL_MS = 1_000;
const MAX_RETRY_MS = 60_000;
const MAX_REFRESH_WAITERS = 32;
const DATABASE_EPOCH_PATTERN = /^[0-9a-f]{32}$/u;

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
    return validEpoch(value.database_epoch) ? value : null;
  }
  if (value.type === "invalidate") {
    if (!validEpoch(value.database_epoch)
        || !["state", "inbox", "epoch"].includes(value.reason)
        || Object.hasOwn(value, "state_version")
          && (!Number.isSafeInteger(value.state_version) || value.state_version < 0)) return null;
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

/**
 * Own the authenticated update socket, bounded fallback polling, and refresh coalescing.
 * The API callback must retain the Mini App's existing authenticated fetch behavior.
 */
export function createSnapshotUpdates({ api, refresh: refreshSnapshot, onAuthorizationExpired, onStatus, environment = {} }) {
  if (typeof api !== "function" || typeof refreshSnapshot !== "function") {
    throw new TypeError("Snapshot updates require API and refresh callbacks.");
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
  const setTimer = environment.setTimeout ?? win?.setTimeout?.bind(win) ?? globalThis.setTimeout.bind(globalThis);
  const clearTimerFn = environment.clearTimeout ?? win?.clearTimeout?.bind(win) ?? globalThis.clearTimeout.bind(globalThis);

  let started = false;
  let expired = false;
  let unsupported = false;
  let telegramActive = true;
  let lifecyclePaused = false;
  let needsCatchup = true;
  let status = "paused";
  let socket = null;
  let connecting = false;
  let connectionSequence = 0;
  let sessionController = null;
  let authExpiresAt = null;
  let retryDelay = 1_000;
  let lastRefreshStartedAt = -Infinity;
  let refreshRetryDelay = 1_000;
  let refreshDirty = false;
  let refreshQuiet = true;
  let refreshTimer = 0;
  let refreshRetryTimer = 0;
  let refreshPromise = null;
  let refreshWaiters = [];
  let activeRefreshWaiters = [];
  let connectionTimer = 0;
  let readyTimer = 0;
  let expiryTimer = 0;
  let retryTimer = 0;
  let fallbackTimer = 0;
  let reconcileTimer = 0;
  const intentionallyClosedSockets = new WeakSet();
  let listenersAttached = false;

  function emitStatus(next) {
    if (status === next) return;
    status = next;
    try { onStatus?.(next); } catch { /* UI callbacks must not interrupt transport recovery. */ }
  }

  function isOnline() {
    return nav?.onLine !== false;
  }

  function isPaused() {
    return !isOnline() || doc?.visibilityState === "hidden" || doc?.hidden === true || telegramActive === false;
  }

  function clearTimer(name) {
    if (!name) return;
    clearTimerFn(name);
  }

  function clearConnectionTimers() {
    clearTimer(connectionTimer);
    clearTimer(readyTimer);
    connectionTimer = 0;
    readyTimer = 0;
  }

  function settleWaiters(waiters, error) {
    for (const waiter of waiters) {
      if (error) waiter.reject(error);
      else waiter.resolve();
    }
  }

  function settleRefreshWaiters(error) {
    const waiters = [...activeRefreshWaiters, ...refreshWaiters];
    activeRefreshWaiters = [];
    refreshWaiters = [];
    settleWaiters(waiters, error);
  }

  function clearRefreshRetryTimer() {
    clearTimer(refreshRetryTimer);
    refreshRetryTimer = 0;
  }

  function scheduleRefresh(delay = 0) {
    if (!started || expired || !refreshDirty || refreshTimer || refreshPromise || isPaused()) return;
    const throttle = Math.max(0, MIN_REFRESH_INTERVAL_MS - (now() - lastRefreshStartedAt));
    refreshTimer = setTimer(runRefresh, Math.max(delay, throttle));
  }

  function requestRefresh(quiet) {
    if (!started || expired) return;
    clearRefreshRetryTimer();
    refreshDirty = true;
    refreshQuiet = refreshQuiet && quiet;
    scheduleRefresh();
  }

  function scheduleRefreshRetry() {
    if (!started || expired || isPaused() || refreshRetryTimer) return;
    const delay = refreshRetryDelay;
    refreshRetryDelay = Math.min(MAX_RETRY_MS, refreshRetryDelay * 2);
    refreshRetryTimer = setTimer(() => {
      refreshRetryTimer = 0;
      if (!started || expired || isPaused()) return;
      refreshDirty = true;
      scheduleRefresh();
    }, delay);
  }

  async function runRefresh() {
    refreshTimer = 0;
    if (!started || expired || !refreshDirty) return;
    if (isPaused()) return;
    refreshDirty = false;
    const quiet = refreshQuiet;
    refreshQuiet = true;
    const attemptWaiters = refreshWaiters;
    refreshWaiters = [];
    activeRefreshWaiters = attemptWaiters;
    lastRefreshStartedAt = now();
    refreshPromise = Promise.resolve().then(() => refreshSnapshot({ quiet }));
    let failure = null;
    try {
      await refreshPromise;
    } catch (error) {
      failure = error;
    } finally {
      refreshPromise = null;
      if (activeRefreshWaiters === attemptWaiters) activeRefreshWaiters = [];
      if (failure) {
        settleWaiters(attemptWaiters, failure);
        if (started && !expired && !isPaused()) {
          if (!refreshDirty) {
            refreshDirty = true;
            refreshQuiet = true;
          }
          scheduleRefreshRetry();
        }
      } else {
        settleWaiters(attemptWaiters);
        refreshRetryDelay = 1_000;
        clearRefreshRetryTimer();
        if (refreshDirty && started && !expired && !isPaused()) {
          scheduleRefresh();
        }
      }
    }
  }

  function refresh(options = {}) {
    if (expired || !started) return Promise.resolve();
    requestRefresh(options.quiet === true);
    if (refreshWaiters.length + activeRefreshWaiters.length >= MAX_REFRESH_WAITERS) return Promise.resolve();
    const promise = new Promise((resolve, reject) => refreshWaiters.push({ resolve, reject }));
    return promise;
  }

  function queueQuietRefresh() {
    requestRefresh(true);
  }

  function clearFallbackTimer() {
    clearTimer(fallbackTimer);
    fallbackTimer = 0;
  }

  function scheduleFallback() {
    if (!started || expired || isPaused() || status === "live" || fallbackTimer) return;
    fallbackTimer = setTimer(() => {
      fallbackTimer = 0;
      if (!started || expired || isPaused() || status === "live") return;
      queueQuietRefresh();
      scheduleFallback();
    }, FALLBACK_INTERVAL_MS);
  }

  function clearReconnectTimer() {
    clearTimer(retryTimer);
    retryTimer = 0;
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

  function scheduleReconciliation() {
    clearTimer(reconcileTimer);
    reconcileTimer = 0;
    if (!started || expired || isPaused() || status !== "live") return;
    reconcileTimer = setTimer(() => {
      reconcileTimer = 0;
      if (!started || expired || isPaused() || status !== "live") return;
      queueQuietRefresh();
      scheduleReconciliation();
    }, RECONCILE_INTERVAL_MS);
  }

  function closeSocket(code = 1000, reason = "Mini App paused") {
    const current = socket;
    socket = null;
    if (!current) return;
    intentionallyClosedSockets.add(current);
    try { current.close(code, reason); } catch { /* The socket may already be closed. */ }
  }

  function clearExpiryTimer() {
    clearTimer(expiryTimer);
    expiryTimer = 0;
  }

  function stopTransport() {
    connectionSequence += 1;
    sessionController?.abort();
    sessionController = null;
    connecting = false;
    clearTimer(refreshTimer);
    refreshTimer = 0;
    clearRefreshRetryTimer();
    clearConnectionTimers();
    clearReconnectTimer();
    clearFallbackTimer();
    clearTimer(reconcileTimer);
    reconcileTimer = 0;
    closeSocket();
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

  function stop() {
    if (!started) return;
    started = false;
    stopTransport();
    clearExpiryTimer();
    clearTimer(refreshTimer);
    refreshTimer = 0;
    refreshDirty = false;
    settleRefreshWaiters();
    removeLifecycleListeners();
    if (!expired) emitStatus("paused");
  }

  function expireAuthorization() {
    if (expired) return;
    expired = true;
    started = false;
    stopTransport();
    clearExpiryTimer();
    clearTimer(refreshTimer);
    refreshTimer = 0;
    refreshDirty = false;
    settleRefreshWaiters();
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

  function enterFallback({ reconnect = true, immediateCatchup = false } = {}) {
    if (!started || expired) return;
    if (isPaused()) {
      handleLifecyclePause();
      return;
    }
    emitStatus("fallback");
    clearTimer(reconcileTimer);
    reconcileTimer = 0;
    if (immediateCatchup && needsCatchup) {
      needsCatchup = false;
      queueQuietRefresh();
    }
    scheduleFallback();
    if (reconnect) scheduleReconnect();
  }

  function finishConnectionFailure(sequence, { unsupportedRoute = false, immediateCatchup = false } = {}) {
    if (sequence !== connectionSequence || !started || expired) return;
    connectionSequence += 1;
    sessionController?.abort();
    sessionController = null;
    connecting = false;
    clearConnectionTimers();
    closeSocket();
    if (unsupportedRoute) unsupported = true;
    if (status === "live") needsCatchup = true;
    enterFallback({ reconnect: !unsupported, immediateCatchup: immediateCatchup || needsCatchup });
  }

  function handleSocketFailure(current, { authExpired = false } = {}) {
    const wasCurrent = socket === current;
    const intentionallyClosed = intentionallyClosedSockets.has(current);
    if (authExpired) {
      expireAuthorization();
      return;
    }
    if (intentionallyClosed || !wasCurrent) return;
    if (status === "live") needsCatchup = true;
    socket = null;
    connecting = false;
    clearConnectionTimers();
    enterFallback({ immediateCatchup: needsCatchup });
  }

  function attachSocketHandlers(current, sequence) {
    current.addEventListener("open", () => {
      if (!started || sequence !== connectionSequence || socket !== current) return;
      clearTimer(connectionTimer);
      connectionTimer = 0;
      if (current.protocol !== PROTOCOL) {
        finishConnectionFailure(sequence, { unsupportedRoute: true, immediateCatchup: true });
        return;
      }
      readyTimer = setTimer(() => {
        readyTimer = 0;
        finishConnectionFailure(sequence, { immediateCatchup: true });
      }, READY_TIMEOUT_MS);
    });
    current.addEventListener("message", (event) => {
      if (!started || sequence !== connectionSequence || socket !== current) return;
      const message = parseMessage(event.data);
      if (!message) {
        finishConnectionFailure(sequence, { immediateCatchup: true });
        return;
      }
      if (message.type === "ready") {
        if (status === "live") return;
        clearTimer(readyTimer);
        readyTimer = 0;
        connecting = false;
        retryDelay = 1_000;
        clearReconnectTimer();
        clearFallbackTimer();
        emitStatus("live");
        needsCatchup = false;
        queueQuietRefresh();
        scheduleReconciliation();
        return;
      }
      scheduleReconciliation();
      queueQuietRefresh();
    });
    current.addEventListener("error", () => {
      if (socket === current) finishConnectionFailure(sequence, { immediateCatchup: status === "live" });
    });
    current.addEventListener("close", (event) => {
      handleSocketFailure(current, { authExpired: event.code === 4401 });
    });
  }

  async function connect() {
    if (!started || expired || unsupported || isPaused() || connecting || socket) return;
    if (typeof WebSocketConstructor !== "function") {
      unsupported = true;
      enterFallback({ reconnect: false, immediateCatchup: true });
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
      finishConnectionFailure(sequence, { immediateCatchup: true });
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
      finishConnectionFailure(sequence, { unsupportedRoute, immediateCatchup: true });
    }
  }

  function handleLifecyclePause() {
    if (!started || expired) return;
    lifecyclePaused = true;
    needsCatchup = true;
    stopTransport();
    refreshDirty = false;
    refreshQuiet = true;
    settleRefreshWaiters(new Error("Snapshot refresh was paused."));
    emitStatus(!isOnline() ? "offline" : "paused");
  }

  function resumeFromLifecycle() {
    if (!started || expired) return;
    if (isPaused()) {
      handleLifecyclePause();
      return;
    }
    if (lifecyclePaused) needsCatchup = true;
    lifecyclePaused = false;
    clearReconnectTimer();
    clearFallbackTimer();
    if (unsupported || typeof WebSocketConstructor !== "function") {
      enterFallback({ reconnect: false, immediateCatchup: true });
      return;
    }
    if (status === "live" && socket) {
      queueQuietRefresh();
      scheduleReconciliation();
      return;
    }
    connect();
  }

  function handleOnline() {
    needsCatchup = true;
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

  function start() {
    if (started || expired) return;
    started = true;
    attachLifecycleListeners();
    if (isPaused()) {
      handleLifecyclePause();
      return;
    }
    if (typeof WebSocketConstructor !== "function") {
      unsupported = true;
      enterFallback({ reconnect: false, immediateCatchup: true });
      return;
    }
    connect();
  }

  return { start, stop, refresh };
}
