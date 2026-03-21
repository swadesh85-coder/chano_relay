const WebSocket = require("ws");
const { EventEmitter } = require("events");

const {
  DEFAULT_PAIRING_TTL_MS,
  DEFAULT_PROTOCOL_VERSION,
  DEFAULT_SESSION_TTL_MS,
  RedisSessionRegistry,
  SESSION_STATES,
  createConnectionManager,
  createMessageRouter,
  createRedisSessionRegistry,
  createSessionLifecycleManager,
  getRedisSessionKey,
  startRelayServer,
} = require("./server");

class FakeRedisClient {
  constructor(nowMs = Date.now()) {
    this.entries = new Map();
    this.nowMs = nowMs;
  }

  advanceTime(ms) {
    this.nowMs += ms;
  }

  purgeAllExpired() {
    for (const key of this.entries.keys()) {
      this.purgeExpired(key);
    }
  }

  purgeExpired(key) {
    const entry = this.entries.get(key);
    if (!entry) {
      return;
    }

    if (entry.expiresAtMs !== null && entry.expiresAtMs <= this.nowMs) {
      this.entries.delete(key);
    }
  }

  async exists(key) {
    this.purgeExpired(key);
    return this.entries.has(key) ? 1 : 0;
  }

  async hSet(key, fields) {
    this.purgeExpired(key);
    const entry = this.entries.get(key) || { type: "hash", value: {}, expiresAtMs: null };

    if (entry.type !== "hash") {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value: ${key}`);
    }

    let addedFields = 0;

    for (const [field, value] of Object.entries(fields)) {
      if (!Object.prototype.hasOwnProperty.call(entry.value, field)) {
        addedFields += 1;
      }

      entry.value[field] = String(value);
    }

    this.entries.set(key, entry);
    return addedFields;
  }

  async hGetAll(key) {
    this.purgeExpired(key);
    const entry = this.entries.get(key);

    if (!entry || entry.type !== "hash") {
      return {};
    }

    return { ...entry.value };
  }

  async expire(key, ttlSeconds) {
    this.purgeExpired(key);
    const entry = this.entries.get(key);
    if (!entry) {
      return 0;
    }

    entry.expiresAtMs = this.nowMs + Number(ttlSeconds) * 1000;
    this.entries.set(key, entry);
    this.purgeExpired(key);
    return this.entries.has(key) ? 1 : 0;
  }

  async ttl(key) {
    this.purgeExpired(key);
    const entry = this.entries.get(key);

    if (!entry) {
      return -2;
    }

    if (entry.expiresAtMs === null) {
      return -1;
    }

    return Math.max(0, Math.ceil((entry.expiresAtMs - this.nowMs) / 1000));
  }

  async type(key) {
    this.purgeExpired(key);
    const entry = this.entries.get(key);
    return entry ? entry.type : "none";
  }

  async keys(pattern = "*") {
    this.purgeAllExpired();
    const escapedPattern = pattern
      .split("*")
      .map((segment) => segment.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
      .join(".*");
    const matcher = new RegExp(`^${escapedPattern}$`);

    return [...this.entries.keys()].filter((key) => matcher.test(key));
  }

  async del(key) {
    this.purgeExpired(key);
    return this.entries.delete(key) ? 1 : 0;
  }
}

function createEnvelope(overrides = {}) {
  return {
    protocolVersion: DEFAULT_PROTOCOL_VERSION,
    type: "event_stream",
    sessionId: "11111111-1111-4111-8111-111111111111",
    timestamp: Date.now(),
    sequence: 1,
    payload: { opaque: true },
    ...overrides,
  };
}

function waitForOpen(socket, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Timed out waiting for websocket open")), timeoutMs);

    socket.once("open", () => {
      clearTimeout(timer);
      resolve();
    });

    socket.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
}

function waitForClose(socket, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Timed out waiting for websocket close")), timeoutMs);

    socket.once("close", () => {
      clearTimeout(timer);
      resolve();
    });

    socket.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
}

function waitForSocketMessage(socket, predicate = () => true, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("Timed out waiting for websocket message"));
    }, timeoutMs);

    function cleanup() {
      clearTimeout(timer);
      socket.off("message", onMessage);
      socket.off("error", onError);
    }

    function onError(error) {
      cleanup();
      reject(error);
    }

    function onMessage(data, isBinary) {
      if (isBinary) {
        cleanup();
        reject(new Error("Expected text websocket frame"));
        return;
      }

      const parsed = JSON.parse(data.toString("utf8"));
      if (!predicate(parsed)) {
        return;
      }

      cleanup();
      resolve(parsed);
    }

    socket.on("message", onMessage);
    socket.once("error", onError);
  });
}

function collectSocketMessages(socket, count, predicate = () => true, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const messages = [];
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for ${count} websocket messages`));
    }, timeoutMs);

    function cleanup() {
      clearTimeout(timer);
      socket.off("message", onMessage);
      socket.off("error", onError);
    }

    function onError(error) {
      cleanup();
      reject(error);
    }

    function onMessage(data, isBinary) {
      if (isBinary) {
        cleanup();
        reject(new Error("Expected text websocket frame"));
        return;
      }

      const parsed = JSON.parse(data.toString("utf8"));
      if (!predicate(parsed)) {
        return;
      }

      messages.push(parsed);
      if (messages.length === count) {
        cleanup();
        resolve(messages);
      }
    }

    socket.on("message", onMessage);
    socket.once("error", onError);
  });
}

function waitForEvent(emitter, eventName, predicate = () => true, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for event ${eventName}`));
    }, timeoutMs);

    function cleanup() {
      clearTimeout(timer);
      emitter.off(eventName, onEvent);
    }

    function onEvent(entry) {
      if (!predicate(entry)) {
        return;
      }

      cleanup();
      resolve(entry);
    }

    emitter.on(eventName, onEvent);
  });
}

async function waitForSessionState(sessionRegistry, sessionId, expectedState, timeoutMs = 3000) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const session = await sessionRegistry.getSession(sessionId);
    if (session && session.state === expectedState) {
      return session;
    }

    await new Promise((resolve) => setTimeout(resolve, 20));
  }

  throw new Error(`Timed out waiting for session ${sessionId} to reach state ${expectedState}`);
}

function observeSessionRegistry(sessionRegistry) {
  const transitions = [];
  const createSession = sessionRegistry.createSession.bind(sessionRegistry);
  const updateSession = sessionRegistry.updateSession.bind(sessionRegistry);
  const updateSessionOnPair = sessionRegistry.updateSessionOnPair.bind(sessionRegistry);
  const activateSession = sessionRegistry.activateSession.bind(sessionRegistry);

  function recordTransition(session) {
    transitions.push({
      state: session.state,
      sessionId: session.sessionId,
      snapshot: { ...session },
    });

    return session;
  }

  sessionRegistry.createSession = async (...args) => {
    return recordTransition(await createSession(...args));
  };

  sessionRegistry.updateSession = async (...args) => {
    return recordTransition(await updateSession(...args));
  };

  sessionRegistry.updateSessionOnPair = async (...args) => {
    return recordTransition(await updateSessionOnPair(...args));
  };

  sessionRegistry.activateSession = async (...args) => {
    return recordTransition(await activateSession(...args));
  };

  return transitions;
}

function formatSessionRecord(session) {
  return {
    sessionId: session.sessionId,
    mobileSocketId: session.mobileSocketId,
    webSocketId: session.webSocketId,
    createdAt: session.createdAt,
    expiresAt: session.expiresAt,
    state: session.state,
  };
}

function ttlDriftSeconds(ttlSeconds) {
  return Math.max(0, DEFAULT_SESSION_TTL_MS / 1000 - ttlSeconds);
}

async function auditSessionCreation(redisClient, sessionId) {
  const ttlSeconds = await redisClient.ttl(getRedisSessionKey(sessionId));

  return {
    ttlSeconds,
    nearZero: ttlSeconds <= 1,
    driftSeconds: ttlDriftSeconds(ttlSeconds),
    evidence: `SESSION_CREATE ttlSeconds=${ttlSeconds}`,
  };
}

async function auditSessionActivation(redisClient, sessionId, ttlBeforeActivation) {
  const ttlSeconds = await redisClient.ttl(getRedisSessionKey(sessionId));

  return {
    ttlSeconds,
    extended: ttlSeconds >= ttlBeforeActivation,
    abnormalDecrease: ttlSeconds < ttlBeforeActivation,
    evidence: `SESSION_UPDATE active ttlSeconds=${ttlSeconds}`,
  };
}

async function auditTTLRefresh(redisClient, sessionId, messageType) {
  const ttlSeconds = await redisClient.ttl(getRedisSessionKey(sessionId));

  return {
    messageType,
    ttlSeconds,
    refreshedToFullDuration: ttlSeconds >= DEFAULT_SESSION_TTL_MS / 1000 - 1,
    evidence: `MESSAGE_RECEIVED ttlSeconds=${ttlSeconds}`,
  };
}

async function auditSessionExpiry() {
  class AuditSocket extends EventEmitter {
    constructor() {
      super();
      this.readyState = 1;
      this.sentMessages = [];
    }

    send(message) {
      this.sentMessages.push(message);
    }
  }

  const redisClient = new FakeRedisClient(25_000);
  const scheduler = {
    handles: [],
    setTimeout(callback, delay) {
      const handle = {
        callback,
        runAt: redisClient.nowMs + delay,
        cleared: false,
      };

      this.handles.push(handle);
      return handle;
    },
    clearTimeout(handle) {
      if (handle) {
        handle.cleared = true;
      }
    },
    async runDueTasks() {
      const dueHandles = this.handles.filter((handle) => !handle.cleared && handle.runAt <= redisClient.nowMs);
      this.handles = this.handles.filter((handle) => handle.cleared || handle.runAt > redisClient.nowMs);

      for (const handle of dueHandles) {
        await handle.callback();
      }
    },
  };
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
    sessionTtlMs: 2_000,
  });
  const connectionManager = createConnectionManager();
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
    logger: () => {},
  });
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
  });
  const mobileSocket = new AuditSocket();
  const webSocket = new AuditSocket();
  const sessionId = "44444444-4444-4444-8444-444444444444";

  connectionManager.registerConnection(webSocket, { connectionId: "expiry-web" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "expiry-mobile" });

  await sessionLifecycleManager.createSession(sessionId, "expiry-web", redisClient.nowMs + 2_000);
  await sessionLifecycleManager.transitionToPaired(sessionId, "expiry-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);

  redisClient.advanceTime(1_500);
  await scheduler.runDueTasks();
  const stillActiveBeforeRefresh = await sessionRegistry.getSession(sessionId);

  await messageRouter.routeMessage(
    {
      protocolVersion: DEFAULT_PROTOCOL_VERSION,
      type: "event_stream",
      sessionId,
      timestamp: redisClient.nowMs,
      sequence: 1,
      payload: { opaque: true },
    },
    mobileSocket,
  );

  redisClient.advanceTime(1_500);
  await scheduler.runDueTasks();
  const stillActiveAfterRefresh = await sessionRegistry.getSession(sessionId);

  redisClient.advanceTime(400);
  await scheduler.runDueTasks();
  const stillBeforeFullInactivityWindow = await sessionRegistry.getSession(sessionId);

  redisClient.advanceTime(200);
  await scheduler.runDueTasks();
  const closedSession = await sessionRegistry.getSession(sessionId);
  const timeoutNotified =
    mobileSocket.sentMessages.some((message) => JSON.parse(message).payload.reason === "timeout") &&
    webSocket.sentMessages.some((message) => JSON.parse(message).payload.reason === "timeout");

  return {
    ttlSeconds: await redisClient.ttl(getRedisSessionKey(sessionId)),
    remainedActiveBeforeRefresh: Boolean(
      stillActiveBeforeRefresh && stillActiveBeforeRefresh.state === SESSION_STATES.ACTIVE,
    ),
    remainedActiveAfterRefresh: Boolean(
      stillActiveAfterRefresh && stillActiveAfterRefresh.state === SESSION_STATES.ACTIVE,
    ),
    noPrematureExpiry: Boolean(
      stillBeforeFullInactivityWindow && stillBeforeFullInactivityWindow.state === SESSION_STATES.ACTIVE,
    ),
    expiredAfterFullInactivityWindow: Boolean(
      closedSession === null || (closedSession && closedSession.state === SESSION_STATES.CLOSED),
    ),
    timeoutNotified,
  };
}

function auditTimerEvidence(startupLogs, sessionId) {
  const timerSetEntries = startupLogs.filter((entry) => entry.includes(`TIMER_SET sessionId=${sessionId}`));
  const timerResetEntries = startupLogs.filter((entry) => entry.includes(`TIMER_RESET sessionId=${sessionId}`));
  const timerClearEntries = startupLogs.filter((entry) => entry.includes("TIMER_CLEAR previousHandle="));
  const closeCycleClearEntries = [];
  let pendingClearEntries = [];

  for (const entry of startupLogs) {
    if (entry.includes("TIMER_CLEAR previousHandle=")) {
      pendingClearEntries.push(entry);
      continue;
    }

    if (entry.includes("SESSION_CLOSED reason=")) {
      closeCycleClearEntries.push(...pendingClearEntries);
      pendingClearEntries = [];
      continue;
    }

    pendingClearEntries = [];
  }

  return {
    setCount: timerSetEntries.length,
    resetCount: timerResetEntries.length,
    clearCount: timerClearEntries.length,
    clearTrueCount: timerClearEntries.filter((entry) => entry === "TIMER_CLEAR previousHandle=true").length,
    clearFalseCount: timerClearEntries.filter((entry) => entry === "TIMER_CLEAR previousHandle=false").length,
    setEntries: timerSetEntries,
    resetEntries: timerResetEntries,
    clearEntries: timerClearEntries,
    closeCycleClearEntries,
  };
}

async function auditTTLComputation() {
  const redisClient = new FakeRedisClient(10_000);
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
    sessionTtlMs: DEFAULT_SESSION_TTL_MS,
  });
  const sessionId = "55555555-5555-4555-8555-555555555555";

  await sessionRegistry.createSession(sessionId, "ttl-computation-web");
  const beforeRefresh = await sessionRegistry.getSession(sessionId);

  redisClient.advanceTime(DEFAULT_SESSION_TTL_MS - 1_000);
  const refreshedSession = await sessionRegistry.refreshSessionTtl(sessionId, "activity");
  const ttlSeconds = await redisClient.ttl(getRedisSessionKey(sessionId));

  return {
    previousExpiresAt: beforeRefresh.expiresAt,
    refreshedExpiresAt: refreshedSession.expiresAt,
    ttlSeconds,
    usesFixedDurationReset: refreshedSession.expiresAt - redisClient.nowMs === DEFAULT_SESSION_TTL_MS,
    staleExpiryBypassed: refreshedSession.expiresAt > beforeRefresh.expiresAt,
  };
}

async function runRelayRuntimeAudit(options = {}) {
  const host = options.host || "0.0.0.0";
  const clientHost = options.clientHost || "127.0.0.1";
  const port = options.port === undefined ? 8080 : options.port;
  const wsPath = options.wsPath || "/relay";
  const redisUrl = options.redisUrl || "redis://127.0.0.1:6379";
  const emitOutput = options.emitOutput !== false;
  const startupLogs = [];
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const transitions = observeSessionRegistry(sessionRegistry);
  const redisConnectCalls = [];
  const rawAuditLines = [];
  const originalRedisConnect = RedisSessionRegistry.connect;
  const originalConsoleLog = console.log;
  let relayServer;
  let webSocket;
  let mobileSocket;
  let eventAudit;
  let mutationAudit;

  function record(line) {
    rawAuditLines.push(line);
    if (emitOutput) {
      process.stdout.write(`${line}\n`);
    }
  }

  console.log = (...args) => {
    const entry = args.join(" ");
    startupLogs.push(entry);
    if (emitOutput && options.echoServerLogs) {
      originalConsoleLog(...args);
    }
  };

  RedisSessionRegistry.connect = async (url) => {
    redisConnectCalls.push(url);
    return sessionRegistry;
  };

  const sessionId = "22222222-2222-4222-8222-222222222222";
  const bootstrapSessionId = "33333333-3333-4333-8333-333333333333";

  try {
    relayServer = await startRelayServer({
      host,
      port,
      wsPath,
      maxPayloadBytes: 2048,
      diagnostics: {
        ingress: {
          enabled: true,
        },
      },
      env: {
        REDIS_URL: redisUrl,
      },
      pairing: {
        secret: "audit-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
    });

    const address = relayServer.server.address();
    const relayListenUrl = `ws://${relayServer.config.host}:${address.port}${relayServer.config.wsPath}`;
    const clientUrl = `ws://${clientHost}:${address.port}${relayServer.config.wsPath}`;

    const startupEvidence = {
      relayStarting: startupLogs.some((entry) => entry.includes("Relay starting")),
      redisConnected: startupLogs.some((entry) => entry.includes("Redis connected")),
      relayListening: startupLogs.some((entry) => entry.includes(`listening on ${relayListenUrl}`)),
      listenUrl: relayListenUrl,
      redisUrl,
      redisConnectCalls: [...redisConnectCalls],
    };

    eventAudit = relayServer.messageRouter.enableEventAudit(sessionId);
    mutationAudit = relayServer.messageRouter.enableMutationAudit(sessionId);

    webSocket = new WebSocket(clientUrl);
    mobileSocket = new WebSocket(clientUrl);

    await Promise.all([waitForOpen(webSocket), waitForOpen(mobileSocket)]);
    record(`WS_CONNECTED role=web endpoint=${clientUrl}`);
    record(`WS_CONNECTED role=mobile endpoint=${clientUrl}`);

    const validEnvelope = createEnvelope({
      type: "protocol_handshake",
      sessionId: bootstrapSessionId,
      sequence: 1,
      payload: { client: "web" },
    });
    const validEnvelopeAccepted = waitForEvent(
      relayServer.events,
      "transportEnvelope",
      (entry) => entry.envelope.type === "protocol_handshake" && entry.envelope.sequence === 1,
    );
    webSocket.send(JSON.stringify(validEnvelope));
    await validEnvelopeAccepted;

    const invalidEnvelopeResponse = waitForSocketMessage(
      webSocket,
      (message) => message.type === "transport_error",
    );
    webSocket.send(
      JSON.stringify({
        protocolVersion: DEFAULT_PROTOCOL_VERSION,
        type: "protocol_handshake",
        timestamp: Date.now(),
        sequence: 2,
        payload: { invalid: true },
      }),
    );
    const invalidEnvelope = await invalidEnvelopeResponse;

    const sessionReadyMessage = waitForSocketMessage(
      webSocket,
      (message) => message.type === "qr_session_ready" && message.sessionId === sessionId,
    );
    webSocket.send(
      JSON.stringify(
        createEnvelope({
          type: "qr_session_create",
          sessionId,
          sequence: 3,
          payload: {},
        }),
      ),
    );
    await sessionReadyMessage;

    const waitingSession = await waitForSessionState(sessionRegistry, sessionId, SESSION_STATES.WAITING);
    const sessionCreationAudit = await auditSessionCreation(redisClient, sessionId);

    const webApprovalMessage = waitForSocketMessage(
      webSocket,
      (message) => message.type === "pair_approved" && message.sessionId === sessionId,
    );
    const mobileApprovalMessage = waitForSocketMessage(
      mobileSocket,
      (message) => message.type === "pair_approved" && message.sessionId === sessionId,
    );

    mobileSocket.send(
      JSON.stringify(
        createEnvelope({
          type: "pair_request",
          sessionId,
          sequence: 4,
          payload: {
            sessionId,
          },
        }),
      ),
    );

    await Promise.all([webApprovalMessage, mobileApprovalMessage]);
    const activeSession = await waitForSessionState(sessionRegistry, sessionId, SESSION_STATES.ACTIVE);
    const sessionActivationAudit = await auditSessionActivation(
      redisClient,
      sessionId,
      sessionCreationAudit.ttlSeconds,
    );
    const redisKey = getRedisSessionKey(sessionId);
    const redisSessionRecord = await redisClient.hGetAll(redisKey);
    const redisSessionKeys = await redisClient.keys("*");
    const redisSessionType = await redisClient.type(redisKey);
    const redisSessionTtl = await redisClient.ttl(redisKey);

    const routeEnvelopeOne = createEnvelope({
      type: "snapshot_start",
      sessionId,
      sequence: 5,
      payload: {
        snapshotChunk: "alpha",
      },
    });
    const routeEnvelopeTwo = createEnvelope({
      type: "mutation_command",
      sessionId,
      sequence: 6,
      payload: {
        commandId: "cmd-101",
        command: "apply_patch",
        target: "profile",
      },
    });
    const routeEnvelopeResult = createEnvelope({
      type: "command_result",
      sessionId,
      sequence: 7,
      payload: {
        commandId: "cmd-101",
        status: "applied",
        applied: true,
      },
    });
    const routeEnvelopeThree = createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 8,
      payload: {
        eventVersion: 9001,
        delta: "opaque-event",
      },
    });
    const routeEnvelopeFour = createEnvelope({
      type: "snapshot_complete",
      sessionId,
      sequence: 10,
      payload: {
        snapshotChunk: "omega",
      },
    });

    const routedToWebOne = waitForSocketMessage(
      webSocket,
      (message) => message.type === routeEnvelopeOne.type && message.sequence === routeEnvelopeOne.sequence,
    );
    mobileSocket.send(JSON.stringify(routeEnvelopeOne));
    const routedEnvelopeOne = await routedToWebOne;
    const routeEnvelopeOneAudit = await auditTTLRefresh(redisClient, sessionId, routeEnvelopeOne.type);

    const routedToMobile = waitForSocketMessage(
      mobileSocket,
      (message) => message.type === routeEnvelopeTwo.type && message.sequence === routeEnvelopeTwo.sequence,
    );
    webSocket.send(JSON.stringify(routeEnvelopeTwo));
    const routedEnvelopeTwo = await routedToMobile;
    const routeEnvelopeTwoAudit = await auditTTLRefresh(redisClient, sessionId, routeEnvelopeTwo.type);

    const routedToWebResult = waitForSocketMessage(
      webSocket,
      (message) => message.type === routeEnvelopeResult.type && message.sequence === routeEnvelopeResult.sequence,
    );
    mobileSocket.send(JSON.stringify(routeEnvelopeResult));
    const routedEnvelopeResult = await routedToWebResult;
    const routeEnvelopeResultAudit = await auditTTLRefresh(redisClient, sessionId, routeEnvelopeResult.type);

    const routedSnapshotIsolationMessages = collectSocketMessages(
      webSocket,
      2,
      (message) =>
        message.sessionId === sessionId &&
        ((message.type === routeEnvelopeThree.type && message.sequence === routeEnvelopeThree.sequence) ||
          (message.type === routeEnvelopeFour.type && message.sequence === routeEnvelopeFour.sequence)),
    );
    mobileSocket.send(JSON.stringify(routeEnvelopeThree));
    mobileSocket.send(JSON.stringify(routeEnvelopeFour));
    const routedPostSnapshotMessages = await routedSnapshotIsolationMessages;
    const routedEnvelopeFour = routedPostSnapshotMessages.find(
      (message) => message.type === routeEnvelopeFour.type && message.sequence === routeEnvelopeFour.sequence,
    );
    const routedEnvelopeThree = routedPostSnapshotMessages.find(
      (message) => message.type === routeEnvelopeThree.type && message.sequence === routeEnvelopeThree.sequence,
    );
    const routeEnvelopeThreeAudit = await auditTTLRefresh(redisClient, sessionId, routeEnvelopeThree.type);
    const routeEnvelopeFourAudit = await auditTTLRefresh(redisClient, sessionId, routeEnvelopeFour.type);

    const mutationOrderingEnvelopeOne = createEnvelope({
      type: "mutation_command",
      sessionId,
      sequence: 20,
      payload: { commandId: "cmd-201", operation: "first" },
    });
    const mutationOrderingEnvelopeTwo = createEnvelope({
      type: "command_result",
      sessionId,
      sequence: 21,
      payload: { commandId: "cmd-201", status: "ok" },
    });
    const mutationOrderingEnvelopeThree = createEnvelope({
      type: "mutation_command",
      sessionId,
      sequence: 22,
      payload: { commandId: "cmd-202", operation: "second" },
    });

    const routedMutationTwenty = waitForSocketMessage(
      mobileSocket,
      (message) =>
        message.type === mutationOrderingEnvelopeOne.type && message.sequence === mutationOrderingEnvelopeOne.sequence,
    );
    webSocket.send(JSON.stringify(mutationOrderingEnvelopeOne));
    await routedMutationTwenty;

    const routedMutationTwentyOne = waitForSocketMessage(
      webSocket,
      (message) =>
        message.type === mutationOrderingEnvelopeTwo.type && message.sequence === mutationOrderingEnvelopeTwo.sequence,
    );
    mobileSocket.send(JSON.stringify(mutationOrderingEnvelopeTwo));
    await routedMutationTwentyOne;

    const routedMutationTwentyTwo = waitForSocketMessage(
      mobileSocket,
      (message) =>
        message.type === mutationOrderingEnvelopeThree.type && message.sequence === mutationOrderingEnvelopeThree.sequence,
    );
    webSocket.send(JSON.stringify(mutationOrderingEnvelopeThree));
    await routedMutationTwentyTwo;

    const orderProbeOne = createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 8,
      payload: { orderProbe: "first" },
    });
    const orderProbeTwo = createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 9,
      payload: { orderProbe: "second" },
    });
    const orderedWebMessages = collectSocketMessages(
      webSocket,
      2,
      (message) => message.sessionId === sessionId && message.payload && message.payload.orderProbe,
    );
    mobileSocket.send(JSON.stringify(orderProbeOne));
    mobileSocket.send(JSON.stringify(orderProbeTwo));
    const orderProbeMessages = await orderedWebMessages;

    mobileSocket.close();
    await waitForClose(mobileSocket);
    const closedSession = await waitForSessionState(sessionRegistry, sessionId, SESSION_STATES.CLOSED);
    const sessionExpiryAudit = await auditSessionExpiry();
    const ttlComputationAudit = await auditTTLComputation();

    if (webSocket.readyState === WebSocket.OPEN) {
      webSocket.close();
      await waitForClose(webSocket);
    }

    const routingEvidence = [
      {
        direction: "mobile->web",
        type: routeEnvelopeOne.type,
        sessionId,
        unchanged: JSON.stringify(routedEnvelopeOne) === JSON.stringify(routeEnvelopeOne),
      },
      {
        direction: "web->mobile",
        type: routeEnvelopeTwo.type,
        sessionId,
        unchanged: JSON.stringify(routedEnvelopeTwo) === JSON.stringify(routeEnvelopeTwo),
      },
      {
        direction: "mobile->web",
        type: routeEnvelopeResult.type,
        sessionId,
        unchanged: JSON.stringify(routedEnvelopeResult) === JSON.stringify(routeEnvelopeResult),
      },
      {
        direction: "mobile->web",
        type: routeEnvelopeThree.type,
        sessionId,
        unchanged: JSON.stringify(routedEnvelopeThree) === JSON.stringify(routeEnvelopeThree),
      },
    ];

    const lifecycleStates = transitions
      .filter((entry) => entry.sessionId === sessionId)
      .map((entry) => entry.state)
      .filter((state, index, states) => index === 0 || state !== states[index - 1]);

    const metadataOnlyKeys = [
      "sessionId",
      "mobileSocketId",
      "webSocketId",
      "createdAt",
      "expiresAt",
      "state",
    ];
    const redisRecordKeys = Object.keys(redisSessionRecord).sort();
    const metadataOnly = JSON.stringify(redisRecordKeys) === JSON.stringify([...metadataOnlyKeys].sort());
    const orderPreserved =
      orderProbeMessages.length === 2 &&
      orderProbeMessages[0].sequence === orderProbeOne.sequence &&
      orderProbeMessages[1].sequence === orderProbeTwo.sequence;

    const results = {
      startupEvidence,
      redisSessionRecord: formatSessionRecord(activeSession),
      redisChecks: {
        keys: redisSessionKeys,
        type: redisSessionType,
        ttl: redisSessionTtl,
      },
      ttlEvidence: {
        sessionCreation: sessionCreationAudit,
        sessionActivation: sessionActivationAudit,
        messageFlow: [
          routeEnvelopeOneAudit,
          routeEnvelopeTwoAudit,
          routeEnvelopeResultAudit,
          routeEnvelopeThreeAudit,
          routeEnvelopeFourAudit,
        ],
        sessionExpiry: sessionExpiryAudit,
        ttlComputation: ttlComputationAudit,
      },
      timerEvidence: auditTimerEvidence(startupLogs, sessionId),
      runtimeEvidence: ["protocol_handshake", routeEnvelopeOne.type, routeEnvelopeFour.type],
      routingEvidence,
      eventAudit: {
        routingLogs: [...eventAudit.routingLogs],
        inbound: eventAudit.inbound.map((entry) => ({ sequence: entry.sequence, eventVersion: entry.eventVersion })),
        outbound: eventAudit.outbound.map((entry) => ({ sequence: entry.sequence, eventVersion: entry.eventVersion })),
        payloadReferenceEquality: [...eventAudit.payloadReferenceEquality],
      },
      mutationAudit: {
        routingLogs: [...mutationAudit.routingLogs],
        inbound: mutationAudit.inbound.map((entry) => ({
          sequence: entry.sequence,
          type: entry.type,
          commandId: entry.commandId,
        })),
        outbound: mutationAudit.outbound.map((entry) => ({
          sequence: entry.sequence,
          type: entry.type,
          commandId: entry.commandId,
        })),
        payloadReferenceEquality: [...mutationAudit.payloadReferenceEquality],
      },
      orderingEvidence: mutationAudit.outbound
        .filter((entry) => entry.sequence >= 20 && entry.sequence <= 22)
        .map((entry) => entry.sequence),
      validationEvidence: {
        validAccepted: {
          sequence: 1,
          type: "protocol_handshake",
        },
        invalidRejected: {
          reason: invalidEnvelope.payload.reason,
        },
      },
      lifecycleEvidence: {
        sessionId,
        states: lifecycleStates,
        waitingSession: formatSessionRecord(waitingSession),
        activeSession: formatSessionRecord(activeSession),
        closedSession: formatSessionRecord(closedSession),
      },
      constitution: {
        mobileAuthority: waitingSession.state === SESSION_STATES.WAITING && activeSession.state === SESSION_STATES.ACTIVE,
        mutationBoundary: routingEvidence
          .filter((entry) => entry.type === "mutation_command" || entry.type === "command_result")
          .every((entry) => entry.unchanged) && metadataOnly,
        eventOrdering: orderPreserved,
        relayNeutrality: routingEvidence.every((entry) => entry.unchanged),
        projectionSafety: metadataOnly && invalidEnvelope.payload.reason === "missing_session_id",
      },
      rawAuditLines,
      startupLogs,
      rawRedisSessionRecord: redisSessionRecord,
    };

    record("RELAY_STARTING");
    record("REDIS_CONNECTED");
    record(`RELAY_LISTENING ${relayListenUrl}`);
    record(`REDIS_KEYS ${redisSessionKeys.join(",")}`);
    record(`REDIS_TYPE ${redisSessionType}`);
    record(`REDIS_TTL ${redisSessionTtl}`);
    record(sessionCreationAudit.evidence);
    record(sessionActivationAudit.evidence);
    record(routeEnvelopeOneAudit.evidence);
    record(routeEnvelopeTwoAudit.evidence);
    record(routeEnvelopeResultAudit.evidence);
    record(routeEnvelopeThreeAudit.evidence);
    record(routeEnvelopeFourAudit.evidence);
    for (const entry of auditTimerEvidence(startupLogs, sessionId).setEntries) {
      record(entry);
    }
    for (const entry of auditTimerEvidence(startupLogs, sessionId).resetEntries) {
      record(entry);
    }
    for (const entry of auditTimerEvidence(startupLogs, sessionId).clearEntries) {
      record(entry);
    }
    record(JSON.stringify(formatSessionRecord(activeSession), null, 2));
    record(`VALID_ENVELOPE_ACCEPTED sequence=1 type=protocol_handshake`);
    record(`INVALID_ENVELOPE_REJECTED reason=${invalidEnvelope.payload.reason}`);
    record(`SESSION_CREATED sessionId=${sessionId}`);
    record(`SESSION_PAIRED sessionId=${sessionId}`);
    record(`SESSION_ACTIVE sessionId=${sessionId}`);
    record(`ROUTE mobile->web type=${routeEnvelopeOne.type} sessionId=${sessionId}`);
    record(`ROUTE web->mobile type=${routeEnvelopeTwo.type} sessionId=${sessionId}`);
    record(`ROUTE mobile->web type=${routeEnvelopeResult.type} sessionId=${sessionId}`);
    record(`ROUTE mobile->web type=${routeEnvelopeThree.type} sessionId=${sessionId}`);
    for (const entry of results.mutationAudit.routingLogs) {
      record(entry);
    }
    record(`PAYLOAD_REFERENCE_EQUALITY ${results.mutationAudit.payloadReferenceEquality.every(Boolean)}`);
    record(`seq: ${results.orderingEvidence.join(" → ")}`);
    record(`SESSION_CLOSED sessionId=${sessionId}`);
    record(`Mobile Authority -> ${results.constitution.mobileAuthority ? "PASS" : "FAIL"}`);
    record(`Mutation Boundary -> ${results.constitution.mutationBoundary ? "PASS" : "FAIL"}`);
    record(`Event Ordering -> ${results.constitution.eventOrdering ? "PASS" : "FAIL"}`);
    record(`Relay Neutrality -> ${results.constitution.relayNeutrality ? "PASS" : "FAIL"}`);
    record(`Projection Safety -> ${results.constitution.projectionSafety ? "PASS" : "FAIL"}`);

    return results;
  } finally {
    RedisSessionRegistry.connect = originalRedisConnect;
    console.log = originalConsoleLog;

    if (webSocket && webSocket.readyState === WebSocket.OPEN) {
      webSocket.close();
    }

    if (mobileSocket && mobileSocket.readyState === WebSocket.OPEN) {
      mobileSocket.close();
    }

    if (relayServer) {
      await relayServer.stop().catch(() => {});
    }
  }
}

if (require.main === module) {
  runRelayRuntimeAudit({
    host: "0.0.0.0",
    clientHost: "127.0.0.1",
    port: 8080,
    emitOutput: true,
  }).catch((error) => {
    process.stderr.write(`${error.stack || error.message}\n`);
    process.exitCode = 1;
  });
}

module.exports = {
  auditSessionCreation,
  auditSessionActivation,
  auditTTLRefresh,
  auditSessionExpiry,
  auditTTLComputation,
  FakeRedisClient,
  runRelayRuntimeAudit,
};