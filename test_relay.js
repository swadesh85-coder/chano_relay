const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const test = require("node:test");
const { EventEmitter } = require("events");
const WebSocket = require("ws");
const selfsigned = require("selfsigned");

const {
  BOOTSTRAP_MESSAGE_TYPES,
  createQRPairingSystem,
  createConnectionManager,
  createRelayIngressDiagnostics,
  createMessageRouter,
  createRelayRateLimiter,
  createRedisSessionRegistry,
  createRelayServer,
  createSessionLifecycleManager,
  DEFAULT_MAX_COMMANDS_PER_SECOND,
  DEFAULT_PAIRING_TTL_MS,
  DEFAULT_PROTOCOL_VERSION,
  DEFAULT_MAX_PAYLOAD_BYTES,
  DEFAULT_MAX_PAYLOAD_SIZE,
  DEFAULT_MAX_SESSIONS_PER_IP,
  RedisSessionRegistry,
  SESSION_STATES,
  DEFAULT_SESSION_TTL_MS,
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

class MockSocket extends EventEmitter {
  constructor() {
    super();
    this.sentMessages = [];
    this.readyState = 1;
  }

  send(message) {
    this.sentMessages.push(message);
  }

  close() {
    this.readyState = 3;
    this.emit("close");
  }
}

class ManualScheduler {
  constructor(now) {
    this.now = now;
    this.handles = [];
  }

  setTimeout(callback, delay) {
    const handle = {
      callback,
      delay,
      runAt: this.now() + delay,
      cleared: false,
    };

    this.handles.push(handle);
    return handle;
  }

  clearTimeout(handle) {
    if (handle) {
      handle.cleared = true;
    }
  }

  async runDueTasks() {
    const dueHandles = this.handles.filter((handle) => !handle.cleared && handle.runAt <= this.now());
    this.handles = this.handles.filter((handle) => handle.cleared || handle.runAt > this.now());

    for (const handle of dueHandles) {
      await handle.callback();
    }
  }

  getActiveHandleCount() {
    return this.handles.filter((handle) => !handle.cleared).length;
  }
}

function createEnvelope(overrides = {}) {
  return {
    protocolVersion: DEFAULT_PROTOCOL_VERSION,
    type: "event_stream",
    sessionId: "session-1",
    timestamp: Date.now(),
    sequence: 1,
    payload: { opaque: true },
    ...overrides,
  };
}

function waitForOpen(socket) {
  return new Promise((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
}

function waitForConnectionCount(relayServer, expectedCount, timeoutMs = 3000) {
  const start = Date.now();

  return new Promise((resolve, reject) => {
    const poll = () => {
      if (relayServer.getConnectionCount() === expectedCount) {
        resolve();
        return;
      }

      if (Date.now() - start >= timeoutMs) {
        reject(new Error(`Timed out waiting for ${expectedCount} active connections`));
        return;
      }

      setTimeout(poll, 10);
    };

    poll();
  });
}

function waitForMessage(socket, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Timed out waiting for websocket message")), timeoutMs);

    socket.once("message", (data, isBinary) => {
      clearTimeout(timer);
      if (isBinary) {
        reject(new Error("Expected text websocket frame"));
        return;
      }

      resolve(JSON.parse(data.toString("utf8")));
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

    socket.once("close", (...args) => {
      clearTimeout(timer);
      resolve(args);
    });

    socket.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
}

function collectMessages(socket, count, timeoutMs = 3000) {
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

      messages.push(JSON.parse(data.toString("utf8")));
      if (messages.length === count) {
        cleanup();
        resolve(messages);
      }
    }

    socket.on("message", onMessage);
    socket.once("error", onError);
  });
}

async function establishActiveRelaySessionOverWebSocket(
  relayServer,
  { sessionId, token, createSequence = 1, pairSequence = 2 },
) {
  const address = relayServer.server.address();
  const webSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
  const mobileSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

  await Promise.all([waitForOpen(webSocket), waitForOpen(mobileSocket)]);

  webSocket.send(
    JSON.stringify({
      protocolVersion: DEFAULT_PROTOCOL_VERSION,
      type: "qr_session_create",
      sessionId,
      timestamp: Date.now(),
      sequence: createSequence,
      payload: { token },
    }),
  );

  const readyMessage = await waitForMessage(webSocket);
  const webApprovalPromise = waitForMessage(webSocket);
  const mobileApprovalPromise = waitForMessage(mobileSocket);

  mobileSocket.send(
    JSON.stringify({
      protocolVersion: readyMessage.protocolVersion,
      type: "pair_request",
      sessionId: readyMessage.payload.sessionId,
      timestamp: Date.now(),
      sequence: pairSequence,
      payload: {
        sessionId: readyMessage.payload.sessionId,
        token,
      },
    }),
  );

  const [webApprovalMessage, mobileApprovalMessage] = await Promise.all([
    webApprovalPromise,
    mobileApprovalPromise,
  ]);

  return {
    webSocket,
    mobileSocket,
    readyMessage,
    webApprovalMessage,
    mobileApprovalMessage,
  };
}

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject,
  };
}

async function sendOrderedMessages({ messageRouter, senderSocket, sessionId, sequences, eventVersions }) {
  const routePromises = [];

  for (let index = 0; index < sequences.length; index += 1) {
    routePromises.push(
      messageRouter.routeMessage(
        createEnvelope({
          type: "event_stream",
          sessionId,
          sequence: sequences[index],
          timestamp: Date.now() + index,
          payload: {
            eventVersion: eventVersions[index],
            opaque: `event-${eventVersions[index]}`,
          },
        }),
        senderSocket,
      ),
    );
  }

  return Promise.all(routePromises);
}

function captureForwardedMessages(socket) {
  return socket.sentMessages.map((message) => JSON.parse(message));
}

async function sendMixedMessages({ messageRouter, senderSocket, sessionId }) {
  const mixedEnvelopes = [
    createEnvelope({
      type: "snapshot_start",
      sessionId,
      sequence: 1,
      timestamp: 1_700_000_010_001,
      payload: { opaque: "snapshot-start" },
    }),
    createEnvelope({
      type: "snapshot_chunk",
      sessionId,
      sequence: 2,
      timestamp: 1_700_000_010_002,
      payload: { opaque: "chunk-1" },
    }),
    createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 101,
      timestamp: 1_700_000_010_003,
      payload: { eventVersion: 101, opaque: "event-101" },
    }),
    createEnvelope({
      type: "snapshot_chunk",
      sessionId,
      sequence: 3,
      timestamp: 1_700_000_010_004,
      payload: { opaque: "chunk-2" },
    }),
    createEnvelope({
      type: "snapshot_complete",
      sessionId,
      sequence: 4,
      timestamp: 1_700_000_010_005,
      payload: { opaque: "snapshot-complete" },
    }),
  ];

  await Promise.all(mixedEnvelopes.map((envelope) => messageRouter.routeMessage(envelope, senderSocket)));

  return mixedEnvelopes;
}

async function sendDuplicateMessages({ messageRouter, senderSocket, sessionId }) {
  const duplicateEnvelopes = [
    createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 101,
      timestamp: 1_700_000_000_101,
      payload: {
        opaque: { marker: "event-101" },
      },
    }),
    createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 102,
      timestamp: 1_700_000_000_102,
      payload: {
        opaque: { marker: "event-102" },
      },
    }),
    createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 102,
      timestamp: 1_700_000_000_102,
      payload: {
        opaque: { marker: "event-102" },
      },
    }),
    createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 103,
      timestamp: 1_700_000_000_103,
      payload: {
        opaque: { marker: "event-103" },
      },
    }),
  ];

  await Promise.all(
    duplicateEnvelopes.map((envelope) => messageRouter.routeMessage(envelope, senderSocket)),
  );

  return duplicateEnvelopes;
}

async function sendOutOfOrderEvents({ messageRouter, senderSocket, sessionId }) {
  const outOfOrderEnvelopes = [
    createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 101,
      timestamp: 1_700_000_001_101,
      payload: {
        opaque: { marker: "event-101" },
      },
    }),
    createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 103,
      timestamp: 1_700_000_001_103,
      payload: {
        opaque: { marker: "event-103" },
      },
    }),
  ];

  await Promise.all(
    outOfOrderEnvelopes.map((envelope) => messageRouter.routeMessage(envelope, senderSocket)),
  );

  return outOfOrderEnvelopes;
}

async function sendSnapshotFallbackFlow({ messageRouter, senderSocket, sessionId }) {
  const snapshotEnvelopes = [
    createEnvelope({
      type: "snapshot_start",
      sessionId,
      sequence: 201,
      payload: {
        opaque: { marker: "snapshot-start" },
      },
    }),
    createEnvelope({
      type: "snapshot_chunk",
      sessionId,
      sequence: 202,
      payload: {
        opaque: { marker: "snapshot-chunk" },
      },
    }),
    createEnvelope({
      type: "snapshot_complete",
      sessionId,
      sequence: 203,
      payload: {
        opaque: { marker: "snapshot-complete" },
      },
    }),
  ];

  await Promise.all(
    snapshotEnvelopes.map((envelope) => messageRouter.routeMessage(envelope, senderSocket)),
  );

  return snapshotEnvelopes;
}

function validateRelayDuplicateBehavior(messages) {
  assert.equal(messages.length, 4);
  assert.deepEqual(
    messages.map((message) => `${message.type} ${message.sequence}`),
    ["event_stream 101", "event_stream 102", "event_stream 102", "event_stream 103"],
  );
  assert.deepEqual(
    messages.map((message) => message.payload),
    [
      { opaque: { marker: "event-101" } },
      { opaque: { marker: "event-102" } },
      { opaque: { marker: "event-102" } },
      { opaque: { marker: "event-103" } },
    ],
  );
}

function validateRelayBehavior(messages) {
  assert.equal(messages.length, 2);
  assert.deepEqual(
    messages.map((message) => message.sequence),
    [101, 103],
  );
  assert.deepEqual(
    messages.map((message) => message.payload),
    [
      { opaque: { marker: "event-101" } },
      { opaque: { marker: "event-103" } },
    ],
  );
}

function validateRelayIsolation(messages) {
  assert.equal(messages.length, 5);
  assert.deepEqual(
    messages.map((message) => `${message.type} ${message.sequence}`),
    [
      "snapshot_start 1",
      "snapshot_chunk 2",
      "snapshot_chunk 3",
      "snapshot_complete 4",
      "event_stream 101",
    ],
  );
  assert.deepEqual(
    messages.map((message) =>
      message.type === "snapshot_start"
        ? "SNAPSHOT_START"
        : message.type === "snapshot_complete"
          ? "SNAPSHOT_COMPLETE"
          : message.type === "event_stream"
            ? `EVENT_STREAM ${message.payload.eventVersion}`
            : `SNAPSHOT_CHUNK ${message.sequence - 1}`,
    ),
    ["SNAPSHOT_START", "SNAPSHOT_CHUNK 1", "SNAPSHOT_CHUNK 2", "SNAPSHOT_COMPLETE", "EVENT_STREAM 101"],
  );
}

function validateRelayOrdering(messages, expectedSequences) {
  assert.deepEqual(
    messages.map((message) => message.sequence),
    expectedSequences,
  );
}

async function withStartedServer(config, callback) {
  const relayServer = createRelayServer(config);
  await relayServer.start();

  try {
    await callback(relayServer);
  } finally {
    await relayServer.stop();
  }
}

async function withMockedRedisConnect(callback) {
  const originalConnect = RedisSessionRegistry.connect;
  const calls = [];
  const sessionRegistry = createRedisSessionRegistry(new FakeRedisClient());

  RedisSessionRegistry.connect = async (redisUrl) => {
    calls.push(redisUrl);
    return sessionRegistry;
  };

  try {
    await callback({ calls, sessionRegistry });
  } finally {
    RedisSessionRegistry.connect = originalConnect;
  }
}

function createDiagnosticsCollector() {
  const entries = [];

  return {
    entries,
    logger(entry) {
      entries.push(entry);
    },
  };
}

function findDiagnosticsEntry(entries, stage) {
  return entries.find((entry) => entry.stage === stage);
}

test("redis_session_create", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const sessionId = "4d92f0cf-a044-4ae3-9e83-6e7dd53fd4ab";
  const expiresAt = redisClient.nowMs + 120_000;
  const redisKey = getRedisSessionKey(sessionId);

  const session = await sessionRegistry.createSession(sessionId, expiresAt);
  const storedSession = await sessionRegistry.getSession(sessionId);
  const rawSession = await redisClient.hGetAll(redisKey);

  assert.equal(session.sessionId, sessionId);
  assert.equal(session.mobileSocketId, null);
  assert.equal(session.webSocketId, null);
  assert.equal(session.expiresAt, expiresAt);
  assert.equal(session.state, SESSION_STATES.WAITING);
  assert.deepEqual(storedSession, session);
  assert.equal(await redisClient.exists(redisKey), 1);
  assert.deepEqual(await redisClient.keys("*"), [redisKey]);
  assert.equal(await redisClient.type(redisKey), "hash");
  assert.equal(await redisClient.ttl(redisKey), 120);
  assert.deepEqual(rawSession, {
    sessionId,
    webSocketId: "null",
    mobileSocketId: "null",
    createdAt: String(session.createdAt),
    expiresAt: String(expiresAt),
    state: SESSION_STATES.WAITING,
  });
});

test("redis_session_persist", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const sessionId = "56b8b39a-9ca7-44d8-a928-b0ef1126d2f4";
  const expiresAt = redisClient.nowMs + 120_000;

  await sessionRegistry.createSession(sessionId, expiresAt);
  await sessionRegistry.attachWebSocket(sessionId, "web-socket-lookup");
  const session = await sessionRegistry.getSession(sessionId);

  assert.deepEqual(session, {
    sessionId,
    mobileSocketId: null,
    webSocketId: "web-socket-lookup",
    createdAt: session.createdAt,
    expiresAt,
    state: SESSION_STATES.WAITING,
  });
});

test("attach_web_socket", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const sessionId = "44ae488a-c7ff-4d0a-b326-a2c9f3cc88ce";

  await sessionRegistry.createSession(sessionId, redisClient.nowMs + 60_000);
  const updatedSession = await sessionRegistry.attachWebSocket(sessionId, "web-socket-attach");

  assert.equal(updatedSession.webSocketId, "web-socket-attach");
  assert.equal(updatedSession.mobileSocketId, null);
  assert.equal(updatedSession.state, SESSION_STATES.WAITING);
  assert.deepEqual(await sessionRegistry.getSession(sessionId), updatedSession);
});

test("attach_mobile_socket", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const sessionId = "63ec0064-77e0-4ee8-a3ba-98dc1f23e2ce";

  await sessionRegistry.createSession(sessionId, redisClient.nowMs + 60_000);
  await sessionRegistry.attachWebSocket(sessionId, "web-socket-attach");
  const updatedSession = await sessionRegistry.attachMobileSocket(sessionId, "mobile-socket-1");

  assert.equal(updatedSession.mobileSocketId, "mobile-socket-1");
  assert.equal(updatedSession.webSocketId, "web-socket-attach");
  assert.equal(updatedSession.state, SESSION_STATES.WAITING);
  assert.deepEqual(await sessionRegistry.getSession(sessionId), updatedSession);
});

test("redis_session_pair_transition", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const sessionId = "8d2dd03d-e2de-477d-947a-08fab41dbc7a";
  const redisKey = getRedisSessionKey(sessionId);

  await sessionRegistry.createSession(sessionId, redisClient.nowMs + 120_000);
  await sessionRegistry.attachWebSocket(sessionId, "web-socket-state");
  const pairedSession = await sessionRegistry.updateSessionOnPair(sessionId, "mobile-socket-state");
  const rawSession = await redisClient.hGetAll(redisKey);

  assert.equal(pairedSession.state, SESSION_STATES.PAIRED);
  assert.equal(pairedSession.webSocketId, "web-socket-state");
  assert.equal(pairedSession.mobileSocketId, "mobile-socket-state");
  assert.equal(rawSession.mobileSocketId, "mobile-socket-state");
  assert.equal(rawSession.state, SESSION_STATES.PAIRED);

  await assert.rejects(
    () => sessionRegistry.setSessionState(sessionId, SESSION_STATES.WAITING),
    /Invalid session state transition/,
  );
});

test("redis_session_activate", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const sessionId = "18e8f865-336f-4d15-a0e8-acd0a62bad55";
  const redisKey = getRedisSessionKey(sessionId);

  await sessionRegistry.createSession(sessionId, redisClient.nowMs + 120_000);
  await sessionRegistry.attachWebSocket(sessionId, "web-socket-state");
  await sessionRegistry.updateSessionOnPair(sessionId, "mobile-socket-state");
  redisClient.advanceTime(119_000);
  const activeSession = await sessionRegistry.activateSession(sessionId);
  const rawSession = await redisClient.hGetAll(redisKey);

  assert.equal(activeSession.state, SESSION_STATES.ACTIVE);
  assert.equal(activeSession.webSocketId, "web-socket-state");
  assert.equal(activeSession.mobileSocketId, "mobile-socket-state");
  assert.equal(rawSession.state, SESSION_STATES.ACTIVE);
  assert.equal(activeSession.expiresAt, redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  assert.equal(await redisClient.ttl(redisKey), 120);

  await assert.rejects(
    () => sessionRegistry.setSessionState(sessionId, SESSION_STATES.WAITING),
    /Invalid session state transition/,
  );
});

test("session_ttl_valid", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const sessionId = "18e8f865-336f-4d15-a0e8-acd0a62bad56";
  const redisKey = getRedisSessionKey(sessionId);

  const session = await sessionRegistry.createSession(sessionId, "ttl-web-socket");

  assert.equal(session.expiresAt, redisClient.nowMs + 120_000);
  assert.equal(await redisClient.ttl(redisKey), 120);
  assert.equal(await redisClient.type(redisKey), "hash");
});

test("state_update", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const sessionId = "19f8f865-336f-4d15-a0e8-acd0a62bad56";

  await sessionRegistry.createSession(sessionId, redisClient.nowMs + 60_000);
  await sessionRegistry.attachWebSocket(sessionId, "web-socket-state");
  await sessionRegistry.attachMobileSocket(sessionId, "mobile-socket-state");
  const pairedSession = await sessionRegistry.updateSession(sessionId, { state: SESSION_STATES.PAIRED });
  const activeSession = await sessionRegistry.updateSession(sessionId, { state: SESSION_STATES.ACTIVE });
  const closedSession = await sessionRegistry.updateState(sessionId, SESSION_STATES.CLOSED);

  assert.equal(pairedSession.state, SESSION_STATES.PAIRED);
  assert.equal(activeSession.state, SESSION_STATES.ACTIVE);
  assert.equal(closedSession.state, SESSION_STATES.CLOSED);
  await assert.rejects(
    () => sessionRegistry.updateState(sessionId, SESSION_STATES.WAITING),
    /Invalid session state transition/,
  );
});

test("session_expiration", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const sessionId = "bb615715-bbaf-4672-b400-1fbe7dfb8ec4";

  await sessionRegistry.createSession(sessionId, redisClient.nowMs + 2_000);
  redisClient.advanceTime(3_000);

  assert.equal(await sessionRegistry.getSession(sessionId), null);
  assert.equal(await redisClient.exists(getRedisSessionKey(sessionId)), 0);
});

test("qr_session_create", async () => {
  const redisClient = new FakeRedisClient();
  const token = "qr-session-create-token";
  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const webSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await waitForOpen(webSocket);
      webSocket.send(
        JSON.stringify(
          createEnvelope({
            type: "qr_session_create",
            sessionId: "11111111-1111-4111-8111-111111111111",
            payload: { token },
          }),
        ),
      );

      const readyMessage = await waitForMessage(webSocket);
      assert.equal(readyMessage.type, "qr_session_ready");
  assert.equal(readyMessage.payload.sessionId, "11111111-1111-4111-8111-111111111111");
      assert.equal(typeof readyMessage.payload.expiresAt, "number");

      const storedSession = await relayServer.sessionRegistry.getSession(readyMessage.payload.sessionId);
      assert.equal(storedSession.state, SESSION_STATES.WAITING);
      assert.equal(storedSession.token, token);
      assert.equal(storedSession.mobileSocketId, null);
      webSocket.close();
    },
  );
});

test("pair_request_attach_mobile", async () => {
  const redisClient = new FakeRedisClient();
  const token = "pair-attach-token";
  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const webSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const mobileSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await Promise.all([waitForOpen(webSocket), waitForOpen(mobileSocket)]);
      webSocket.send(
        JSON.stringify(
          createEnvelope({
            type: "qr_session_create",
            sessionId: "22222222-2222-4222-8222-222222222222",
            payload: { token },
          }),
        ),
      );
      const readyMessage = await waitForMessage(webSocket);
      const webApprovalPromise = waitForMessage(webSocket);
      const mobileApprovalPromise = waitForMessage(mobileSocket);

      mobileSocket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "pair_request",
          sessionId: readyMessage.payload.sessionId,
          timestamp: Date.now(),
          sequence: readyMessage.sequence + 1,
          payload: {
            sessionId: readyMessage.payload.sessionId,
            token,
          },
        }),
      );

      await Promise.all([webApprovalPromise, mobileApprovalPromise]);
      const storedSession = await relayServer.sessionRegistry.getSession(readyMessage.payload.sessionId);

      assert.equal(storedSession.state, SESSION_STATES.ACTIVE);
      assert.equal(typeof storedSession.mobileSocketId, "string");
      assert.ok(relayServer.connectionManager.lookupConnection(readyMessage.payload.sessionId));

      webSocket.close();
      mobileSocket.close();
    },
  );
});

test("pair_request_rejects_invalid_token", async () => {
  const redisClient = new FakeRedisClient();
  const sessionToken = "session-token-match";
  const requestToken = "session-token-mismatch";

  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const webSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const mobileSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await Promise.all([waitForOpen(webSocket), waitForOpen(mobileSocket)]);
      webSocket.send(
        JSON.stringify(
          createEnvelope({
            type: "qr_session_create",
            sessionId: "2f7451e1-e4a2-4db5-a60d-6f4349d16a5d",
            payload: { token: sessionToken },
          }),
        ),
      );
      const readyMessage = await waitForMessage(webSocket);

      mobileSocket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "pair_request",
          sessionId: readyMessage.payload.sessionId,
          timestamp: Date.now(),
          sequence: readyMessage.sequence + 1,
          payload: {
            sessionId: readyMessage.payload.sessionId,
            token: requestToken,
          },
        }),
      );

      const rejectionMessage = await waitForMessage(mobileSocket);
      const storedSession = await relayServer.sessionRegistry.getSession(readyMessage.payload.sessionId);

      assert.equal(rejectionMessage.type, "pair_rejected");
      assert.equal(rejectionMessage.payload.reason, "INVALID_TOKEN");
      assert.equal(storedSession.state, SESSION_STATES.WAITING);

      webSocket.close();
      mobileSocket.close();
    },
  );
});

test("pair_request_requires_existing_session", async () => {
  const redisClient = new FakeRedisClient();
  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const mobileSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await waitForOpen(mobileSocket);

      mobileSocket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "pair_request",
          sessionId: "ef28ee13-63c7-4dd5-aecf-9d7dcf89d1f8",
          timestamp: Date.now(),
          sequence: 1,
          payload: {
            sessionId: "ef28ee13-63c7-4dd5-aecf-9d7dcf89d1f8",
          },
        }),
      );

      const rejectionMessage = await waitForMessage(mobileSocket);

      assert.equal(rejectionMessage.type, "pair_rejected");
      assert.equal(rejectionMessage.payload.reason, "session_not_found");

      mobileSocket.close();
    },
  );
});

test("pair_request_expired", async () => {
  const redisClient = new FakeRedisClient(1_000);
  const token = "expired-pair-token";
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const connectionManager = createConnectionManager();
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
  });
  const webSocket = new MockSocket();
  const mobileSocket = new MockSocket();
  const sessionId = "bb18ca91-a858-4f8b-8ed7-eb39c883733d";
  const expiresAt = redisClient.nowMs + 250;

  connectionManager.registerConnection(webSocket, { connectionId: "web:1:relay:1" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "mobile:1:relay:1" });

  await sessionRegistry.createSession(sessionId, "web:1:relay:1", expiresAt, token);
  const pairingSystem = createQRPairingSystem({
    connectionManager,
    sessionLifecycleManager,
    sessionRegistry,
    pairingTtlMs: 250,
    now: () => redisClient.nowMs,
  });

  redisClient.advanceTime(500);
  const result = await pairingSystem.handlePairRequest(mobileSocket, {
    protocolVersion: DEFAULT_PROTOCOL_VERSION,
    type: "pair_request",
    sessionId,
    timestamp: redisClient.nowMs,
    sequence: 1,
    payload: { sessionId, token },
  });

  assert.equal(result.handled, true);
  assert.equal(result.rejected, true);
  assert.equal(result.reason, "pair_request_expired");
  assert.equal(mobileSocket.sentMessages.length, 1);
  assert.deepEqual(JSON.parse(mobileSocket.sentMessages[0]), {
    protocolVersion: DEFAULT_PROTOCOL_VERSION,
    type: "pair_rejected",
    sessionId,
    timestamp: JSON.parse(mobileSocket.sentMessages[0]).timestamp,
    sequence: 2,
    payload: {
      reason: "pair_request_expired",
    },
  });
  assert.equal(await sessionRegistry.getSession(sessionId), null);
});

test("session_create", async () => {
  const redisClient = new FakeRedisClient();
  const token = "lifecycle-create-token";
  const connectionManager = createConnectionManager();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const logs = [];
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry: createRedisSessionRegistry(redisClient, {
      now: () => redisClient.nowMs,
      logger: (entry) => logs.push(entry),
    }),
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
    logger: (entry) => logs.push(entry),
  });
  const sessionId = "32d0e189-e11d-42e5-819a-a9f528ef1b0a";
  const expiresAt = redisClient.nowMs + 120_000;

  const session = await sessionLifecycleManager.createSession({
    sessionId,
    token,
    webSocketId: "web-lifecycle-1",
    expiresAt,
  });

  assert.equal(session.sessionId, sessionId);
  assert.equal(session.token, token);
  assert.equal(session.webSocketId, "web-lifecycle-1");
  assert.equal(session.mobileSocketId, null);
  assert.equal(session.state, SESSION_STATES.WAITING);
  assert.equal(scheduler.handles.length, 1);
  assert.equal(logs.includes("SESSION_CREATE persisted"), true);
});

test("session_paired_to_active_lifecycle", async () => {
  const redisClient = new FakeRedisClient();
  const logs = [];
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
    logger: (entry) => logs.push(entry),
  });
  const connectionManager = createConnectionManager();
  const webSocket = new MockSocket();
  const mobileSocket = new MockSocket();
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    logger: (entry) => logs.push(entry),
  });
  const sessionId = "a54fa7f0-2b0d-4663-af29-0d53c53380a1";

  connectionManager.registerConnection(webSocket, { connectionId: "web-lifecycle-activate" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "mobile-lifecycle-activate" });

  await sessionLifecycleManager.createSession(sessionId, "web-lifecycle-activate", redisClient.nowMs + 120_000);
  const pairedSession = await sessionLifecycleManager.transitionToPaired(sessionId, "mobile-lifecycle-activate");
  const activeSession = await sessionLifecycleManager.transitionToActive(sessionId);
  const binding = connectionManager.lookupConnection(sessionId);

  assert.equal(pairedSession.state, SESSION_STATES.PAIRED);
  assert.equal(activeSession.state, SESSION_STATES.ACTIVE);
  assert.equal(binding.webSocket, webSocket);
  assert.equal(binding.mobileSocket, mobileSocket);
  assert.equal(logs.includes("SESSION_UPDATE paired"), true);
  assert.equal(logs.includes("SESSION_UPDATE active"), true);
});

test("session_active_to_closed", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const connectionManager = createConnectionManager();
  const webSocket = new MockSocket();
  const mobileSocket = new MockSocket();
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
  });
  const sessionId = "5b63f98a-7cd5-4878-b260-0f50093733c4";

  connectionManager.registerConnection(webSocket, { connectionId: "web-lifecycle-close" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "mobile-lifecycle-close" });

  await sessionLifecycleManager.createSession(sessionId, "web-lifecycle-close", redisClient.nowMs + 60_000);
  await sessionLifecycleManager.transitionToPaired(sessionId, "mobile-lifecycle-close");
  await sessionLifecycleManager.transitionToActive(sessionId);
  await sessionLifecycleManager.transitionToClosed(sessionId, "manual_logout");

  const session = await sessionRegistry.getSession(sessionId);

  assert.equal(session.state, SESSION_STATES.CLOSED);
  assert.deepEqual(JSON.parse(webSocket.sentMessages[0]), {
    type: "session_close",
    payload: {
      reason: "manual_logout",
    },
  });
  assert.deepEqual(JSON.parse(mobileSocket.sentMessages[0]), {
    type: "session_close",
    payload: {
      reason: "manual_logout",
    },
  });
});

test("disconnect_cleanup", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const connectionManager = createConnectionManager();
  const webSocket = new MockSocket();
  const mobileSocket = new MockSocket();
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
  });
  const sessionId = "e4d97709-18ec-44d1-bb79-cb1fe2474042";

  connectionManager.registerConnection(webSocket, { connectionId: "web-lifecycle-disconnect" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "mobile-lifecycle-disconnect" });

  await sessionLifecycleManager.createSession(sessionId, "web-lifecycle-disconnect", redisClient.nowMs + 60_000);
  await sessionLifecycleManager.transitionToPaired(sessionId, "mobile-lifecycle-disconnect");
  await sessionLifecycleManager.transitionToActive(sessionId);
  const handled = await sessionLifecycleManager.handleDisconnect("mobile-lifecycle-disconnect");
  const session = await sessionRegistry.getSession(sessionId);

  assert.equal(handled, true);
  assert.equal(session.state, SESSION_STATES.CLOSED);
  assert.deepEqual(JSON.parse(webSocket.sentMessages[0]), {
    type: "session_close",
    payload: {
      reason: "disconnect",
    },
  });
});

test("session_expiration_cleanup", async () => {
  const redisClient = new FakeRedisClient(5_000);
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const connectionManager = createConnectionManager();
  const webSocket = new MockSocket();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
  });
  const sessionId = "d505df5e-ff38-4816-ab9f-b67e85af527b";

  connectionManager.registerConnection(webSocket, { connectionId: "web-lifecycle-expire" });

  await sessionLifecycleManager.createSession(sessionId, "web-lifecycle-expire", redisClient.nowMs + 100);
  redisClient.advanceTime(150);
  await scheduler.runDueTasks();

  const session = await sessionRegistry.getSession(sessionId);

  assert.equal(session.state, SESSION_STATES.CLOSED);
  assert.deepEqual(JSON.parse(webSocket.sentMessages[0]), {
    type: "session_close",
    payload: {
      reason: "timeout",
    },
  });
});

test("session_ttl_on_create", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const sessionId = "f9ebbfb2-f4c2-4ef6-b4ec-c88509be8ab1";

  await sessionRegistry.createSession(sessionId, "ttl-create-web");

  assert.equal(await redisClient.ttl(getRedisSessionKey(sessionId)), 120);
  assert.equal((await sessionRegistry.getSession(sessionId)).expiresAt, redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
});

test("session_ttl_on_activate", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const connectionManager = createConnectionManager();
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
  });
  const sessionId = "ef950ee6-f312-49fd-ae75-c34dc3ab71a1";
  const webSocket = new MockSocket();
  const mobileSocket = new MockSocket();

  connectionManager.registerConnection(webSocket, { connectionId: "ttl-activate-web" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "ttl-activate-mobile" });

  await sessionLifecycleManager.createSession(sessionId, "ttl-activate-web", redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  redisClient.advanceTime(DEFAULT_SESSION_TTL_MS - 1_000);
  await sessionLifecycleManager.transitionToPaired(sessionId, "ttl-activate-mobile");
  const activeSession = await sessionLifecycleManager.transitionToActive(sessionId);

  assert.equal(activeSession.expiresAt, redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  assert.equal(await redisClient.ttl(getRedisSessionKey(sessionId)), 120);
});

test("single_timer_per_session", async () => {
  const redisClient = new FakeRedisClient();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const connectionManager = createConnectionManager();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
  });
  const sessionId = "1ca41f76-1a36-4fb7-84b2-12291690d7cf";
  const webSocket = new MockSocket();
  const mobileSocket = new MockSocket();

  connectionManager.registerConnection(webSocket, { connectionId: "single-timer-web" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "single-timer-mobile" });

  await sessionLifecycleManager.createSession(sessionId, "single-timer-web", redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  await sessionLifecycleManager.transitionToPaired(sessionId, "single-timer-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);

  assert.equal(scheduler.getActiveHandleCount(), 1);
});

test("single_timeout_source", async () => {
  const redisClient = new FakeRedisClient();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const connectionManager = createConnectionManager();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
  });
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
  });
  const sessionId = "f3b6ab14-0ddf-4786-970d-d702335cfd2f";
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket, { connectionId: "single-timeout-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "single-timeout-web" });

  await sessionLifecycleManager.createSession(sessionId, "single-timeout-web", redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  await sessionLifecycleManager.transitionToPaired(sessionId, "single-timeout-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);

  const activeHandle = sessionLifecycleManager.getTimerHandle(sessionId);
  assert.equal(scheduler.getActiveHandleCount(), 1);

  await messageRouter.routeMessage(createEnvelope({ sessionId, type: "snapshot_start", sequence: 21 }), mobileSocket);

  assert.notEqual(sessionLifecycleManager.getTimerHandle(sessionId), activeHandle);
  assert.equal(scheduler.getActiveHandleCount(), 1);
});

test("timer_reset_on_activation", async () => {
  const redisClient = new FakeRedisClient();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const logs = [];
  const connectionManager = createConnectionManager();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
    logger: (entry) => logs.push(entry),
  });
  const sessionId = "b6a868c8-a330-4aea-b57c-2027dff5e2ef";
  const webSocket = new MockSocket();
  const mobileSocket = new MockSocket();

  connectionManager.registerConnection(webSocket, { connectionId: "timer-reset-web" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "timer-reset-mobile" });

  await sessionLifecycleManager.createSession(sessionId, "timer-reset-web", redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  const initialHandle = sessionLifecycleManager.getTimerHandle(sessionId);
  await sessionLifecycleManager.transitionToPaired(sessionId, "timer-reset-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);

  assert.notEqual(sessionLifecycleManager.getTimerHandle(sessionId), initialHandle);
  assert.equal(logs.some((entry) => entry === `TIMER_RESET sessionId=${sessionId}`), true);
  assert.equal(scheduler.getActiveHandleCount(), 1);
});

test("session_ttl_refresh_on_message", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const connectionManager = createConnectionManager();
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
  });
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
  });
  const sessionId = "3f548d67-c48f-4f43-bc9d-66371507e0da";
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({ sessionId, type: "event_stream" });

  connectionManager.registerConnection(mobileSocket, { connectionId: "ttl-refresh-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "ttl-refresh-web" });

  await sessionLifecycleManager.createSession(sessionId, "ttl-refresh-web", redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  await sessionLifecycleManager.transitionToPaired(sessionId, "ttl-refresh-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);

  redisClient.advanceTime(DEFAULT_SESSION_TTL_MS - 1_000);
  const routed = await messageRouter.routeMessage(envelope, mobileSocket);
  const refreshedSession = await sessionRegistry.getSession(sessionId);

  assert.equal(routed, true);
  assert.equal(await redisClient.ttl(getRedisSessionKey(sessionId)), 120);
  assert.equal(refreshedSession.expiresAt, redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
});

test("timer_refresh_on_message", async () => {
  const redisClient = new FakeRedisClient();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const logs = [];
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const connectionManager = createConnectionManager();
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
    logger: (entry) => logs.push(entry),
  });
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
  });
  const sessionId = "ca1dcd6d-b314-41ca-a989-5fc50f654ae3";
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket, { connectionId: "timer-refresh-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "timer-refresh-web" });

  await sessionLifecycleManager.createSession(sessionId, "timer-refresh-web", redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  await sessionLifecycleManager.transitionToPaired(sessionId, "timer-refresh-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);

  const activeHandle = sessionLifecycleManager.getTimerHandle(sessionId);
  await messageRouter.routeMessage(createEnvelope({ sessionId, type: "snapshot_start", sequence: 11 }), mobileSocket);

  assert.notEqual(sessionLifecycleManager.getTimerHandle(sessionId), activeHandle);
  assert.equal(logs.some((entry) => entry === `TIMER_RESET sessionId=${sessionId}`), true);
  assert.equal(scheduler.getActiveHandleCount(), 1);
});

test("no_duplicate_timer_clear", async () => {
  const redisClient = new FakeRedisClient();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const logs = [];
  const connectionManager = createConnectionManager();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
    logger: (entry) => logs.push(entry),
  });
  const sessionId = "6326b198-fec8-4d5d-b7d3-7d8289d8934f";
  const webSocket = new MockSocket();
  const mobileSocket = new MockSocket();

  connectionManager.registerConnection(webSocket, { connectionId: "duplicate-clear-web" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "duplicate-clear-mobile" });

  await sessionLifecycleManager.createSession(sessionId, "duplicate-clear-web", redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  await sessionLifecycleManager.transitionToPaired(sessionId, "duplicate-clear-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);

  const closeLogStart = logs.length;
  await sessionLifecycleManager.closeSession(sessionId, "manual_logout");
  const closeLogs = logs.slice(closeLogStart);

  assert.deepEqual(
    closeLogs.filter((entry) => entry.startsWith("TIMER_CLEAR previousHandle=")),
    ["TIMER_CLEAR previousHandle=true"],
  );
  assert.equal(closeLogs.includes("SESSION_CLOSED reason=manual_logout"), true);
  assert.equal(scheduler.getActiveHandleCount(), 0);
});

test("no_premature_timeout_after_handshake", async () => {
  const redisClient = new FakeRedisClient(5_000);
  const connectionManager = createConnectionManager();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
    sessionTtlMs: 2_100,
  });
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
  });
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
  });
  const sessionId = "7bf98dc5-1f13-4940-81fb-74f1e7df57be";
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket, { connectionId: "handshake-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "handshake-web" });

  await sessionLifecycleManager.createSession(sessionId, "handshake-web", redisClient.nowMs + 2_100);
  await sessionLifecycleManager.transitionToPaired(sessionId, "handshake-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);

  redisClient.advanceTime(1_500);
  await scheduler.runDueTasks();
  await messageRouter.routeMessage(createEnvelope({ sessionId, type: "protocol_handshake", sequence: 12 }), mobileSocket);
  redisClient.advanceTime(1_500);
  await scheduler.runDueTasks();

  assert.equal((await sessionRegistry.getSession(sessionId)).state, SESSION_STATES.ACTIVE);
  assert.equal(webSocket.sentMessages.some((message) => JSON.parse(message).type === "session_close"), false);
});

test("close_session_idempotent", async () => {
  const redisClient = new FakeRedisClient();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const logs = [];
  const connectionManager = createConnectionManager();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
    logger: (entry) => logs.push(entry),
  });
  const sessionId = "8aa05a03-f0f1-4e15-96b5-a49a838ce262";
  const webSocket = new MockSocket();
  const mobileSocket = new MockSocket();

  connectionManager.registerConnection(webSocket, { connectionId: "idempotent-close-web" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "idempotent-close-mobile" });

  await sessionLifecycleManager.createSession(sessionId, "idempotent-close-web", redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  await sessionLifecycleManager.transitionToPaired(sessionId, "idempotent-close-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);

  const firstCloseResult = await sessionLifecycleManager.closeSession(sessionId, "manual_logout");
  const logCountAfterFirstClose = logs.length;
  const secondCloseResult = await sessionLifecycleManager.closeSession(sessionId, "manual_logout");

  assert.equal(firstCloseResult, true);
  assert.equal(secondCloseResult, false);
  assert.equal(logs.length, logCountAfterFirstClose);
  assert.equal(logs.filter((entry) => entry === "SESSION_CLOSED reason=manual_logout").length, 1);
  assert.equal(scheduler.getActiveHandleCount(), 0);
});

test("no_premature_close_after_handshake", async () => {
  const redisClient = new FakeRedisClient(5_000);
  const connectionManager = createConnectionManager();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
    sessionTtlMs: 2_100,
  });
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
  });
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
  });
  const sessionId = "7af8406c-8f98-4644-aa11-d6190f208700";
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket, { connectionId: "post-handshake-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "post-handshake-web" });

  await sessionLifecycleManager.createSession(sessionId, "post-handshake-web", redisClient.nowMs + 2_100);
  await sessionLifecycleManager.transitionToPaired(sessionId, "post-handshake-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);

  redisClient.advanceTime(1_500);
  await scheduler.runDueTasks();
  await messageRouter.routeMessage(createEnvelope({ sessionId, type: "protocol_handshake", sequence: 12 }), mobileSocket);
  redisClient.advanceTime(1_500);
  await scheduler.runDueTasks();

  const routed = await messageRouter.routeMessage(
    createEnvelope({ sessionId, type: "snapshot_start", sequence: 13 }), mobileSocket,
  );

  assert.equal(routed, true);
  assert.equal((await sessionRegistry.getSession(sessionId)).state, SESSION_STATES.ACTIVE);
  assert.equal(webSocket.sentMessages.some((message) => JSON.parse(message).type === "session_close"), false);
});

test("no_timer_leak_after_session_close", async () => {
  const redisClient = new FakeRedisClient();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const connectionManager = createConnectionManager();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
  });
  const sessionId = "6d62b7dc-00cf-482f-ba6c-ab24b50e14c2";
  const webSocket = new MockSocket();
  const mobileSocket = new MockSocket();

  connectionManager.registerConnection(webSocket, { connectionId: "timer-leak-web" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "timer-leak-mobile" });

  await sessionLifecycleManager.createSession(sessionId, "timer-leak-web", redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  await sessionLifecycleManager.transitionToPaired(sessionId, "timer-leak-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);
  await sessionLifecycleManager.closeSession(sessionId, "manual_logout");

  assert.equal(sessionLifecycleManager.getTimerHandle(sessionId), null);
  assert.equal(scheduler.getActiveHandleCount(), 0);
});

test("no_premature_session_expiry", async () => {
  const redisClient = new FakeRedisClient(5_000);
  const connectionManager = createConnectionManager();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const sessionTtlMs = 2_100;
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
    sessionTtlMs,
  });
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
  });
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
  });
  const sessionId = "610a1142-e1c6-4ca1-80a2-859ab0d68a77";
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket, { connectionId: "no-expiry-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "no-expiry-web" });

  await sessionLifecycleManager.createSession(sessionId, "no-expiry-web", redisClient.nowMs + sessionTtlMs);
  await sessionLifecycleManager.transitionToPaired(sessionId, "no-expiry-mobile");
  await sessionLifecycleManager.transitionToActive(sessionId);

  redisClient.advanceTime(1_500);
  await scheduler.runDueTasks();
  assert.equal((await sessionRegistry.getSession(sessionId)).state, SESSION_STATES.ACTIVE);

  await messageRouter.routeMessage(createEnvelope({ sessionId, type: "snapshot_start", sequence: 3 }), mobileSocket);
  redisClient.advanceTime(sessionTtlMs - 1);
  await scheduler.runDueTasks();
  assert.equal((await sessionRegistry.getSession(sessionId)).state, SESSION_STATES.ACTIVE);

  redisClient.advanceTime(1);
  await scheduler.runDueTasks();
  assert.equal((await sessionRegistry.getSession(sessionId)).state, SESSION_STATES.CLOSED);
  assert.deepEqual(JSON.parse(webSocket.sentMessages.at(-1)), {
    type: "session_close",
    payload: {
      reason: "timeout",
    },
  });
});

test("ttl_computation_correct", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient, {
    now: () => redisClient.nowMs,
  });
  const sessionId = "dd122ebb-331e-4e3b-b6c0-fb4f117120df";

  await sessionRegistry.createSession(sessionId, "ttl-computation-web");
  const firstSession = await sessionRegistry.getSession(sessionId);

  redisClient.advanceTime(DEFAULT_SESSION_TTL_MS - 1_000);
  const refreshedSession = await sessionRegistry.refreshSessionTtl(sessionId, "activity");

  assert.equal(refreshedSession.expiresAt, redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  assert.equal(refreshedSession.expiresAt > firstSession.expiresAt, true);
  assert.equal(await redisClient.ttl(getRedisSessionKey(sessionId)), 120);
});

test("rate_limit_commands", () => {
  let nowMs = 1_000;
  const relayRateLimiter = createRelayRateLimiter({
    maxCommandsPerSecond: 2,
    now: () => nowMs,
  });

  assert.equal(relayRateLimiter.evaluateInboundMessage({ socketId: "socket-a", payloadSize: 32 }), null);
  assert.equal(relayRateLimiter.evaluateInboundMessage({ socketId: "socket-a", payloadSize: 32 }), null);
  assert.deepEqual(relayRateLimiter.evaluateInboundMessage({ socketId: "socket-a", payloadSize: 32 }), {
    reason: "command_rate_exceeded",
  });

  nowMs += 1_000;

  assert.equal(relayRateLimiter.evaluateInboundMessage({ socketId: "socket-a", payloadSize: 32 }), null);
});

test("payload_size_limit", async () => {
  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      rateLimiting: {
        maxCommandsPerSecond: DEFAULT_MAX_COMMANDS_PER_SECOND,
        maxPayloadSize: 64,
        maxSessionsPerIp: DEFAULT_MAX_SESSIONS_PER_IP,
      },
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const closePromise = waitForClose(socket);

      await waitForOpen(socket);
      socket.send(
        JSON.stringify(
          createEnvelope({
            payload: { blob: "x".repeat(512) },
          }),
        ),
      );

      const response = await waitForMessage(socket);
      await closePromise;

      assert.deepEqual(response, {
        type: "control_error",
        payload: {
          reason: "payload_too_large",
        },
      });
    },
  );
});

test("session_limit_per_ip", async () => {
  const redisClient = new FakeRedisClient();
  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      rateLimiting: {
        maxCommandsPerSecond: DEFAULT_MAX_COMMANDS_PER_SECOND,
        maxPayloadSize: DEFAULT_MAX_PAYLOAD_SIZE,
        maxSessionsPerIp: 2,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const closePromise = waitForClose(socket);

      await waitForOpen(socket);

      socket.send(
        JSON.stringify(
          createEnvelope({ type: "qr_session_create", sessionId: "33333333-3333-4333-8333-333333333331", payload: {}, sequence: 1 }),
        ),
      );
      assert.equal((await waitForMessage(socket)).type, "qr_session_ready");

      socket.send(
        JSON.stringify(
          createEnvelope({ type: "qr_session_create", sessionId: "33333333-3333-4333-8333-333333333332", payload: {}, sequence: 2 }),
        ),
      );
      assert.equal((await waitForMessage(socket)).type, "qr_session_ready");

      socket.send(
        JSON.stringify(
          createEnvelope({ type: "qr_session_create", sessionId: "33333333-3333-4333-8333-333333333333", payload: {}, sequence: 3 }),
        ),
      );
      const response = await waitForMessage(socket);
      await closePromise;

      assert.deepEqual(response, {
        type: "control_error",
        payload: {
          reason: "session_rate_exceeded",
        },
      });
    },
  );
});

test("connection_termination_on_violation", async () => {
  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      rateLimiting: {
        maxCommandsPerSecond: 2,
        maxPayloadSize: DEFAULT_MAX_PAYLOAD_SIZE,
        maxSessionsPerIp: DEFAULT_MAX_SESSIONS_PER_IP,
      },
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const closePromise = waitForClose(socket);

      await waitForOpen(socket);

      socket.send(JSON.stringify(createEnvelope({ sessionId: "rate-limit-1" })));
      socket.send(JSON.stringify(createEnvelope({ sessionId: "rate-limit-1", sequence: 2 })));
      socket.send(JSON.stringify(createEnvelope({ sessionId: "rate-limit-1", sequence: 3 })));

      const response = await waitForMessage(socket);
      await closePromise;

      assert.deepEqual(response, {
        type: "control_error",
        payload: {
          reason: "command_rate_exceeded",
        },
      });
    },
  );
});

test("ws_message_received", async () => {
  const diagnostics = createDiagnosticsCollector();

  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      diagnostics: {
        ingress: {
          enabled: true,
          logger: diagnostics.logger,
        },
      },
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await waitForOpen(socket);
      socket.send(JSON.stringify(createEnvelope({ sessionId: "diag-session-1" })));
      await new Promise((resolve) => relayServer.events.once("messageDropped", resolve));

      const entry = findDiagnosticsEntry(diagnostics.entries, "ws_message_received");

      assert.equal(typeof entry.socketId, "string");
      assert.equal(entry.messageType, null);
      assert.equal(entry.sessionId, null);

      socket.close();
    },
  );
});

test("transport_envelope_parsed", async () => {
  const diagnostics = createDiagnosticsCollector();

  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      diagnostics: {
        ingress: {
          enabled: true,
          logger: diagnostics.logger,
        },
      },
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await waitForOpen(socket);
      socket.send(JSON.stringify(createEnvelope({ sessionId: "diag-session-2", type: "event_stream" })));
      await new Promise((resolve) => relayServer.events.once("messageDropped", resolve));

      const parseEntry = findDiagnosticsEntry(diagnostics.entries, "json_parse_success");
      const validationEntry = findDiagnosticsEntry(diagnostics.entries, "transport_envelope_validated");

      assert.equal(parseEntry.messageType, "event_stream");
      assert.equal(parseEntry.sessionId, "diag-session-2");
      assert.equal(validationEntry.messageType, "event_stream");
      assert.equal(validationEntry.sessionId, "diag-session-2");
      assert.equal(validationEntry.valid, true);

      socket.close();
    },
  );
});

test("router_dispatch_logged", async () => {
  const diagnosticsEntries = [];
  const diagnostics = createRelayIngressDiagnostics({
    enabled: true,
    logger(entry) {
      diagnosticsEntries.push(entry);
    },
  });
  const connectionManager = createConnectionManager({ diagnostics });
  const messageRouter = createMessageRouter(connectionManager, { diagnostics });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({ sessionId: "diag-router-session", type: "event_stream" });

  connectionManager.registerConnection(mobileSocket, { connectionId: "diag-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "diag-web" });
  connectionManager.bindSessionSockets("diag-router-session", mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);
  const dispatchEntry = findDiagnosticsEntry(diagnosticsEntries, "message_router_dispatch");

  assert.equal(routed, true);
  assert.equal(dispatchEntry.socketId, "diag-mobile");
  assert.equal(dispatchEntry.messageType, "event_stream");
  assert.equal(dispatchEntry.sessionId, "diag-router-session");
  assert.equal(dispatchEntry.routed, true);
});

test("bootstrap_message_allowed", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const socket = new MockSocket();

  for (const messageType of BOOTSTRAP_MESSAGE_TYPES) {
    const routed = await messageRouter.routeMessage(
      createEnvelope({
        type: messageType,
        sessionId: messageType === "qr_session_create" ? "bootstrap-session" : "bootstrap-session",
      }),
      socket,
    );

    assert.equal(routed, true);
  }
});

test("non_bootstrap_blocked", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const socket = new MockSocket();

  const routed = await messageRouter.routeMessage(
    createEnvelope({
      type: "event_stream",
      sessionId: "non-bootstrap-session",
    }),
    socket,
  );

  assert.equal(routed, false);
});

test("router_dispatch_bootstrap", async () => {
  const diagnosticsEntries = [];
  const diagnostics = createRelayIngressDiagnostics({
    enabled: true,
    logger(entry) {
      diagnosticsEntries.push(entry);
    },
  });
  const connectionManager = createConnectionManager({ diagnostics });
  const messageRouter = createMessageRouter(connectionManager, { diagnostics });
  const socket = new MockSocket();

  const routed = await messageRouter.routeMessage(
    createEnvelope({
      type: "protocol_handshake",
      sessionId: "bootstrap-dispatch-session",
    }),
    socket,
  );
  const dispatchEntry = findDiagnosticsEntry(diagnosticsEntries, "message_router_dispatch");

  assert.equal(routed, true);
  assert.equal(dispatchEntry.messageType, "protocol_handshake");
  assert.equal(dispatchEntry.sessionId, "bootstrap-dispatch-session");
  assert.equal(dispatchEntry.routed, true);
  assert.equal(dispatchEntry.reason, "bootstrap_message_allowed");
});

test("bootstrap_dispatch_continues", async () => {
  const connectionManager = createConnectionManager();
  const socket = new MockSocket();
  let handlerCallCount = 0;
  let receivedEnvelope = null;
  const messageRouter = createMessageRouter(connectionManager, {
    dispatchTable: {
      qr_session_create(senderSocket, envelope) {
        handlerCallCount += 1;
        assert.equal(senderSocket, socket);
        receivedEnvelope = envelope;
      },
    },
  });
  const envelope = createEnvelope({
    type: "qr_session_create",
    sessionId: "bootstrap-dispatch-continues",
    payload: { opaque: "unchanged" },
  });
  const originalSnapshot = JSON.stringify(envelope);

  connectionManager.registerConnection(socket, { connectionId: "bootstrap-dispatch-socket" });

  const routed = await messageRouter.routeMessage(envelope, socket);

  assert.equal(routed, true);
  assert.equal(handlerCallCount, 1);
  assert.equal(receivedEnvelope, envelope);
  assert.equal(JSON.stringify(envelope), originalSnapshot);
});

test("dispatch_table_handler_logged", async () => {
  const diagnosticsEntries = [];
  const diagnostics = createRelayIngressDiagnostics({
    enabled: true,
    logger(entry) {
      diagnosticsEntries.push(entry);
    },
  });
  const connectionManager = createConnectionManager({ diagnostics });
  const socket = new MockSocket();
  const messageRouter = createMessageRouter(connectionManager, {
    diagnostics,
    dispatchTable: {
      qr_session_create() {},
    },
  });

  connectionManager.registerConnection(socket, { connectionId: "dispatch-log-socket" });

  const routed = await messageRouter.routeMessage(
    createEnvelope({
      type: "qr_session_create",
      sessionId: "dispatch-log-session",
      payload: {},
    }),
    socket,
  );
  const dispatchEntry = diagnosticsEntries.find(
    (entry) => entry.stage === "message_router_dispatch" && entry.reason === "dispatch_table_handler",
  );

  assert.equal(routed, true);
  assert.equal(Boolean(dispatchEntry), true);
  assert.equal(dispatchEntry.messageType, "qr_session_create");
  assert.equal(dispatchEntry.sessionId, "dispatch-log-session");
  assert.equal(dispatchEntry.socketId, "dispatch-log-socket");
});

test("router_dispatch_registered", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({ sessionId: "registered-dispatch-session", type: "event_stream" });

  connectionManager.registerConnection(mobileSocket, { connectionId: "registered-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "registered-web" });
  connectionManager.bindSessionSockets("registered-dispatch-session", mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, true);
  assert.equal(webSocket.sentMessages[0], JSON.stringify(envelope));
});

test("router_rejects_non_active_session", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, { sessionRegistry });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "0a7fd6af-2a78-47a4-884d-a2ef10b7d161";
  const envelope = createEnvelope({ sessionId, type: "event_stream" });

  connectionManager.registerConnection(mobileSocket, { connectionId: "router-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "router-web" });
  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await sessionRegistry.createSession(sessionId, "router-web", redisClient.nowMs + 60_000);
  await sessionRegistry.updateSession(sessionId, {
    mobileSocketId: "router-mobile",
    state: SESSION_STATES.PAIRED,
  });

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, false);
  assert.equal(webSocket.sentMessages.length, 0);
});

test("router_allows_active_session", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, { sessionRegistry });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "3eb4cf16-0cd3-4c6e-90d9-080bf46c5ddb";
  const envelope = createEnvelope({ sessionId, type: "event_stream", payload: { opaque: true } });

  connectionManager.registerConnection(mobileSocket, { connectionId: "router-active-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "router-active-web" });
  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await sessionRegistry.createSession(sessionId, "router-active-web", redisClient.nowMs + 60_000);
  await sessionRegistry.updateSession(sessionId, {
    mobileSocketId: "router-active-mobile",
    state: SESSION_STATES.PAIRED,
  });
  await sessionRegistry.updateSession(sessionId, {
    state: SESSION_STATES.ACTIVE,
  });

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, true);
  assert.equal(webSocket.sentMessages[0], JSON.stringify(envelope));
});

test("qr_handler_invoked", async () => {
  const diagnostics = createDiagnosticsCollector();
  const redisClient = new FakeRedisClient();

  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      diagnostics: {
        ingress: {
          enabled: true,
          logger: diagnostics.logger,
        },
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await waitForOpen(socket);
      socket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "qr_session_create",
          sessionId: "44444444-4444-4444-8444-444444444444",
          timestamp: Date.now(),
          sequence: 1,
          payload: {},
        }),
      );
      await waitForMessage(socket);

      const qrEntry = findDiagnosticsEntry(diagnostics.entries, "qr_handler_invoked");

      assert.equal(qrEntry.messageType, "qr_session_create");
      assert.equal(qrEntry.handled, true);
      assert.equal(typeof qrEntry.socketId, "string");

      socket.close();
    },
  );
});

test("session_created_after_router", async () => {
  const redisClient = new FakeRedisClient();

  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await waitForOpen(socket);
      socket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "qr_session_create",
            sessionId: "55555555-5555-4555-8555-555555555555",
          timestamp: Date.now(),
          sequence: 9,
          payload: {},
        }),
      );

      const readyMessage = await waitForMessage(socket);
      const storedSession = await relayServer.sessionRegistry.getSession(readyMessage.payload.sessionId);

      assert.equal(readyMessage.type, "qr_session_ready");
      assert.equal(storedSession.sessionId, readyMessage.payload.sessionId);
      assert.equal(storedSession.state, SESSION_STATES.WAITING);

      socket.close();
    },
  );
});

test("pair_approved_emitted", async () => {
  const redisClient = new FakeRedisClient();
  const token = "pair-approved-token";
  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const webSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const mobileSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await Promise.all([waitForOpen(webSocket), waitForOpen(mobileSocket)]);
      webSocket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "qr_session_create",
          sessionId: "66666666-6666-4666-8666-666666666666",
          timestamp: Date.now(),
          sequence: 1,
          payload: { token },
        }),
      );
      const readyMessage = await waitForMessage(webSocket);
      const webApprovalPromise = waitForMessage(webSocket);
      const mobileApprovalPromise = waitForMessage(mobileSocket);

      mobileSocket.send(
        JSON.stringify({
          protocolVersion: readyMessage.protocolVersion,
          type: "pair_request",
          sessionId: readyMessage.payload.sessionId,
          timestamp: Date.now(),
          sequence: readyMessage.sequence + 1,
          payload: {
            sessionId: readyMessage.payload.sessionId,
            token,
          },
        }),
      );

      const [webApprovalMessage, mobileApprovalMessage] = await Promise.all([
        webApprovalPromise,
        mobileApprovalPromise,
      ]);

      assert.equal(webApprovalMessage.protocolVersion, readyMessage.protocolVersion);
      assert.equal(webApprovalMessage.type, "pair_approved");
      assert.equal(webApprovalMessage.sessionId, readyMessage.payload.sessionId);
      assert.deepEqual(webApprovalMessage.payload, {
        sessionId: readyMessage.payload.sessionId,
        state: SESSION_STATES.ACTIVE,
      });
      assert.deepEqual(mobileApprovalMessage, webApprovalMessage);

      webSocket.close();
      mobileSocket.close();
    },
  );
});

test("routing_after_activation", async () => {
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "7c8f250c-c7ef-4d27-a302-f5d0f0dca101";
  const envelope = createEnvelope({
    sessionId,
    type: "event_stream",
    payload: { opaque: "post-activation-route" },
  });
  const persistGate = createDeferred();
  const persistenceOrder = [];
  const sessionRegistry = {
    async getSession(requestedSessionId) {
      assert.equal(requestedSessionId, sessionId);
      return {
        sessionId,
        webSocketId: "route-web",
        mobileSocketId: "route-mobile",
        state: SESSION_STATES.ACTIVE,
      };
    },
  };
  const sessionLifecycleManager = {
    async refreshSessionActivity(requestedSessionId, messageType) {
      persistenceOrder.push(`refresh:${requestedSessionId}:${messageType}`);
      return {
        sessionId: requestedSessionId,
        state: SESSION_STATES.ACTIVE,
      };
    },
    async persistBeforeRouting(operation) {
      persistenceOrder.push("persist:start");
      const result = await operation();
      await persistGate.promise;
      persistenceOrder.push("persist:done");
      return result;
    },
  };
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
  });

  connectionManager.registerConnection(mobileSocket, { connectionId: "route-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "route-web" });
  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const routePromise = messageRouter.routeMessage(envelope, mobileSocket);
  await new Promise((resolve) => setImmediate(resolve));

  assert.deepEqual(persistenceOrder, ["persist:start", `refresh:${sessionId}:event_stream`]);
  assert.equal(webSocket.sentMessages.length, 0);

  persistGate.resolve();
  const routed = await routePromise;

  assert.equal(routed, true);
  assert.deepEqual(persistenceOrder, [
    "persist:start",
    `refresh:${sessionId}:event_stream`,
    "persist:done",
  ]);
  assert.equal(webSocket.sentMessages.length, 1);
  assert.equal(webSocket.sentMessages[0], JSON.stringify(envelope));
});

test("redis_connect_success", async () => {
  await withMockedRedisConnect(async ({ calls }) => {
    const relayServer = await startRelayServer({
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      env: {
        REDIS_URL: "redis://127.0.0.1:6379",
      },
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
    });

    try {
      assert.deepEqual(calls, ["redis://127.0.0.1:6379"]);
    } finally {
      await relayServer.stop();
    }
  });
});

test("server_bootstrap_registry", async () => {
  await withMockedRedisConnect(async ({ sessionRegistry }) => {
    const relayServer = await startRelayServer({
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      env: {
        REDIS_URL: "redis://127.0.0.1:6379",
      },
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
    });

    try {
      assert.equal(relayServer.sessionRegistry, sessionRegistry);
      assert.equal(relayServer.config.sessionRegistry, sessionRegistry);
    } finally {
      await relayServer.stop();
    }
  });
});

test("relay_start_with_registry", async () => {
  const capturedLogs = [];
  const originalConsoleLog = console.log;

  console.log = (...args) => {
    capturedLogs.push(args.join(" "));
  };

  try {
    await withMockedRedisConnect(async () => {
      const relayServer = await startRelayServer({
        host: "127.0.0.1",
        port: 0,
        wsPath: "/relay",
        env: {
          REDIS_URL: "redis://127.0.0.1:6379",
        },
        pairing: {
          secret: "test-pairing-secret",
          ttlMs: DEFAULT_PAIRING_TTL_MS,
        },
      });

      try {
        assert.equal(relayServer.getConnectionCount(), 0);
        assert.equal(Boolean(relayServer.sessionRegistry), true);
      } finally {
        await relayServer.stop();
      }
    });
  } finally {
    console.log = originalConsoleLog;
  }

  assert.equal(capturedLogs.some((entry) => entry.includes("Relay starting")), true);
  assert.equal(capturedLogs.some((entry) => entry.includes("Redis connected")), true);
  assert.equal(capturedLogs.some((entry) => entry.includes("listening on ws://127.0.0.1:")), true);
});

test("server_start", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    assert.equal(typeof address.port, "number");
    assert.ok(address.port > 0);

    const response = await fetch(`http://127.0.0.1:${address.port}/health`);
    const body = await response.json();

    assert.equal(response.status, 200);
    assert.equal(body.status, "ok");
    assert.equal(body.wsPath, "/relay");
    assert.equal(body.transport, "ws");
    assert.equal(body.protocolVersion, DEFAULT_PROTOCOL_VERSION);
    assert.equal(body.maxPayloadBytes, DEFAULT_MAX_PAYLOAD_BYTES);
  });
});

test("websocket_upgrade", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "chano-relay-"));
  const keyPath = path.join(tempDir, "relay.key");
  const certPath = path.join(tempDir, "relay.crt");
  const cert = await selfsigned.generate([{ name: "commonName", value: "127.0.0.1" }], {
    algorithm: "rsa",
    days: 7,
    keySize: 2048,
  });

  fs.writeFileSync(keyPath, cert.private);
  fs.writeFileSync(certPath, cert.cert);

  try {
    await withStartedServer(
      {
        host: "127.0.0.1",
        port: 0,
        wsPath: "/relay",
        tls: {
          enabled: true,
          keyPath,
          certPath,
          minVersion: "TLSv1.2",
        },
      },
      async (relayServer) => {
        const address = relayServer.server.address();
        const socket = new WebSocket(`wss://127.0.0.1:${address.port}/relay`, {
          rejectUnauthorized: false,
        });

        await waitForOpen(socket);
        assert.equal(relayServer.getConnectionCount(), 1);
        socket.close();
      },
    );
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
});

test("connection_accept", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
    const expectedEnvelope = createEnvelope();
    const envelopePromise = new Promise((resolve) => {
      relayServer.events.once("transportEnvelope", resolve);
    });

    await waitForOpen(socket);
    socket.send(JSON.stringify(expectedEnvelope));

    const result = await envelopePromise;
    assert.deepEqual(result.envelope, expectedEnvelope);

    socket.close();
  });
});

test("invalid_protocol_version", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
    const envelope = createEnvelope({ protocolVersion: 1 });

    await waitForOpen(socket);
    socket.send(JSON.stringify(envelope));

    const response = await waitForMessage(socket);
    assert.equal(response.type, "transport_error");
    assert.equal(response.payload.reason, "unsupported_protocol_version");

    socket.close();
  });
});

test("missing_session_id", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
    const envelope = createEnvelope({ sessionId: "" });

    await waitForOpen(socket);
    socket.send(JSON.stringify(envelope));

    const response = await waitForMessage(socket);
    assert.equal(response.type, "transport_error");
    assert.equal(response.payload.reason, "missing_session_id");

    socket.close();
  });
});

test("missing_type", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
    const envelope = createEnvelope({ type: "" });

    await waitForOpen(socket);
    socket.send(JSON.stringify(envelope));

    const response = await waitForMessage(socket);
    assert.equal(response.type, "transport_error");
    assert.equal(response.payload.reason, "missing_type");

    socket.close();
  });
});

test("transport_payload_size_limit", async () => {
  await withStartedServer(
    { host: "127.0.0.1", port: 0, wsPath: "/relay", maxPayloadBytes: 32 },
    async (relayServer) => {
      const address = relayServer.server.address();
      const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const envelope = createEnvelope({ payload: { blob: "x".repeat(64) } });

      await waitForOpen(socket);
      socket.send(JSON.stringify(envelope));

      const response = await waitForMessage(socket);
      assert.equal(response.type, "transport_error");
      assert.equal(response.payload.reason, "payload_too_large");

      socket.close();
    },
  );
});

test("valid_envelope_pass", async () => {
  await withStartedServer(
    { host: "127.0.0.1", port: 0, wsPath: "/relay", maxPayloadBytes: 256 },
    async (relayServer) => {
      const address = relayServer.server.address();
      const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const expectedEnvelope = createEnvelope({ payload: { blob: "ok" } });
      const envelopePromise = new Promise((resolve) => {
        relayServer.events.once("transportEnvelope", resolve);
      });

      await waitForOpen(socket);
      socket.send(JSON.stringify(expectedEnvelope));

      const result = await envelopePromise;
      assert.deepEqual(result.envelope, expectedEnvelope);

      socket.close();
    },
  );
});

test("socket_register", () => {
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket);
  connectionManager.registerConnection(webSocket);

  assert.equal(connectionManager.getConnectionCount(), 2);
});

test("socket_remove", () => {
  const connectionManager = createConnectionManager();
  const socket = new MockSocket();

  connectionManager.registerConnection(socket);
  assert.equal(connectionManager.getConnectionCount(), 1);

  const removed = connectionManager.removeConnection(socket);
  assert.equal(removed, true);
  assert.equal(connectionManager.getConnectionCount(), 0);
});

test("session_binding", () => {
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket);
  connectionManager.registerConnection(webSocket);
  connectionManager.bindSessionSockets("session-42", mobileSocket, webSocket);

  const binding = connectionManager.lookupConnection("session-42");
  assert.equal(binding.mobileSocket, mobileSocket);
  assert.equal(binding.webSocket, webSocket);
});

test("disconnect_cleanup", async () => {
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket);
  connectionManager.registerConnection(webSocket);
  connectionManager.bindSessionSockets("session-cleanup", mobileSocket, webSocket);
  assert.equal(connectionManager.getConnectionCount(), 2);

  mobileSocket.emit("close");

  assert.equal(connectionManager.getConnectionCount(), 1);
  const binding = connectionManager.lookupConnection("session-cleanup");
  assert.equal(binding.mobileSocket, null);
  assert.equal(binding.webSocket, webSocket);
});

test("mobile_to_web_routing", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({ sessionId: "session-route-1", payload: { opaque: "mobile" } });

  connectionManager.bindSessionSockets("session-route-1", mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, true);
  assert.equal(webSocket.sentMessages.length, 1);
  assert.equal(webSocket.sentMessages[0], JSON.stringify(envelope));
  assert.equal(mobileSocket.sentMessages.length, 0);
});

test("relay_forward_envelope", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({
    type: "snapshot_start",
    sessionId: "relay-forward-session",
    payload: { opaque: "transport-only" },
  });

  connectionManager.bindSessionSockets("relay-forward-session", mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, true);
  assert.equal(webSocket.sentMessages.length, 1);
  assert.equal(webSocket.sentMessages[0], JSON.stringify(envelope));
});

test("snapshot_start_forward", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({
    type: "snapshot_start",
    sessionId: "snapshot-start-session",
    sequence: 10,
    payload: { opaque: "start" },
  });

  connectionManager.bindSessionSockets("snapshot-start-session", mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, true);
  assert.equal(webSocket.sentMessages[0], JSON.stringify(envelope));
  assert.equal(logs.includes("RELAY_ROUTE mobile→web type=snapshot_start"), true);
});

test("snapshot_chunk_forward", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "snapshot-chunk-session";
  const startEnvelope = createEnvelope({
    type: "snapshot_start",
    sessionId,
    sequence: 10,
    payload: { opaque: "start" },
  });
  const firstChunk = createEnvelope({
    type: "snapshot_chunk",
    sessionId,
    sequence: 11,
    payload: { opaque: "chunk-0" },
  });
  const secondChunk = createEnvelope({
    type: "snapshot_chunk",
    sessionId,
    sequence: 12,
    payload: { opaque: "chunk-1" },
  });

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await messageRouter.routeMessage(startEnvelope, mobileSocket);
  await messageRouter.routeMessage(firstChunk, mobileSocket);
  await messageRouter.routeMessage(secondChunk, mobileSocket);

  assert.equal(webSocket.sentMessages[1], JSON.stringify(firstChunk));
  assert.equal(webSocket.sentMessages[2], JSON.stringify(secondChunk));
  assert.equal(logs.includes("RELAY_ROUTE mobile→web type=snapshot_chunk index=0"), true);
  assert.equal(logs.includes("RELAY_ROUTE mobile→web type=snapshot_chunk index=1"), true);
});

test("snapshot_complete_forward", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "snapshot-complete-session";
  const startEnvelope = createEnvelope({
    type: "snapshot_start",
    sessionId,
    sequence: 10,
    payload: { opaque: "start" },
  });
  const completeEnvelope = createEnvelope({
    type: "snapshot_complete",
    sessionId,
    sequence: 13,
    payload: { opaque: "complete" },
  });

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await messageRouter.routeMessage(startEnvelope, mobileSocket);
  const routed = await messageRouter.routeMessage(completeEnvelope, mobileSocket);

  assert.equal(routed, true);
  assert.equal(webSocket.sentMessages[1], JSON.stringify(completeEnvelope));
  assert.equal(logs.includes("RELAY_ROUTE mobile→web type=snapshot_complete"), true);
});

test("relay_payload_opaque", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "relay-payload-opaque";
  const payload = { nested: { opaque: true } };
  const envelope = createEnvelope({
    type: "event_stream",
    sessionId,
    sequence: 11,
    payload: { eventVersion: 101, nested: payload.nested },
  });
  const eventAudit = messageRouter.enableEventAudit(sessionId);
  const originalPayloadReference = envelope.payload;
  const originalPayloadSnapshot = JSON.stringify(envelope.payload);

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, true);
  assert.equal(envelope.payload, originalPayloadReference);
  assert.equal(JSON.stringify(envelope.payload), originalPayloadSnapshot);
  assert.deepEqual(eventAudit.payloadReferenceEquality, [true]);
  assert.equal(logs.includes("PAYLOAD_REFERENCE_EQUALITY true"), true);
});

test("relay_event_forward_order", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "relay-event-forward-order";
  const eventAudit = messageRouter.enableEventAudit(sessionId);
  const envelopes = [
    createEnvelope({
      sessionId,
      sequence: 10,
      payload: { eventVersion: 101, opaque: "event-10" },
    }),
    createEnvelope({
      sessionId,
      sequence: 11,
      payload: { eventVersion: 102, opaque: "event-11" },
    }),
    createEnvelope({
      sessionId,
      sequence: 12,
      payload: { eventVersion: 103, opaque: "event-12" },
    }),
  ];

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  for (const envelope of envelopes) {
    await messageRouter.routeMessage(envelope, mobileSocket);
  }

  assert.deepEqual(
    eventAudit.inbound.map((entry) => [entry.sequence, entry.eventVersion]),
    [
      [10, 101],
      [11, 102],
      [12, 103],
    ],
  );
  assert.deepEqual(
    eventAudit.outbound.map((entry) => [entry.sequence, entry.eventVersion]),
    [
      [10, 101],
      [11, 102],
      [12, 103],
    ],
  );
  assert.deepEqual(
    webSocket.sentMessages.map((message) => JSON.parse(message).sequence),
    [10, 11, 12],
  );
  assert.equal(eventAudit.inbound.length, eventAudit.outbound.length);
  assert.equal(logs.includes("INBOUND seq=10 eventVersion=101"), true);
  assert.equal(logs.includes("OUTBOUND seq=10 eventVersion=101"), true);
  assert.equal(logs.includes("INBOUND seq=11 eventVersion=102"), true);
  assert.equal(logs.includes("OUTBOUND seq=11 eventVersion=102"), true);
});

test("ordering_preserved", async () => {
  const startedSequences = [];
  const releases = new Map();
  const sequences = [10, 11, 12, 13];
  const startSequence = 9;
  const completeSequence = 14;
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "ordering-preserved-session";
  const sessionRegistry = {
    async getSession(requestedSessionId) {
      assert.equal(requestedSessionId, sessionId);
      return {
        sessionId,
        state: SESSION_STATES.ACTIVE,
        webSocketId: "ordering-web",
        mobileSocketId: "ordering-mobile",
      };
    },
  };
  const sessionLifecycleManager = {
    async refreshSessionActivity(requestedSessionId, messageType) {
      assert.equal(requestedSessionId, sessionId);
      if (messageType !== "snapshot_chunk") {
        return { sessionId: requestedSessionId, state: SESSION_STATES.ACTIVE };
      }

      const sequence = sequences[startedSequences.length];
      startedSequences.push(sequence);
      await releases.get(sequence).promise;
      return { sessionId: requestedSessionId, state: SESSION_STATES.ACTIVE };
    },
    async persistBeforeRouting(operation) {
      return operation();
    },
  };
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
  });

  connectionManager.registerConnection(mobileSocket, { connectionId: "ordering-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "ordering-web" });
  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const routePromises = [
    messageRouter.routeMessage(
      createEnvelope({
        type: "snapshot_start",
        sessionId,
        sequence: startSequence,
        payload: { opaque: "snapshot-start" },
      }),
      mobileSocket,
    ),
  ];

  routePromises.push(
    ...sequences.map((sequence) => {
      releases.set(sequence, createDeferred());
      return messageRouter.routeMessage(
        createEnvelope({
          type: "snapshot_chunk",
          sessionId,
          sequence,
          payload: { opaque: `chunk-${sequence}` },
        }),
        mobileSocket,
      );
    }),
  );

  routePromises.push(
    messageRouter.routeMessage(
      createEnvelope({
        type: "snapshot_complete",
        sessionId,
        sequence: completeSequence,
        payload: { opaque: "snapshot-complete" },
      }),
      mobileSocket,
    ),
  );

  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(startedSequences, [sequences[0]]);
  assert.deepEqual(
    webSocket.sentMessages.map((message) => JSON.parse(message).sequence),
    [startSequence],
  );

  for (const sequence of sequences) {
    releases.get(sequence).resolve();
    await new Promise((resolve) => setImmediate(resolve));
  }

  await Promise.all(routePromises);

  assert.deepEqual(startedSequences, sequences);
  assert.deepEqual(
    webSocket.sentMessages.map((message) => JSON.parse(message).sequence),
    [startSequence, ...sequences, completeSequence],
  );
});

test("relay_session_ordering", async () => {
  const startedSequences = [];
  const releases = new Map();
  const sendOrder = [13, 10, 12, 11];
  const expectedOrder = [...sendOrder];
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "relay-session-ordering";
  const sessionRegistry = {
    async getSession(requestedSessionId) {
      assert.equal(requestedSessionId, sessionId);
      return {
        sessionId,
        state: SESSION_STATES.ACTIVE,
        webSocketId: "relay-session-ordering-web",
        mobileSocketId: "relay-session-ordering-mobile",
      };
    },
  };
  const sessionLifecycleManager = {
    async refreshSessionActivity(requestedSessionId, messageType) {
      assert.equal(requestedSessionId, sessionId);
      assert.equal(messageType, "event_stream");
      const sequence = sendOrder[startedSequences.length];
      startedSequences.push(sequence);
      await releases.get(sequence).promise;
      return { sessionId: requestedSessionId, state: SESSION_STATES.ACTIVE };
    },
    async persistBeforeRouting(operation) {
      return operation();
    },
  };
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
  });

  connectionManager.registerConnection(mobileSocket, { connectionId: "relay-session-ordering-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "relay-session-ordering-web" });
  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const routePromises = sendOrder.map((sequence) => {
    releases.set(sequence, createDeferred());
    return messageRouter.routeMessage(
      createEnvelope({
        type: "event_stream",
        sessionId,
        sequence,
        payload: { opaque: `event-${sequence}` },
      }),
      mobileSocket,
    );
  });

  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(startedSequences, [sendOrder[0]]);

  for (const sequence of expectedOrder) {
    releases.get(sequence).resolve();
    await new Promise((resolve) => setImmediate(resolve));
  }

  await Promise.all(routePromises);

  assert.deepEqual(startedSequences, expectedOrder);
  assert.deepEqual(
    webSocket.sentMessages.map((message) => JSON.parse(message).sequence),
    expectedOrder,
  );
});

test("relay_no_reorder_async", async () => {
  const logs = [];
  const startedSequences = [];
  const releases = new Map();
  const sendOrder = [12, 10, 11];
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "relay-no-reorder-async";
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
    sessionRegistry: {
      async getSession(requestedSessionId) {
        assert.equal(requestedSessionId, sessionId);
        return {
          sessionId,
          state: SESSION_STATES.ACTIVE,
          webSocketId: "relay-no-reorder-async-web",
          mobileSocketId: "relay-no-reorder-async-mobile",
        };
      },
    },
    sessionLifecycleManager: {
      async refreshSessionActivity(requestedSessionId, messageType) {
        assert.equal(requestedSessionId, sessionId);
        assert.equal(messageType, "event_stream");
        const sequence = sendOrder[startedSequences.length];
        startedSequences.push(sequence);
        await releases.get(sequence).promise;
        return { sessionId: requestedSessionId, state: SESSION_STATES.ACTIVE };
      },
      async persistBeforeRouting(operation) {
        return operation();
      },
    },
  });
  const eventAudit = messageRouter.enableEventAudit(sessionId);

  connectionManager.registerConnection(mobileSocket, { connectionId: "relay-no-reorder-async-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "relay-no-reorder-async-web" });
  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const routePromises = sendOrder.map((sequence, index) => {
    releases.set(sequence, createDeferred());
    return messageRouter.routeMessage(
      createEnvelope({
        sessionId,
        sequence,
        timestamp: Date.now() + index,
        payload: { eventVersion: 200 + sequence, opaque: `event-${sequence}` },
      }),
      mobileSocket,
    );
  });

  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(startedSequences, [12]);

  for (const sequence of sendOrder) {
    releases.get(sequence).resolve();
    await new Promise((resolve) => setImmediate(resolve));
  }

  await Promise.all(routePromises);

  assert.deepEqual(startedSequences, sendOrder);
  assert.deepEqual(
    eventAudit.outbound.map((entry) => entry.sequence),
    sendOrder,
  );
  assert.deepEqual(
    webSocket.sentMessages.map((message) => JSON.parse(message).sequence),
    sendOrder,
  );
  assert.equal(logs.includes("INBOUND seq=12 eventVersion=212"), true);
  assert.equal(logs.includes("OUTBOUND seq=11 eventVersion=211"), true);
});

test("relay_snapshot_isolation_transparency", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "abc";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const sentEnvelopes = await sendMixedMessages({
    messageRouter,
    senderSocket: mobileSocket,
    sessionId,
  });
  const forwardedMessages = captureForwardedMessages(webSocket);
  const expectedForwardOrder = [sentEnvelopes[0], sentEnvelopes[1], sentEnvelopes[3], sentEnvelopes[4], sentEnvelopes[2]];

  assert.deepEqual(forwardedMessages, expectedForwardOrder);
  validateRelayIsolation(forwardedMessages);
  assert.deepEqual(
    logs.filter((entry) => entry.startsWith("RELAY_QUEUE_PROCESS session=abc")),
    [
      "RELAY_QUEUE_PROCESS session=abc type=snapshot_start sequence=1",
      "RELAY_QUEUE_PROCESS session=abc type=snapshot_chunk sequence=2 index=0",
      "RELAY_QUEUE_PROCESS session=abc type=snapshot_chunk sequence=3 index=1",
      "RELAY_QUEUE_PROCESS session=abc type=snapshot_complete sequence=4",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=101",
    ],
  );
  assert.equal(logs.includes("snapshot_lock_acquired session=abc type=snapshot_start sequence=1"), true);
  assert.equal(logs.includes("snapshot_lock_released session=abc type=snapshot_complete sequence=4"), true);
  assert.equal(
    logs.some((entry) => entry === "message_deferred_due_to_snapshot session=abc type=event_stream sequence=101"),
    true,
  );
});

test("relay_no_interleaving_fix", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "abc";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await sendMixedMessages({
    messageRouter,
    senderSocket: mobileSocket,
    sessionId,
  });

  const forwardedMessages = captureForwardedMessages(webSocket);
  const queueLogs = logs.filter((entry) => entry.startsWith("RELAY_QUEUE_PROCESS session=abc"));

  validateRelayIsolation(forwardedMessages);
  assert.deepEqual(
    queueLogs,
    [
      "RELAY_QUEUE_PROCESS session=abc type=snapshot_start sequence=1",
      "RELAY_QUEUE_PROCESS session=abc type=snapshot_chunk sequence=2 index=0",
      "RELAY_QUEUE_PROCESS session=abc type=snapshot_chunk sequence=3 index=1",
      "RELAY_QUEUE_PROCESS session=abc type=snapshot_complete sequence=4",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=101",
    ],
  );
  assert.equal(logs.includes("snapshot_message_processed session=abc type=snapshot_start sequence=1"), true);
  assert.equal(logs.includes("snapshot_message_processed session=abc type=snapshot_chunk sequence=2 index=0"), true);
  assert.equal(logs.includes("snapshot_message_processed session=abc type=snapshot_complete sequence=4"), true);
});

test("relay_snapshot_event_isolation", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "relay-snapshot-event-isolation";
  const eventAudit = messageRouter.enableEventAudit(sessionId);

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await Promise.all([
    messageRouter.routeMessage(createEnvelope({ type: "snapshot_start", sessionId, sequence: 10 }), mobileSocket),
    messageRouter.routeMessage(
      createEnvelope({
        type: "event_stream",
        sessionId,
        sequence: 13,
        payload: { eventVersion: 103, opaque: "blocked-during-snapshot" },
      }),
      mobileSocket,
    ),
    messageRouter.routeMessage(createEnvelope({ type: "snapshot_chunk", sessionId, sequence: 11 }), mobileSocket),
    messageRouter.routeMessage(createEnvelope({ type: "snapshot_complete", sessionId, sequence: 12 }), mobileSocket),
  ]);

  assert.deepEqual(
    webSocket.sentMessages.map((message) => JSON.parse(message).type),
    ["snapshot_start", "snapshot_chunk", "snapshot_complete", "event_stream"],
  );
  assert.deepEqual(eventAudit.inbound.map((entry) => entry.sequence), [13]);
  assert.deepEqual(eventAudit.outbound.map((entry) => entry.sequence), [13]);
  assert.deepEqual(
    logs.filter((entry) => entry.startsWith(`RELAY_QUEUE_PROCESS session=${sessionId}`)),
    [
      `RELAY_QUEUE_PROCESS session=${sessionId} type=snapshot_start sequence=10`,
      `RELAY_QUEUE_PROCESS session=${sessionId} type=snapshot_chunk sequence=11 index=0`,
      `RELAY_QUEUE_PROCESS session=${sessionId} type=snapshot_complete sequence=12`,
      `RELAY_QUEUE_PROCESS session=${sessionId} type=event_stream sequence=13`,
    ],
  );
  assert.equal(
    logs.some((entry) => entry === `message_deferred_due_to_snapshot session=${sessionId} type=event_stream sequence=13`),
    true,
  );
  assert.equal(logs.includes("INBOUND seq=13 eventVersion=103"), true);
  assert.equal(logs.includes("OUTBOUND seq=13 eventVersion=103"), true);
});

test("relay_fifo_ordering", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "abc";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await sendOrderedMessages({
    messageRouter,
    senderSocket: mobileSocket,
    sessionId,
    sequences: [101, 102, 103],
    eventVersions: [101, 102, 103],
  });

  const forwardedMessages = captureForwardedMessages(webSocket);

  validateRelayOrdering(forwardedMessages, [101, 102, 103]);
  assert.deepEqual(
    forwardedMessages.map((message) => `${message.type} ${message.payload.eventVersion}`),
    ["event_stream 101", "event_stream 102", "event_stream 103"],
  );
  assert.deepEqual(
    logs.filter((entry) => entry.startsWith("RELAY_QUEUE_PROCESS session=abc")),
    [
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=101",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=102",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=103",
    ],
  );
});

test("relay_async_order_preservation", async () => {
  const logs = [];
  const startedSequences = [];
  const releases = new Map();
  const sendOrder = [101, 102, 103];
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "abc";
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
    sessionRegistry: {
      async getSession(requestedSessionId) {
        assert.equal(requestedSessionId, sessionId);
        return {
          sessionId,
          state: SESSION_STATES.ACTIVE,
          webSocketId: "relay-async-order-web",
          mobileSocketId: "relay-async-order-mobile",
        };
      },
    },
    sessionLifecycleManager: {
      async refreshSessionActivity(requestedSessionId, messageType) {
        assert.equal(requestedSessionId, sessionId);
        assert.equal(messageType, "event_stream");
        const sequence = sendOrder[startedSequences.length];
        startedSequences.push(sequence);
        await releases.get(sequence).promise;
        return { sessionId: requestedSessionId, state: SESSION_STATES.ACTIVE };
      },
      async persistBeforeRouting(operation) {
        return operation();
      },
    },
  });

  connectionManager.registerConnection(mobileSocket, { connectionId: "relay-async-order-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "relay-async-order-web" });
  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  for (const sequence of sendOrder) {
    releases.set(sequence, createDeferred());
  }

  const routePromise = sendOrderedMessages({
    messageRouter,
    senderSocket: mobileSocket,
    sessionId,
    sequences: sendOrder,
    eventVersions: sendOrder,
  });

  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(startedSequences, [101]);

  for (const sequence of sendOrder) {
    releases.get(sequence).resolve();
    await new Promise((resolve) => setImmediate(resolve));
  }

  await routePromise;

  const forwardedMessages = captureForwardedMessages(webSocket);

  assert.deepEqual(startedSequences, sendOrder);
  validateRelayOrdering(forwardedMessages, sendOrder);
  assert.deepEqual(
    logs.filter((entry) => entry.startsWith("RELAY_QUEUE_PROCESS session=abc")),
    [
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=101",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=102",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=103",
    ],
  );
});

test("relay_duplicate_forwarding", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "abc";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const sentEnvelopes = await sendDuplicateMessages({
    messageRouter,
    senderSocket: mobileSocket,
    sessionId,
  });
  const forwardedMessages = captureForwardedMessages(webSocket);

  validateRelayDuplicateBehavior(forwardedMessages);
  assert.deepEqual(forwardedMessages, sentEnvelopes);
  assert.deepEqual(
    logs.filter((entry) => entry.startsWith("RELAY_QUEUE_PROCESS session=abc")),
    [
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=101",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=102",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=102",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=103",
    ],
  );
});

test("relay_no_deduplication", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "duplicate-no-dedup";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await sendDuplicateMessages({
    messageRouter,
    senderSocket: mobileSocket,
    sessionId,
  });

  const forwardedMessages = captureForwardedMessages(webSocket);
  const duplicateMessages = forwardedMessages.filter((message) => message.sequence === 102);

  validateRelayDuplicateBehavior(forwardedMessages);
  assert.equal(forwardedMessages.length, 4);
  assert.equal(duplicateMessages.length, 2);
  assert.deepEqual(duplicateMessages[0], duplicateMessages[1]);
});

test("relay_preserve_duplicate_order", async () => {
  const logs = [];
  const startedSequences = [];
  const releases = [];
  const sendOrder = [101, 102, 102, 103];
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "abc";
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
    sessionRegistry: {
      async getSession(requestedSessionId) {
        assert.equal(requestedSessionId, sessionId);
        return {
          sessionId,
          state: SESSION_STATES.ACTIVE,
          webSocketId: "relay-duplicate-order-web",
          mobileSocketId: "relay-duplicate-order-mobile",
        };
      },
    },
    sessionLifecycleManager: {
      async refreshSessionActivity(requestedSessionId, messageType) {
        assert.equal(requestedSessionId, sessionId);
        assert.equal(messageType, "event_stream");
        const releaseIndex = startedSequences.length;
        const sequence = sendOrder[releaseIndex];
        startedSequences.push(sequence);
        await releases[releaseIndex].promise;
        return { sessionId: requestedSessionId, state: SESSION_STATES.ACTIVE };
      },
      async persistBeforeRouting(operation) {
        return operation();
      },
    },
  });

  connectionManager.registerConnection(mobileSocket, { connectionId: "relay-duplicate-order-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "relay-duplicate-order-web" });
  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  sendOrder.forEach(() => {
    releases.push(createDeferred());
  });

  const sentEnvelopes = [
    createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 101,
      timestamp: 1_700_000_000_101,
      payload: { opaque: { marker: "event-101" } },
    }),
    createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 102,
      timestamp: 1_700_000_000_102,
      payload: { opaque: { marker: "event-102-a" } },
    }),
    createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 102,
      timestamp: 1_700_000_000_103,
      payload: { opaque: { marker: "event-102-b" } },
    }),
    createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 103,
      timestamp: 1_700_000_000_104,
      payload: { opaque: { marker: "event-103" } },
    }),
  ];

  const routePromise = Promise.all(
    sentEnvelopes.map((envelope) => messageRouter.routeMessage(envelope, mobileSocket)),
  );

  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(startedSequences, [101]);

  releases[0].resolve();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(startedSequences, [101, 102]);

  releases[1].resolve();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(startedSequences, [101, 102, 102]);

  releases[2].resolve();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(startedSequences, [101, 102, 102, 103]);

  releases[3].resolve();
  await routePromise;

  const forwardedMessages = captureForwardedMessages(webSocket);

  assert.deepEqual(
    forwardedMessages.map((message) => `${message.type} ${message.sequence}`),
    ["event_stream 101", "event_stream 102", "event_stream 102", "event_stream 103"],
  );
  assert.deepEqual(
    forwardedMessages.map((message) => message.payload),
    [
      { opaque: { marker: "event-101" } },
      { opaque: { marker: "event-102-a" } },
      { opaque: { marker: "event-102-b" } },
      { opaque: { marker: "event-103" } },
    ],
  );
  assert.deepEqual(
    logs.filter((entry) => entry.startsWith("RELAY_QUEUE_PROCESS session=abc")),
    [
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=101",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=102",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=102",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=103",
    ],
  );
});

test("relay_gap_transparency", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "abc";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const sentEnvelopes = await sendOutOfOrderEvents({
    messageRouter,
    senderSocket: mobileSocket,
    sessionId,
  });
  const forwardedMessages = captureForwardedMessages(webSocket);

  validateRelayBehavior(forwardedMessages);
  assert.deepEqual(forwardedMessages, sentEnvelopes);
  assert.deepEqual(
    logs.filter((entry) => entry.startsWith("RELAY_QUEUE_PROCESS session=abc")),
    [
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=101",
      "RELAY_QUEUE_PROCESS session=abc type=event_stream sequence=103",
    ],
  );
});

test("relay_snapshot_forwarding", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "snapshot-gap-fallback";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const sentEnvelopes = await sendSnapshotFallbackFlow({
    messageRouter,
    senderSocket: mobileSocket,
    sessionId,
  });
  const forwardedMessages = captureForwardedMessages(webSocket);

  assert.deepEqual(forwardedMessages, sentEnvelopes);
  assert.deepEqual(
    forwardedMessages.map((message) => message.type),
    ["snapshot_start", "snapshot_chunk", "snapshot_complete"],
  );
  assert.deepEqual(
    forwardedMessages.map((message) =>
      message.type === "snapshot_start"
        ? "SNAPSHOT_START"
        : message.type === "snapshot_chunk"
          ? "SNAPSHOT_CHUNK"
          : "SNAPSHOT_COMPLETE",
    ),
    ["SNAPSHOT_START", "SNAPSHOT_CHUNK", "SNAPSHOT_COMPLETE"],
  );
  assert.equal(logs.includes("RELAY_ROUTE mobile→web type=snapshot_start"), true);
  assert.equal(logs.includes("RELAY_ROUTE mobile→web type=snapshot_chunk index=0"), true);
  assert.equal(logs.includes("RELAY_ROUTE mobile→web type=snapshot_complete"), true);
});

test("relay_no_recovery_logic", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "gap-no-recovery";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await sendOutOfOrderEvents({
    messageRouter,
    senderSocket: mobileSocket,
    sessionId,
  });

  const forwardedMessages = captureForwardedMessages(webSocket);

  validateRelayBehavior(forwardedMessages);
  assert.deepEqual(
    forwardedMessages.map((message) => message.type),
    ["event_stream", "event_stream"],
  );
  assert.equal(
    logs.some((entry) => entry.includes("snapshot_start") || entry.includes("snapshot_chunk") || entry.includes("snapshot_complete")),
    false,
  );
  assert.equal(
    forwardedMessages.some((message) => message.type.startsWith("snapshot")),
    false,
  );
});

test("snapshot_stream_contiguity", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "snapshot-ordering";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await Promise.all([
    messageRouter.routeMessage(createEnvelope({ type: "snapshot_start", sessionId, sequence: 1 }), mobileSocket),
    messageRouter.routeMessage(createEnvelope({ type: "snapshot_chunk", sessionId, sequence: 2, payload: { opaque: "chunk-1" } }), mobileSocket),
    messageRouter.routeMessage(createEnvelope({ type: "event_stream", sessionId, sequence: 5, payload: { eventVersion: 101, opaque: "blocked" } }), mobileSocket),
    messageRouter.routeMessage(createEnvelope({ type: "snapshot_chunk", sessionId, sequence: 3, payload: { opaque: "chunk-2" } }), mobileSocket),
    messageRouter.routeMessage(createEnvelope({ type: "snapshot_complete", sessionId, sequence: 4 }), mobileSocket),
  ]);

  const forwardedMessages = captureForwardedMessages(webSocket);

  assert.deepEqual(
    forwardedMessages.map((message) => message.type),
    ["snapshot_start", "snapshot_chunk", "snapshot_chunk", "snapshot_complete", "event_stream"],
  );
  assert.deepEqual(
    forwardedMessages.map((message) =>
      message.type === "event_stream"
        ? `EVENT_STREAM ${message.payload.eventVersion}`
        : message.type === "snapshot_start"
          ? "SNAPSHOT_START"
          : message.type === "snapshot_complete"
            ? "SNAPSHOT_COMPLETE"
            : `SNAPSHOT_CHUNK ${message.sequence - 1}`,
    ),
      ["SNAPSHOT_START", "SNAPSHOT_CHUNK 1", "SNAPSHOT_CHUNK 2", "SNAPSHOT_COMPLETE", "EVENT_STREAM 101"],
  );
});

test("event_block_during_snapshot", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "snapshot-blocked-event";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await Promise.all([
    messageRouter.routeMessage(createEnvelope({ type: "snapshot_start", sessionId, sequence: 1 }), mobileSocket),
    messageRouter.routeMessage(createEnvelope({ type: "snapshot_chunk", sessionId, sequence: 2, payload: { opaque: "chunk-1" } }), mobileSocket),
    messageRouter.routeMessage(createEnvelope({ type: "event_stream", sessionId, sequence: 5, payload: { eventVersion: 101, opaque: "blocked" } }), mobileSocket),
    messageRouter.routeMessage(createEnvelope({ type: "snapshot_chunk", sessionId, sequence: 3, payload: { opaque: "chunk-2" } }), mobileSocket),
    messageRouter.routeMessage(createEnvelope({ type: "snapshot_complete", sessionId, sequence: 4 }), mobileSocket),
  ]);

  const queueLogs = logs.filter((entry) => entry.startsWith("RELAY_QUEUE_PROCESS session=snapshot-blocked-event"));

  assert.deepEqual(queueLogs, [
    "RELAY_QUEUE_PROCESS session=snapshot-blocked-event type=snapshot_start sequence=1",
    "RELAY_QUEUE_PROCESS session=snapshot-blocked-event type=snapshot_chunk sequence=2 index=0",
    "RELAY_QUEUE_PROCESS session=snapshot-blocked-event type=snapshot_chunk sequence=3 index=1",
    "RELAY_QUEUE_PROCESS session=snapshot-blocked-event type=snapshot_complete sequence=4",
    "RELAY_QUEUE_PROCESS session=snapshot-blocked-event type=event_stream sequence=5",
  ]);
  assert.equal(
    logs.some(
      (entry) => entry === "message_deferred_due_to_snapshot session=snapshot-blocked-event type=event_stream sequence=5",
    ),
    true,
  );
  assert.deepEqual(
    captureForwardedMessages(webSocket).map((message) => message.type),
    ["snapshot_start", "snapshot_chunk", "snapshot_chunk", "snapshot_complete", "event_stream"],
  );
});

test("snapshot_chunk_without_start_rejected", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "snapshot-chunk-without-start";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await assert.rejects(
    () =>
      messageRouter.routeMessage(
        createEnvelope({ type: "snapshot_chunk", sessionId, sequence: 1, payload: { opaque: "chunk" } }),
        mobileSocket,
      ),
    /snapshot_chunk_without_start/,
  );

  assert.deepEqual(webSocket.sentMessages, []);
  assert.equal(
    logs.includes(
      "SNAPSHOT_PROTOCOL_ERROR session=snapshot-chunk-without-start type=snapshot_chunk sequence=1 reason=snapshot_chunk_without_start",
    ),
    true,
  );
});

test("nested_snapshot_start_rejected", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "nested-snapshot-start";

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  await messageRouter.routeMessage(createEnvelope({ type: "snapshot_start", sessionId, sequence: 1 }), mobileSocket);

  await assert.rejects(
    () => messageRouter.routeMessage(createEnvelope({ type: "snapshot_start", sessionId, sequence: 2 }), mobileSocket),
    /nested_snapshot_start/,
  );

  assert.deepEqual(captureForwardedMessages(webSocket).map((message) => message.type), ["snapshot_start"]);
  assert.equal(
    logs.includes(
      "SNAPSHOT_PROTOCOL_ERROR session=nested-snapshot-start type=snapshot_start sequence=2 reason=nested_snapshot_start",
    ),
    true,
  );
});

test("queue_process_sequential", async () => {
  const startedSequences = [];
  const activeSequences = [];
  const releases = new Map();
  const sendOrder = [3, 1, 2];
  const expectedOrder = [...sendOrder];
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "queue-process-sequential";
  const sessionRegistry = {
    async getSession(requestedSessionId) {
      assert.equal(requestedSessionId, sessionId);
      return {
        sessionId,
        state: SESSION_STATES.ACTIVE,
        webSocketId: "queue-process-sequential-web",
        mobileSocketId: "queue-process-sequential-mobile",
      };
    },
  };
  const sessionLifecycleManager = {
    async refreshSessionActivity(requestedSessionId, messageType) {
      assert.equal(requestedSessionId, sessionId);
      assert.equal(messageType, "event_stream");
      const sequence = sendOrder[startedSequences.length];
      startedSequences.push(sequence);
      activeSequences.push(sequence);
      assert.equal(activeSequences.length, 1);
      await releases.get(sequence).promise;
      activeSequences.pop();
      return { sessionId: requestedSessionId, state: SESSION_STATES.ACTIVE };
    },
    async persistBeforeRouting(operation) {
      return operation();
    },
  };
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
  });

  connectionManager.registerConnection(mobileSocket, { connectionId: "queue-process-sequential-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "queue-process-sequential-web" });
  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const routePromises = sendOrder.map((sequence) => {
    releases.set(sequence, createDeferred());
    return messageRouter.routeMessage(
      createEnvelope({
        type: "event_stream",
        sessionId,
        sequence,
        payload: { opaque: `event-${sequence}` },
      }),
      mobileSocket,
    );
  });

  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(startedSequences, [sendOrder[0]]);

  for (const sequence of expectedOrder) {
    releases.get(sequence).resolve();
    await new Promise((resolve) => setImmediate(resolve));
  }

  await Promise.all(routePromises);

  assert.deepEqual(startedSequences, expectedOrder);
  assert.deepEqual(
    webSocket.sentMessages.map((message) => JSON.parse(message).sequence),
    expectedOrder,
  );
});

test("web_to_mobile_routing", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({ sessionId: "session-route-2", payload: { opaque: "web" } });

  connectionManager.bindSessionSockets("session-route-2", mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, webSocket);

  assert.equal(routed, true);
  assert.equal(mobileSocket.sentMessages.length, 1);
  assert.equal(mobileSocket.sentMessages[0], JSON.stringify(envelope));
  assert.equal(webSocket.sentMessages.length, 0);
});

test("missing_destination_drop", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({ sessionId: "session-route-3" });

  connectionManager.bindSessionSockets("session-route-3", mobileSocket, webSocket);
  webSocket.close();

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, false);
  assert.equal(webSocket.sentMessages.length, 0);
});

test("envelope_integrity_preserved", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({
    sessionId: "session-route-4",
    timestamp: 1234567890,
    sequence: 77,
    payload: { nested: { opaque: true } },
  });
  const originalSnapshot = JSON.stringify(envelope);

  connectionManager.bindSessionSockets("session-route-4", mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, true);
  assert.equal(webSocket.sentMessages[0], originalSnapshot);
  assert.equal(JSON.stringify(envelope), originalSnapshot);
});

test("relay_preserve_envelope_fields", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({
    type: "command_result",
    sessionId: "relay-preserve-session",
    timestamp: 1773577601000,
    sequence: 29,
    payload: { opaque: { unchanged: true } },
  });

  connectionManager.bindSessionSockets("relay-preserve-session", mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);
  const forwardedEnvelope = JSON.parse(webSocket.sentMessages[0]);

  assert.equal(routed, true);
  assert.deepEqual(forwardedEnvelope, envelope);
  assert.equal(forwardedEnvelope.timestamp, envelope.timestamp);
  assert.equal(forwardedEnvelope.sequence, envelope.sequence);
});

test("relay_mutation_forward", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "relay-mutation-forward";
  const mutationAudit = messageRouter.enableMutationAudit(sessionId);
  const envelope = createEnvelope({
    type: "mutation_command",
    sessionId,
    sequence: 20,
    payload: {
      commandId: "cmd-101",
      patch: [{ op: "replace", path: "/profile/name", value: "Taboo" }],
    },
  });
  const originalPayloadReference = envelope.payload;

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, webSocket);

  assert.equal(routed, true);
  assert.equal(envelope.payload, originalPayloadReference);
  assert.equal(mobileSocket.sentMessages.length, 1);
  assert.deepEqual(JSON.parse(mobileSocket.sentMessages[0]), envelope);
  assert.deepEqual(
    mutationAudit.inbound.map((entry) => ({ sequence: entry.sequence, type: entry.type, commandId: entry.commandId })),
    [{ sequence: 20, type: "mutation_command", commandId: "cmd-101" }],
  );
  assert.deepEqual(
    mutationAudit.outbound.map((entry) => ({ sequence: entry.sequence, type: entry.type, commandId: entry.commandId })),
    [{ sequence: 20, type: "mutation_command", commandId: "cmd-101" }],
  );
  assert.deepEqual(mutationAudit.payloadReferenceEquality, [true]);
  assert.equal(logs.includes("INBOUND seq=20 type=mutation_command cmd=cmd-101"), true);
  assert.equal(logs.includes("OUTBOUND seq=20 type=mutation_command cmd=cmd-101"), true);
});

test("relay_command_result_routing", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocketOne = new MockSocket();
  const webSocketOne = new MockSocket();
  const mobileSocketTwo = new MockSocket();
  const webSocketTwo = new MockSocket();
  const sessionIdOne = "relay-command-result-routing-1";
  const sessionIdTwo = "relay-command-result-routing-2";
  const mutationAudit = messageRouter.enableMutationAudit(sessionIdOne);
  const envelope = createEnvelope({
    type: "command_result",
    sessionId: sessionIdOne,
    sequence: 21,
    payload: {
      commandId: "cmd-101",
      status: "ok",
      result: { applied: true },
    },
  });

  connectionManager.bindSessionSockets(sessionIdOne, mobileSocketOne, webSocketOne);
  connectionManager.bindSessionSockets(sessionIdTwo, mobileSocketTwo, webSocketTwo);

  const routed = await messageRouter.routeMessage(envelope, mobileSocketOne);

  assert.equal(routed, true);
  assert.equal(webSocketOne.sentMessages.length, 1);
  assert.equal(webSocketTwo.sentMessages.length, 0);
  assert.equal(mobileSocketOne.sentMessages.length, 0);
  assert.deepEqual(JSON.parse(webSocketOne.sentMessages[0]), envelope);
  assert.deepEqual(
    mutationAudit.outbound.map((entry) => ({ sequence: entry.sequence, type: entry.type, commandId: entry.commandId })),
    [{ sequence: 21, type: "command_result", commandId: "cmd-101" }],
  );
  assert.equal(logs.includes("INBOUND seq=21 type=command_result cmd=cmd-101"), true);
  assert.equal(logs.includes("OUTBOUND seq=21 type=command_result cmd=cmd-101"), true);
});

test("relay_mutation_ordering", async () => {
  const logs = [];
  const startedSequences = [];
  const releases = new Map();
  const sendOrder = [20, 21, 22];
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "relay-mutation-ordering";
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
    sessionRegistry: {
      async getSession(requestedSessionId) {
        assert.equal(requestedSessionId, sessionId);
        return {
          sessionId,
          state: SESSION_STATES.ACTIVE,
          webSocketId: "relay-mutation-ordering-web",
          mobileSocketId: "relay-mutation-ordering-mobile",
        };
      },
    },
    sessionLifecycleManager: {
      async refreshSessionActivity(requestedSessionId, messageType) {
        assert.equal(requestedSessionId, sessionId);
        assert.equal(messageType, "mutation_command");
        const sequence = sendOrder[startedSequences.length];
        startedSequences.push(sequence);
        await releases.get(sequence).promise;
        return { sessionId: requestedSessionId, state: SESSION_STATES.ACTIVE };
      },
      async persistBeforeRouting(operation) {
        return operation();
      },
    },
  });
  const mutationAudit = messageRouter.enableMutationAudit(sessionId);

  connectionManager.registerConnection(mobileSocket, { connectionId: "relay-mutation-ordering-mobile" });
  connectionManager.registerConnection(webSocket, { connectionId: "relay-mutation-ordering-web" });
  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const routePromises = sendOrder.map((sequence, index) => {
    releases.set(sequence, createDeferred());
    return messageRouter.routeMessage(
      createEnvelope({
        type: "mutation_command",
        sessionId,
        sequence,
        timestamp: Date.now() + index,
        payload: {
          commandId: `cmd-${100 + sequence}`,
          op: `mutation-${sequence}`,
        },
      }),
      webSocket,
    );
  });

  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(startedSequences, [20]);

  for (const sequence of sendOrder) {
    releases.get(sequence).resolve();
    await new Promise((resolve) => setImmediate(resolve));
  }

  await Promise.all(routePromises);

  assert.deepEqual(startedSequences, sendOrder);
  assert.deepEqual(
    mutationAudit.outbound.map((entry) => entry.sequence),
    sendOrder,
  );
  assert.deepEqual(
    mobileSocket.sentMessages.map((message) => JSON.parse(message).sequence),
    sendOrder,
  );
  assert.equal(logs.includes("RELAY_QUEUE_PROCESS session=relay-mutation-ordering type=mutation_command sequence=20"), true);
  assert.equal(logs.includes("RELAY_QUEUE_PROCESS session=relay-mutation-ordering type=mutation_command sequence=21"), true);
  assert.equal(logs.includes("RELAY_QUEUE_PROCESS session=relay-mutation-ordering type=mutation_command sequence=22"), true);
});

test("relay_payload_opaque_mutation", async () => {
  const logs = [];
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager, {
    logger: (entry) => logs.push(entry),
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const sessionId = "relay-payload-opaque-mutation";
  const mutationAudit = messageRouter.enableMutationAudit(sessionId);
  const payload = {
    commandId: "cmd-opaque",
    nested: {
      operations: [{ op: "add", path: "/vault/ref", value: "opaque" }],
    },
  };
  const envelope = createEnvelope({
    type: "mutation_command",
    sessionId,
    sequence: 22,
    payload,
  });
  const originalPayloadReference = envelope.payload;
  const originalPayloadSnapshot = JSON.stringify(envelope.payload);

  connectionManager.bindSessionSockets(sessionId, mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, webSocket);

  assert.equal(routed, true);
  assert.equal(envelope.payload, originalPayloadReference);
  assert.equal(JSON.stringify(envelope.payload), originalPayloadSnapshot);
  assert.deepEqual(JSON.parse(mobileSocket.sentMessages[0]).payload, payload);
  assert.deepEqual(mutationAudit.payloadReferenceEquality, [true]);
  assert.equal(logs.includes("PAYLOAD_REFERENCE_EQUALITY true"), true);
  assert.equal(logs.includes("INBOUND seq=22 type=mutation_command cmd=cmd-opaque"), true);
  assert.equal(logs.includes("OUTBOUND seq=22 type=mutation_command cmd=cmd-opaque"), true);
});

test("relay_no_message_dispatch", async () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({
    type: "totally_unknown_type",
    sessionId: "relay-no-dispatch-session",
    payload: { opaque: "still-forwarded" },
  });

  connectionManager.bindSessionSockets("relay-no-dispatch-session", mobileSocket, webSocket);

  const routed = await messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, true);
  assert.equal(webSocket.sentMessages.length, 1);
  assert.equal(webSocket.sentMessages[0], JSON.stringify(envelope));
});

test("qr_session_ready_sent", async () => {
  const redisClient = new FakeRedisClient();
  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const webSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await waitForOpen(webSocket);
      webSocket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "qr_session_create",
          sessionId: "77777777-7777-4777-8777-777777777777",
          timestamp: Date.now(),
          sequence: 11,
          payload: {},
        }),
      );

      const readyMessage = await waitForMessage(webSocket);

      assert.equal(readyMessage.protocolVersion, DEFAULT_PROTOCOL_VERSION);
      assert.equal(readyMessage.type, "qr_session_ready");
      assert.equal(readyMessage.sequence, 12);
      assert.equal(readyMessage.sessionId, readyMessage.payload.sessionId);

      webSocket.close();
    },
  );
});

test("session_created_in_registry", async () => {
  const redisClient = new FakeRedisClient();
  const token = "session-created-token";
  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const webSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await waitForOpen(webSocket);
      webSocket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "qr_session_create",
          sessionId: "88888888-8888-4888-8888-888888888888",
          timestamp: Date.now(),
          sequence: 3,
          payload: { token },
        }),
      );

      const readyMessage = await waitForMessage(webSocket);
      const session = await relayServer.sessionRegistry.getSession(readyMessage.payload.sessionId);

      assert.equal(session.state, SESSION_STATES.WAITING);
      assert.equal(session.sessionId, readyMessage.payload.sessionId);
      assert.equal(session.token, token);

      webSocket.close();
    },
  );
});

test("session_state_active", async () => {
  const redisClient = new FakeRedisClient();
  const token = "session-state-token";

  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const webSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const mobileSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await Promise.all([waitForOpen(webSocket), waitForOpen(mobileSocket)]);
      webSocket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "qr_session_create",
          sessionId: "99999999-9999-4999-8999-999999999999",
          timestamp: Date.now(),
          sequence: 1,
          payload: { token },
        }),
      );
      const readyMessage = await waitForMessage(webSocket);
      const webApprovalPromise = waitForMessage(webSocket);
      const mobileApprovalPromise = waitForMessage(mobileSocket);

      mobileSocket.send(
        JSON.stringify({
          protocolVersion: readyMessage.protocolVersion,
          type: "pair_request",
          sessionId: readyMessage.payload.sessionId,
          timestamp: Date.now(),
          sequence: 2,
          payload: {
            sessionId: readyMessage.payload.sessionId,
            token,
          },
        }),
      );

      await Promise.all([webApprovalPromise, mobileApprovalPromise]);

      const session = await relayServer.sessionRegistry.getSession(readyMessage.payload.sessionId);
      assert.equal(session.state, SESSION_STATES.ACTIVE);

      webSocket.close();
      mobileSocket.close();
    },
  );
});

test("pair_request_routed", async () => {
  const diagnostics = createDiagnosticsCollector();
  const redisClient = new FakeRedisClient();
  const token = "pair-routed-token";

  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      diagnostics: {
        ingress: {
          enabled: true,
          logger: diagnostics.logger,
        },
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const webSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const mobileSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await Promise.all([waitForOpen(webSocket), waitForOpen(mobileSocket)]);
      webSocket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "qr_session_create",
          sessionId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
          timestamp: Date.now(),
          sequence: 4,
          payload: { token },
        }),
      );
      const readyMessage = await waitForMessage(webSocket);
      const webApprovalPromise = waitForMessage(webSocket);
      const mobileApprovalPromise = waitForMessage(mobileSocket);

      mobileSocket.send(
        JSON.stringify({
          protocolVersion: readyMessage.protocolVersion,
          type: "pair_request",
          sessionId: readyMessage.payload.sessionId,
          timestamp: Date.now(),
          sequence: 5,
          payload: {
            sessionId: readyMessage.payload.sessionId,
            token,
          },
        }),
      );
      await Promise.all([webApprovalPromise, mobileApprovalPromise]);

      const pairRequestEntry = diagnostics.entries.find(
        (entry) =>
          entry.stage === "message_router_dispatch" &&
          entry.messageType === "pair_request" &&
          entry.reason === "dispatch_table_handler",
      );

      assert.equal(Boolean(pairRequestEntry), true);

      webSocket.close();
      mobileSocket.close();
    },
  );

});

test("routing_enabled_after_pairing", async () => {
  const redisClient = new FakeRedisClient();
  const token = "routing-enabled-token";

  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const address = relayServer.server.address();
      const webSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const mobileSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await Promise.all([waitForOpen(webSocket), waitForOpen(mobileSocket)]);
      webSocket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "qr_session_create",
          sessionId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
          timestamp: Date.now(),
          sequence: 1,
          payload: { token },
        }),
      );
      const readyMessage = await waitForMessage(webSocket);
      const webApprovalPromise = waitForMessage(webSocket);
      const mobileApprovalPromise = waitForMessage(mobileSocket);

      mobileSocket.send(
        JSON.stringify({
          protocolVersion: readyMessage.protocolVersion,
          type: "pair_request",
          sessionId: readyMessage.payload.sessionId,
          timestamp: Date.now(),
          sequence: 2,
          payload: {
            sessionId: readyMessage.payload.sessionId,
            token,
          },
        }),
      );

      await Promise.all([webApprovalPromise, mobileApprovalPromise]);

      const routedEnvelope = createEnvelope({
        type: "event_stream",
        sessionId: readyMessage.payload.sessionId,
        sequence: 3,
        payload: { opaque: "unchanged" },
      });

      mobileSocket.send(JSON.stringify(routedEnvelope));

      const forwardedEnvelope = await waitForMessage(webSocket);
      assert.deepEqual(forwardedEnvelope, routedEnvelope);
      assert.ok(relayServer.connectionManager.lookupConnection(readyMessage.payload.sessionId));

      webSocket.close();
      mobileSocket.close();
    },
  );
});

test("snapshot_schema_v2_start_preserved_end_to_end", async () => {
  const redisClient = new FakeRedisClient();
  const token = "snapshot-schema-v2-start-token";

  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const { webSocket, mobileSocket, readyMessage } = await establishActiveRelaySessionOverWebSocket(
        relayServer,
        {
          sessionId: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
          token,
        },
      );

      const snapshotStartEnvelope = createEnvelope({
        protocolVersion: readyMessage.protocolVersion,
        type: "snapshot_start",
        sessionId: readyMessage.payload.sessionId,
        sequence: 3,
        payload: {
          schemaVersion: 2,
          lastEventVersion: 7001,
          snapshotId: "snap-start-1",
          unknownField: {
            preserved: true,
            tags: ["alpha", "beta"],
          },
        },
      });

      const forwardedPromise = waitForMessage(webSocket);
      mobileSocket.send(JSON.stringify(snapshotStartEnvelope));

      const forwardedEnvelope = await forwardedPromise;

      assert.deepEqual(forwardedEnvelope, snapshotStartEnvelope);
      assert.equal(forwardedEnvelope.payload.schemaVersion, 2);
      assert.equal(forwardedEnvelope.payload.lastEventVersion, 7001);

      webSocket.close();
      mobileSocket.close();
      await Promise.all([waitForClose(webSocket), waitForClose(mobileSocket)]);
    },
  );
});

test("snapshot_schema_v2_chunk_transfer_preserved_end_to_end", async () => {
  const redisClient = new FakeRedisClient();
  const token = "snapshot-schema-v2-chunk-token";

  await withStartedServer(
    {
      host: "127.0.0.1",
      port: 0,
      wsPath: "/relay",
      pairing: {
        secret: "test-pairing-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry: createRedisSessionRegistry(redisClient),
    },
    async (relayServer) => {
      const { webSocket, mobileSocket, readyMessage } = await establishActiveRelaySessionOverWebSocket(
        relayServer,
        {
          sessionId: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
          token,
        },
      );

      const chunkPayload = Buffer.from("snapshot-schema-v2-payload", "utf8").toString("base64");
      const snapshotStartEnvelope = createEnvelope({
        protocolVersion: readyMessage.protocolVersion,
        type: "snapshot_start",
        sessionId: readyMessage.payload.sessionId,
        sequence: 3,
        payload: {
          schemaVersion: 2,
          lastEventVersion: 8100,
          snapshotId: "snap-chunk-1",
        },
      });
      const eventEnvelope = createEnvelope({
        protocolVersion: readyMessage.protocolVersion,
        type: "event_stream",
        sessionId: readyMessage.payload.sessionId,
        sequence: 4,
        payload: {
          eventVersion: 8101,
          opaque: "deferred-until-snapshot-release",
        },
      });
      const snapshotChunkEnvelope = createEnvelope({
        protocolVersion: readyMessage.protocolVersion,
        type: "snapshot_chunk",
        sessionId: readyMessage.payload.sessionId,
        sequence: 5,
        payload: {
          schemaVersion: 2,
          chunkIndex: 0,
          encoding: "base64",
          data: chunkPayload,
        },
      });
      const snapshotCompleteEnvelope = createEnvelope({
        protocolVersion: readyMessage.protocolVersion,
        type: "snapshot_complete",
        sessionId: readyMessage.payload.sessionId,
        sequence: 6,
        payload: {
          schemaVersion: 2,
          lastEventVersion: 8100,
          checksum: "complete",
        },
      });

      const forwardedMessagesPromise = collectMessages(webSocket, 4);

      mobileSocket.send(JSON.stringify(snapshotStartEnvelope));
      mobileSocket.send(JSON.stringify(eventEnvelope));
      mobileSocket.send(JSON.stringify(snapshotChunkEnvelope));
      mobileSocket.send(JSON.stringify(snapshotCompleteEnvelope));

      const forwardedMessages = await forwardedMessagesPromise;

      assert.deepEqual(forwardedMessages, [
        snapshotStartEnvelope,
        snapshotChunkEnvelope,
        snapshotCompleteEnvelope,
        eventEnvelope,
      ]);
      assert.equal(forwardedMessages[0].payload.schemaVersion, 2);
      assert.equal(forwardedMessages[0].payload.lastEventVersion, 8100);
      assert.equal(forwardedMessages[1].payload.schemaVersion, 2);
      assert.equal(forwardedMessages[1].payload.data, chunkPayload);
      assert.equal(forwardedMessages[2].payload.lastEventVersion, 8100);

      webSocket.close();
      mobileSocket.close();
      await Promise.all([waitForClose(webSocket), waitForClose(mobileSocket)]);
    },
  );
});

test("multiple_connection_handling", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    const sockets = Array.from({ length: 8 }, () => new WebSocket(`ws://127.0.0.1:${address.port}/relay`));

    await Promise.all(sockets.map(waitForOpen));
    assert.equal(relayServer.getConnectionCount(), sockets.length);

    await Promise.all(
      sockets.map(
        (socket) =>
          new Promise((resolve) => {
            socket.once("close", resolve);
            socket.close();
          }),
      ),
    );

    await waitForConnectionCount(relayServer, 0);
    assert.equal(relayServer.getConnectionCount(), 0);
  });
});
