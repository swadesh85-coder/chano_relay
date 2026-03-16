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

  purgeExpired(key) {
    const entry = this.entries.get(key);
    if (!entry) {
      return;
    }

    if (entry.expiresAtSeconds !== null && entry.expiresAtSeconds * 1000 <= this.nowMs) {
      this.entries.delete(key);
    }
  }

  async exists(key) {
    this.purgeExpired(key);
    return this.entries.has(key) ? 1 : 0;
  }

  async set(key, value) {
    this.purgeExpired(key);
    const entry = this.entries.get(key) || { value: null, expiresAtSeconds: null };

    entry.value = String(value);
    this.entries.set(key, entry);
    return "OK";
  }

  async get(key) {
    this.purgeExpired(key);
    const entry = this.entries.get(key);
    return entry ? entry.value : null;
  }

  async hSet(key, fields) {
    const currentValue = await this.get(key);
    const currentHash = currentValue ? JSON.parse(currentValue) : {};
    const nextHash = {
      ...currentHash,
      ...fields,
    };
    await this.set(key, JSON.stringify(nextHash));
    return Object.keys(fields).length;
  }

  async hGetAll(key) {
    const currentValue = await this.get(key);
    return currentValue ? JSON.parse(currentValue) : {};
  }

  async expireAt(key, epochSeconds) {
    this.purgeExpired(key);
    const entry = this.entries.get(key);
    if (!entry) {
      return 0;
    }

    entry.expiresAtSeconds = Number(epochSeconds);
    this.entries.set(key, entry);
    this.purgeExpired(key);
    return this.entries.has(key) ? 1 : 0;
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

test("create_session", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const sessionId = "4d92f0cf-a044-4ae3-9e83-6e7dd53fd4ab";
  const expiresAt = redisClient.nowMs + 60_000;

  const session = await sessionRegistry.createSession(sessionId, expiresAt);
  const storedSession = await sessionRegistry.getSession(sessionId);
  const rawSession = JSON.parse(await redisClient.get(getRedisSessionKey(sessionId)));

  assert.equal(session.sessionId, sessionId);
  assert.equal(session.mobileSocketId, null);
  assert.equal(session.webSocketId, null);
  assert.equal(session.expiresAt, expiresAt);
  assert.equal(session.state, SESSION_STATES.WAITING);
  assert.deepEqual(storedSession, session);
  assert.deepEqual(rawSession, session);
  assert.equal(await redisClient.exists(getRedisSessionKey(sessionId)), 1);
});

test("session_lookup", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const sessionId = "56b8b39a-9ca7-44d8-a928-b0ef1126d2f4";
  const expiresAt = redisClient.nowMs + 60_000;

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

test("activate_session", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const sessionId = "8d2dd03d-e2de-477d-947a-08fab41dbc7a";

  await sessionRegistry.createSession(sessionId, redisClient.nowMs + 60_000);
  await sessionRegistry.attachWebSocket(sessionId, "web-socket-state");
  const waitingSession = await sessionRegistry.attachMobileSocket(sessionId, "mobile-socket-state");

  assert.equal(waitingSession.state, SESSION_STATES.WAITING);
  assert.equal(waitingSession.webSocketId, "web-socket-state");
  assert.equal(waitingSession.mobileSocketId, "mobile-socket-state");

  const activeSession = await sessionRegistry.setSessionState(sessionId, SESSION_STATES.ACTIVE);

  assert.equal(activeSession.state, SESSION_STATES.ACTIVE);
  assert.equal(activeSession.webSocketId, "web-socket-state");
  assert.equal(activeSession.mobileSocketId, "mobile-socket-state");

  const closedSession = await sessionRegistry.setSessionState(sessionId, SESSION_STATES.CLOSED);

  assert.equal(closedSession.state, SESSION_STATES.CLOSED);
  await assert.rejects(
    () => sessionRegistry.setSessionState(sessionId, SESSION_STATES.WAITING),
    /Invalid session state transition/,
  );
});

test("state_update", async () => {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const sessionId = "18e8f865-336f-4d15-a0e8-acd0a62bad55";

  await sessionRegistry.createSession(sessionId, redisClient.nowMs + 60_000);
  await sessionRegistry.attachWebSocket(sessionId, "web-socket-state");
  const pairedSession = await sessionRegistry.setSessionState(sessionId, SESSION_STATES.PAIRED);
  await sessionRegistry.attachMobileSocket(sessionId, "mobile-socket-state");
  const activeSession = await sessionRegistry.setSessionState(sessionId, SESSION_STATES.ACTIVE);
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
            payload: {},
          }),
        ),
      );

      const readyMessage = await waitForMessage(webSocket);
      assert.equal(readyMessage.type, "qr_session_ready");
  assert.equal(readyMessage.payload.sessionId, "11111111-1111-4111-8111-111111111111");
      assert.equal(typeof readyMessage.payload.expiresAt, "number");

      const storedSession = await relayServer.sessionRegistry.getSession(readyMessage.payload.sessionId);
      assert.equal(storedSession.state, SESSION_STATES.WAITING);
      assert.equal(storedSession.mobileSocketId, null);
      webSocket.close();
    },
  );
});

test("pair_request_attach_mobile", async () => {
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
      const mobileSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await Promise.all([waitForOpen(webSocket), waitForOpen(mobileSocket)]);
      webSocket.send(
        JSON.stringify(
          createEnvelope({
            type: "qr_session_create",
            sessionId: "22222222-2222-4222-8222-222222222222",
            payload: {},
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
  const sessionRegistry = createRedisSessionRegistry(redisClient);
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

  await sessionRegistry.createSession(sessionId, "web:1:relay:1", expiresAt);
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
    payload: { sessionId },
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
  const connectionManager = createConnectionManager();
  const scheduler = new ManualScheduler(() => redisClient.nowMs);
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry: createRedisSessionRegistry(redisClient),
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: (callback, delay) => scheduler.setTimeout(callback, delay),
    clearTimer: (handle) => scheduler.clearTimeout(handle),
  });
  const sessionId = "32d0e189-e11d-42e5-819a-a9f528ef1b0a";
  const expiresAt = redisClient.nowMs + 60_000;

  const session = await sessionLifecycleManager.createSession(sessionId, "web-lifecycle-1", expiresAt);

  assert.equal(session.sessionId, sessionId);
  assert.equal(session.webSocketId, "web-lifecycle-1");
  assert.equal(session.mobileSocketId, null);
  assert.equal(session.state, SESSION_STATES.WAITING);
  assert.equal(scheduler.handles.length, 1);
});

test("session_activate", async () => {
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
  const sessionId = "a54fa7f0-2b0d-4663-af29-0d53c53380a1";

  connectionManager.registerConnection(webSocket, { connectionId: "web-lifecycle-activate" });
  connectionManager.registerConnection(mobileSocket, { connectionId: "mobile-lifecycle-activate" });

  await sessionLifecycleManager.createSession(sessionId, "web-lifecycle-activate", redisClient.nowMs + 60_000);
  await sessionLifecycleManager.attachMobile(sessionId, "mobile-lifecycle-activate");
  const activeSession = await sessionLifecycleManager.activateSession(sessionId);
  const binding = connectionManager.lookupConnection(sessionId);

  assert.equal(activeSession.state, SESSION_STATES.ACTIVE);
  assert.equal(binding.webSocket, webSocket);
  assert.equal(binding.mobileSocket, mobileSocket);
});

test("session_close", async () => {
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
  await sessionLifecycleManager.attachMobile(sessionId, "mobile-lifecycle-close");
  await sessionLifecycleManager.closeSession(sessionId, "manual_close");

  const session = await sessionRegistry.getSession(sessionId);

  assert.equal(session.state, SESSION_STATES.CLOSED);
  assert.deepEqual(JSON.parse(webSocket.sentMessages[0]), {
    type: "session_close",
    payload: {
      reason: "manual_close",
    },
  });
  assert.deepEqual(JSON.parse(mobileSocket.sentMessages[0]), {
    type: "session_close",
    payload: {
      reason: "manual_close",
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
  await sessionLifecycleManager.attachMobile(sessionId, "mobile-lifecycle-disconnect");
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
  const sessionRegistry = createRedisSessionRegistry(redisClient);
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
      reason: "expired",
    },
  });
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
          payload: {},
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
          payload: {},
        }),
      );

      const readyMessage = await waitForMessage(webSocket);
      const session = await relayServer.sessionRegistry.getSession(readyMessage.payload.sessionId);

      assert.equal(session.state, SESSION_STATES.WAITING);
      assert.equal(session.sessionId, readyMessage.payload.sessionId);

      webSocket.close();
    },
  );
});

test("session_state_active", async () => {
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
      const mobileSocket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);

      await Promise.all([waitForOpen(webSocket), waitForOpen(mobileSocket)]);
      webSocket.send(
        JSON.stringify({
          protocolVersion: DEFAULT_PROTOCOL_VERSION,
          type: "qr_session_create",
          sessionId: "99999999-9999-4999-8999-999999999999",
          timestamp: Date.now(),
          sequence: 1,
          payload: {},
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
          payload: {},
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
          payload: {},
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
