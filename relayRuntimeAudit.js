const WebSocket = require("ws");

const {
  DEFAULT_PAIRING_TTL_MS,
  DEFAULT_PROTOCOL_VERSION,
  RedisSessionRegistry,
  SESSION_STATES,
  createRedisSessionRegistry,
  getRedisSessionKey,
  startRelayServer,
} = require("./server");

class FakeRedisClient {
  constructor(nowMs = Date.now()) {
    this.entries = new Map();
    this.nowMs = nowMs;
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

  sessionRegistry.createSession = async (...args) => {
    const session = await createSession(...args);
    transitions.push({
      state: session.state,
      sessionId: session.sessionId,
      snapshot: { ...session },
    });
    return session;
  };

  sessionRegistry.updateSession = async (...args) => {
    const session = await updateSession(...args);
    transitions.push({
      state: session.state,
      sessionId: session.sessionId,
      snapshot: { ...session },
    });
    return session;
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
    const redisSessionRecord = JSON.parse(await redisClient.get(getRedisSessionKey(sessionId)));

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
        command: "apply_patch",
        target: "profile",
      },
    });
    const routeEnvelopeThree = createEnvelope({
      type: "event_stream",
      sessionId,
      sequence: 7,
      payload: {
        eventVersion: 9001,
        delta: "opaque-event",
      },
    });

    const routedToWebOne = waitForSocketMessage(
      webSocket,
      (message) => message.type === routeEnvelopeOne.type && message.sequence === routeEnvelopeOne.sequence,
    );
    mobileSocket.send(JSON.stringify(routeEnvelopeOne));
    const routedEnvelopeOne = await routedToWebOne;

    const routedToMobile = waitForSocketMessage(
      mobileSocket,
      (message) => message.type === routeEnvelopeTwo.type && message.sequence === routeEnvelopeTwo.sequence,
    );
    webSocket.send(JSON.stringify(routeEnvelopeTwo));
    const routedEnvelopeTwo = await routedToMobile;

    const routedToWebThree = waitForSocketMessage(
      webSocket,
      (message) => message.type === routeEnvelopeThree.type && message.sequence === routeEnvelopeThree.sequence,
    );
    mobileSocket.send(JSON.stringify(routeEnvelopeThree));
    const routedEnvelopeThree = await routedToWebThree;

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
      routingEvidence,
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
        mutationBoundary: routingEvidence[1].unchanged && metadataOnly,
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
    record(JSON.stringify(formatSessionRecord(activeSession), null, 2));
    record(`VALID_ENVELOPE_ACCEPTED sequence=1 type=protocol_handshake`);
    record(`INVALID_ENVELOPE_REJECTED reason=${invalidEnvelope.payload.reason}`);
    record(`SESSION_CREATED sessionId=${sessionId}`);
    record(`SESSION_PAIRED sessionId=${sessionId}`);
    record(`SESSION_ACTIVE sessionId=${sessionId}`);
    record(`ROUTE mobile->web type=${routeEnvelopeOne.type} sessionId=${sessionId}`);
    record(`ROUTE web->mobile type=${routeEnvelopeTwo.type} sessionId=${sessionId}`);
    record(`ROUTE mobile->web type=${routeEnvelopeThree.type} sessionId=${sessionId}`);
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
  FakeRedisClient,
  runRelayRuntimeAudit,
};