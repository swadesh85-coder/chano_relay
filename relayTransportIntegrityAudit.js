const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");

const {
  DEFAULT_PAIRING_TTL_MS,
  DEFAULT_PROTOCOL_VERSION,
  DEFAULT_SESSION_TTL_MS,
  SESSION_STATES,
  createConnectionManager,
  createMessageRouter,
  createRedisSessionRegistry,
  createSessionLifecycleManager,
  startRelayServer,
} = require("./server");
const { FakeRedisClient } = require("./relayRuntimeAudit");

const EVIDENCE_FILE_PATH = path.join(__dirname, "relay_transport_integrity_evidence.txt");

function resolveSnapshotLoaderObservation() {
  const candidatePaths = [
    path.join(__dirname, "src", "app", "projection", "snapshot_loader.ts"),
    path.join(__dirname, "..", "chano_web", "src", "app", "projection", "snapshot_loader.ts"),
  ];

  const snapshotLoaderPath = candidatePaths.find((candidatePath) => fs.existsSync(candidatePath)) || null;

  if (!snapshotLoaderPath) {
    return {
      observed: false,
      note:
        "snapshot_loader.ts not present in the relay workspace or sibling chano_web workspace; web verification is limited to raw frame receipt and JSON parse integrity.",
    };
  }

  const relativePath = path.relative(__dirname, snapshotLoaderPath).replace(/\\/g, "/");

  return {
    observed: true,
    note:
      `snapshot_loader.ts present at ${relativePath}; this relay-only harness verifies byte transparency and raw frame receipt, while web ingestion/projection is validated separately by the web audit specs.`,
  };
}

class MockSocket {
  constructor() {
    this.sentMessages = [];
    this.readyState = 1;
  }

  send(message) {
    this.sentMessages.push(message);
  }

  close() {
    this.readyState = 3;
  }
}

function assertCondition(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function createEnvelope(overrides = {}) {
  return {
    protocolVersion: DEFAULT_PROTOCOL_VERSION,
    type: "event_stream",
    sessionId: "44444444-4444-4444-8444-444444444444",
    timestamp: Date.now(),
    sequence: 1,
    payload: { opaque: true },
    ...overrides,
  };
}

function sha256Hex(rawFrame) {
  return crypto.createHash("sha256").update(Buffer.from(rawFrame, "utf8")).digest("hex");
}

function formatTimestamp(timestamp) {
  return new Date(timestamp).toISOString();
}

function waitForOpen(socket, timeoutMs = 4000) {
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

function waitForClose(socket, timeoutMs = 4000) {
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

function waitForSocketMessage(socket, predicate = () => true, timeoutMs = 4000) {
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

      const raw = Buffer.isBuffer(data) ? data.toString("utf8") : String(data);
      const parsed = JSON.parse(raw);
      if (!predicate(parsed)) {
        return;
      }

      cleanup();
      resolve({ parsed, raw, receivedAt: Date.now() });
    }

    socket.on("message", onMessage);
    socket.once("error", onError);
  });
}

function waitForSessionState(sessionRegistry, sessionId, expectedState, timeoutMs = 4000) {
  const startedAt = Date.now();

  return new Promise((resolve, reject) => {
    async function poll() {
      try {
        const session = await sessionRegistry.getSession(sessionId);
        if (session && session.state === expectedState) {
          resolve(session);
          return;
        }

        if (Date.now() - startedAt >= timeoutMs) {
          reject(new Error(`Timed out waiting for session ${sessionId} to reach state ${expectedState}`));
          return;
        }

        setTimeout(poll, 20);
      } catch (error) {
        reject(error);
      }
    }

    poll();
  });
}

function parseHashLogEntries(logEntries, label, sessionId) {
  const matcher = new RegExp(`^${label}\\s+sessionId=${sessionId}\\s+hash=([a-f0-9]{64})$`);

  return logEntries
    .map((entry) => {
      const match = entry.line.match(matcher);
      if (!match) {
        return null;
      }

      return {
        timestamp: entry.timestamp,
        hash: match[1],
        line: entry.line,
      };
    })
    .filter(Boolean);
}

function hasLogLabel(logEntries, label, sessionId) {
  return logEntries.some((entry) => entry.line.includes(label) && entry.line.includes(`sessionId=${sessionId}`));
}

function createPrettyRawFrame(envelope) {
  return JSON.stringify(envelope, null, 2);
}

function createSessionEnvelope(sessionId, sequence, type, payload, timestamp) {
  return createPrettyRawFrame(
    createEnvelope({
      sessionId,
      sequence,
      type,
      timestamp,
      payload,
    }),
  );
}

async function establishPairedSession(clientUrl, sessionRegistry, sessionId, pairingToken) {
  const webSocket = new WebSocket(clientUrl);
  const mobileSocket = new WebSocket(clientUrl);

  await Promise.all([waitForOpen(webSocket), waitForOpen(mobileSocket)]);

  const readyPromise = waitForSocketMessage(
    webSocket,
    (message) => message.type === "qr_session_ready" && message.sessionId === sessionId,
  );

  webSocket.send(
    JSON.stringify(
      createEnvelope({
        type: "qr_session_create",
        sessionId,
        sequence: 1,
        payload: { token: pairingToken },
      }),
    ),
  );
  const readyMessage = await readyPromise;
  await waitForSessionState(sessionRegistry, sessionId, SESSION_STATES.WAITING);

  const webApprovedPromise = waitForSocketMessage(
    webSocket,
    (message) => message.type === "pair_approved" && message.sessionId === sessionId,
  );

  mobileSocket.send(
    JSON.stringify(
      createEnvelope({
        type: "pair_request",
        sessionId,
        sequence: 2,
        payload: { sessionId, token: readyMessage.parsed.payload.token },
      }),
    ),
  );

  await webApprovedPromise;

  const handshakePromise = waitForSocketMessage(
    webSocket,
    (message) => message.type === "protocol_handshake" && message.sessionId === sessionId,
  );

  mobileSocket.send(
    JSON.stringify(
      createEnvelope({
        type: "protocol_handshake",
        sessionId,
        sequence: 3,
        payload: { client: "mobile" },
      }),
    ),
  );

  await handshakePromise;
  await waitForSessionState(sessionRegistry, sessionId, SESSION_STATES.ACTIVE);

  return { webSocket, mobileSocket };
}

function extractSessionState(session) {
  return session ? session.state : SESSION_STATES.CLOSED;
}

async function runNegativeControl({ label, rawFrame, metadata, sessionId }) {
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  const connectionManager = createConnectionManager();
  const sessionLifecycleManager = createSessionLifecycleManager({
    sessionRegistry,
    connectionManager,
    now: () => redisClient.nowMs,
    setTimer: () => ({ synthetic: true }),
    clearTimer: () => {},
    logger: () => {},
  });
  const messageRouter = createMessageRouter(connectionManager, {
    sessionRegistry,
    sessionLifecycleManager,
    logger: () => {},
  });
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket, { connectionId: `${sessionId}-mobile` });
  connectionManager.registerConnection(webSocket, { connectionId: `${sessionId}-web` });

  await sessionLifecycleManager.createSession(sessionId, `${sessionId}-web`, redisClient.nowMs + DEFAULT_SESSION_TTL_MS);
  await sessionLifecycleManager.transitionToPaired(sessionId, `${sessionId}-mobile`);
  await sessionLifecycleManager.transitionToActive(sessionId);

  const envelope = createEnvelope({
    sessionId,
    type: "event_stream",
    sequence: 50,
    payload: { control: label },
  });

  let thrownError = null;

  try {
    await messageRouter.routeMessage(rawFrame, envelope, mobileSocket, metadata || {});
  } catch (error) {
    thrownError = error;
  }

  const session = await sessionRegistry.getSession(sessionId);
  const forwardedEventStream = webSocket.sentMessages.some((message) => {
    try {
      return JSON.parse(message).type === "event_stream";
    } catch {
      return false;
    }
  });

  return {
    label,
    error: thrownError,
    sessionState: extractSessionState(session),
    forwardedEventStream,
    webMessages: [...webSocket.sentMessages],
  };
}

function buildEvidenceReport(results) {
  const lines = [];
  lines.push("Relay Transport Integrity Verification");
  lines.push(`GeneratedAt: ${formatTimestamp(results.generatedAt)}`);
  lines.push(`Verdict: ${results.verdict}`);
  lines.push(`SessionId: ${results.sessionId}`);
  lines.push(`SnapshotLoaderObserved: ${results.snapshotLoaderObserved ? "yes" : "no"}`);
  lines.push(`SnapshotLoaderNote: ${results.snapshotLoaderNote}`);
  lines.push("");
  lines.push("Normal Flow");
  lines.push(`ObservedForwardOrder: ${results.observedOrder.join(" -> ")}`);
  lines.push(`ExpectedForwardOrder: snapshot_start -> snapshot_chunk -> snapshot_complete -> event_stream`);
  lines.push(`NoRelayByteViolation: ${results.normalFlow.noViolationLogs}`);
  lines.push(`NoMissingRawFrameViolation: ${results.normalFlow.noMissingRawFrameLogs}`);
  lines.push(`NoInvalidFrameTypeViolation: ${results.normalFlow.noInvalidFrameTypeLogs}`);
  lines.push("");
  lines.push("Per-Message Evidence");
  lines.push("type | sequence | sentAt | receivedAt | localHash | ingressHash | egressHash | ingressEqLocal | egressEqLocal | rawForwardEq | parseOk");

  for (const message of results.normalFlow.messages) {
    lines.push(
      [
        message.type,
        message.sequence,
        formatTimestamp(message.sentAt),
        formatTimestamp(message.receivedAt),
        message.localHash,
        message.ingressHash,
        message.egressHash,
        String(message.ingressHash === message.localHash),
        String(message.egressHash === message.localHash),
        String(message.receivedRaw === message.raw),
        String(message.parseOk),
      ].join(" | "),
    );
  }

  lines.push("");
  lines.push("Negative Controls");
  for (const control of results.negativeControls) {
    lines.push(`Control: ${control.label}`);
    lines.push(`ExpectedLog: ${control.expectedLog}`);
    lines.push(`ObservedLog: ${control.observedLog}`);
    lines.push(`SessionClosed: ${control.sessionClosed}`);
    lines.push(`ForwardedEventStream: ${control.forwardedEventStream}`);
    lines.push(`Error: ${control.errorMessage}`);
    lines.push("");
  }

  lines.push("Validation Summary");
  lines.push(`IngressEqualsEgressForAllMessages: ${results.summary.ingressEqualsEgressForAllMessages}`);
  lines.push(`WebReceivedIntactFrames: ${results.summary.webReceivedIntactFrames}`);
  lines.push(`SnapshotSequenceValid: ${results.summary.snapshotSequenceValid}`);
  lines.push(`EventStreamAfterSnapshotComplete: ${results.summary.eventStreamAfterSnapshotComplete}`);
  lines.push(`NegativeControlsEnforced: ${results.summary.negativeControlsEnforced}`);

  return `${lines.join("\n")}\n`;
}

async function runRelayTransportIntegrityAudit(options = {}) {
  const host = options.host || "127.0.0.1";
  const clientHost = options.clientHost || "127.0.0.1";
  const sessionId = options.sessionId || "44444444-4444-4444-8444-444444444444";
  const pairingToken = options.pairingToken || "transport-integrity-token";
  const logEntries = [];
  const originalConsoleLog = console.log;
  const emitOutput = options.emitOutput === true;
  const snapshotLoaderObservation = resolveSnapshotLoaderObservation();
  const redisClient = new FakeRedisClient();
  const sessionRegistry = createRedisSessionRegistry(redisClient);
  let relayServer;
  let webSocket;
  let mobileSocket;

  console.log = (...args) => {
    const line = args.map((value) => String(value)).join(" ");
    logEntries.push({ timestamp: Date.now(), line });
    if (emitOutput) {
      originalConsoleLog(...args);
    }
  };

  try {
    relayServer = await startRelayServer({
      host,
      port: 0,
      wsPath: "/relay",
      diagnostics: {
        ingress: {
          enabled: true,
        },
      },
      pairing: {
        secret: "transport-integrity-secret",
        ttlMs: DEFAULT_PAIRING_TTL_MS,
      },
      sessionRegistry,
    });

    const address = relayServer.server.address();
    const clientUrl = `ws://${clientHost}:${address.port}${relayServer.config.wsPath}`;
    const paired = await establishPairedSession(clientUrl, sessionRegistry, sessionId, pairingToken);
    webSocket = paired.webSocket;
    mobileSocket = paired.mobileSocket;

    const normalFlowDefinitions = [
      {
        type: "snapshot_start",
        sequence: 3,
        timestamp: 1700000000000,
        payload: {
          snapshotId: "snapshot-transport-audit",
          schemaVersion: 2,
          lastEventVersion: 0,
        },
      },
      {
        type: "snapshot_chunk",
        sequence: 4,
        timestamp: 1700000000001,
        payload: {
          chunkIndex: 0,
          chunkCount: 1,
          snapshotChunk: "alpha-beta-gamma",
        },
      },
      {
        type: "snapshot_complete",
        sequence: 5,
        timestamp: 1700000000002,
        payload: {
          snapshotId: "snapshot-transport-audit",
          chunkCount: 1,
          lastEventVersion: 0,
        },
      },
      {
        type: "event_stream",
        sequence: 6,
        timestamp: 1700000000003,
        payload: {
          eventVersion: 1,
          delta: {
            op: "replace",
            path: "/profile/displayName",
            value: "transport-integrity-ok",
          },
        },
      },
    ].map((definition) => ({
      ...definition,
      raw: createSessionEnvelope(sessionId, definition.sequence, definition.type, definition.payload, definition.timestamp),
    }));

    for (const message of normalFlowDefinitions) {
      const receiptPromise = waitForSocketMessage(
        webSocket,
        (parsed) => parsed.sessionId === sessionId && parsed.type === message.type && parsed.sequence === message.sequence,
      );
      message.sentAt = Date.now();
      mobileSocket.send(message.raw);
      const receipt = await receiptPromise;
      message.receivedAt = receipt.receivedAt;
      message.receivedRaw = receipt.raw;
      message.parseOk = receipt.parsed.type === message.type && receipt.parsed.sequence === message.sequence;
      message.localHash = sha256Hex(message.raw);
    }

    const ingressHashes = parseHashLogEntries(logEntries, "RELAY_INGRESS_HASH", sessionId);
    const egressHashes = parseHashLogEntries(logEntries, "RELAY_EGRESS_HASH", sessionId);

    for (const message of normalFlowDefinitions) {
      const ingressMatch = ingressHashes.find((entry) => entry.hash === message.localHash);
      const egressMatch = egressHashes.find((entry) => entry.hash === message.localHash);

      assertCondition(ingressMatch, `Missing RELAY_INGRESS_HASH for ${message.type}`);
      assertCondition(egressMatch, `Missing RELAY_EGRESS_HASH for ${message.type}`);

      message.ingressHash = ingressMatch.hash;
      message.egressHash = egressMatch.hash;
      message.ingressLoggedAt = ingressMatch.timestamp;
      message.egressLoggedAt = egressMatch.timestamp;
    }

    const negativeControls = [];

    const mismatchSessionId = "55555555-5555-4555-8555-555555555555";
    const mismatchEnvelope = createEnvelope({
      sessionId: mismatchSessionId,
      type: "event_stream",
      sequence: 50,
      payload: { control: "hash-mismatch" },
    });
    const mismatchRawFrame = JSON.stringify(mismatchEnvelope, null, 2);
    const mismatchResult = await runNegativeControl({
      label: "hash_mismatch",
      rawFrame: mismatchRawFrame,
      metadata: { rawFrame: mismatchRawFrame, ingressHash: "tampered-ingress-hash" },
      sessionId: mismatchSessionId,
    });
    negativeControls.push({
      label: "hash_mismatch",
      expectedLog: "RELAY_BYTE_VIOLATION",
      observedLog: hasLogLabel(logEntries, "RELAY_BYTE_VIOLATION", mismatchSessionId),
      sessionClosed: mismatchResult.sessionState === SESSION_STATES.CLOSED,
      forwardedEventStream: mismatchResult.forwardedEventStream,
      errorMessage: mismatchResult.error ? mismatchResult.error.message : "none",
    });

    const missingRawSessionId = "66666666-6666-4666-8666-666666666666";
    const missingRawResult = await runNegativeControl({
      label: "missing_raw_frame",
      rawFrame: undefined,
      metadata: undefined,
      sessionId: missingRawSessionId,
    });
    negativeControls.push({
      label: "missing_raw_frame",
      expectedLog: "RELAY_RAW_FRAME_MISSING",
      observedLog: hasLogLabel(logEntries, "RELAY_RAW_FRAME_MISSING", missingRawSessionId),
      sessionClosed: missingRawResult.sessionState === SESSION_STATES.CLOSED,
      forwardedEventStream: missingRawResult.forwardedEventStream,
      errorMessage: missingRawResult.error ? missingRawResult.error.message : "none",
    });

    const invalidFrameSessionId = "77777777-7777-4777-8777-777777777777";
    const invalidFrameResult = await runNegativeControl({
      label: "invalid_frame_type",
      rawFrame: { invalid: true },
      metadata: undefined,
      sessionId: invalidFrameSessionId,
    });
    negativeControls.push({
      label: "invalid_frame_type",
      expectedLog: "RELAY_INVALID_FRAME_TYPE",
      observedLog: hasLogLabel(logEntries, "RELAY_INVALID_FRAME_TYPE", invalidFrameSessionId),
      sessionClosed: invalidFrameResult.sessionState === SESSION_STATES.CLOSED,
      forwardedEventStream: invalidFrameResult.forwardedEventStream,
      errorMessage: invalidFrameResult.error ? invalidFrameResult.error.message : "none",
    });

    const observedOrder = normalFlowDefinitions.map((message) => message.type);
    const summary = {
      ingressEqualsEgressForAllMessages: normalFlowDefinitions.every(
        (message) => message.localHash === message.ingressHash && message.localHash === message.egressHash,
      ),
      webReceivedIntactFrames: normalFlowDefinitions.every(
        (message) => message.receivedRaw === message.raw && message.parseOk === true,
      ),
      snapshotSequenceValid:
        observedOrder[0] === "snapshot_start" &&
        observedOrder[1] === "snapshot_chunk" &&
        observedOrder[2] === "snapshot_complete",
      eventStreamAfterSnapshotComplete: observedOrder[3] === "event_stream",
      negativeControlsEnforced: negativeControls.every(
        (control) => control.observedLog === true && control.sessionClosed === true && control.forwardedEventStream === false,
      ),
    };

    const results = {
      generatedAt: Date.now(),
      verdict:
        summary.ingressEqualsEgressForAllMessages &&
        summary.webReceivedIntactFrames &&
        summary.snapshotSequenceValid &&
        summary.eventStreamAfterSnapshotComplete &&
        summary.negativeControlsEnforced &&
        !hasLogLabel(logEntries, "RELAY_BYTE_VIOLATION", sessionId) &&
        !hasLogLabel(logEntries, "RELAY_RAW_FRAME_MISSING", sessionId) &&
        !hasLogLabel(logEntries, "RELAY_INVALID_FRAME_TYPE", sessionId)
          ? "PASS"
          : "FAIL",
      sessionId,
      snapshotLoaderObserved: snapshotLoaderObservation.observed,
      snapshotLoaderNote: snapshotLoaderObservation.note,
      observedOrder,
      normalFlow: {
        messages: normalFlowDefinitions,
        noViolationLogs: !hasLogLabel(logEntries, "RELAY_BYTE_VIOLATION", sessionId),
        noMissingRawFrameLogs: !hasLogLabel(logEntries, "RELAY_RAW_FRAME_MISSING", sessionId),
        noInvalidFrameTypeLogs: !hasLogLabel(logEntries, "RELAY_INVALID_FRAME_TYPE", sessionId),
      },
      negativeControls,
      summary,
    };

    const report = buildEvidenceReport(results);
    fs.writeFileSync(EVIDENCE_FILE_PATH, report, "utf8");
    return {
      ...results,
      evidenceFilePath: EVIDENCE_FILE_PATH,
    };
  } finally {
    console.log = originalConsoleLog;

    if (mobileSocket && mobileSocket.readyState === WebSocket.OPEN) {
      mobileSocket.close();
      await waitForClose(mobileSocket).catch(() => {});
    }

    if (webSocket && webSocket.readyState === WebSocket.OPEN) {
      webSocket.close();
      await waitForClose(webSocket).catch(() => {});
    }

    if (relayServer) {
      await relayServer.stop().catch(() => {});
    }
  }
}

if (require.main === module) {
  runRelayTransportIntegrityAudit({ emitOutput: false })
    .then((results) => {
      process.stdout.write(`Relay transport integrity audit ${results.verdict}. Evidence: ${results.evidenceFilePath}\n`);
      if (results.verdict !== "PASS") {
        process.exitCode = 1;
      }
    })
    .catch((error) => {
      process.stderr.write(`${error.stack || error.message}\n`);
      process.exitCode = 1;
    });
}

module.exports = {
  EVIDENCE_FILE_PATH,
  runRelayTransportIntegrityAudit,
};