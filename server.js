const fs = require("fs");
const http = require("http");
const https = require("https");
const path = require("path");
const crypto = require("crypto");
const { EventEmitter } = require("events");
const { WebSocketServer } = require("ws");
const {
  DEFAULT_PAIRING_TTL_MS,
  QRPairingSystem,
  createQRPairingSystem,
  signPairingTokenPayload,
} = require("./qrPairingSystem");
const {
  DEFAULT_MAX_COMMANDS_PER_SECOND,
  DEFAULT_MAX_PAYLOAD_SIZE,
  DEFAULT_MAX_SESSIONS_PER_IP,
  RelayRateLimiter,
  createRelayRateLimiter,
} = require("./relayRateLimiter");
const {
  SessionLifecycleManager,
  createSessionLifecycleManager,
} = require("./sessionLifecycleManager");
const { createSessionQueueManager } = require("./session_queue_manager");
const {
  DEFAULT_SESSION_TTL_MS,
  SESSION_STATES,
  RedisSessionRegistry,
  connectRedisSessionRegistry,
  createRedisSessionRegistry,
  getRedisSessionKey,
} = require("./redisSessionRegistry");

const DEFAULT_PROTOCOL_VERSION = 2;
const DEFAULT_MAX_PAYLOAD_BYTES = DEFAULT_MAX_PAYLOAD_SIZE;
const DEFAULT_CONFIG_PATH = path.join(__dirname, "relay.config.json");
const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;
const BOOTSTRAP_MESSAGE_TYPES = new Set(["protocol_handshake", "qr_session_create", "pair_request"]);
function createRelayIngressDiagnostics(options = {}) {
  const enabled = Boolean(options.enabled);
  const logger =
    typeof options.logger === "function"
      ? options.logger
      : (entry) => {
          console.log(`[relay_ingress] ${JSON.stringify(entry)}`);
        };

  function log(stage, details = {}) {
    if (!enabled) {
      return;
    }

    logger({
      stage,
      timestamp: Date.now(),
      ...details,
    });
  }

  return {
    enabled,
    log,
  };
}

function formatAuditValue(value, fallback = "unknown") {
  if (value === undefined || value === null) {
    return fallback;
  }

  if (typeof value === "string") {
    return value.trim() === "" ? fallback : value;
  }

  return String(value);
}

function formatRawAuditPreview(rawMessage, maxChars = 200) {
  const rawText = Buffer.isBuffer(rawMessage) ? rawMessage.toString("utf8") : String(rawMessage);
  return rawText.slice(0, maxChars).replace(/\r/g, "\\r").replace(/\n/g, "\\n");
}

function coerceRawFrameBuffer(rawFrame) {
  if (Buffer.isBuffer(rawFrame)) {
    return rawFrame;
  }

  if (typeof rawFrame === "string") {
    return Buffer.from(rawFrame, "utf8");
  }

  if (rawFrame instanceof ArrayBuffer) {
    return Buffer.from(rawFrame);
  }

  if (ArrayBuffer.isView(rawFrame)) {
    return Buffer.from(rawFrame.buffer, rawFrame.byteOffset, rawFrame.byteLength);
  }

  if (Array.isArray(rawFrame)) {
    return Buffer.concat(rawFrame.map((chunk) => coerceRawFrameBuffer(chunk)));
  }

  return Buffer.from(String(rawFrame), "utf8");
}

function computeRawFrameHash(rawFrame) {
  return crypto.createHash("sha256").update(coerceRawFrameBuffer(rawFrame)).digest("hex");
}

function isSupportedRawFrameType(rawFrame) {
  return typeof rawFrame === "string" || Buffer.isBuffer(rawFrame);
}

function sendRawFrame(destinationSocket, rawFrame, isBinary = false) {
  if (typeof rawFrame === "string") {
    destinationSocket.send(rawFrame);
    return;
  }

  destinationSocket.send(rawFrame, { binary: isBinary });
}

function logRelayAudit(diagnostics, label, fields = []) {
  if (!diagnostics || !diagnostics.enabled) {
    return;
  }

  const suffix = fields.map(([key, value]) => `${key}=${formatAuditValue(value)}`).join(" ");
  console.log(`${label}${suffix ? ` ${suffix}` : ""}`);
}

function logRelayIntegrity(label, fields = []) {
  const suffix = fields.map(([key, value]) => `${key}=${formatAuditValue(value)}`).join(" ");
  console.log(`${label}${suffix ? ` ${suffix}` : ""}`);
}

function parseBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  if (typeof value === "boolean") {
    return value;
  }

  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function parseInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) ? parsed : fallback;
}

function loadRelayConfiguration(options = {}) {
  const env = options.env || process.env;
  const configPath = options.configPath || env.RELAY_CONFIG_PATH || DEFAULT_CONFIG_PATH;
  let fileConfig = {};

  if (fs.existsSync(configPath)) {
    fileConfig = JSON.parse(fs.readFileSync(configPath, "utf8"));
  }

  const tlsConfig = {
    enabled: parseBoolean(env.RELAY_TLS_ENABLED, fileConfig.tls && fileConfig.tls.enabled),
    keyPath: env.RELAY_TLS_KEY_PATH || (fileConfig.tls && fileConfig.tls.keyPath) || "",
    certPath: env.RELAY_TLS_CERT_PATH || (fileConfig.tls && fileConfig.tls.certPath) || "",
    minVersion:
      env.RELAY_TLS_MIN_VERSION ||
      (fileConfig.tls && fileConfig.tls.minVersion) ||
      "TLSv1.2",
  };

  const config = {
    host: env.HOST || fileConfig.host || "0.0.0.0",
    port: parseInteger(env.PORT, parseInteger(fileConfig.port, 8080)),
    wsPath: env.RELAY_WS_PATH || fileConfig.wsPath || "/relay",
    redisUrl: env.REDIS_URL || fileConfig.redisUrl || "",
    protocolVersion: parseInteger(
      env.RELAY_PROTOCOL_VERSION,
      parseInteger(fileConfig.protocolVersion, DEFAULT_PROTOCOL_VERSION),
    ),
    maxPayloadBytes: parseInteger(
      env.RELAY_MAX_PAYLOAD_BYTES,
      parseInteger(fileConfig.maxPayloadBytes, DEFAULT_MAX_PAYLOAD_BYTES),
    ),
    pairing: {
      secret: env.RELAY_PAIRING_SECRET || (fileConfig.pairing && fileConfig.pairing.secret) || "",
      ttlMs: parseInteger(
        env.RELAY_PAIRING_TTL_MS,
        parseInteger(fileConfig.pairing && fileConfig.pairing.ttlMs, DEFAULT_PAIRING_TTL_MS),
      ),
    },
    rateLimiting: {
      maxCommandsPerSecond: parseInteger(
        env.RELAY_MAX_COMMANDS_PER_SECOND,
        parseInteger(
          fileConfig.rateLimiting && fileConfig.rateLimiting.maxCommandsPerSecond,
          DEFAULT_MAX_COMMANDS_PER_SECOND,
        ),
      ),
      maxPayloadSize: parseInteger(
        env.RELAY_MAX_PAYLOAD_SIZE,
        parseInteger(fileConfig.rateLimiting && fileConfig.rateLimiting.maxPayloadSize, DEFAULT_MAX_PAYLOAD_SIZE),
      ),
      maxSessionsPerIp: parseInteger(
        env.RELAY_MAX_SESSIONS_PER_IP,
        parseInteger(
          fileConfig.rateLimiting &&
            (fileConfig.rateLimiting.maxSessionsPerIp || fileConfig.rateLimiting.maxSessionsPerIP),
          DEFAULT_MAX_SESSIONS_PER_IP,
        ),
      ),
    },
    diagnostics: {
      ingress: {
        enabled: parseBoolean(
          env.RELAY_INGRESS_DIAGNOSTICS_ENABLED,
          fileConfig.diagnostics && fileConfig.diagnostics.ingress && fileConfig.diagnostics.ingress.enabled,
        ),
      },
    },
    heartbeat: {
      enabled: parseBoolean(
        env.RELAY_HEARTBEAT_ENABLED,
        fileConfig.heartbeat && fileConfig.heartbeat.enabled !== undefined
          ? fileConfig.heartbeat.enabled
          : true,
      ),
      intervalMs: parseInteger(
        env.RELAY_HEARTBEAT_INTERVAL_MS,
        parseInteger(fileConfig.heartbeat && fileConfig.heartbeat.intervalMs, DEFAULT_HEARTBEAT_INTERVAL_MS),
      ),
    },
    tls: tlsConfig,
  };

  if (tlsConfig.enabled) {
    if (!tlsConfig.keyPath || !tlsConfig.certPath) {
      throw new Error("TLS is enabled but RELAY_TLS_KEY_PATH or RELAY_TLS_CERT_PATH is missing");
    }

    if (!fs.existsSync(tlsConfig.keyPath) || !fs.existsSync(tlsConfig.certPath)) {
      throw new Error("TLS key or certificate file does not exist");
    }
  }

  return config;
}

function createTransportEnvelopeError(reason, protocolVersion) {
  return JSON.stringify({
    protocolVersion,
    type: "transport_error",
    sessionId: null,
    timestamp: Date.now(),
    sequence: 0,
    payload: { reason },
  });
}

function getPayloadSizeBytes(payload) {
  return Buffer.byteLength(JSON.stringify(payload));
}

function validateTransportEnvelope(message, options) {
  const protocolVersion = options.protocolVersion;
  const maxPayloadBytes = options.maxPayloadBytes;
  const diagnostics = options.diagnostics || null;
  const socketId = options.socketId || null;

  function logValidation(result) {
    if (diagnostics) {
      diagnostics.log("transport_envelope_validated", {
        socketId,
        messageType: message && typeof message === "object" && !Array.isArray(message) ? message.type || null : null,
        sessionId:
          message && typeof message === "object" && !Array.isArray(message)
            ? message.sessionId || null
            : null,
        valid: result.valid,
        reason: result.reason || null,
      });
    }

    return result;
  }

  if (!message || typeof message !== "object" || Array.isArray(message)) {
    return logValidation({ valid: false, reason: "invalid_transport_envelope" });
  }

  const hasSequence = Number.isInteger(message.sequence) && message.sequence >= 0;
  const hasTimestamp = typeof message.timestamp === "number" && Number.isFinite(message.timestamp);
  const hasType = typeof message.type === "string";
  const hasPayload = Object.prototype.hasOwnProperty.call(message, "payload");

  if (!Object.prototype.hasOwnProperty.call(message, "protocolVersion")) {
    return logValidation({ valid: false, reason: "missing_protocol_version" });
  }

  if (message.protocolVersion !== protocolVersion) {
    return logValidation({ valid: false, reason: "unsupported_protocol_version" });
  }

  if (typeof message.sessionId !== "string" || message.sessionId.trim() === "") {
    return logValidation({ valid: false, reason: "missing_session_id" });
  }

  if (typeof message.type !== "string" || message.type.trim() === "") {
    return logValidation({ valid: false, reason: "missing_type" });
  }

  if (!hasType || !hasTimestamp || !hasSequence || !hasPayload) {
    return logValidation({ valid: false, reason: "invalid_transport_envelope" });
  }

  if (getPayloadSizeBytes(message.payload) > maxPayloadBytes) {
    return logValidation({ valid: false, reason: "payload_too_large" });
  }

  return logValidation({ valid: true });
}

function createConnectionManager(options = {}) {
  const diagnostics = options.diagnostics || null;
  const socketRegistry = new Map();
  const connectionIdRegistry = new Map();
  const sessionRegistry = new Map();

  function ensureSession(sessionId) {
    let session = sessionRegistry.get(sessionId);
    if (!session) {
      session = {
        sessionId,
        mobileSocket: null,
        webSocket: null,
      };
      sessionRegistry.set(sessionId, session);
    }

    return session;
  }

  function cleanupSessionIfEmpty(sessionId) {
    const session = sessionRegistry.get(sessionId);
    if (!session) {
      return;
    }

    if (!session.mobileSocket && !session.webSocket) {
      sessionRegistry.delete(sessionId);
    }
  }

  function detachSocket(socket) {
    const registration = socketRegistry.get(socket);
    if (!registration || !registration.sessionId || !registration.role) {
      return;
    }

    const session = sessionRegistry.get(registration.sessionId);
    if (!session) {
      registration.sessionId = null;
      registration.role = null;
      return;
    }

    if (registration.role === "mobile" && session.mobileSocket === socket) {
      session.mobileSocket = null;
    }

    if (registration.role === "web" && session.webSocket === socket) {
      session.webSocket = null;
    }

    registration.sessionId = null;
    registration.role = null;
    cleanupSessionIfEmpty(session.sessionId);
  }

  function registerConnection(socket, options = {}) {
    const existing = socketRegistry.get(socket);
    if (existing) {
      if (options.connectionId && existing.connectionId !== options.connectionId) {
        if (existing.connectionId) {
          connectionIdRegistry.delete(existing.connectionId);
        }

        existing.connectionId = options.connectionId;
        connectionIdRegistry.set(options.connectionId, existing);
      }

      return existing;
    }

    const onClose = () => {
      removeConnection(socket);
    };

    const registration = {
      socket,
      connectionId: options.connectionId || null,
      clientRole: options.clientRole || null,
      remoteAddress: options.remoteAddress || null,
      sessionId: null,
      role: null,
      onClose,
    };

    socketRegistry.set(socket, registration);
    if (registration.connectionId) {
      connectionIdRegistry.set(registration.connectionId, registration);
    }

    if (diagnostics) {
      diagnostics.log("socket_registered", {
        socketId: registration.connectionId,
        messageType: null,
        sessionId: null,
      });
    }

    if (typeof socket.once === "function") {
      socket.once("close", onClose);
    } else if (typeof socket.on === "function") {
      socket.on("close", onClose);
    }

    return registration;
  }

  function removeConnection(socket) {
    const registration = socketRegistry.get(socket);
    if (!registration) {
      return false;
    }

    detachSocket(socket);
    if (typeof socket.off === "function") {
      socket.off("close", registration.onClose);
    } else if (typeof socket.removeListener === "function") {
      socket.removeListener("close", registration.onClose);
    }

    if (registration.connectionId) {
      connectionIdRegistry.delete(registration.connectionId);
    }

    socketRegistry.delete(socket);
    return true;
  }

  function lookupConnection(sessionId) {
    return sessionRegistry.get(sessionId) || null;
  }

  function lookupSocket(socket) {
    const registration = socketRegistry.get(socket) || null;

    if (diagnostics && registration) {
      diagnostics.log("socket_lookup", {
        socketId: registration.connectionId,
        messageType: null,
        sessionId: registration.sessionId || null,
      });
    }

    return registration;
  }

  function lookupConnectionById(connectionId) {
    return connectionIdRegistry.get(connectionId) || null;
  }

  function bindSessionSockets(sessionId, mobileSocket, webSocket) {
    if (typeof sessionId !== "string" || sessionId.trim() === "") {
      throw new Error("sessionId is required");
    }

    const mobileRegistration = registerConnection(mobileSocket);
    const webRegistration = registerConnection(webSocket);

    detachSocket(mobileSocket);
    detachSocket(webSocket);

    const session = ensureSession(sessionId);
    session.mobileSocket = mobileSocket;
    session.webSocket = webSocket;

    mobileRegistration.sessionId = sessionId;
    mobileRegistration.role = "mobile";
    webRegistration.sessionId = sessionId;
    webRegistration.role = "web";

    if (diagnostics) {
      diagnostics.log("session_sockets_bound", {
        socketId: webRegistration.connectionId,
        messageType: null,
        sessionId,
      });
    }

    return session;
  }

  function unbindSession(sessionId) {
    if (typeof sessionId !== "string" || sessionId.trim() === "") {
      throw new Error("sessionId is required");
    }

    const session = sessionRegistry.get(sessionId);
    if (!session) {
      return false;
    }

    const clearRegistration = (socket, role) => {
      if (!socket) {
        return;
      }

      const registration = socketRegistry.get(socket);
      if (!registration) {
        return;
      }

      if (registration.sessionId === sessionId && registration.role === role) {
        registration.sessionId = null;
        registration.role = null;
      }
    };

    clearRegistration(session.mobileSocket, "mobile");
    clearRegistration(session.webSocket, "web");
    session.mobileSocket = null;
    session.webSocket = null;
    sessionRegistry.delete(sessionId);
    return true;
  }

  function getConnectionCount() {
    return socketRegistry.size;
  }

  function closeAllConnections(code = 1001, reason = "server_shutdown") {
    for (const socket of socketRegistry.keys()) {
      socket.close(code, reason);
    }
  }

  return {
    registerConnection,
    removeConnection,
    lookupConnection,
    lookupConnectionById,
    lookupSocket,
    bindSessionSockets,
    unbindSession,
    closeAllConnections,
    getConnectionCount,
  };
}

function buildConnectionId(req) {
  return [
    req.socket.remoteAddress || "unknown",
    req.socket.remotePort || 0,
    req.socket.localAddress || "unknown",
    req.socket.localPort || 0,
  ].join(":");
}

function canSendToSocket(socket) {
  if (!socket || typeof socket.send !== "function") {
    return false;
  }

  if (socket.readyState === undefined) {
    return true;
  }

  return socket.readyState === 1;
}

function createMessageRouter(connectionManager, options = {}) {
  const diagnostics = options.diagnostics || null;
  const dispatchTable = options.dispatchTable || {};
  const sessionRegistry = options.sessionRegistry || null;
  const sessionLifecycleManager = options.sessionLifecycleManager || null;
  const logger = typeof options.logger === "function" ? options.logger : console.log;
  const eventAudits = new Map();
  const mutationAudits = new Map();
  const sessionQueueManager = createSessionQueueManager({
    logger,
    forwardEnvelope: (envelope, metadata) =>
      routeEnvelope(metadata.rawFrame, envelope, metadata.senderSocket, undefined, metadata),
  });

  function createRoutingMetadata(rawFrame, metadata = {}) {
    return {
      ...metadata,
      rawFrame,
      ingressHash: metadata.ingressHash || computeRawFrameHash(rawFrame),
      isBinary: metadata.isBinary === true,
    };
  }

  async function terminateTransportViolation(envelope, reason, auditLabel, fields = []) {
    logRelayIntegrity(auditLabel, [["sessionId", envelope && envelope.sessionId ? envelope.sessionId : null], ...fields]);

    if (sessionLifecycleManager && envelope && envelope.sessionId) {
      await sessionLifecycleManager.closeSession(envelope.sessionId, reason);
    }

    throw new Error(reason);
  }

  async function assertForwardableRawFrame(rawFrame, envelope) {
    if (rawFrame === null || rawFrame === undefined) {
      await terminateTransportViolation(envelope, "relay_raw_frame_missing", "RELAY_RAW_FRAME_MISSING");
    }

    if (!isSupportedRawFrameType(rawFrame)) {
      await terminateTransportViolation(
        envelope,
        "relay_invalid_frame_type",
        "RELAY_INVALID_FRAME_TYPE",
        [["frameType", rawFrame && rawFrame.constructor ? rawFrame.constructor.name : typeof rawFrame]],
      );
    }
  }

  function getRouteDirection(senderRole) {
    return senderRole === "mobile" ? "mobile→web" : "web→mobile";
  }

  function isMutationAuditEnvelope(envelope) {
    return Boolean(
      envelope && (envelope.type === "mutation_command" || envelope.type === "command_result"),
    );
  }

  function enableEventAudit(sessionId) {
    if (typeof sessionId !== "string" || sessionId.trim() === "") {
      throw new Error("sessionId is required");
    }

    const normalizedSessionId = sessionId.trim();
    const existingAudit = eventAudits.get(normalizedSessionId);
    if (existingAudit) {
      return existingAudit;
    }

    const auditState = {
      sessionId: normalizedSessionId,
      inbound: [],
      outbound: [],
      routingLogs: [],
      payloadReferenceEquality: [],
    };

    eventAudits.set(normalizedSessionId, auditState);
    return auditState;
  }

  function enableMutationAudit(sessionId) {
    if (typeof sessionId !== "string" || sessionId.trim() === "") {
      throw new Error("sessionId is required");
    }

    const normalizedSessionId = sessionId.trim();
    const existingAudit = mutationAudits.get(normalizedSessionId);
    if (existingAudit) {
      return existingAudit;
    }

    const auditState = {
      sessionId: normalizedSessionId,
      inbound: [],
      outbound: [],
      routingLogs: [],
      payloadReferenceEquality: [],
    };

    mutationAudits.set(normalizedSessionId, auditState);
    return auditState;
  }

  function getEventAudit(sessionId) {
    if (typeof sessionId !== "string" || sessionId.trim() === "") {
      return null;
    }

    return eventAudits.get(sessionId.trim()) || null;
  }

  function getMutationAudit(sessionId) {
    if (typeof sessionId !== "string" || sessionId.trim() === "") {
      return null;
    }

    return mutationAudits.get(sessionId.trim()) || null;
  }

  function recordEventAudit(direction, envelope, payloadReferenceEquality = null) {
    if (!envelope || envelope.type !== "event_stream") {
      return;
    }

    const auditState = getEventAudit(envelope.sessionId);
    if (!auditState) {
      return;
    }

    const entry = {
      envelope,
      type: envelope.type,
      sequence: envelope.sequence,
      payloadReferenceEquality,
    };
    const logLine = `${direction} seq=${entry.sequence} type=${entry.type}`;

    if (direction === "INBOUND") {
      auditState.inbound.push(entry);
    } else {
      auditState.outbound.push(entry);
      auditState.payloadReferenceEquality.push(payloadReferenceEquality === true);
    }

    auditState.routingLogs.push(logLine);
    logger(logLine);
  }

  function recordMutationAudit(direction, envelope, payloadReferenceEquality = null) {
    if (!isMutationAuditEnvelope(envelope)) {
      return;
    }

    const auditState = getMutationAudit(envelope.sessionId);
    if (!auditState) {
      return;
    }

    const entry = {
      envelope,
      type: envelope.type,
      sequence: envelope.sequence,
      payloadReferenceEquality,
    };
    const logLine = `${direction} seq=${entry.sequence} type=${entry.type}`;

    if (direction === "INBOUND") {
      auditState.inbound.push(entry);
    } else {
      auditState.outbound.push(entry);
      auditState.payloadReferenceEquality.push(payloadReferenceEquality === true);
    }

    auditState.routingLogs.push(logLine);
    logger(logLine);
  }

  function ensureNoPayloadMutation(envelope, payloadReference) {
    const isReferenceEqual = envelope.payload === payloadReference;
    logger(`PAYLOAD_REFERENCE_EQUALITY ${isReferenceEqual}`);
    return isReferenceEqual;
  }

  async function auditForwardedFrame(envelope, routingMetadata) {
    await assertForwardableRawFrame(routingMetadata.rawFrame, envelope);

    const egressHash = computeRawFrameHash(routingMetadata.rawFrame);

    logRelayIntegrity("RELAY_EGRESS_HASH", [
      ["sessionId", envelope.sessionId || null],
      ["hash", egressHash],
    ]);

    if (routingMetadata.ingressHash !== egressHash) {
      await terminateTransportViolation(envelope, "relay_byte_violation", "RELAY_BYTE_VIOLATION", [
        ["sessionId", envelope.sessionId || null],
        ["ingressHash", routingMetadata.ingressHash],
        ["egressHash", egressHash],
      ]);
    }

    return egressHash;
  }

  async function forwardEnvelopeAsIs(envelope, destinationSocket, routingMetadata) {
    const payloadReference = envelope.payload;
    await auditForwardedFrame(envelope, routingMetadata);
    sendRawFrame(destinationSocket, routingMetadata.rawFrame, routingMetadata.isBinary);
    const payloadReferenceEquality = ensureNoPayloadMutation(envelope, payloadReference);
    return payloadReferenceEquality;
  }

  async function forwardRoutedEnvelope(envelope, destinationSocket, senderRegistration, routingMetadata) {
    const payloadReferenceEquality = await forwardEnvelopeAsIs(envelope, destinationSocket, routingMetadata);

    recordEventAudit("OUTBOUND", envelope, payloadReferenceEquality);
    recordMutationAudit("OUTBOUND", envelope, payloadReferenceEquality);

    logger(
      `RELAY_ROUTE ${getRouteDirection(senderRegistration.role)} type=${envelope.type} sequence=${envelope.sequence}`,
    );
  }

  async function routeEnvelope(rawFrame, envelope, senderSocket, senderRegistration = connectionManager.lookupSocket(senderSocket), metadata = {}) {
    await assertForwardableRawFrame(rawFrame, envelope);

    const routingMetadata = createRoutingMetadata(rawFrame, metadata);
    const hasSenderSessionBinding = Boolean(
      senderRegistration && senderRegistration.sessionId && senderRegistration.role,
    );
    const isBootstrapMessage = BOOTSTRAP_MESSAGE_TYPES.has(envelope.type);
    const dispatchHandler = dispatchTable[envelope.type] || null;

    logRelayAudit(diagnostics, "DISPATCH_LOOKUP", [
      ["type", envelope.type || null],
      ["sessionId", envelope.sessionId || null],
      ["handlerExists", Boolean(dispatchHandler)],
    ]);

    function logRoutingDecision(routed, reason) {
      logRelayAudit(diagnostics, "ROUTING_DECISION", [
        ["type", envelope.type || null],
        ["sessionId", envelope.sessionId || null],
        ["routed", routed],
        ["reason", reason || "none"],
      ]);
    }

    function logMessageDropped(reason) {
      logRelayAudit(diagnostics, "MESSAGE_DROPPED", [
        ["type", envelope.type || null],
        ["sessionId", envelope.sessionId || null],
        ["reason", reason],
      ]);
    }

    if (!hasSenderSessionBinding && !isBootstrapMessage) {
      if (diagnostics) {
        diagnostics.log("message_router_dispatch", {
          socketId: senderRegistration ? senderRegistration.connectionId : null,
          messageType: envelope.type || null,
          sessionId: envelope.sessionId || null,
          routed: false,
          reason: "missing_sender_registration",
        });
      }

      logRoutingDecision(false, "missing_sender_registration");
      logMessageDropped("missing_sender_registration");

      return false;
    }

    if (dispatchHandler) {
      await dispatchHandler(senderSocket, envelope);

      if (diagnostics) {
        diagnostics.log("message_router_dispatch", {
          socketId: senderRegistration.connectionId,
          messageType: envelope.type || null,
          sessionId: envelope.sessionId || null,
          routed: true,
          reason: "dispatch_table_handler",
        });
      }

      logRoutingDecision(true, "dispatch_table_handler");

      return true;
    }

    if (isBootstrapMessage && !hasSenderSessionBinding) {
      if (diagnostics) {
        diagnostics.log("message_router_dispatch", {
          socketId: senderRegistration ? senderRegistration.connectionId : null,
          messageType: envelope.type || null,
          sessionId: envelope.sessionId || null,
          routed: true,
          reason: "bootstrap_message_allowed",
        });
      }

      logRoutingDecision(true, "bootstrap_message_allowed");

      return true;
    }

    if (senderRegistration.sessionId !== envelope.sessionId) {
      if (diagnostics) {
        diagnostics.log("message_router_dispatch", {
          socketId: senderRegistration.connectionId,
          messageType: envelope.type || null,
          sessionId: envelope.sessionId || null,
          routed: false,
          reason: "session_mismatch",
        });
      }

      logRoutingDecision(false, "session_mismatch");
      logMessageDropped("session_mismatch");

      return false;
    }

    const session = connectionManager.lookupConnection(envelope.sessionId);
    if (!session) {
      logRelayAudit(diagnostics, "SESSION_LOOKUP", [
        ["type", envelope.type || null],
        ["sessionId", envelope.sessionId || null],
        ["found", false],
        ["state", "unbound"],
      ]);

      if (diagnostics) {
        diagnostics.log("message_router_dispatch", {
          socketId: senderRegistration.connectionId,
          messageType: envelope.type || null,
          sessionId: envelope.sessionId || null,
          routed: false,
          reason: "session_not_bound",
        });
      }

      logRoutingDecision(false, "session_not_bound");
      logMessageDropped("session_not_bound");

      return false;
    }

    let persistedSession = null;

    if (sessionRegistry) {
      persistedSession = await sessionRegistry.getSession(envelope.sessionId);

      logRelayAudit(diagnostics, "SESSION_LOOKUP", [
        ["type", envelope.type || null],
        ["sessionId", envelope.sessionId || null],
        ["found", Boolean(persistedSession)],
        ["state", persistedSession ? persistedSession.state : "not_found"],
      ]);

      if (!persistedSession) {
        if (diagnostics) {
          diagnostics.log("message_router_dispatch", {
            socketId: senderRegistration.connectionId,
            messageType: envelope.type || null,
            sessionId: envelope.sessionId || null,
            routed: false,
            reason: "session_not_found",
          });
        }

        logRoutingDecision(false, "session_not_found");
        logMessageDropped("session_not_found");

        return false;
      }

      const handshakeDuringPairing =
        envelope.type === "protocol_handshake" && persistedSession.state === SESSION_STATES.PAIRED;

      if (persistedSession.state !== SESSION_STATES.ACTIVE && !handshakeDuringPairing) {
        if (diagnostics) {
          diagnostics.log("message_router_dispatch", {
            socketId: senderRegistration.connectionId,
            messageType: envelope.type || null,
            sessionId: envelope.sessionId || null,
            routed: false,
            reason: "session_not_active",
          });
        }

        logRoutingDecision(false, "session_not_active");
        logMessageDropped("session_not_active");

        return false;
      }
    } else {
      logRelayAudit(diagnostics, "SESSION_LOOKUP", [
        ["type", envelope.type || null],
        ["sessionId", envelope.sessionId || null],
        ["found", true],
        ["state", "runtime_bound"],
      ]);
    }

    const destinationSocket =
      senderRegistration.role === "mobile" ? session.webSocket : session.mobileSocket;

    if (!canSendToSocket(destinationSocket)) {
      if (diagnostics) {
        diagnostics.log("message_router_dispatch", {
          socketId: senderRegistration.connectionId,
          messageType: envelope.type || null,
          sessionId: envelope.sessionId || null,
          routed: false,
          reason: "destination_unavailable",
        });
      }

      logRoutingDecision(false, "destination_unavailable");
      logMessageDropped("destination_unavailable");

      return false;
    }

    if (sessionLifecycleManager && (!persistedSession || persistedSession.state === SESSION_STATES.ACTIVE)) {
      await sessionLifecycleManager.persistBeforeRouting(() =>
        sessionLifecycleManager.refreshSessionActivity(envelope.sessionId, envelope.type),
      );
    }

    await forwardRoutedEnvelope(envelope, destinationSocket, senderRegistration, routingMetadata);

    if (
      sessionLifecycleManager &&
      persistedSession &&
      persistedSession.state === SESSION_STATES.PAIRED &&
      envelope.type === "protocol_handshake"
    ) {
      await sessionLifecycleManager.persistBeforeRouting(() =>
        sessionLifecycleManager.onPairApproved(envelope.sessionId),
      );
    }

    const targetRegistration = connectionManager.lookupSocket(destinationSocket);

    logRelayAudit(diagnostics, "FORWARD_MESSAGE", [
      ["type", envelope.type || null],
      ["sessionId", envelope.sessionId || null],
      ["targetSocket", targetRegistration ? targetRegistration.connectionId : null],
    ]);

    if (diagnostics) {
      diagnostics.log("message_router_dispatch", {
        socketId: senderRegistration.connectionId,
        messageType: envelope.type || null,
        sessionId: envelope.sessionId || null,
        routed: true,
        reason: null,
      });
    }

    logRoutingDecision(true, "forwarded");

    return true;
  }

  async function routeMessage(rawFrame, envelope, senderSocket, metadata = {}) {
    await assertForwardableRawFrame(rawFrame, envelope);

    if (envelope && typeof envelope === "object" && !Object.isFrozen(envelope)) {
      Object.freeze(envelope);
    }

    const routingMetadata = createRoutingMetadata(rawFrame, metadata);

    recordEventAudit("INBOUND", envelope);
    recordMutationAudit("INBOUND", envelope);

    if (typeof envelope.sessionId === "string" && envelope.sessionId.trim() !== "") {
      return sessionQueueManager.enqueueMessage(envelope.sessionId, envelope, {
        ...routingMetadata,
        senderSocket,
      });
    }

    return routeEnvelope(rawFrame, envelope, senderSocket, undefined, routingMetadata);
  }

  return {
    enableEventAudit,
    enableMutationAudit,
    getEventAudit,
    getMutationAudit,
    routeMessage,
    enqueueMessage: sessionQueueManager.enqueueMessage,
    initializeSessionQueue: sessionQueueManager.initializeSessionQueue,
    processQueue: sessionQueueManager.processQueue,
  };
}

function writeUpgradeRejection(socket, statusCode, statusText) {
  socket.write(`HTTP/1.1 ${statusCode} ${statusText}\r\nConnection: close\r\n\r\n`);
  socket.destroy();
}

function writeControlError(socket, reason, onSent) {
  socket.send(
    JSON.stringify({
      type: "control_error",
      payload: {
        reason,
      },
    }),
    onSent,
  );
}

function mergeConfiguration(configOverrides = {}) {
  const baseConfig = loadRelayConfiguration(configOverrides);
  return {
    ...baseConfig,
    ...configOverrides,
    pairing: {
      ...baseConfig.pairing,
      ...(configOverrides.pairing || {}),
    },
    rateLimiting: {
      ...baseConfig.rateLimiting,
      ...(configOverrides.rateLimiting || {}),
    },
    diagnostics: {
      ...baseConfig.diagnostics,
      ...(configOverrides.diagnostics || {}),
      ingress: {
        ...(baseConfig.diagnostics ? baseConfig.diagnostics.ingress : {}),
        ...((configOverrides.diagnostics && configOverrides.diagnostics.ingress) || {}),
      },
    },
    heartbeat: {
      ...(baseConfig.heartbeat || {}),
      ...(configOverrides.heartbeat || {}),
    },
    tls: {
      ...baseConfig.tls,
      ...(configOverrides.tls || {}),
    },
  };
}

function createRelayServer(configOverrides = {}) {
  const config = mergeConfiguration(configOverrides);
  const setRepeatingTimer =
    typeof configOverrides.setInterval === "function" ? configOverrides.setInterval : setInterval;
  const clearRepeatingTimer =
    typeof configOverrides.clearInterval === "function" ? configOverrides.clearInterval : clearInterval;
  const ingressDiagnostics = createRelayIngressDiagnostics({
    ...(config.diagnostics && config.diagnostics.ingress),
  });
  const connectionManager = createConnectionManager({ diagnostics: ingressDiagnostics });
  const events = new EventEmitter();
  const relayRateLimiter = createRelayRateLimiter(config.rateLimiting);
  const sessionRegistry = config.sessionRegistry || null;
  const sessionLifecycleManager = sessionRegistry
    ? createSessionLifecycleManager({
        sessionRegistry,
        connectionManager,
        events,
      })
    : null;
  const qrPairingSystem = sessionRegistry
    ? createQRPairingSystem({
        connectionManager,
        diagnostics: ingressDiagnostics,
      pairingSecret: config.pairing.secret,
        sessionLifecycleManager,
        sessionRegistry,
        pairingTtlMs: config.pairing.ttlMs,
        events,
      })
    : null;
  const messageRouter = createMessageRouter(connectionManager, {
    diagnostics: ingressDiagnostics,
    sessionRegistry,
    sessionLifecycleManager,
    dispatchTable: qrPairingSystem
      ? {
          qr_session_create: (senderSocket, envelope) =>
            qrPairingSystem.handleQrSessionCreate(senderSocket, envelope),
          pair_request: (senderSocket, envelope) => qrPairingSystem.handlePairRequest(senderSocket, envelope),
        }
      : {},
  });
  const heartbeatState = new Map();
  const heartbeatEnabled = Boolean(config.heartbeat && config.heartbeat.enabled);
  const heartbeatIntervalMs = heartbeatEnabled
    ? parseInteger(config.heartbeat && config.heartbeat.intervalMs, DEFAULT_HEARTBEAT_INTERVAL_MS)
    : 0;
  let heartbeatTimer = null;

  let relayServer;

  function unregisterHeartbeat(socket) {
    const state = heartbeatState.get(socket);
    if (!state) {
      return false;
    }

    if (typeof socket.off === "function") {
      socket.off("pong", state.onPong);
      socket.off("close", state.onClose);
    } else if (typeof socket.removeListener === "function") {
      socket.removeListener("pong", state.onPong);
      socket.removeListener("close", state.onClose);
    }

    heartbeatState.delete(socket);
    return true;
  }

  async function handleHeartbeatTimeout(socket, state) {
    events.emit("heartbeatTimeout", { connectionId: state.connectionId });

    const registration = connectionManager.lookupSocket(socket);
    const sessionId = registration && registration.sessionId ? registration.sessionId : null;

    if (sessionId && sessionLifecycleManager) {
      await sessionLifecycleManager.closeSession(sessionId, "heartbeat_timeout").catch((error) => {
        events.emit("heartbeatError", { connectionId: state.connectionId, error });
      });
    }

    unregisterHeartbeat(socket);

    if (typeof socket.terminate === "function") {
      socket.terminate();
      return;
    }

    if (typeof socket.close === "function") {
      socket.close(1001, "heartbeat_timeout");
    }
  }

  function registerHeartbeat(socket, connectionId) {
    if (!heartbeatEnabled || heartbeatIntervalMs <= 0) {
      return;
    }

    const state = {
      connectionId,
      awaitingPong: false,
      onPong() {
        const currentState = heartbeatState.get(socket);
        if (!currentState) {
          return;
        }

        currentState.awaitingPong = false;
        events.emit("heartbeatPong", { connectionId });

        const registration = connectionManager.lookupSocket(socket);
        if (!registration || !registration.sessionId || !sessionLifecycleManager) {
          return;
        }

        sessionLifecycleManager
          .refreshSessionActivity(registration.sessionId, "heartbeat_pong")
          .catch((error) => {
            events.emit("heartbeatError", { connectionId, error });
          });
      },
      onClose() {
        unregisterHeartbeat(socket);
      },
    };

    heartbeatState.set(socket, state);
    socket.on("pong", state.onPong);
    socket.on("close", state.onClose);
  }

  async function runHeartbeatSweep() {
    const sockets = [...heartbeatState.entries()];

    for (const [socket, state] of sockets) {
      if (!heartbeatState.has(socket)) {
        continue;
      }

      if (state.awaitingPong) {
        await handleHeartbeatTimeout(socket, state);
        continue;
      }

      state.awaitingPong = true;
      events.emit("heartbeatPing", { connectionId: state.connectionId });

      try {
        if (typeof socket.ping === "function") {
          socket.ping();
          continue;
        }
      } catch (error) {
        events.emit("heartbeatError", { connectionId: state.connectionId, error });
      }

      await handleHeartbeatTimeout(socket, state);
    }
  }

  if (heartbeatEnabled && heartbeatIntervalMs > 0) {
    heartbeatTimer = setRepeatingTimer(() => {
      runHeartbeatSweep().catch((error) => {
        events.emit("heartbeatError", { connectionId: null, error });
      });
    }, heartbeatIntervalMs);
  }

  function handleRateLimitViolation(socket, connectionId, remoteAddress, reason) {
    events.emit("rateLimitViolation", { connectionId, remoteAddress, reason });
    writeControlError(socket, reason, () => {
      if (typeof socket.close === "function") {
        socket.close(1008, reason);
      }
    });
  }

  const requestHandler = (req, res) => {
    if (req.method === "GET" && req.url === "/health") {
      const body = JSON.stringify({
        status: "ok",
        transport: config.tls.enabled ? "wss" : "ws",
        wsPath: config.wsPath,
        protocolVersion: config.protocolVersion,
        maxPayloadBytes: config.maxPayloadBytes,
        activeConnections: connectionManager.getConnectionCount(),
      });
      res.writeHead(200, {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body),
        "Cache-Control": "no-store",
      });
      res.end(body);
      return;
    }

    res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("Not Found");
  };

  const server = config.tls.enabled
    ? https.createServer(
        {
          key: fs.readFileSync(config.tls.keyPath, "utf8"),
          cert: fs.readFileSync(config.tls.certPath, "utf8"),
          minVersion: config.tls.minVersion,
        },
        requestHandler,
      )
    : http.createServer(requestHandler);

  const wsServer = new WebSocketServer({ noServer: true, clientTracking: false });

  server.on("upgrade", (req, socket, head) => {
    const requestUrl = new URL(
      req.url,
      `${config.tls.enabled ? "https" : "http"}://${req.headers.host || "localhost"}`,
    );

    if (requestUrl.pathname !== config.wsPath) {
      writeUpgradeRejection(socket, 404, "Not Found");
      return;
    }

    wsServer.handleUpgrade(req, socket, head, (ws) => {
      wsServer.emit("connection", ws, req);
    });
  });

  wsServer.on("connection", (socket, req) => {
    const remoteAddress = req.socket.remoteAddress || "unknown";
    const connectionId = buildConnectionId(req);
    connectionManager.registerConnection(socket, { connectionId, remoteAddress });
    registerHeartbeat(socket, connectionId);
    events.emit("connection", { remoteAddress });

    socket.on("message", async (rawMessage, isBinary) => {
      const rawFrame = Buffer.isBuffer(rawMessage) || typeof rawMessage === "string"
        ? rawMessage
        : coerceRawFrameBuffer(rawMessage);
      const rawMessageBuffer = coerceRawFrameBuffer(rawFrame);
      const rawMessageBytes = rawMessageBuffer.length;
      const rawPreview = formatRawAuditPreview(rawFrame);

      logRelayAudit(ingressDiagnostics, "RAW_WS_MESSAGE", [
        ["bytes", rawMessageBytes],
        ["raw", rawPreview],
        ["type", "unknown"],
        ["sessionId", "unknown"],
      ]);

      if (isBinary) {
        logRelayAudit(ingressDiagnostics, "MESSAGE_DROPPED", [
          ["type", "unknown"],
          ["sessionId", "unknown"],
          ["reason", "binary_frames_not_supported"],
        ]);
        socket.send(createTransportEnvelopeError("binary_frames_not_supported", config.protocolVersion));
        return;
      }

      ingressDiagnostics.log("ws_message_received", {
        socketId: connectionId,
        messageType: null,
        sessionId: null,
        rawMessageBytes,
      });

      const inboundViolation = relayRateLimiter.evaluateInboundMessage({
        socketId: connectionId,
        payloadSize: rawMessageBytes,
      });
      if (inboundViolation) {
        logRelayAudit(ingressDiagnostics, "MESSAGE_DROPPED", [
          ["type", "unknown"],
          ["sessionId", "unknown"],
          ["reason", inboundViolation.reason],
        ]);
        handleRateLimitViolation(socket, connectionId, remoteAddress, inboundViolation.reason);
        return;
      }

      let parsed;
      try {
        parsed = JSON.parse(rawMessageBuffer.toString("utf8"));
        logRelayAudit(ingressDiagnostics, "JSON_PARSE_SUCCESS", [
          ["type", parsed.type || null],
          ["sessionId", parsed.sessionId || null],
        ]);
        ingressDiagnostics.log("json_parse_success", {
          socketId: connectionId,
          messageType: parsed.type || null,
          sessionId: parsed.sessionId || null,
        });
      } catch {
        logRelayAudit(ingressDiagnostics, "JSON_PARSE_ERROR", [
          ["type", "unknown"],
          ["sessionId", "unknown"],
          ["raw", rawPreview],
        ]);
        ingressDiagnostics.log("json_parse_failure", {
          socketId: connectionId,
          messageType: null,
          sessionId: null,
        });
        logRelayAudit(ingressDiagnostics, "MESSAGE_DROPPED", [
          ["type", "unknown"],
          ["sessionId", "unknown"],
          ["reason", "invalid_json"],
        ]);
        socket.send(createTransportEnvelopeError("invalid_json", config.protocolVersion));
        return;
      }

      if (parsed && typeof parsed === "object" && !Object.isFrozen(parsed)) {
        Object.freeze(parsed);
      }

      const ingressHash = computeRawFrameHash(rawFrame);
      logRelayIntegrity("RELAY_INGRESS_HASH", [
        ["sessionId", parsed.sessionId || null],
        ["hash", ingressHash],
      ]);

      if (parsed.type === "qr_session_create") {
        const sessionCreationViolation = relayRateLimiter.recordSessionCreation(remoteAddress);
        if (sessionCreationViolation) {
          logRelayAudit(ingressDiagnostics, "MESSAGE_DROPPED", [
            ["type", parsed.type || null],
            ["sessionId", parsed.sessionId || null],
            ["reason", sessionCreationViolation.reason],
          ]);
          handleRateLimitViolation(socket, connectionId, remoteAddress, sessionCreationViolation.reason);
          return;
        }
      }

      const validation = validateTransportEnvelope(parsed, {
        protocolVersion: config.protocolVersion,
        maxPayloadBytes: config.maxPayloadBytes,
        diagnostics: ingressDiagnostics,
        socketId: connectionId,
      });
      logRelayAudit(ingressDiagnostics, "ENVELOPE_VALIDATION", [
        ["type", parsed.type || null],
        ["sessionId", parsed.sessionId || null],
        ["valid", validation.valid],
        ["reason", validation.reason || "none"],
      ]);
      if (!validation.valid) {
        logRelayAudit(ingressDiagnostics, "MESSAGE_DROPPED", [
          ["type", parsed.type || null],
          ["sessionId", parsed.sessionId || null],
          ["reason", validation.reason],
        ]);
        socket.send(createTransportEnvelopeError(validation.reason, config.protocolVersion));
        return;
      }

      events.emit("transportEnvelope", {
        envelope: parsed,
        rawFrame,
        ingressHash,
        remoteAddress,
      });

      let routed;
      try {
        routed = await messageRouter.routeMessage(rawFrame, parsed, socket, {
          rawFrame,
          ingressHash,
          isBinary,
        });
      } catch (error) {
        logRelayAudit(ingressDiagnostics, "ROUTING_DECISION", [
          ["type", parsed.type || null],
          ["sessionId", parsed.sessionId || null],
          ["routed", false],
          ["reason", error.message],
        ]);
        logRelayAudit(ingressDiagnostics, "MESSAGE_DROPPED", [
          ["type", parsed.type || null],
          ["sessionId", parsed.sessionId || null],
          ["reason", error.message],
        ]);
        events.emit("controlMessageError", {
          error,
          message: parsed,
          remoteAddress,
        });
        socket.send(
          JSON.stringify({
            type: "control_error",
            payload: {
              reason: error.message,
            },
          }),
        );
        return;
      }

      if (routed) {
        events.emit("messageRouted", {
          envelope: parsed,
          remoteAddress,
        });
        return;
      }

      events.emit("messageDropped", {
        envelope: parsed,
        remoteAddress,
        reason: "route_returned_false",
      });
    });

    socket.on("close", () => {
      if (sessionLifecycleManager) {
        sessionLifecycleManager.handleDisconnect(connectionId).catch((error) => {
          events.emit("sessionDisconnectError", { connectionId, remoteAddress, error });
        });
      }

      connectionManager.removeConnection(socket);
      events.emit("disconnect", { remoteAddress });
    });

    socket.on("error", (error) => {
      events.emit("socketError", { remoteAddress, error });
    });
  });

  relayServer = {
    config,
    connectionManager,
    events,
    messageRouter,
    qrPairingSystem,
    relayRateLimiter,
    ingressDiagnostics,
    sessionLifecycleManager,
    sessionRegistry,
    server,
    wsServer,
    start() {
      return new Promise((resolve, reject) => {
        server.once("error", reject);
        server.listen(config.port, config.host, () => {
          server.removeListener("error", reject);
          resolve(relayServer);
        });
      });
    },
    stop() {
      if (heartbeatTimer) {
        clearRepeatingTimer(heartbeatTimer);
        heartbeatTimer = null;
      }

      for (const socket of [...heartbeatState.keys()]) {
        unregisterHeartbeat(socket);
      }

      connectionManager.closeAllConnections();

      return new Promise((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        });
      });
    },
    getConnectionCount() {
      return connectionManager.getConnectionCount();
    },
  };

  return relayServer;
}

async function startRelayServer(configOverrides = {}) {
  const config = mergeConfiguration(configOverrides);
  let sessionRegistry = config.sessionRegistry || null;

  console.log("[chano_relay] Relay starting");

  if (!sessionRegistry) {
    if (!config.redisUrl) {
      throw new Error("REDIS_URL is required for relay startup");
    }

    sessionRegistry = await RedisSessionRegistry.connect(config.redisUrl);
    console.log("[chano_relay] Redis connected");
  }

  const relayServer = createRelayServer({
    ...configOverrides,
    sessionRegistry,
  });
  await relayServer.start();

  const address = relayServer.server.address();
  const protocol = relayServer.config.tls.enabled ? "wss" : "ws";
  console.log(
    `[chano_relay] listening on ${protocol}://${relayServer.config.host}:${address.port}${relayServer.config.wsPath}`,
  );

  return relayServer;
}

if (require.main === module) {
  startRelayServer().catch((error) => {
    console.error("[chano_relay] failed to start", error);
    process.exitCode = 1;
  });
}

module.exports = {
  DEFAULT_PROTOCOL_VERSION,
  DEFAULT_MAX_PAYLOAD_BYTES,
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  DEFAULT_PAIRING_TTL_MS,
  DEFAULT_SESSION_TTL_MS,
  DEFAULT_MAX_COMMANDS_PER_SECOND,
  DEFAULT_MAX_PAYLOAD_SIZE,
  DEFAULT_MAX_SESSIONS_PER_IP,
  BOOTSTRAP_MESSAGE_TYPES,
  QRPairingSystem,
  RelayRateLimiter,
  SessionLifecycleManager,
  SESSION_STATES,
  RedisSessionRegistry,
  connectRedisSessionRegistry,
  createRelayIngressDiagnostics,
  createConnectionManager,
  createMessageRouter,
  createQRPairingSystem,
  createRelayRateLimiter,
  createRedisSessionRegistry,
  createRelayServer,
  createSessionQueueManager,
  createSessionLifecycleManager,
  getRedisSessionKey,
  loadRelayConfiguration,
  signPairingTokenPayload,
  startRelayServer,
  validateTransportEnvelope,
};