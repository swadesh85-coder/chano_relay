const fs = require("fs");
const http = require("http");
const https = require("https");
const path = require("path");
const { EventEmitter } = require("events");
const { WebSocketServer } = require("ws");

const DEFAULT_PROTOCOL_VERSION = 2;
const DEFAULT_MAX_PAYLOAD_BYTES = 1024 * 1024;
const DEFAULT_CONFIG_PATH = path.join(__dirname, "relay.config.json");

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
    protocolVersion: parseInteger(
      env.RELAY_PROTOCOL_VERSION,
      parseInteger(fileConfig.protocolVersion, DEFAULT_PROTOCOL_VERSION),
    ),
    maxPayloadBytes: parseInteger(
      env.RELAY_MAX_PAYLOAD_BYTES,
      parseInteger(fileConfig.maxPayloadBytes, DEFAULT_MAX_PAYLOAD_BYTES),
    ),
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

  if (!message || typeof message !== "object" || Array.isArray(message)) {
    return { valid: false, reason: "invalid_transport_envelope" };
  }

  const hasSequence = Number.isInteger(message.sequence) && message.sequence >= 0;
  const hasTimestamp = typeof message.timestamp === "number" && Number.isFinite(message.timestamp);
  const hasType = typeof message.type === "string";
  const hasPayload = Object.prototype.hasOwnProperty.call(message, "payload");

  if (!Object.prototype.hasOwnProperty.call(message, "protocolVersion")) {
    return { valid: false, reason: "missing_protocol_version" };
  }

  if (message.protocolVersion !== protocolVersion) {
    return { valid: false, reason: "unsupported_protocol_version" };
  }

  if (typeof message.sessionId !== "string" || message.sessionId.trim() === "") {
    return { valid: false, reason: "missing_session_id" };
  }

  if (typeof message.type !== "string" || message.type.trim() === "") {
    return { valid: false, reason: "missing_type" };
  }

  if (!hasType || !hasTimestamp || !hasSequence || !hasPayload) {
    return { valid: false, reason: "invalid_transport_envelope" };
  }

  if (getPayloadSizeBytes(message.payload) > maxPayloadBytes) {
    return { valid: false, reason: "payload_too_large" };
  }

  return { valid: true };
}

function createConnectionManager() {
  const socketRegistry = new Map();
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

  function registerConnection(socket) {
    const existing = socketRegistry.get(socket);
    if (existing) {
      return existing;
    }

    const onClose = () => {
      removeConnection(socket);
    };

    const registration = {
      socket,
      sessionId: null,
      role: null,
      onClose,
    };

    socketRegistry.set(socket, registration);
    socket.once("close", onClose);
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

    socketRegistry.delete(socket);
    return true;
  }

  function lookupConnection(sessionId) {
    return sessionRegistry.get(sessionId) || null;
  }

  function lookupSocket(socket) {
    return socketRegistry.get(socket) || null;
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

    return session;
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
    lookupSocket,
    bindSessionSockets,
    closeAllConnections,
    getConnectionCount,
  };
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

function createMessageRouter(connectionManager) {
  function routeMessage(envelope, senderSocket) {
    const senderRegistration = connectionManager.lookupSocket(senderSocket);
    if (!senderRegistration || !senderRegistration.sessionId || !senderRegistration.role) {
      return false;
    }

    if (senderRegistration.sessionId !== envelope.sessionId) {
      return false;
    }

    const session = connectionManager.lookupConnection(envelope.sessionId);
    if (!session) {
      return false;
    }

    const destinationSocket =
      senderRegistration.role === "mobile" ? session.webSocket : session.mobileSocket;

    if (!canSendToSocket(destinationSocket)) {
      return false;
    }

    destinationSocket.send(JSON.stringify(envelope));
    return true;
  }

  return {
    routeMessage,
  };
}

function writeUpgradeRejection(socket, statusCode, statusText) {
  socket.write(`HTTP/1.1 ${statusCode} ${statusText}\r\nConnection: close\r\n\r\n`);
  socket.destroy();
}

function mergeConfiguration(configOverrides = {}) {
  const baseConfig = loadRelayConfiguration(configOverrides);
  return {
    ...baseConfig,
    ...configOverrides,
    tls: {
      ...baseConfig.tls,
      ...(configOverrides.tls || {}),
    },
  };
}

function createRelayServer(configOverrides = {}) {
  const config = mergeConfiguration(configOverrides);
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const events = new EventEmitter();

  let relayServer;

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
    connectionManager.registerConnection(socket);
    events.emit("connection", { remoteAddress });

    socket.on("message", (rawMessage, isBinary) => {
      if (isBinary) {
        socket.send(createTransportEnvelopeError("binary_frames_not_supported", config.protocolVersion));
        return;
      }

      let parsed;
      try {
        parsed = JSON.parse(rawMessage.toString("utf8"));
      } catch {
        socket.send(createTransportEnvelopeError("invalid_json", config.protocolVersion));
        return;
      }

      const validation = validateTransportEnvelope(parsed, {
        protocolVersion: config.protocolVersion,
        maxPayloadBytes: config.maxPayloadBytes,
      });
      if (!validation.valid) {
        socket.send(createTransportEnvelopeError(validation.reason, config.protocolVersion));
        return;
      }

      events.emit("transportEnvelope", {
        envelope: parsed,
        remoteAddress,
      });

      const routed = messageRouter.routeMessage(parsed, socket);
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
      });
    });

    socket.on("close", () => {
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

async function startRelayServer() {
  const relayServer = createRelayServer();
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
  createConnectionManager,
  createMessageRouter,
  createRelayServer,
  loadRelayConfiguration,
  startRelayServer,
  validateTransportEnvelope,
};