const crypto = require("crypto");

const { SESSION_STATES } = require("./redisSessionRegistry");

const DEFAULT_PAIRING_TTL_MS = 2 * 60 * 1000;
const DEFAULT_PROTOCOL_VERSION = 2;

function assertNonEmptyString(value, label) {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`${label} is required`);
  }
}

function encodeTokenSegment(value) {
  return Buffer.from(value, "utf8").toString("base64url");
}

function decodeTokenSegment(value) {
  return Buffer.from(value, "base64url").toString("utf8");
}

function createTransportResponseEnvelope(sourceEnvelope, type, sessionId, payload) {
  return {
    protocolVersion:
      sourceEnvelope && Number.isInteger(sourceEnvelope.protocolVersion)
        ? sourceEnvelope.protocolVersion
        : DEFAULT_PROTOCOL_VERSION,
    type,
    sessionId,
    timestamp: Date.now(),
    sequence:
      sourceEnvelope && Number.isInteger(sourceEnvelope.sequence) && sourceEnvelope.sequence >= 0
        ? sourceEnvelope.sequence + 1
        : 0,
    payload,
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

function signPairingTokenPayload(payload, secret) {
  const payloadJson = JSON.stringify(payload);
  const payloadSegment = encodeTokenSegment(payloadJson);
  const signatureSegment = crypto
    .createHmac("sha256", secret)
    .update(payloadSegment)
    .digest("base64url");

  return `${payloadSegment}.${signatureSegment}`;
}

function verifyPairingToken(token, secret, expectedSessionId, now = Date.now()) {
  assertNonEmptyString(token, "token");
  assertNonEmptyString(secret, "pairing secret");

  const [payloadSegment, signatureSegment] = token.split(".");
  if (!payloadSegment || !signatureSegment) {
    return { valid: false, reason: "invalid_token" };
  }

  const expectedSignature = crypto
    .createHmac("sha256", secret)
    .update(payloadSegment)
    .digest();

  let actualSignature;
  try {
    actualSignature = Buffer.from(signatureSegment, "base64url");
  } catch {
    return { valid: false, reason: "invalid_token" };
  }

  if (
    expectedSignature.length !== actualSignature.length ||
    !crypto.timingSafeEqual(expectedSignature, actualSignature)
  ) {
    return { valid: false, reason: "invalid_token" };
  }

  let payload;
  try {
    payload = JSON.parse(decodeTokenSegment(payloadSegment));
  } catch {
    return { valid: false, reason: "invalid_token" };
  }

  if (payload.sessionId !== expectedSessionId) {
    return { valid: false, reason: "session_mismatch" };
  }

  if (typeof payload.expiresAt !== "number" || !Number.isFinite(payload.expiresAt)) {
    return { valid: false, reason: "invalid_token" };
  }

  if (payload.expiresAt <= now) {
    return { valid: false, reason: "pair_request_expired" };
  }

  return {
    valid: true,
    payload,
  };
}

class QRPairingSystem {
  constructor(options) {
    const {
      connectionManager,
      diagnostics,
      sessionLifecycleManager,
      sessionRegistry,
      pairingTtlMs = DEFAULT_PAIRING_TTL_MS,
      now = () => Date.now(),
      events,
      logger = console.log,
    } = options || {};

    if (!connectionManager) {
      throw new Error("connectionManager is required");
    }

    if (!sessionRegistry) {
      throw new Error("sessionRegistry is required");
    }

    if (!sessionLifecycleManager) {
      throw new Error("sessionLifecycleManager is required");
    }

    this.connectionManager = connectionManager;
    this.diagnostics = diagnostics || null;
    this.sessionLifecycleManager = sessionLifecycleManager;
    this.sessionRegistry = sessionRegistry;
    this.pairingTtlMs = pairingTtlMs;
    this.now = now;
    this.events = events;
    this.logger = typeof logger === "function" ? logger : console.log;
  }

  logInvocation(message, senderSocket) {
    const senderRegistration = this.connectionManager.lookupSocket(senderSocket);

    if (this.diagnostics) {
      this.diagnostics.log("qr_handler_invoked", {
        socketId: senderRegistration ? senderRegistration.connectionId : null,
        messageType: message && typeof message === "object" ? message.type || null : null,
        sessionId: message && typeof message === "object" ? message.sessionId || null : null,
        handled:
          Boolean(message) &&
          typeof message === "object" &&
          (message.type === "qr_session_create" || message.type === "pair_request"),
      });
    }
  }

  async handleMessage(message, senderSocket) {
    this.logInvocation(message, senderSocket);

    if (!message || typeof message !== "object" || Array.isArray(message)) {
      return { handled: false };
    }

    if (message.type === "qr_session_create") {
      return this.handleQrSessionCreate(senderSocket, message);
    }

    if (message.type === "pair_request") {
      return this.handlePairRequest(senderSocket, message);
    }

    return { handled: false };
  }

  async handleQrSessionCreate(senderSocket, envelope = {}) {
    this.logInvocation(envelope, senderSocket);
    this.logger("[relay_ingress] qr_session_create received");

    assertNonEmptyString(envelope.sessionId, "sessionId");

    const senderRegistration = this.connectionManager.lookupSocket(senderSocket);
    if (!senderRegistration || !senderRegistration.connectionId) {
      throw new Error("Web connection is not registered for pairing");
    }

    this.assertSocketRole(senderRegistration, "web", "web_socket_required");

    const sessionId = envelope.sessionId;
    const expiresAt = this.now() + this.pairingTtlMs;

    await this.sessionLifecycleManager.createSession(sessionId, senderRegistration.connectionId, expiresAt);
    this.logger("[relay_session] session created");

    senderSocket.send(
      JSON.stringify(
        createTransportResponseEnvelope(envelope, "qr_session_ready", sessionId, {
          sessionId,
          expiresAt,
        }),
      ),
    );

    if (this.events) {
      this.events.emit("qrSessionReady", { sessionId, expiresAt, webSocketId: senderRegistration.connectionId });
    }

    return { handled: true, sessionId };
  }

  async handlePairRequest(senderSocket, envelope) {
    this.logInvocation(envelope, senderSocket);
    this.logger("[relay_ingress] pair_request received");

    assertNonEmptyString(envelope.sessionId, "sessionId");

    const senderRegistration = this.connectionManager.lookupSocket(senderSocket);
    if (!senderRegistration || !senderRegistration.connectionId) {
      throw new Error("Mobile connection is not registered for pairing");
    }

    this.assertSocketRole(senderRegistration, "mobile", "mobile_socket_required");

    const session = await this.sessionRegistry.getSession(envelope.sessionId);
    if (!session) {
      return this.rejectPairRequest(senderSocket, envelope, "session_not_found");
    }

    if (session.expiresAt <= this.now()) {
      await this.sessionRegistry.deleteSession(envelope.sessionId);
      return this.rejectPairRequest(senderSocket, envelope, "pair_request_expired");
    }

    if (session.state !== SESSION_STATES.WAITING) {
      return this.rejectPairRequest(senderSocket, envelope, "invalid_session_state");
    }

    if (session.webSocketId === senderRegistration.connectionId) {
      return this.rejectPairRequest(senderSocket, envelope, "mobile_socket_required");
    }

    const webRegistration = this.connectionManager.lookupConnectionById(session.webSocketId);
    if (!webRegistration || !canSendToSocket(webRegistration.socket)) {
      return this.rejectPairRequest(senderSocket, envelope, "web_connection_not_available");
    }

    const pairedSession = await this.sessionLifecycleManager.transitionToPaired(
      envelope.sessionId,
      senderRegistration.connectionId,
    );
    this.logger("[relay_session] state=paired");

    if (!pairedSession.webSocketId || !pairedSession.mobileSocketId) {
      return this.rejectPairRequest(senderSocket, envelope, "session_not_ready");
    }

    const activeSession = await this.sessionLifecycleManager.transitionToActive(envelope.sessionId);
    this.logger("[relay_session] state=active");

    const approvalEnvelope = createTransportResponseEnvelope(envelope, "pair_approved", envelope.sessionId, {
      sessionId: envelope.sessionId,
      state: SESSION_STATES.ACTIVE,
    });

    senderSocket.send(JSON.stringify(approvalEnvelope));
    webRegistration.socket.send(JSON.stringify(approvalEnvelope));
    this.logger(`[relay_router] routing enabled for session ${activeSession.sessionId}`);

    if (this.events) {
      this.events.emit("pairApproved", {
        sessionId: envelope.sessionId,
        mobileSocketId: senderRegistration.connectionId,
        webSocketId: session.webSocketId,
      });
    }

    return { handled: true, sessionId: envelope.sessionId };
  }

  assertSocketRole(registration, expectedRole, reason) {
    if (registration.clientRole && registration.clientRole !== expectedRole) {
      throw new Error(reason);
    }

    registration.clientRole = expectedRole;
  }

  rejectPairRequest(socket, envelope, reason) {
    socket.send(
      JSON.stringify({
        ...createTransportResponseEnvelope(
          envelope,
          "pair_rejected",
          envelope && envelope.sessionId ? envelope.sessionId : null,
          { reason },
        ),
      }),
    );

    return { handled: true, rejected: true, reason };
  }
}

function createQRPairingSystem(options) {
  return new QRPairingSystem(options);
}

module.exports = {
  DEFAULT_PAIRING_TTL_MS,
  QRPairingSystem,
  createQRPairingSystem,
  signPairingTokenPayload,
  verifyPairingToken,
};