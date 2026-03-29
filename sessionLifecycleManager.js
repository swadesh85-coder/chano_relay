const { SESSION_STATES } = require("./redisSessionRegistry");

function assertNonEmptyString(value, label) {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`${label} is required`);
  }
}

function assertTransportEnvelope(envelope, expectedType) {
  if (!envelope || typeof envelope !== "object" || Array.isArray(envelope)) {
    throw new Error("TransportEnvelope is required");
  }

  assertNonEmptyString(envelope.type, "type");
  assertNonEmptyString(envelope.sessionId, "sessionId");

  if (expectedType && envelope.type !== expectedType) {
    throw new Error(`Expected transport envelope type ${expectedType}`);
  }
}

function formatSessionAuditRecord(session) {
  return JSON.stringify({
    sessionId: session.sessionId,
    token: session.token || null,
    webSocketId: session.webSocketId || null,
    mobileSocketId: session.mobileSocketId || null,
    state: session.state,
  });
}

function normalizeCreateSessionRequest(sessionIdOrOptions, webSocketId, expiresAt) {
  if (sessionIdOrOptions && typeof sessionIdOrOptions === "object" && !Array.isArray(sessionIdOrOptions)) {
    return {
      sessionId: sessionIdOrOptions.sessionId,
      token: sessionIdOrOptions.token,
      webSocketId: sessionIdOrOptions.webSocketId,
      expiresAt: sessionIdOrOptions.expiresAt,
    };
  }

  return {
    sessionId: sessionIdOrOptions,
    token: null,
    webSocketId,
    expiresAt,
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

class SessionLifecycleManager {
  constructor(options) {
    const {
      sessionRegistry,
      connectionManager,
      now = () => Date.now(),
      setTimer = (callback, delay) => setTimeout(callback, delay),
      clearTimer = (handle) => clearTimeout(handle),
      events,
      logger = console.log,
    } = options || {};

    if (!sessionRegistry) {
      throw new Error("sessionRegistry is required");
    }

    if (!connectionManager) {
      throw new Error("connectionManager is required");
    }

    this.sessionRegistry = sessionRegistry;
    this.connectionManager = connectionManager;
    this.now = now;
    this.setTimer = setTimer;
    this.clearTimer = clearTimer;
    this.events = events;
    this.logger = typeof logger === "function" ? logger : console.log;
    this.sessionSocketIds = new Map();
    this.socketSessionIds = new Map();
  }

  async createSession(sessionIdOrOptions, webSocketId, expiresAt) {
    const createRequest = normalizeCreateSessionRequest(sessionIdOrOptions, webSocketId, expiresAt);

    assertNonEmptyString(createRequest.sessionId, "sessionId");
    assertNonEmptyString(createRequest.webSocketId, "webSocketId");

    const session = await this.sessionRegistry.createSession(createRequest);
    this.indexSession({
      ...session,
      webSocketId: createRequest.webSocketId,
    });
    this.scheduleTimeout(session.sessionId, this.resolveTimeoutDelayMs(session));
    this.logger(`RELAY_SESSION_CREATED session=${formatSessionAuditRecord(session)}`);
    this.logger("SESSION_CREATE persisted");
    return session;
  }

  async onQrSessionCreate(envelope, webSocketId, expiresAt, tokenOverride = null) {
    assertTransportEnvelope(envelope, "qr_session_create");
    assertNonEmptyString(webSocketId, "webSocketId");

    return this.createSession({
      sessionId: envelope.sessionId,
      token: tokenOverride,
      webSocketId,
      expiresAt,
    });
  }

  async transitionToPaired(sessionId, mobileSocketId) {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(mobileSocketId, "mobileSocketId");

    const session = await this.sessionRegistry.getSession(sessionId);
    if (!session) {
      throw new Error(`Session not found for ${sessionId}`);
    }

    if (session.state !== SESSION_STATES.WAITING) {
      throw new Error(`Session ${sessionId} must be waiting before pairing`);
    }

    const runtimeSession = this.getOrCreateRuntimeSession(sessionId);

    if (!runtimeSession.webSocketId) {
      throw new Error(`Session ${sessionId} requires a waiting web socket before pairing`);
    }

    const sessionWithMobile = await this.sessionRegistry.updateSessionOnPair(
      sessionId,
      mobileSocketId,
      runtimeSession.webSocketId,
    );

    this.indexSession(sessionWithMobile);
    this.bindLiveSockets(sessionWithMobile);
    this.logger("SESSION_UPDATE paired");
    return sessionWithMobile;
  }

  async onPairRequest(envelope, mobileSocketId) {
    assertTransportEnvelope(envelope, "pair_request");
    assertNonEmptyString(mobileSocketId, "mobileSocketId");

    return this.transitionToPaired(envelope.sessionId, mobileSocketId);
  }

  async transitionToActive(sessionId) {
    assertNonEmptyString(sessionId, "sessionId");

    const session = await this.sessionRegistry.getSession(sessionId);
    if (!session) {
      throw new Error(`Session not found for ${sessionId}`);
    }

    if (session.state !== SESSION_STATES.PAIRED) {
      throw new Error(`Session ${sessionId} must be paired before activation`);
    }

    if (!session.mobileSocketId || !session.webSocketId) {
      throw new Error(`Session ${sessionId} requires mobile and web sockets before activation`);
    }

    const activeSession = await this.sessionRegistry.activateSession(sessionId);
    this.indexSession(activeSession);
    this.scheduleTimeout(activeSession.sessionId, this.getFullSessionTtlMs());
    this.logger("SESSION_UPDATE active");
    return activeSession;
  }

  async onPairApproved(sessionId) {
    assertNonEmptyString(sessionId, "sessionId");

    return this.transitionToActive(sessionId);
  }

  async persistBeforeRouting(operation) {
    if (typeof operation !== "function") {
      throw new Error("operation must be a function");
    }

    return await operation();
  }

  async refreshSessionActivity(sessionId, messageType = "activity") {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(messageType, "messageType");

    const session = await this.sessionRegistry.getSession(sessionId);
    if (!session) {
      throw new Error(`Session not found for ${sessionId}`);
    }

    if (session.state !== SESSION_STATES.ACTIVE) {
      throw new Error(`Session ${sessionId} must be active before refreshing TTL`);
    }

    const refreshedSession = await this.sessionRegistry.refreshSessionTtl(sessionId, "activity");
    this.scheduleTimeout(refreshedSession.sessionId, this.getFullSessionTtlMs());
    this.logger(`MESSAGE_RECEIVED type=${messageType}`);

    if (this.events) {
      this.events.emit("sessionActivity", {
        sessionId,
        messageType,
        expiresAt: refreshedSession.expiresAt,
      });
    }

    return refreshedSession;
  }

  async transitionToClosed(sessionId, reason) {
    return this.closeSession(sessionId, reason);
  }

  async closeSession(sessionId, reason) {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(reason, "reason");

    const session = await this.sessionRegistry.getSession(sessionId);
    const indexedSession = this.getIndexedSession(sessionId);
    if (!session) {
      this.clearPreviousTimer(sessionId);

      if (!indexedSession) {
        return false;
      }

      this.notifySessionClosed(indexedSession, reason);
      if (typeof this.connectionManager.unbindSession === "function") {
        this.connectionManager.unbindSession(sessionId);
      }
      this.unindexSession(indexedSession, { clearTimer: false });
      this.logger(`SESSION_CLOSED reason=${reason}`);

      if (this.events) {
        this.events.emit("sessionClosed", { sessionId, reason });
      }

      return true;
    }

    if (session.state === SESSION_STATES.CLOSED) {
      this.unindexSession(session, { clearTimer: false });
      return false;
    }

    this.clearPreviousTimer(sessionId);

    const closedSession = {
      ...session,
      webSocketId: session.webSocketId || (indexedSession ? indexedSession.webSocketId : null),
      mobileSocketId: session.mobileSocketId || (indexedSession ? indexedSession.mobileSocketId : null),
      state: SESSION_STATES.CLOSED,
    };

    this.notifySessionClosed(closedSession, reason);
    if (typeof this.connectionManager.unbindSession === "function") {
      this.connectionManager.unbindSession(sessionId);
    }
    await this.sessionRegistry.deleteSession(sessionId);
    this.unindexSession(closedSession, { clearTimer: false });
    this.logger(`SESSION_CLOSED reason=${reason}`);

    if (this.events) {
      this.events.emit("sessionClosed", { sessionId, reason });
    }

    return true;
  }

  async handleDisconnect(socketId) {
    assertNonEmptyString(socketId, "socketId");

    const sessionId = this.socketSessionIds.get(socketId);
    if (!sessionId) {
      return false;
    }

    return this.closeSession(sessionId, "disconnect");
  }

  async attachMobile(sessionId, mobileSocketId) {
    return this.transitionToPaired(sessionId, mobileSocketId);
  }

  async activateSession(sessionId) {
    return this.transitionToActive(sessionId);
  }

  indexSession(session) {
    const runtimeSession = this.getOrCreateRuntimeSession(session.sessionId);
    runtimeSession.webSocketId = session.webSocketId || null;
    runtimeSession.mobileSocketId = session.mobileSocketId || null;

    if (session.webSocketId) {
      this.socketSessionIds.set(session.webSocketId, session.sessionId);
    }
    if (session.mobileSocketId) {
      this.socketSessionIds.set(session.mobileSocketId, session.sessionId);
    }
  }

  getRuntimeSession(sessionId) {
    const runtimeSession = this.sessionSocketIds.get(sessionId);
    if (!runtimeSession) {
      return null;
    }

    return {
      sessionId,
      webSocketId: runtimeSession.webSocketId || null,
      mobileSocketId: runtimeSession.mobileSocketId || null,
    };
  }

  unindexSession(session, options = {}) {
    const shouldClearTimer = options.clearTimer !== false;
    if (shouldClearTimer) {
      this.clearPreviousTimer(session.sessionId);
    }

    this.sessionSocketIds.delete(session.sessionId);
    if (session.webSocketId) {
      this.socketSessionIds.delete(session.webSocketId);
    }
    if (session.mobileSocketId) {
      this.socketSessionIds.delete(session.mobileSocketId);
    }
  }

  getIndexedSession(sessionId) {
    const runtimeSession = this.sessionSocketIds.get(sessionId);
    if (!runtimeSession) {
      return null;
    }

    return {
      sessionId,
      state: SESSION_STATES.CLOSED,
      webSocketId: runtimeSession.webSocketId || null,
      mobileSocketId: runtimeSession.mobileSocketId || null,
    };
  }

  getOrCreateRuntimeSession(sessionId) {
    let runtimeSession = this.sessionSocketIds.get(sessionId);
    if (!runtimeSession) {
      runtimeSession = {
        sessionId,
        webSocketId: null,
        mobileSocketId: null,
        timeoutHandle: null,
      };
      this.sessionSocketIds.set(sessionId, runtimeSession);
    }

    return runtimeSession;
  }

  bindLiveSockets(session) {
    const webRegistration = this.connectionManager.lookupConnectionById(session.webSocketId);
    const mobileRegistration = this.connectionManager.lookupConnectionById(session.mobileSocketId);

    if (!webRegistration || !webRegistration.socket) {
      throw new Error(`Web socket is unavailable for ${session.sessionId}`);
    }

    if (!mobileRegistration || !mobileRegistration.socket) {
      throw new Error(`Mobile socket is unavailable for ${session.sessionId}`);
    }

    this.connectionManager.bindSessionSockets(session.sessionId, mobileRegistration.socket, webRegistration.socket);
  }

  storeTimerHandle(sessionId, timeoutHandle) {
    const runtimeSession = this.getOrCreateRuntimeSession(sessionId);
    runtimeSession.timeoutHandle = timeoutHandle;
  }

  clearPreviousTimer(sessionId) {
    const runtimeSession = this.sessionSocketIds.get(sessionId);
    const previousHandle = runtimeSession ? runtimeSession.timeoutHandle : null;

    this.logger(`TIMER_CLEAR previousHandle=${Boolean(previousHandle)}`);

    if (!previousHandle) {
      return false;
    }

    this.clearTimer(previousHandle);
    runtimeSession.timeoutHandle = null;
    return true;
  }

  scheduleTimeout(sessionId, ttlMs) {
    const reset = this.clearPreviousTimer(sessionId);
    if (reset) {
      this.logger(`TIMER_RESET sessionId=${sessionId}`);
    }

    const boundedTtlMs = Math.max(0, ttlMs);
    const timer = this.setTimer(() => {
      return this.closeSession(sessionId, "timeout").catch((error) => {
        if (this.events) {
          this.events.emit("sessionExpirationError", { sessionId, error });
        }
      });
    }, boundedTtlMs);

    this.storeTimerHandle(sessionId, timer);
    this.logger(`TIMER_SET sessionId=${sessionId} ttlMs=${boundedTtlMs}`);
    return timer;
  }

  resolveTimeoutDelayMs(session) {
    if (session.state === SESSION_STATES.ACTIVE) {
      return this.getFullSessionTtlMs();
    }

    return Math.max(0, session.expiresAt - this.now());
  }

  getFullSessionTtlMs() {
    if (typeof this.sessionRegistry.getSessionTtlMs === "function") {
      return this.sessionRegistry.getSessionTtlMs();
    }

    return 0;
  }

  getTimerHandle(sessionId) {
    const runtimeSession = this.sessionSocketIds.get(sessionId);
    return runtimeSession ? runtimeSession.timeoutHandle : null;
  }

  notifySessionClosed(session, reason) {
    const payload = JSON.stringify({
      type: "session_close",
      payload: {
        reason,
      },
    });

    for (const socketId of [session.webSocketId, session.mobileSocketId]) {
      if (!socketId) {
        continue;
      }

      const registration = this.connectionManager.lookupConnectionById(socketId);
      if (registration && canSendToSocket(registration.socket)) {
        registration.socket.send(payload);
      }
    }
  }
}

function createSessionLifecycleManager(options) {
  return new SessionLifecycleManager(options);
}

module.exports = {
  SessionLifecycleManager,
  createSessionLifecycleManager,
};