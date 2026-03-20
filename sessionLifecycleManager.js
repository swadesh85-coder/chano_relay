const { SESSION_STATES } = require("./redisSessionRegistry");

function assertNonEmptyString(value, label) {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`${label} is required`);
  }
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
    this.expirationTimers = new Map();
  }

  async createSession(sessionId, webSocketId, expiresAt) {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(webSocketId, "webSocketId");

    const session = await this.sessionRegistry.createSession(sessionId, webSocketId, expiresAt);
    this.indexSession(session);
    this.scheduleExpiration(session.sessionId, session.expiresAt);
    this.logger("SESSION_CREATE persisted");
    return session;
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

    const sessionWithMobile = await this.sessionRegistry.updateSessionOnPair(sessionId, mobileSocketId);

    this.indexSession(sessionWithMobile);
    this.logger("SESSION_UPDATE paired");
    return sessionWithMobile;
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

    this.bindLiveSockets(session);
    const activeSession = await this.sessionRegistry.activateSession(sessionId);
    this.indexSession(activeSession);
    this.logger("SESSION_UPDATE active");
    return activeSession;
  }

  async transitionToClosed(sessionId, reason) {
    assertNonEmptyString(sessionId, "sessionId");
    assertNonEmptyString(reason, "reason");

    const session = await this.sessionRegistry.getSession(sessionId);
    if (!session) {
      this.clearExpiration(sessionId);
      return false;
    }

    this.clearExpiration(sessionId);

    let closedSession = session;
    if (session.state !== SESSION_STATES.CLOSED) {
      closedSession = await this.sessionRegistry.updateSession(sessionId, {
        state: SESSION_STATES.CLOSED,
      });
    }

    this.notifySessionClosed(closedSession, reason);
    this.unindexSession(closedSession);
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

    return this.transitionToClosed(sessionId, "disconnect");
  }

  async attachMobile(sessionId, mobileSocketId) {
    return this.transitionToPaired(sessionId, mobileSocketId);
  }

  async activateSession(sessionId) {
    return this.transitionToActive(sessionId);
  }

  async closeSession(sessionId, reason) {
    return this.transitionToClosed(sessionId, reason);
  }

  indexSession(session) {
    this.sessionSocketIds.set(session.sessionId, {
      webSocketId: session.webSocketId,
      mobileSocketId: session.mobileSocketId,
    });

    this.socketSessionIds.set(session.webSocketId, session.sessionId);
    if (session.mobileSocketId) {
      this.socketSessionIds.set(session.mobileSocketId, session.sessionId);
    }
  }

  unindexSession(session) {
    this.sessionSocketIds.delete(session.sessionId);
    if (session.webSocketId) {
      this.socketSessionIds.delete(session.webSocketId);
    }
    if (session.mobileSocketId) {
      this.socketSessionIds.delete(session.mobileSocketId);
    }
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

  scheduleExpiration(sessionId, expiresAt) {
    this.clearExpiration(sessionId);
    const delayMs = Math.max(0, expiresAt - this.now());
    const timer = this.setTimer(() => {
      return this.transitionToClosed(sessionId, "timeout").catch((error) => {
        if (this.events) {
          this.events.emit("sessionExpirationError", { sessionId, error });
        }
      });
    }, delayMs);

    this.expirationTimers.set(sessionId, timer);
  }

  clearExpiration(sessionId) {
    const timer = this.expirationTimers.get(sessionId);
    if (!timer) {
      return;
    }

    this.clearTimer(timer);
    this.expirationTimers.delete(sessionId);
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