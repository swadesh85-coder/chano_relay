const SESSION_STATES = Object.freeze({
  WAITING: "waiting",
  PAIRED: "paired",
  ACTIVE: "active",
  CLOSED: "closed",
});

const UUID_V4_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const ALLOWED_STATE_TRANSITIONS = Object.freeze({
  [SESSION_STATES.WAITING]: new Set([
    SESSION_STATES.WAITING,
    SESSION_STATES.PAIRED,
    SESSION_STATES.CLOSED,
  ]),
  [SESSION_STATES.PAIRED]: new Set([SESSION_STATES.PAIRED, SESSION_STATES.ACTIVE, SESSION_STATES.CLOSED]),
  [SESSION_STATES.ACTIVE]: new Set([SESSION_STATES.ACTIVE, SESSION_STATES.CLOSED]),
  [SESSION_STATES.CLOSED]: new Set([SESSION_STATES.CLOSED]),
});

const REDIS_CLIENT_PACKAGE_NAME = "redis";

function assertNonEmptyString(value, label) {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`${label} is required`);
  }
}

function assertSessionId(sessionId) {
  assertNonEmptyString(sessionId, "sessionId");

  if (!UUID_V4_PATTERN.test(sessionId)) {
    throw new Error("sessionId must be a UUID");
  }
}

function assertState(state) {
  assertNonEmptyString(state, "state");

  if (!Object.values(SESSION_STATES).includes(state)) {
    throw new Error(`Unsupported session state: ${state}`);
  }
}

function normalizeExpiresAt(expiresAt) {
  if (expiresAt instanceof Date) {
    const timestamp = expiresAt.getTime();
    if (Number.isFinite(timestamp)) {
      return timestamp;
    }
  }

  if (typeof expiresAt === "number" && Number.isFinite(expiresAt)) {
    return expiresAt;
  }

  if (typeof expiresAt === "string" && expiresAt.trim() !== "") {
    const timestamp = Date.parse(expiresAt);
    if (Number.isFinite(timestamp)) {
      return timestamp;
    }
  }

  throw new Error("expiresAt must be a valid timestamp");
}

function assertSocketId(socketId, label) {
  assertNonEmptyString(socketId, label);
}

function toRedisEpochSeconds(expiresAtMs) {
  return Math.ceil(expiresAtMs / 1000);
}

function getRedisSessionKey(sessionId) {
  return `session:${sessionId}`;
}

function serializeSession(session) {
  return JSON.stringify({
    sessionId: session.sessionId,
    webSocketId: session.webSocketId || null,
    mobileSocketId: session.mobileSocketId || null,
    createdAt: session.createdAt,
    expiresAt: session.expiresAt,
    state: session.state,
  });
}

function deserializeSession(serializedSession) {
  if (!serializedSession) {
    return null;
  }

  const session = JSON.parse(serializedSession);

  return {
    sessionId: session.sessionId,
    webSocketId: session.webSocketId || null,
    mobileSocketId: session.mobileSocketId || null,
    createdAt: Number(session.createdAt),
    expiresAt: Number(session.expiresAt),
    state: session.state,
  };
}

function deriveAttachedState(session) {
  if (session.state === SESSION_STATES.CLOSED) {
    return SESSION_STATES.CLOSED;
  }

  return session.state || SESSION_STATES.WAITING;
}

function buildUpdatedSession(session, updates) {
  if (!updates || typeof updates !== "object" || Array.isArray(updates)) {
    throw new Error("updates must be an object");
  }

  const nextSession = {
    ...session,
  };

  if (Object.prototype.hasOwnProperty.call(updates, "webSocketId")) {
    if (updates.webSocketId !== null) {
      assertSocketId(updates.webSocketId, "webSocketId");
    }

    nextSession.webSocketId = updates.webSocketId || null;
  }

  if (Object.prototype.hasOwnProperty.call(updates, "mobileSocketId")) {
    if (updates.mobileSocketId !== null) {
      assertSocketId(updates.mobileSocketId, "mobileSocketId");
    }

    nextSession.mobileSocketId = updates.mobileSocketId || null;
  }

  if (Object.prototype.hasOwnProperty.call(updates, "expiresAt")) {
    nextSession.expiresAt = normalizeExpiresAt(updates.expiresAt);
  }

  if (Object.prototype.hasOwnProperty.call(updates, "state")) {
    assertState(updates.state);

    const allowedTransitions = ALLOWED_STATE_TRANSITIONS[session.state];
    if (!allowedTransitions || !allowedTransitions.has(updates.state)) {
      throw new Error(`Invalid session state transition from ${session.state} to ${updates.state}`);
    }

    nextSession.state = updates.state;
  }

  if (nextSession.state === SESSION_STATES.ACTIVE && (!nextSession.webSocketId || !nextSession.mobileSocketId)) {
    throw new Error(
      `Session ${session.sessionId} requires webSocketId and mobileSocketId before activation`,
    );
  }

  return nextSession;
}

class RedisSessionRegistry {
  constructor(redisClient) {
    if (!redisClient) {
      throw new Error("redisClient is required");
    }

    for (const methodName of ["exists", "get", "set", "expireAt", "del"]) {
      if (typeof redisClient[methodName] !== "function") {
        throw new Error(`redisClient.${methodName} must be a function`);
      }
    }

    this.redisClient = redisClient;
  }

  static async connect(redisUrl) {
    assertNonEmptyString(redisUrl, "redisUrl");

    let redisModule;
    try {
      redisModule = require(REDIS_CLIENT_PACKAGE_NAME);
    } catch {
      throw new Error(`${REDIS_CLIENT_PACKAGE_NAME} package is required to connect to Redis`);
    }

    if (typeof redisModule.createClient !== "function") {
      throw new Error("redis.createClient must be available");
    }

    const redisClient = redisModule.createClient({ url: redisUrl });
    await redisClient.connect();
    return new RedisSessionRegistry(redisClient);
  }

  async createSession(sessionId, webSocketIdOrExpiresAt, maybeExpiresAt) {
    assertSessionId(sessionId);

    const expiresAtMs = normalizeExpiresAt(
      maybeExpiresAt === undefined ? webSocketIdOrExpiresAt : maybeExpiresAt,
    );
    const webSocketId = maybeExpiresAt === undefined ? null : webSocketIdOrExpiresAt;

    if (webSocketId !== null && webSocketId !== undefined) {
      assertSocketId(webSocketId, "webSocketId");
    }

    const key = getRedisSessionKey(sessionId);
    const session = {
      sessionId,
      mobileSocketId: null,
      webSocketId: webSocketId || null,
      createdAt: Date.now(),
      expiresAt: expiresAtMs,
      state: SESSION_STATES.WAITING,
    };

    if (Number(await this.redisClient.exists(key)) > 0) {
      throw new Error(`Session already exists for ${sessionId}`);
    }

    await this.redisClient.set(key, serializeSession(session));

    if (!(await this.redisClient.expireAt(key, toRedisEpochSeconds(expiresAtMs)))) {
      await this.redisClient.del(key);
      throw new Error(`Failed to set TTL for ${sessionId}`);
    }

    return session;
  }

  async attachWebSocket(sessionId, webSocketId) {
    assertSessionId(sessionId);
    assertSocketId(webSocketId, "webSocketId");

    return this.updateSession(sessionId, {
      webSocketId,
      state: deriveAttachedState(await this.requireSession(sessionId)),
    });
  }

  async attachMobileSocket(sessionId, mobileSocketId) {
    assertSessionId(sessionId);
    assertSocketId(mobileSocketId, "mobileSocketId");

    return this.updateSession(sessionId, {
      mobileSocketId,
      state: deriveAttachedState(await this.requireSession(sessionId)),
    });
  }

  async getSession(sessionId) {
    assertSessionId(sessionId);
    const serializedSession = await this.redisClient.get(getRedisSessionKey(sessionId));
    return deserializeSession(serializedSession);
  }

  async setSessionState(sessionId, state) {
    return this.updateSession(sessionId, { state });
  }

  async updateSession(sessionId, updates) {
    assertSessionId(sessionId);

    const session = await this.requireSession(sessionId);
    const updatedSession = buildUpdatedSession(session, updates);
    await this.writeSession(updatedSession);
    return updatedSession;
  }

  async updateState(sessionId, state) {
    return this.updateSession(sessionId, { state });
  }

  async attachMobile(sessionId, mobileSocketId) {
    return this.attachMobileSocket(sessionId, mobileSocketId);
  }

  async writeSession(session) {
    const key = getRedisSessionKey(session.sessionId);
    await this.redisClient.set(key, serializeSession(session));

    if (!(await this.redisClient.expireAt(key, toRedisEpochSeconds(session.expiresAt)))) {
      await this.redisClient.del(key);
      throw new Error(`Failed to set TTL for ${session.sessionId}`);
    }

    return session;
  }

  async deleteSession(sessionId) {
    assertSessionId(sessionId);
    return Number(await this.redisClient.del(getRedisSessionKey(sessionId))) > 0;
  }

  async requireSession(sessionId) {
    const session = await this.getSession(sessionId);

    if (!session) {
      throw new Error(`Session not found for ${sessionId}`);
    }

    return session;
  }
}

function createRedisSessionRegistry(redisClient) {
  return new RedisSessionRegistry(redisClient);
}

async function connectRedisSessionRegistry(redisUrl) {
  return RedisSessionRegistry.connect(redisUrl);
}

module.exports = {
  SESSION_STATES,
  RedisSessionRegistry,
  connectRedisSessionRegistry,
  createRedisSessionRegistry,
  getRedisSessionKey,
};