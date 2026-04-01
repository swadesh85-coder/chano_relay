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
const DEFAULT_SESSION_TTL_MS = 2 * 60 * 1000;
const NULL_REDIS_FIELD_VALUE = "null";
const DEFAULT_ATOMIC_RETRY_COUNT = 3;

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

function normalizeToken(token) {
  if (token === undefined || token === null) {
    return null;
  }

  assertNonEmptyString(token, "token");
  return token;
}

function assertPositiveInteger(value, label) {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }
}

function toRedisTtlSeconds(expiresAtMs, nowMs, options = {}) {
  const allowExpired = Boolean(options.allowExpired);
  const ttlSeconds = Math.ceil((expiresAtMs - nowMs) / 1000);

  if (!Number.isInteger(ttlSeconds)) {
    throw new Error("expiresAt must be in the future");
  }

  if (ttlSeconds <= 0) {
    if (allowExpired) {
      return 1;
    }

    throw new Error("expiresAt must be in the future");
  }

  return ttlSeconds;
}

function getRedisSessionKey(sessionId) {
  return `session:${sessionId}`;
}

function serializeRedisFieldValue(value) {
  if (value === null || value === undefined) {
    return NULL_REDIS_FIELD_VALUE;
  }

  return String(value);
}

function deserializeRedisFieldValue(value) {
  if (value === undefined || value === null || value === "" || value === NULL_REDIS_FIELD_VALUE) {
    return null;
  }

  return value;
}

function serializeSessionHash(session) {
  return {
    sessionId: serializeRedisFieldValue(session.sessionId),
    token: serializeRedisFieldValue(session.token),
    webSocketId: serializeRedisFieldValue(session.webSocketId),
    mobileSocketId: serializeRedisFieldValue(session.mobileSocketId),
    createdAt: serializeRedisFieldValue(session.createdAt),
    expiresAt: serializeRedisFieldValue(session.expiresAt),
    state: serializeRedisFieldValue(session.state),
  };
}

function deserializeSessionHash(serializedSession) {
  if (!serializedSession || Object.keys(serializedSession).length === 0) {
    return null;
  }

  const sessionId = deserializeRedisFieldValue(serializedSession.sessionId);
  const state = deserializeRedisFieldValue(serializedSession.state);
  const createdAt = Number(deserializeRedisFieldValue(serializedSession.createdAt));
  const expiresAt = Number(deserializeRedisFieldValue(serializedSession.expiresAt));

  if (!sessionId || !state || !Number.isFinite(createdAt) || !Number.isFinite(expiresAt)) {
    throw new Error("Stored Redis session hash is invalid");
  }

  return {
    sessionId,
    token: deserializeRedisFieldValue(serializedSession.token),
    webSocketId: deserializeRedisFieldValue(serializedSession.webSocketId),
    mobileSocketId: deserializeRedisFieldValue(serializedSession.mobileSocketId),
    createdAt,
    expiresAt,
    state,
  };
}

function buildRedisHashUpdates(updates) {
  const redisHashUpdates = {};

  if (Object.prototype.hasOwnProperty.call(updates, "token")) {
    redisHashUpdates.token = serializeRedisFieldValue(updates.token);
  }

  if (Object.prototype.hasOwnProperty.call(updates, "webSocketId")) {
    redisHashUpdates.webSocketId = serializeRedisFieldValue(updates.webSocketId);
  }

  if (Object.prototype.hasOwnProperty.call(updates, "mobileSocketId")) {
    redisHashUpdates.mobileSocketId = serializeRedisFieldValue(updates.mobileSocketId);
  }

  if (Object.prototype.hasOwnProperty.call(updates, "expiresAt")) {
    redisHashUpdates.expiresAt = serializeRedisFieldValue(updates.expiresAt);
  }

  if (Object.prototype.hasOwnProperty.call(updates, "state")) {
    redisHashUpdates.state = serializeRedisFieldValue(updates.state);
  }

  return redisHashUpdates;
}

function resolveLegacyCreateSessionArguments(now, webSocketIdOrExpiresAt, maybeExpiresAt) {
  if (maybeExpiresAt !== undefined) {
    return {
      webSocketId: webSocketIdOrExpiresAt,
      expiresAtMs: normalizeExpiresAt(maybeExpiresAt),
    };
  }

  if (webSocketIdOrExpiresAt === undefined || webSocketIdOrExpiresAt === null) {
    return {
      webSocketId: null,
      expiresAtMs: now + DEFAULT_SESSION_TTL_MS,
    };
  }

  if (typeof webSocketIdOrExpiresAt === "string") {
    const parsedTimestamp = Date.parse(webSocketIdOrExpiresAt);
    if (Number.isFinite(parsedTimestamp)) {
      return {
        webSocketId: null,
        expiresAtMs: parsedTimestamp,
      };
    }

    return {
      webSocketId: webSocketIdOrExpiresAt,
      expiresAtMs: now + DEFAULT_SESSION_TTL_MS,
    };
  }

  return {
    webSocketId: null,
    expiresAtMs: normalizeExpiresAt(webSocketIdOrExpiresAt),
  };
}

function resolveCreateSessionArguments(now, sessionIdOrOptions, webSocketIdOrExpiresAt, maybeExpiresAt, maybeToken) {
  if (sessionIdOrOptions && typeof sessionIdOrOptions === "object" && !Array.isArray(sessionIdOrOptions)) {
    return {
      sessionId: sessionIdOrOptions.sessionId,
      token: normalizeToken(sessionIdOrOptions.token),
      webSocketId: sessionIdOrOptions.webSocketId || null,
      expiresAtMs:
        sessionIdOrOptions.expiresAt === undefined
          ? now + DEFAULT_SESSION_TTL_MS
          : normalizeExpiresAt(sessionIdOrOptions.expiresAt),
    };
  }

  const resolvedArguments = resolveLegacyCreateSessionArguments(now, webSocketIdOrExpiresAt, maybeExpiresAt);

  return {
    sessionId: sessionIdOrOptions,
    token: normalizeToken(maybeToken),
    ...resolvedArguments,
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

  if (Object.prototype.hasOwnProperty.call(updates, "token")) {
    nextSession.token = normalizeToken(updates.token);
  }

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
  constructor(redisClient, options = {}) {
    if (!redisClient) {
      throw new Error("redisClient is required");
    }

    for (const methodName of ["exists", "hGetAll", "hSet", "expire", "del"]) {
      if (typeof redisClient[methodName] !== "function") {
        throw new Error(`redisClient.${methodName} must be a function`);
      }
    }

    this.redisClient = redisClient;
    this.logger = typeof options.logger === "function" ? options.logger : console.log;
    this.now = typeof options.now === "function" ? options.now : () => Date.now();
    this.atomicRetryCount =
      options.atomicRetryCount === undefined ? DEFAULT_ATOMIC_RETRY_COUNT : Number(options.atomicRetryCount);
    this.sessionTtlMs =
      options.sessionTtlMs === undefined ? DEFAULT_SESSION_TTL_MS : Number(options.sessionTtlMs);
    assertPositiveInteger(this.atomicRetryCount, "atomicRetryCount");
    assertPositiveInteger(this.sessionTtlMs, "sessionTtlMs");
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

  async createSession(sessionIdOrOptions, webSocketIdOrExpiresAt, maybeExpiresAt, maybeToken) {
    const { sessionId, token, webSocketId, expiresAtMs } = resolveCreateSessionArguments(
      this.now(),
      sessionIdOrOptions,
      webSocketIdOrExpiresAt,
      maybeExpiresAt,
      maybeToken,
    );

    assertSessionId(sessionId);

    if (webSocketId !== null && webSocketId !== undefined) {
      assertSocketId(webSocketId, "webSocketId");
    }

    const key = getRedisSessionKey(sessionId);
    const session = {
      sessionId,
      token,
      mobileSocketId: null,
      webSocketId,
      createdAt: this.now(),
      expiresAt: expiresAtMs,
      state: SESSION_STATES.WAITING,
    };

    if (Number(await this.redisClient.exists(key)) > 0) {
      throw new Error(`Session already exists for ${sessionId}`);
    }

    await this.writeSession(session, "create", true);

    return session;
  }

  async updateSessionOnPair(sessionId, mobileSocketId, webSocketId) {
    assertSessionId(sessionId);
    assertSocketId(mobileSocketId, "mobileSocketId");
    assertSocketId(webSocketId, "webSocketId");

    return this.runAtomicSessionUpdate(
      sessionId,
      "pair",
      (session) => {
        if (session.state !== SESSION_STATES.WAITING) {
          throw new Error(`Session ${sessionId} must be waiting before pairing`);
        }

        return buildUpdatedSession(session, {
          token: null,
          webSocketId,
          mobileSocketId,
          state: SESSION_STATES.PAIRED,
        });
      },
    );
  }

  async activateSession(sessionId) {
    assertSessionId(sessionId);

    return this.runAtomicSessionUpdate(
      sessionId,
      "activate",
      (session) => {
        if (session.state !== SESSION_STATES.PAIRED) {
          throw new Error(`Session ${sessionId} must be paired before activation`);
        }

        return buildUpdatedSession(session, {
          state: SESSION_STATES.ACTIVE,
          expiresAt: this.computeExpiresAt(),
        });
      },
    );
  }

  async refreshSessionTtl(sessionId, action = "refresh") {
    assertSessionId(sessionId);
    assertNonEmptyString(action, "action");

    const session = await this.requireSession(sessionId);
    const updatedSession = buildUpdatedSession(session, {
      expiresAt: this.computeExpiresAt(),
    });

    await this.writeSessionFields(
      updatedSession.sessionId,
      {
        expiresAt: updatedSession.expiresAt,
      },
      updatedSession.expiresAt,
      action,
    );

    return updatedSession;
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
    const serializedSession = await this.redisClient.hGetAll(getRedisSessionKey(sessionId));
    return deserializeSessionHash(serializedSession);
  }

  async setSessionState(sessionId, state) {
    return this.updateSession(sessionId, { state });
  }

  async updateSession(sessionId, updates) {
    assertSessionId(sessionId);

    if (
      updates &&
      updates.state === SESSION_STATES.PAIRED &&
      Object.keys(updates).every((key) => key === "mobileSocketId" || key === "state") &&
      Object.prototype.hasOwnProperty.call(updates, "mobileSocketId")
    ) {
      return this.updateSessionOnPair(sessionId, updates.mobileSocketId);
    }

    if (
      updates &&
      updates.state === SESSION_STATES.ACTIVE &&
      Object.keys(updates).length === 1
    ) {
      return this.activateSession(sessionId);
    }

    const session = await this.requireSession(sessionId);
    const updatedSession = buildUpdatedSession(session, updates);
    const allowExpiredTtl = updatedSession.state === SESSION_STATES.CLOSED;
    await this.writeSessionFields(
      updatedSession.sessionId,
      buildRedisHashUpdates(updatedSession),
      updatedSession.expiresAt,
      "update",
      allowExpiredTtl,
    );
    return updatedSession;
  }

  async updateState(sessionId, state) {
    return this.updateSession(sessionId, { state });
  }

  async attachMobile(sessionId, mobileSocketId) {
    return this.attachMobileSocket(sessionId, mobileSocketId);
  }

  getSessionTtlMs() {
    return this.sessionTtlMs;
  }

  computeExpiresAt(nowMs = this.now()) {
    return nowMs + this.sessionTtlMs;
  }

  async writeSession(session, action = "write", deleteOnExpireFailure = false, allowExpiredTtl = false) {
    const key = getRedisSessionKey(session.sessionId);
    const ttlSeconds = toRedisTtlSeconds(session.expiresAt, this.now(), {
      allowExpired: allowExpiredTtl,
    });

    try {
      await this.redisClient.hSet(key, serializeSessionHash(session));

      if (!(await this.redisClient.expire(key, ttlSeconds))) {
        if (deleteOnExpireFailure) {
          await this.redisClient.del(key);
        }

        throw new Error(`Failed to set TTL for ${session.sessionId}`);
      }

      this.logRedisWrite("success", action, session.sessionId, ttlSeconds);
    } catch (error) {
      this.logRedisWrite("failure", action, session.sessionId, ttlSeconds, error);
      throw error;
    }

    return session;
  }

  async writeSessionFields(sessionId, updates, expiresAt, action, allowExpiredTtl = false) {
    const key = getRedisSessionKey(sessionId);
    const ttlSeconds = toRedisTtlSeconds(expiresAt, this.now(), {
      allowExpired: allowExpiredTtl,
    });

    try {
      await this.redisClient.hSet(key, buildRedisHashUpdates(updates));

      if (!(await this.redisClient.expire(key, ttlSeconds))) {
        throw new Error(`Failed to set TTL for ${sessionId}`);
      }

      this.logRedisWrite("success", action, sessionId, ttlSeconds);
    } catch (error) {
      this.logRedisWrite("failure", action, sessionId, ttlSeconds, error);
      throw error;
    }
  }

  async runAtomicSessionUpdate(sessionId, action, buildUpdatedSessionFn) {
    assertSessionId(sessionId);
    assertNonEmptyString(action, "action");

    const key = getRedisSessionKey(sessionId);

    if (typeof buildUpdatedSessionFn !== "function") {
      throw new Error("buildUpdatedSessionFn must be a function");
    }

    if (!this.supportsOptimisticTransactions()) {
      const session = await this.requireSession(sessionId);
      const updatedSession = buildUpdatedSessionFn(session);
      await this.writeSessionFields(
        updatedSession.sessionId,
        buildRedisHashUpdates(updatedSession),
        updatedSession.expiresAt,
        action,
      );
      return updatedSession;
    }

    for (let attempt = 1; attempt <= this.atomicRetryCount; attempt += 1) {
      await this.redisClient.watch(key);

      try {
        const currentSession = deserializeSessionHash(await this.redisClient.hGetAll(key));
        if (!currentSession) {
          throw new Error(`Session not found for ${sessionId}`);
        }

        const nextSession = buildUpdatedSessionFn(currentSession);
        const ttlSeconds = toRedisTtlSeconds(nextSession.expiresAt, this.now());
        const transaction = this.redisClient.multi();

        transaction.hSet(key, buildRedisHashUpdates(nextSession));
        transaction.expire(key, ttlSeconds);

        const transactionResult = await transaction.exec();
        if (transactionResult === null) {
          this.logRedisWrite("retry", `${action}_conflict`, sessionId, ttlSeconds);
          continue;
        }

        this.logRedisWrite("success", action, sessionId, ttlSeconds);
        return nextSession;
      } catch (error) {
        if (typeof this.redisClient.unwatch === "function") {
          await this.redisClient.unwatch();
        }

        this.logRedisWrite("failure", action, sessionId, 0, error);
        throw error;
      }
    }

    throw new Error(`Atomic ${action} failed for ${sessionId} after ${this.atomicRetryCount} retries`);
  }

  supportsOptimisticTransactions() {
    return (
      typeof this.redisClient.watch === "function" &&
      typeof this.redisClient.unwatch === "function" &&
      typeof this.redisClient.multi === "function"
    );
  }

  logRedisWrite(status, action, sessionId, ttlSeconds, error) {
    const parts = [
      `[redis_session_registry] status=${status}`,
      `action=${action}`,
      `sessionId=${sessionId}`,
      `ttlSeconds=${ttlSeconds}`,
    ];

    if (error) {
      parts.push(`error=${error.message}`);
    }

    this.logger(parts.join(" "));
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

function createRedisSessionRegistry(redisClient, options) {
  return new RedisSessionRegistry(redisClient, options);
}

async function connectRedisSessionRegistry(redisUrl) {
  return RedisSessionRegistry.connect(redisUrl);
}

module.exports = {
  DEFAULT_SESSION_TTL_MS,
  SESSION_STATES,
  RedisSessionRegistry,
  connectRedisSessionRegistry,
  createRedisSessionRegistry,
  getRedisSessionKey,
};