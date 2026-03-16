const DEFAULT_MAX_COMMANDS_PER_SECOND = 30;
const DEFAULT_MAX_PAYLOAD_SIZE = 2 * 1024 * 1024;
const DEFAULT_MAX_SESSIONS_PER_IP = 10;

function getWindowBucket(now) {
  return Math.floor(now / 1000);
}

function getCounter(counterMap, key, now) {
  const bucket = getWindowBucket(now);
  const existing = counterMap.get(key);

  if (!existing || existing.bucket !== bucket) {
    const freshCounter = { bucket, count: 0 };
    counterMap.set(key, freshCounter);
    return freshCounter;
  }

  return existing;
}

class RelayRateLimiter {
  constructor(options = {}) {
    this.maxCommandsPerSecond =
      options.maxCommandsPerSecond === undefined
        ? DEFAULT_MAX_COMMANDS_PER_SECOND
        : options.maxCommandsPerSecond;
    this.maxPayloadSize =
      options.maxPayloadSize === undefined ? DEFAULT_MAX_PAYLOAD_SIZE : options.maxPayloadSize;
    this.maxSessionsPerIp =
      options.maxSessionsPerIp === undefined ? DEFAULT_MAX_SESSIONS_PER_IP : options.maxSessionsPerIp;
    this.now = options.now || (() => Date.now());
    this.commandCounters = new Map();
    this.sessionCounters = new Map();
  }

  evaluateInboundMessage({ socketId, payloadSize }) {
    const payloadViolation = this.validatePayloadSize(payloadSize);
    if (payloadViolation) {
      return payloadViolation;
    }

    return this.recordCommand(socketId);
  }

  validatePayloadSize(payloadSize) {
    if (payloadSize > this.maxPayloadSize) {
      return { reason: "payload_too_large" };
    }

    return null;
  }

  recordCommand(socketId) {
    const counter = getCounter(this.commandCounters, socketId, this.now());
    counter.count += 1;

    if (counter.count > this.maxCommandsPerSecond) {
      return { reason: "command_rate_exceeded" };
    }

    return null;
  }

  recordSessionCreation(ipAddress) {
    const counter = getCounter(this.sessionCounters, ipAddress, this.now());
    counter.count += 1;

    if (counter.count > this.maxSessionsPerIp) {
      return { reason: "session_rate_exceeded" };
    }

    return null;
  }
}

function createRelayRateLimiter(options) {
  return new RelayRateLimiter(options);
}

module.exports = {
  DEFAULT_MAX_COMMANDS_PER_SECOND,
  DEFAULT_MAX_PAYLOAD_SIZE,
  DEFAULT_MAX_SESSIONS_PER_IP,
  RelayRateLimiter,
  createRelayRateLimiter,
};