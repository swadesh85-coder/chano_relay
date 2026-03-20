const assert = require("node:assert/strict");
const test = require("node:test");

const { runRelayRuntimeAudit } = require("./relayRuntimeAudit");

let auditResultPromise;

test.before(() => {
  auditResultPromise = runRelayRuntimeAudit({
    host: "127.0.0.1",
    clientHost: "127.0.0.1",
    port: 0,
    emitOutput: false,
  });
});

test("relay_startup_runtime", async () => {
  const auditResult = await auditResultPromise;

  assert.equal(auditResult.startupEvidence.relayStarting, true);
  assert.equal(auditResult.startupEvidence.redisConnected, true);
  assert.equal(auditResult.startupEvidence.relayListening, true);
  assert.deepEqual(auditResult.startupEvidence.redisConnectCalls, ["redis://127.0.0.1:6379"]);
  assert.match(auditResult.startupEvidence.listenUrl, /^ws:\/\/127\.0\.0\.1:\d+\/relay$/);
});

test("redis_session_registry_runtime", async () => {
  const auditResult = await auditResultPromise;
  const sessionRecord = auditResult.redisSessionRecord;

  assert.equal(sessionRecord.state, "active");
  assert.equal(typeof sessionRecord.sessionId, "string");
  assert.equal(typeof sessionRecord.webSocketId, "string");
  assert.equal(typeof sessionRecord.mobileSocketId, "string");
  assert.equal(typeof sessionRecord.createdAt, "number");
  assert.equal(typeof sessionRecord.expiresAt, "number");
  assert.deepEqual(Object.keys(auditResult.rawRedisSessionRecord).sort(), [
    "createdAt",
    "expiresAt",
    "mobileSocketId",
    "sessionId",
    "state",
    "webSocketId",
  ]);
  assert.equal(auditResult.redisChecks.keys.some((key) => key === `session:${sessionRecord.sessionId}`), true);
  assert.equal(auditResult.redisChecks.type, "hash");
  assert.equal(auditResult.redisChecks.ttl, 120);
  assert.equal(auditResult.rawRedisSessionRecord.sessionId, sessionRecord.sessionId);
  assert.equal(auditResult.rawRedisSessionRecord.state, "active");
});

test("redis_session_logs_runtime", async () => {
  const auditResult = await auditResultPromise;

  assert.equal(auditResult.startupLogs.some((entry) => entry.includes("SESSION_CREATE persisted")), true);
  assert.equal(auditResult.startupLogs.some((entry) => entry.includes("SESSION_UPDATE paired")), true);
  assert.equal(auditResult.startupLogs.some((entry) => entry.includes("SESSION_UPDATE active")), true);
});

test("relay_message_routing_runtime", async () => {
  const auditResult = await auditResultPromise;

  assert.equal(auditResult.routingEvidence.length, 3);
  assert.deepEqual(
    auditResult.routingEvidence.map((entry) => `${entry.direction}:${entry.type}`),
    [
      "mobile->web:snapshot_start",
      "web->mobile:mutation_command",
      "mobile->web:event_stream",
    ],
  );
  assert.equal(
    auditResult.routingEvidence.every((entry) => entry.sessionId === auditResult.lifecycleEvidence.sessionId),
    true,
  );
  assert.equal(auditResult.routingEvidence.every((entry) => entry.unchanged), true);
});

test("transport_envelope_validation_runtime", async () => {
  const auditResult = await auditResultPromise;

  assert.deepEqual(auditResult.validationEvidence.validAccepted, {
    sequence: 1,
    type: "protocol_handshake",
  });
  assert.equal(auditResult.validationEvidence.invalidRejected.reason, "missing_session_id");
});

test("session_lifecycle_transition_runtime", async () => {
  const auditResult = await auditResultPromise;

  assert.deepEqual(auditResult.lifecycleEvidence.states, ["waiting", "paired", "active", "closed"]);
  assert.equal(auditResult.constitution.mobileAuthority, true);
  assert.equal(auditResult.constitution.mutationBoundary, true);
  assert.equal(auditResult.constitution.eventOrdering, true);
  assert.equal(auditResult.constitution.relayNeutrality, true);
  assert.equal(auditResult.constitution.projectionSafety, true);
});