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
  assert.equal(typeof sessionRecord.token, "string");
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
    "token",
    "webSocketId",
  ]);
  assert.equal(auditResult.redisChecks.keys.some((key) => key === `session:${sessionRecord.sessionId}`), true);
  assert.equal(auditResult.redisChecks.type, "hash");
  assert.equal(auditResult.redisChecks.ttl, 120);
  assert.equal(auditResult.ttlEvidence.sessionCreation.ttlSeconds, 120);
  assert.equal(auditResult.ttlEvidence.sessionActivation.ttlSeconds, 120);
  assert.equal(auditResult.rawRedisSessionRecord.sessionId, sessionRecord.sessionId);
  assert.equal(auditResult.rawRedisSessionRecord.state, "active");
  assert.equal(auditResult.rawRedisSessionRecord.token, sessionRecord.token);
});

test("relay_session_ttl_runtime", async () => {
  const auditResult = await auditResultPromise;

  assert.equal(auditResult.ttlEvidence.sessionCreation.nearZero, false);
  assert.equal(auditResult.ttlEvidence.sessionActivation.extended, true);
  assert.equal(auditResult.ttlEvidence.sessionActivation.abnormalDecrease, false);
  assert.deepEqual(
    auditResult.ttlEvidence.messageFlow.map((entry) => entry.messageType),
    ["snapshot_start", "event_stream", "snapshot_complete", "mutation_command", "command_result"],
  );
  assert.equal(
    auditResult.ttlEvidence.messageFlow.every((entry) => entry.refreshedToFullDuration),
    true,
  );
  assert.equal(auditResult.ttlEvidence.sessionExpiry.remainedActiveBeforeRefresh, true);
  assert.equal(auditResult.ttlEvidence.sessionExpiry.remainedActiveAfterRefresh, true);
  assert.equal(auditResult.ttlEvidence.sessionExpiry.noPrematureExpiry, true);
  assert.equal(auditResult.ttlEvidence.sessionExpiry.expiredAfterFullInactivityWindow, true);
  assert.equal(auditResult.ttlEvidence.sessionExpiry.timeoutNotified, true);
  assert.equal(auditResult.ttlEvidence.ttlComputation.usesFixedDurationReset, true);
  assert.equal(auditResult.ttlEvidence.ttlComputation.staleExpiryBypassed, true);
  assert.equal(auditResult.timerEvidence.setCount >= 3, true);
  assert.equal(auditResult.timerEvidence.resetCount >= 2, true);
  assert.equal(auditResult.timerEvidence.clearCount >= 1, true);
  assert.deepEqual(auditResult.timerEvidence.closeCycleClearEntries, ["TIMER_CLEAR previousHandle=true"]);
  assert.deepEqual(auditResult.runtimeEvidence, ["protocol_handshake", "snapshot_start", "snapshot_complete"]);
});

test("redis_session_logs_runtime", async () => {
  const auditResult = await auditResultPromise;

  assert.equal(auditResult.startupLogs.some((entry) => entry.includes("SESSION_CREATE persisted")), true);
  assert.equal(auditResult.startupLogs.some((entry) => entry.includes("SESSION_UPDATE paired")), true);
  assert.equal(auditResult.startupLogs.some((entry) => entry.includes("SESSION_UPDATE active")), true);
  assert.equal(auditResult.startupLogs.some((entry) => entry.includes("MESSAGE_RECEIVED type=snapshot_start")), true);
  assert.equal(auditResult.startupLogs.some((entry) => entry.includes("RAW_WS_MESSAGE bytes=")), true);
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("snapshot_lock_acquired") &&
        entry.includes(`session=${auditResult.lifecycleEvidence.sessionId}`) &&
        entry.includes("type=snapshot_start"),
    ),
    true,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("message_deferred_due_to_snapshot") &&
        entry.includes(`session=${auditResult.lifecycleEvidence.sessionId}`) &&
        entry.includes("type=event_stream"),
    ),
    true,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("snapshot_lock_released") &&
        entry.includes(`session=${auditResult.lifecycleEvidence.sessionId}`) &&
        entry.includes("type=snapshot_complete"),
    ),
    true,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("JSON_PARSE_SUCCESS type=snapshot_start") &&
        entry.includes(`sessionId=${auditResult.lifecycleEvidence.sessionId}`),
    ),
    true,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("ENVELOPE_VALIDATION type=snapshot_start") &&
        entry.includes(`sessionId=${auditResult.lifecycleEvidence.sessionId}`) &&
        entry.includes("valid=true"),
    ),
    true,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("DISPATCH_LOOKUP type=snapshot_start") &&
        entry.includes(`sessionId=${auditResult.lifecycleEvidence.sessionId}`) &&
        entry.includes("handlerExists=false"),
    ),
    true,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("SESSION_LOOKUP type=snapshot_start") &&
        entry.includes(`sessionId=${auditResult.lifecycleEvidence.sessionId}`) &&
        entry.includes("found=true") &&
        entry.includes("state=active"),
    ),
    true,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("FORWARD_MESSAGE type=snapshot_start") &&
        entry.includes(`sessionId=${auditResult.lifecycleEvidence.sessionId}`),
    ),
    true,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("RELAY_INGRESS_HASH") &&
        entry.includes(`sessionId=${auditResult.lifecycleEvidence.sessionId}`) &&
        entry.includes("hash="),
    ),
    true,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("RELAY_EGRESS_HASH") &&
        entry.includes(`sessionId=${auditResult.lifecycleEvidence.sessionId}`) &&
        entry.includes("hash="),
    ),
    true,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) => entry.includes("RELAY_BYTE_VIOLATION") && entry.includes(`sessionId=${auditResult.lifecycleEvidence.sessionId}`),
    ),
    false,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("ROUTING_DECISION type=snapshot_start") &&
        entry.includes(`sessionId=${auditResult.lifecycleEvidence.sessionId}`) &&
        entry.includes("routed=true") &&
        entry.includes("reason=forwarded"),
    ),
    true,
  );
  assert.equal(
    auditResult.startupLogs.some(
      (entry) =>
        entry.includes("MESSAGE_DROPPED type=snapshot_start") &&
        entry.includes(`sessionId=${auditResult.lifecycleEvidence.sessionId}`),
    ),
    false,
  );
});

test("relay_message_routing_runtime", async () => {
  const auditResult = await auditResultPromise;

  assert.equal(auditResult.routingEvidence.length, 4);
  assert.deepEqual(
    auditResult.routingEvidence.map((entry) => `${entry.direction}:${entry.type}`),
    [
      "mobile->web:snapshot_start",
      "mobile->web:event_stream",
      "web->mobile:mutation_command",
      "mobile->web:command_result",
    ],
  );
  assert.equal(
    auditResult.routingEvidence.every((entry) => entry.sessionId === auditResult.lifecycleEvidence.sessionId),
    true,
  );
  assert.equal(auditResult.routingEvidence.every((entry) => entry.unchanged), true);
});

test("relay_mutation_transport_runtime", async () => {
  const auditResult = await auditResultPromise;

  assert.deepEqual(
    auditResult.mutationAudit.inbound.slice(0, 2),
    [
      { sequence: 8, type: "mutation_command", commandId: "cmd-101" },
      { sequence: 9, type: "command_result", commandId: "cmd-101" },
    ],
  );
  assert.deepEqual(
    auditResult.mutationAudit.outbound.slice(0, 2),
    [
      { sequence: 8, type: "mutation_command", commandId: "cmd-101" },
      { sequence: 9, type: "command_result", commandId: "cmd-101" },
    ],
  );
  assert.equal(
    auditResult.mutationAudit.routingLogs.includes("INBOUND seq=20 type=mutation_command cmd=cmd-201"),
    true,
  );
  assert.equal(
    auditResult.mutationAudit.routingLogs.includes("OUTBOUND seq=21 type=command_result cmd=cmd-201"),
    true,
  );
  assert.equal(auditResult.mutationAudit.payloadReferenceEquality.every(Boolean), true);
  assert.deepEqual(auditResult.orderingEvidence, [20, 21, 22]);
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