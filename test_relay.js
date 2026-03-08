/**
 * Integration test for Chano Relay Server — envelope-enforced protocol.
 *
 * Every message must conform to:
 * { type: string, sessionId: string|null, timestamp: number, payload: object }
 */
const WebSocket = require("ws");

const URL = "ws://localhost:8080";
let passed = 0;
let failed = 0;

function assert(condition, label) {
  if (condition) {
    console.log(`  PASS: ${label}`);
    passed++;
  } else {
    console.error(`  FAIL: ${label}`);
    failed++;
  }
}

/** Validate that a parsed message follows the standard envelope. */
function assertEnvelope(msg, expectedType, label) {
  assert(msg.type === expectedType, `${label} → type is "${expectedType}"`);
  assert("sessionId" in msg, `${label} → has sessionId field`);
  assert(typeof msg.timestamp === "number" && msg.timestamp > 0, `${label} → has valid timestamp`);
  assert(typeof msg.payload === "object" && msg.payload !== null, `${label} → has payload object`);
}

function connect() {
  return new Promise((resolve) => {
    const ws = new WebSocket(URL);
    ws.on("open", () => resolve(ws));
  });
}

function waitMsg(ws, filter, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timeout waiting for ${filter}`)), timeoutMs);
    const handler = (raw) => {
      const msg = JSON.parse(raw);
      if (typeof filter === "string" ? msg.type === filter : filter(msg)) {
        clearTimeout(timer);
        ws.removeListener("message", handler);
        resolve(msg);
      }
    };
    ws.on("message", handler);
  });
}

/** Envelope helper for client → server messages. */
function env(type, sessionId, payload) {
  return JSON.stringify({
    type,
    sessionId: sessionId || null,
    timestamp: Date.now(),
    payload: payload || {},
  });
}

async function testFullFlow() {
  console.log("\n--- Test 1: Full flow (create → pair → relay → disconnect) ---");

  const web = await connect();
  const mobile = await connect();

  // 1. Web creates session
  web.send(env("qr_session_create"));
  const ready = await waitMsg(web, "qr_session_ready");

  assertEnvelope(ready, "qr_session_ready", "qr_session_ready");
  assert(typeof ready.sessionId === "string" && ready.sessionId.length > 0, "sessionId is a valid UUID string");
  assert(typeof ready.payload.expiresAt === "number" && ready.payload.expiresAt > Date.now(), "payload.expiresAt is a future timestamp");

  const { sessionId } = ready;

  // 2. Mobile pairs
  mobile.send(env("pair_request", sessionId));

  const pairWeb = await waitMsg(web, "pair_approved");
  const pairMobile = await waitMsg(mobile, "pair_approved");

  assertEnvelope(pairWeb, "pair_approved", "pair_approved → web");
  assert(pairWeb.sessionId === sessionId, "pair_approved has correct sessionId (web)");
  assertEnvelope(pairMobile, "pair_approved", "pair_approved → mobile");
  assert(pairMobile.sessionId === sessionId, "pair_approved has correct sessionId (mobile)");

  // 3. Relay: mobile → web
  mobile.send(env("relay", sessionId, { action: "scan_result", data: "hello" }));
  const relayedToWeb = await waitMsg(web, "relay");

  assertEnvelope(relayedToWeb, "relay", "relay → web");
  assert(relayedToWeb.sessionId === sessionId, "relay has correct sessionId");
  assert(relayedToWeb.payload.from === "mobile", "relay payload.from is mobile");
  assert(relayedToWeb.payload.payload.action === "scan_result", "original payload forwarded intact (not mutated)");

  // 4. Relay: web → mobile
  web.send(env("relay", sessionId, { action: "ack" }));
  const relayedToMobile = await waitMsg(mobile, "relay");

  assertEnvelope(relayedToMobile, "relay", "relay → mobile");
  assert(relayedToMobile.payload.from === "web", "reverse relay payload.from is web");
  assert(relayedToMobile.payload.payload.action === "ack", "reverse relay payload forwarded intact");

  // 5. Disconnect: mobile closes → web should get session_close
  const webClosePromise = waitMsg(web, "session_close");
  mobile.close();
  const closeMsg = await webClosePromise;

  assertEnvelope(closeMsg, "session_close", "session_close → web");
  assert(closeMsg.sessionId === sessionId, "session_close has correct sessionId");
  assert(closeMsg.payload.reason === "peer_disconnected", "session_close payload.reason is peer_disconnected");

  web.close();
}

async function testExpiredSession() {
  console.log("\n--- Test 2: Expired session rejection ---");

  const web = await connect();
  web.send(env("qr_session_create"));
  await waitMsg(web, "qr_session_ready");

  const mobile = await connect();
  mobile.send(env("pair_request", "nonexistent-id"));
  const err = await waitMsg(mobile, "error");

  assertEnvelope(err, "error", "error → expired");
  assert(err.payload.message === "session_not_found", "payload.message is session_not_found");

  web.close();
  mobile.close();
}

async function testDuplicatePairing() {
  console.log("\n--- Test 3: Duplicate mobile pairing rejected ---");

  const web = await connect();
  const mobile1 = await connect();
  const mobile2 = await connect();

  web.send(env("qr_session_create"));
  const ready = await waitMsg(web, "qr_session_ready");
  const { sessionId } = ready;

  mobile1.send(env("pair_request", sessionId));
  await waitMsg(mobile1, "pair_approved");

  mobile2.send(env("pair_request", sessionId));
  const err = await waitMsg(mobile2, "error");

  assertEnvelope(err, "error", "error → duplicate pair");
  assert(err.payload.message === "session_already_paired", "payload.message is session_already_paired");

  web.close();
  mobile1.close();
  mobile2.close();
}

async function testAlreadyBound() {
  console.log("\n--- Test 4: Already-bound socket cannot create or pair again ---");

  const web = await connect();
  web.send(env("qr_session_create"));
  await waitMsg(web, "qr_session_ready");

  web.send(env("qr_session_create"));
  const err = await waitMsg(web, "error");

  assertEnvelope(err, "error", "error → already bound");
  assert(err.payload.message === "already_bound", "payload.message is already_bound");

  web.close();
}

async function testInvalidJson() {
  console.log("\n--- Test 5: Invalid JSON rejected ---");

  const ws = await connect();
  ws.send("not json at all");
  const err = await waitMsg(ws, "error");

  assertEnvelope(err, "error", "error → invalid json");
  assert(err.payload.message === "invalid_json", "payload.message is invalid_json");

  ws.close();
}

async function testMissingType() {
  console.log("\n--- Test 6: Missing type rejected ---");

  const ws = await connect();
  ws.send(JSON.stringify({ sessionId: null, timestamp: Date.now(), payload: {} }));
  const err = await waitMsg(ws, "error");

  assertEnvelope(err, "error", "error → missing type");
  assert(err.payload.message === "missing_type", "payload.message is missing_type");

  ws.close();
}

async function testHealthEndpoint() {
  console.log("\n--- Test 7: Health endpoint ---");

  const res = await fetch("http://localhost:8080/health");
  const body = await res.json();

  assert(res.status === 200, "Health returns 200");
  assert(body.status === "ok", "Health status is ok");
  assert(typeof body.sessions === "number", "Health reports session count");
}

(async () => {
  try {
    await testFullFlow();
    await testExpiredSession();
    await testDuplicatePairing();
    await testAlreadyBound();
    await testInvalidJson();
    await testMissingType();
    await testHealthEndpoint();

    console.log(`\n=============================`);
    console.log(`  PASSED: ${passed}  |  FAILED: ${failed}`);
    console.log(`=============================\n`);
    process.exit(failed > 0 ? 1 : 0);
  } catch (err) {
    console.error("FATAL:", err);
    process.exit(1);
  }
})();
