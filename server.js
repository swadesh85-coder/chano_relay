const http = require("http");
const WebSocket = require("ws");
const { WebSocketServer } = WebSocket;
const { v4: uuidv4 } = require("uuid");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const PORT = parseInt(process.env.PORT, 10) || 8080;
const SESSION_TTL_MS = parseInt(process.env.SESSION_TTL_MS, 10) || 60 * 1000; // 60 seconds
const REAPER_INTERVAL_MS = 10 * 1000; // sweep every 10s (tight for 60s TTL)

// ---------------------------------------------------------------------------
// Envelope helper — every outbound message conforms to the standard shape:
// { type, sessionId, timestamp, payload }
// ---------------------------------------------------------------------------
function envelope(type, sessionId, payload) {
  return JSON.stringify({
    type,
    sessionId: sessionId || null,
    timestamp: Date.now(),
    payload: payload || {},
  });
}

// ---------------------------------------------------------------------------
// Session Registry (in-memory only — zero persistence by design)
// ---------------------------------------------------------------------------
/** @type {Map<string, RelaySession>} */
const sessions = new Map();

/**
 * @typedef {Object} RelaySession
 * @property {string}         sessionId
 * @property {number}         createdAt    - epoch ms
 * @property {number}         expiresAt    - epoch ms
 * @property {WebSocket|null} webSocket
 * @property {WebSocket|null} mobileSocket
 */

function createSession(webSocket) {
  const now = Date.now();
  /** @type {RelaySession} */
  const session = {
    sessionId: uuidv4(),
    createdAt: now,
    expiresAt: now + SESSION_TTL_MS,
    webSocket,
    mobileSocket: null,
  };
  sessions.set(session.sessionId, session);
  return session;
}

/**
 * Send session_close to both sides and remove from registry.
 * The relay never stores user vault data, never decrypts payloads,
 * and never mutates message contents.
 */
function destroySession(sessionId, reason) {
  const session = sessions.get(sessionId);
  if (!session) return;

  const closeMsg = envelope("session_close", sessionId, { reason });

  for (const role of ["webSocket", "mobileSocket"]) {
    const sock = session[role];
    if (sock && sock.readyState === WebSocket.OPEN) {
      sock.send(closeMsg);
      sock.close(1000, reason || "session_destroyed");
    }
  }
  sessions.delete(sessionId);
}

// ---------------------------------------------------------------------------
// Session Reaper — automatic expiry
// ---------------------------------------------------------------------------
const reaper = setInterval(() => {
  const now = Date.now();
  for (const [id, session] of sessions) {
    if (now >= session.expiresAt) {
      console.log(`[reaper] Session ${id} expired`);
      destroySession(id, "session_expired");
    }
  }
}, REAPER_INTERVAL_MS);
reaper.unref(); // don't prevent process exit

// ---------------------------------------------------------------------------
// HTTP Server — health endpoint only (session creation is over WS)
// ---------------------------------------------------------------------------
const httpServer = http.createServer((req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(204, corsHeaders());
    res.end();
    return;
  }

  if (req.method === "GET" && req.url === "/health") {
    const body = JSON.stringify({ status: "ok", sessions: sessions.size });
    res.writeHead(200, {
      ...corsHeaders(),
      "Content-Type": "application/json",
      "Content-Length": Buffer.byteLength(body),
    });
    res.end(body);
    return;
  }

  res.writeHead(404, corsHeaders());
  res.end();
});

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  };
}

// ---------------------------------------------------------------------------
// WebSocket Server — relay transport
//
// SECURITY INVARIANTS:
// • The relay NEVER stores user vault data.
// • The relay NEVER decrypts payloads.
// • The relay NEVER mutates message contents — payload is forwarded opaquely.
// ---------------------------------------------------------------------------
const wss = new WebSocketServer({ server: httpServer });

wss.on("connection", (socket) => {
  let boundSessionId = null;
  let boundRole = null; // "web" | "mobile"

  socket.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      socket.send(envelope("error", null, { message: "invalid_json" }));
      return;
    }

    // Validate inbound envelope shape
    if (!msg.type) {
      socket.send(envelope("error", boundSessionId, { message: "missing_type" }));
      return;
    }

    // ------ QR SESSION CREATE (Web client) ------
    if (msg.type === "qr_session_create") {
      if (boundSessionId) {
        socket.send(envelope("error", boundSessionId, { message: "already_bound" }));
        return;
      }

      const session = createSession(socket);
      boundSessionId = session.sessionId;
      boundRole = "web";

      socket.send(envelope("qr_session_ready", session.sessionId, {
        expiresAt: session.expiresAt,
      }));

      console.log(`[ws] Web created session ${session.sessionId}`);
      return;
    }

    // ------ PAIR REQUEST (Mobile client) ------
    if (msg.type === "pair_request") {
      const { sessionId } = msg;

      if (!sessionId) {
        socket.send(envelope("error", null, { message: "missing_session_id" }));
        return;
      }

      if (boundSessionId) {
        socket.send(envelope("error", boundSessionId, { message: "already_bound" }));
        return;
      }

      const session = sessions.get(sessionId);
      if (!session) {
        socket.send(envelope("error", sessionId, { message: "session_not_found" }));
        return;
      }

      if (Date.now() >= session.expiresAt) {
        destroySession(sessionId, "session_expired");
        socket.send(envelope("error", sessionId, { message: "session_expired" }));
        return;
      }

      if (session.mobileSocket) {
        socket.send(envelope("error", sessionId, { message: "session_already_paired" }));
        return;
      }

      // Bind mobile socket to session
      session.mobileSocket = socket;
      boundSessionId = sessionId;
      boundRole = "mobile";

      // Notify the Web client that pairing succeeded
      if (session.webSocket && session.webSocket.readyState === WebSocket.OPEN) {
        session.webSocket.send(envelope("pair_approved", sessionId, {}));
      }

      // Confirm to mobile
      socket.send(envelope("pair_approved", sessionId, {}));

      console.log(`[ws] Mobile paired to session ${sessionId}`);
      return;
    }

    // ------ RELAY: forward payload to the peer ------
    if (msg.type === "relay") {
      if (!boundSessionId || !boundRole) {
        socket.send(envelope("error", null, { message: "not_paired" }));
        return;
      }

      const session = sessions.get(boundSessionId);
      if (!session) {
        socket.send(envelope("error", boundSessionId, { message: "session_not_found" }));
        return;
      }

      const peerKey = boundRole === "mobile" ? "webSocket" : "mobileSocket";
      const peer = session[peerKey];

      if (!peer || peer.readyState !== WebSocket.OPEN) {
        socket.send(envelope("error", boundSessionId, { message: "peer_not_connected" }));
        return;
      }

      // Forward payload OPAQUELY — relay never inspects, stores, or mutates
      peer.send(envelope("relay", boundSessionId, {
        from: boundRole,
        payload: msg.payload,
      }));
      return;
    }

    socket.send(envelope("error", boundSessionId, { message: "unknown_type" }));
  });

  // ------ DISCONNECT: tear down entire session ------
  socket.on("close", () => {
    if (!boundSessionId) return;
    console.log(`[ws] ${boundRole} disconnected from session ${boundSessionId}`);
    destroySession(boundSessionId, "peer_disconnected");
  });

  socket.on("error", (err) => {
    console.error(`[ws] Socket error:`, err.message);
  });
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------
httpServer.listen(PORT, () => {
  console.log(`[chano_relay] Relay server listening on port ${PORT}`);
  console.log(`[chano_relay] Session TTL: ${SESSION_TTL_MS / 1000}s | Reaper interval: ${REAPER_INTERVAL_MS / 1000}s`);
});