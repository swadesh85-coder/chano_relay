const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const test = require("node:test");
const { EventEmitter } = require("events");
const WebSocket = require("ws");
const selfsigned = require("selfsigned");

const {
  createConnectionManager,
  createMessageRouter,
  createRelayServer,
  DEFAULT_PROTOCOL_VERSION,
  DEFAULT_MAX_PAYLOAD_BYTES,
} = require("./server");

class MockSocket extends EventEmitter {
  constructor() {
    super();
    this.sentMessages = [];
    this.readyState = 1;
  }

  send(message) {
    this.sentMessages.push(message);
  }

  close() {
    this.readyState = 3;
    this.emit("close");
  }
}

function createEnvelope(overrides = {}) {
  return {
    protocolVersion: DEFAULT_PROTOCOL_VERSION,
    type: "event_stream",
    sessionId: "session-1",
    timestamp: Date.now(),
    sequence: 1,
    payload: { opaque: true },
    ...overrides,
  };
}

function waitForOpen(socket) {
  return new Promise((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
}

function waitForConnectionCount(relayServer, expectedCount, timeoutMs = 3000) {
  const start = Date.now();

  return new Promise((resolve, reject) => {
    const poll = () => {
      if (relayServer.getConnectionCount() === expectedCount) {
        resolve();
        return;
      }

      if (Date.now() - start >= timeoutMs) {
        reject(new Error(`Timed out waiting for ${expectedCount} active connections`));
        return;
      }

      setTimeout(poll, 10);
    };

    poll();
  });
}

function waitForMessage(socket, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Timed out waiting for websocket message")), timeoutMs);

    socket.once("message", (data, isBinary) => {
      clearTimeout(timer);
      if (isBinary) {
        reject(new Error("Expected text websocket frame"));
        return;
      }

      resolve(JSON.parse(data.toString("utf8")));
    });

    socket.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
}

async function withStartedServer(config, callback) {
  const relayServer = createRelayServer(config);
  await relayServer.start();

  try {
    await callback(relayServer);
  } finally {
    await relayServer.stop();
  }
}

test("server_start", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    assert.equal(typeof address.port, "number");
    assert.ok(address.port > 0);

    const response = await fetch(`http://127.0.0.1:${address.port}/health`);
    const body = await response.json();

    assert.equal(response.status, 200);
    assert.equal(body.status, "ok");
    assert.equal(body.wsPath, "/relay");
    assert.equal(body.transport, "ws");
    assert.equal(body.protocolVersion, DEFAULT_PROTOCOL_VERSION);
    assert.equal(body.maxPayloadBytes, DEFAULT_MAX_PAYLOAD_BYTES);
  });
});

test("websocket_upgrade", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "chano-relay-"));
  const keyPath = path.join(tempDir, "relay.key");
  const certPath = path.join(tempDir, "relay.crt");
  const cert = await selfsigned.generate([{ name: "commonName", value: "127.0.0.1" }], {
    algorithm: "rsa",
    days: 7,
    keySize: 2048,
  });

  fs.writeFileSync(keyPath, cert.private);
  fs.writeFileSync(certPath, cert.cert);

  try {
    await withStartedServer(
      {
        host: "127.0.0.1",
        port: 0,
        wsPath: "/relay",
        tls: {
          enabled: true,
          keyPath,
          certPath,
          minVersion: "TLSv1.2",
        },
      },
      async (relayServer) => {
        const address = relayServer.server.address();
        const socket = new WebSocket(`wss://127.0.0.1:${address.port}/relay`, {
          rejectUnauthorized: false,
        });

        await waitForOpen(socket);
        assert.equal(relayServer.getConnectionCount(), 1);
        socket.close();
      },
    );
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
});

test("connection_accept", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
    const expectedEnvelope = createEnvelope();
    const envelopePromise = new Promise((resolve) => {
      relayServer.events.once("transportEnvelope", resolve);
    });

    await waitForOpen(socket);
    socket.send(JSON.stringify(expectedEnvelope));

    const result = await envelopePromise;
    assert.deepEqual(result.envelope, expectedEnvelope);

    socket.close();
  });
});

test("invalid_protocol_version", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
    const envelope = createEnvelope({ protocolVersion: 1 });

    await waitForOpen(socket);
    socket.send(JSON.stringify(envelope));

    const response = await waitForMessage(socket);
    assert.equal(response.type, "transport_error");
    assert.equal(response.payload.reason, "unsupported_protocol_version");

    socket.close();
  });
});

test("missing_session_id", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
    const envelope = createEnvelope({ sessionId: "" });

    await waitForOpen(socket);
    socket.send(JSON.stringify(envelope));

    const response = await waitForMessage(socket);
    assert.equal(response.type, "transport_error");
    assert.equal(response.payload.reason, "missing_session_id");

    socket.close();
  });
});

test("missing_type", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
    const envelope = createEnvelope({ type: "" });

    await waitForOpen(socket);
    socket.send(JSON.stringify(envelope));

    const response = await waitForMessage(socket);
    assert.equal(response.type, "transport_error");
    assert.equal(response.payload.reason, "missing_type");

    socket.close();
  });
});

test("payload_size_limit", async () => {
  await withStartedServer(
    { host: "127.0.0.1", port: 0, wsPath: "/relay", maxPayloadBytes: 32 },
    async (relayServer) => {
      const address = relayServer.server.address();
      const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const envelope = createEnvelope({ payload: { blob: "x".repeat(64) } });

      await waitForOpen(socket);
      socket.send(JSON.stringify(envelope));

      const response = await waitForMessage(socket);
      assert.equal(response.type, "transport_error");
      assert.equal(response.payload.reason, "payload_too_large");

      socket.close();
    },
  );
});

test("valid_envelope_pass", async () => {
  await withStartedServer(
    { host: "127.0.0.1", port: 0, wsPath: "/relay", maxPayloadBytes: 256 },
    async (relayServer) => {
      const address = relayServer.server.address();
      const socket = new WebSocket(`ws://127.0.0.1:${address.port}/relay`);
      const expectedEnvelope = createEnvelope({ payload: { blob: "ok" } });
      const envelopePromise = new Promise((resolve) => {
        relayServer.events.once("transportEnvelope", resolve);
      });

      await waitForOpen(socket);
      socket.send(JSON.stringify(expectedEnvelope));

      const result = await envelopePromise;
      assert.deepEqual(result.envelope, expectedEnvelope);

      socket.close();
    },
  );
});

test("socket_register", () => {
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket);
  connectionManager.registerConnection(webSocket);

  assert.equal(connectionManager.getConnectionCount(), 2);
});

test("socket_remove", () => {
  const connectionManager = createConnectionManager();
  const socket = new MockSocket();

  connectionManager.registerConnection(socket);
  assert.equal(connectionManager.getConnectionCount(), 1);

  const removed = connectionManager.removeConnection(socket);
  assert.equal(removed, true);
  assert.equal(connectionManager.getConnectionCount(), 0);
});

test("session_binding", () => {
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket);
  connectionManager.registerConnection(webSocket);
  connectionManager.bindSessionSockets("session-42", mobileSocket, webSocket);

  const binding = connectionManager.lookupConnection("session-42");
  assert.equal(binding.mobileSocket, mobileSocket);
  assert.equal(binding.webSocket, webSocket);
});

test("disconnect_cleanup", async () => {
  const connectionManager = createConnectionManager();
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();

  connectionManager.registerConnection(mobileSocket);
  connectionManager.registerConnection(webSocket);
  connectionManager.bindSessionSockets("session-cleanup", mobileSocket, webSocket);
  assert.equal(connectionManager.getConnectionCount(), 2);

  mobileSocket.emit("close");

  assert.equal(connectionManager.getConnectionCount(), 1);
  const binding = connectionManager.lookupConnection("session-cleanup");
  assert.equal(binding.mobileSocket, null);
  assert.equal(binding.webSocket, webSocket);
});

test("mobile_to_web_routing", () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({ sessionId: "session-route-1", payload: { opaque: "mobile" } });

  connectionManager.bindSessionSockets("session-route-1", mobileSocket, webSocket);

  const routed = messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, true);
  assert.equal(webSocket.sentMessages.length, 1);
  assert.equal(webSocket.sentMessages[0], JSON.stringify(envelope));
  assert.equal(mobileSocket.sentMessages.length, 0);
});

test("web_to_mobile_routing", () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({ sessionId: "session-route-2", payload: { opaque: "web" } });

  connectionManager.bindSessionSockets("session-route-2", mobileSocket, webSocket);

  const routed = messageRouter.routeMessage(envelope, webSocket);

  assert.equal(routed, true);
  assert.equal(mobileSocket.sentMessages.length, 1);
  assert.equal(mobileSocket.sentMessages[0], JSON.stringify(envelope));
  assert.equal(webSocket.sentMessages.length, 0);
});

test("missing_destination_drop", () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({ sessionId: "session-route-3" });

  connectionManager.bindSessionSockets("session-route-3", mobileSocket, webSocket);
  webSocket.close();

  const routed = messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, false);
  assert.equal(webSocket.sentMessages.length, 0);
});

test("envelope_integrity_preserved", () => {
  const connectionManager = createConnectionManager();
  const messageRouter = createMessageRouter(connectionManager);
  const mobileSocket = new MockSocket();
  const webSocket = new MockSocket();
  const envelope = createEnvelope({
    sessionId: "session-route-4",
    timestamp: 1234567890,
    sequence: 77,
    payload: { nested: { opaque: true } },
  });
  const originalSnapshot = JSON.stringify(envelope);

  connectionManager.bindSessionSockets("session-route-4", mobileSocket, webSocket);

  const routed = messageRouter.routeMessage(envelope, mobileSocket);

  assert.equal(routed, true);
  assert.equal(webSocket.sentMessages[0], originalSnapshot);
  assert.equal(JSON.stringify(envelope), originalSnapshot);
});

test("multiple_connection_handling", async () => {
  await withStartedServer({ host: "127.0.0.1", port: 0, wsPath: "/relay" }, async (relayServer) => {
    const address = relayServer.server.address();
    const sockets = Array.from({ length: 8 }, () => new WebSocket(`ws://127.0.0.1:${address.port}/relay`));

    await Promise.all(sockets.map(waitForOpen));
    assert.equal(relayServer.getConnectionCount(), sockets.length);

    await Promise.all(
      sockets.map(
        (socket) =>
          new Promise((resolve) => {
            socket.once("close", resolve);
            socket.close();
          }),
      ),
    );

    await waitForConnectionCount(relayServer, 0);
    assert.equal(relayServer.getConnectionCount(), 0);
  });
});
