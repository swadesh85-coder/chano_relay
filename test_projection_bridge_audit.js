const assert = require('node:assert/strict');
const test = require('node:test');
const { runRelayProjectionBridgeAudit } = require('./relayProjectionBridgeAudit');

test('relay_projection_bridge_audit', async () => {
  const result = await runRelayProjectionBridgeAudit({ emitOutput: false });

  assert.equal(result.transport.verdict, 'PASS');
  assert.equal(result.transport.summary.snapshotSequenceValid, true);
  assert.equal(result.transport.summary.eventStreamAfterSnapshotComplete, true);
  assert.equal(result.web.status, 0);
  assert.equal(result.verdict, 'PASS');
});