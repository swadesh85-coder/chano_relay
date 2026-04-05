const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');
const { runRelayTransportIntegrityAudit } = require('./relayTransportIntegrityAudit');

const EVIDENCE_FILE_PATH = path.join(__dirname, 'relay_projection_bridge_evidence.txt');

const WEB_PROJECTION_AUDIT_COMMAND = process.platform === 'win32'
  ? 'ng test --watch=false --browsers=chromium --include src/app/projection/web_sync_e2e.spec.ts'
  : 'npx ng test --watch=false --browsers=chromium --include src/app/projection/web_sync_e2e.spec.ts';

function resolveNpxCommand() {
  return process.platform === 'win32' ? 'npx.cmd' : 'npx';
}

function runWebProjectionAudit() {
  const webWorkspace = path.resolve(__dirname, '..', 'chano_web');
  const isWindows = process.platform === 'win32';
  const command = isWindows ? 'cmd.exe' : resolveNpxCommand();
  const args = isWindows
    ? ['/d', '/s', '/c', WEB_PROJECTION_AUDIT_COMMAND]
    : ['ng', 'test', '--watch=false', '--browsers=chromium', '--include', 'src/app/projection/web_sync_e2e.spec.ts'];

  const result = spawnSync(command, args, {
    cwd: webWorkspace,
    encoding: 'utf8',
    shell: false,
  });

  return {
    cwd: webWorkspace,
    command: `${command} ${args.join(' ')}`,
    status: result.status ?? 1,
    stdout: result.stdout ?? '',
    stderr: `${result.stderr ?? ''}${result.error ? `\n${result.error.message}` : ''}`.trim(),
  };
}

function buildEvidenceReport(result) {
  const lines = [];
  lines.push('Relay Projection Bridge Audit');
  lines.push(`GeneratedAt: ${new Date(result.generatedAt).toISOString()}`);
  lines.push(`Verdict: ${result.verdict}`);
  lines.push('');
  lines.push('Relay Transport Evidence');
  lines.push(`TransportVerdict: ${result.transport.verdict}`);
  lines.push(`ObservedForwardOrder: ${result.transport.observedOrder.join(' -> ')}`);
  lines.push(`SnapshotSequenceValid: ${result.transport.summary.snapshotSequenceValid}`);
  lines.push(`EventStreamAfterSnapshotComplete: ${result.transport.summary.eventStreamAfterSnapshotComplete}`);
  lines.push('');
  lines.push('Web Projection Evidence');
  lines.push(`ProjectionSpecStatus: ${result.web.status}`);
  lines.push(`ProjectionSpecCommand: ${result.web.command}`);
  lines.push(`ProjectionSpecCwd: ${result.web.cwd}`);
  lines.push('ProjectionSuccessMarkersAsserted: SNAPSHOT_ASSEMBLY_STARTED, BOUNDARY_OK, EVENT_APPLY, EVENT_IGNORE_DUPLICATE, SNAPSHOT_RESYNC_REQUIRED');
  lines.push('');
  lines.push('Web Stdout');
  lines.push(result.web.stdout.trim() || '<empty>');
  if (result.web.stderr.trim()) {
    lines.push('');
    lines.push('Web Stderr');
    lines.push(result.web.stderr.trim());
  }

  return `${lines.join('\n')}\n`;
}

async function runRelayProjectionBridgeAudit(options = {}) {
  const transport = await runRelayTransportIntegrityAudit({ emitOutput: options.emitOutput === true });
  const web = runWebProjectionAudit();
  const verdict = transport.verdict === 'PASS' && web.status === 0 ? 'PASS' : 'FAIL';

  const result = {
    generatedAt: Date.now(),
    verdict,
    transport,
    web,
    evidenceFilePath: EVIDENCE_FILE_PATH,
  };

  fs.writeFileSync(EVIDENCE_FILE_PATH, buildEvidenceReport(result), 'utf8');
  return result;
}

if (require.main === module) {
  runRelayProjectionBridgeAudit({ emitOutput: false })
    .then((result) => {
      process.stdout.write(`Relay projection bridge audit ${result.verdict}. Evidence: ${result.evidenceFilePath}\n`);
      if (result.verdict !== 'PASS') {
        process.exitCode = 1;
      }
    })
    .catch((error) => {
      process.stderr.write(`${error.stack || error.message}\n`);
      process.exitCode = 1;
    });
}

module.exports = {
  EVIDENCE_FILE_PATH,
  runRelayProjectionBridgeAudit,
};