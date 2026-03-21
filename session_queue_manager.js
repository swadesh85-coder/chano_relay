function assertSessionId(sessionId) {
  if (typeof sessionId !== "string" || sessionId.trim() === "") {
    throw new Error("sessionId is required");
  }
}

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject,
  };
}

function createSessionQueueManager(options = {}) {
  const logger = typeof options.logger === "function" ? options.logger : console.log;
  const forwardEnvelope = options.forwardEnvelope;

  if (typeof forwardEnvelope !== "function") {
    throw new Error("forwardEnvelope is required");
  }

  const sessionQueues = new Map();

  function initializeSessionQueue(sessionId) {
    assertSessionId(sessionId);

    let queueState = sessionQueues.get(sessionId);
    if (!queueState) {
      queueState = {
        pending: [],
        processing: false,
        scheduled: false,
        inSnapshotMode: false,
        snapshotChunkIndex: 0,
        arrivalOrder: 0,
      };
      sessionQueues.set(sessionId, queueState);
    }

    return queueState;
  }

  function dequeueNextEnvelope(queueState) {
    if (queueState.pending.length === 0) {
      return null;
    }

    if (queueState.inSnapshotMode) {
      const nextPendingEntry = queueState.pending[0] || null;

      if (!nextPendingEntry || nextPendingEntry.envelope.type !== "event_stream") {
        return queueState.pending.shift() || null;
      }

      const snapshotEntryIndex = queueState.pending.findIndex(
        (entry) => typeof entry.envelope.type === "string" && entry.envelope.type.startsWith("snapshot"),
      );

      if (snapshotEntryIndex === -1) {
        return null;
      }

      const [snapshotEntry] = queueState.pending.splice(snapshotEntryIndex, 1);
      return snapshotEntry;
    }

    return queueState.pending.shift() || null;
  }

  function snapshotContiguityGuard(queueState, envelope) {
    if (envelope.type === "snapshot_start") {
      queueState.inSnapshotMode = true;
      queueState.snapshotChunkIndex = 0;
      return { chunkIndex: null };
    }

    if (envelope.type === "snapshot_chunk") {
      const chunkIndex = queueState.snapshotChunkIndex;
      queueState.snapshotChunkIndex += 1;
      return { chunkIndex };
    }

    if (envelope.type === "snapshot_complete") {
      queueState.inSnapshotMode = false;
      queueState.snapshotChunkIndex = 0;
      return { chunkIndex: null };
    }

    return { chunkIndex: null };
  }

  function cleanupSessionQueue(sessionId, queueState) {
    if (!queueState.processing && !queueState.inSnapshotMode && queueState.pending.length === 0) {
      sessionQueues.delete(sessionId);
    }
  }

  function scheduleProcessQueue(sessionId, queueState) {
    if (queueState.processing || queueState.scheduled) {
      return;
    }

    queueState.scheduled = true;

    queueMicrotask(() => {
      queueState.scheduled = false;
      processQueue(sessionId).catch(() => {});
    });
  }

  async function processQueue(sessionId) {
    const queueState = initializeSessionQueue(sessionId);
    if (queueState.processing) {
      return;
    }

    queueState.processing = true;

    try {
      while (queueState.pending.length > 0) {
        const queuedEntry = dequeueNextEnvelope(queueState);
        if (!queuedEntry) {
          break;
        }

        try {
          const guardState = snapshotContiguityGuard(queueState, queuedEntry.envelope);
          const routeResult = await forwardEnvelope(queuedEntry.envelope, queuedEntry.metadata);
          const chunkSuffix = guardState.chunkIndex === null ? "" : ` index=${guardState.chunkIndex}`;

          logger(
            `RELAY_QUEUE_PROCESS session=${sessionId} type=${queuedEntry.envelope.type} sequence=${queuedEntry.envelope.sequence}${chunkSuffix}`,
          );

          queuedEntry.deferred.resolve(routeResult);
        } catch (error) {
          queuedEntry.deferred.reject(error);
        }
      }
    } finally {
      queueState.processing = false;
      cleanupSessionQueue(sessionId, queueState);
    }
  }

  function enqueueMessage(sessionId, envelope, metadata = {}) {
    const queueState = initializeSessionQueue(sessionId);
    const deferred = createDeferred();

    queueState.pending.push({
      envelope,
      metadata,
      deferred,
      arrivalOrder: queueState.arrivalOrder,
    });
    queueState.arrivalOrder += 1;

    scheduleProcessQueue(sessionId, queueState);

    return deferred.promise;
  }

  return {
    initializeSessionQueue,
    enqueueMessage,
    processQueue,
    snapshotContiguityGuard,
  };
}

module.exports = {
  createSessionQueueManager,
};