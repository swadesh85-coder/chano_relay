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

function isSnapshotEnvelopeType(messageType) {
  return (
    messageType === "snapshot_start" ||
    messageType === "snapshot_chunk" ||
    messageType === "snapshot_complete" ||
    messageType === "snapshot_error"
  );
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
        isSnapshotLocked: false,
        snapshotPhase: "idle",
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

    return queueState.pending.shift() || null;
  }

  function findNextProcessableEntryIndex(queueState) {
    if (!queueState.isSnapshotLocked) {
      return queueState.pending.length === 0 ? -1 : 0;
    }

    for (let index = 0; index < queueState.pending.length; index += 1) {
      if (isSnapshotEnvelopeType(queueState.pending[index].envelope.type)) {
        return index;
      }
    }

    return -1;
  }

  function dequeueProcessableEntry(queueState, sessionId) {
    const processableIndex = findNextProcessableEntryIndex(queueState);

    if (processableIndex < 0) {
      return null;
    }

    if (processableIndex === 0) {
      return dequeueNextEnvelope(queueState);
    }

    for (let index = 0; index < processableIndex; index += 1) {
      logSnapshotEvent(
        "message_deferred_due_to_snapshot",
        sessionId,
        queueState.pending[index].envelope,
      );
    }

    return queueState.pending.splice(processableIndex, 1)[0] || null;
  }

  function logSnapshotEvent(label, sessionId, envelope, extra = "") {
    logger(
      `${label} session=${sessionId} type=${envelope.type} sequence=${envelope.sequence}${extra ? ` ${extra}` : ""}`,
    );
  }

  function validateSnapshotEnvelope(queueState, envelope, sessionId) {
    if (envelope.type === "snapshot_start") {
      if (queueState.snapshotPhase === "active") {
        logSnapshotEvent("SNAPSHOT_PROTOCOL_ERROR", sessionId, envelope, "reason=nested_snapshot_start");
        throw new Error("nested_snapshot_start");
      }

      queueState.isSnapshotLocked = true;
      queueState.snapshotPhase = "active";
      queueState.snapshotChunkIndex = 0;
      logSnapshotEvent("snapshot_lock_acquired", sessionId, envelope);
      return { chunkIndex: null, lockAcquired: true };
    }

    if (envelope.type === "snapshot_chunk") {
      if (queueState.snapshotPhase !== "active") {
        logSnapshotEvent("SNAPSHOT_PROTOCOL_ERROR", sessionId, envelope, "reason=snapshot_chunk_without_start");
        throw new Error("snapshot_chunk_without_start");
      }

      const chunkIndex = queueState.snapshotChunkIndex;
      queueState.snapshotChunkIndex += 1;
      return { chunkIndex };
    }

    if (envelope.type === "snapshot_complete" || envelope.type === "snapshot_error") {
      if (queueState.snapshotPhase !== "active") {
        logSnapshotEvent(
          "SNAPSHOT_PROTOCOL_ERROR",
          sessionId,
          envelope,
          `reason=${envelope.type}_without_active_snapshot`,
        );
        return { chunkIndex: null, skipProcessing: true };
      }

      return { chunkIndex: null, lockAcquired: false };
    }

    return { chunkIndex: null, lockAcquired: false };
  }

  function finalizeSnapshotEnvelope(queueState, envelope, sessionId) {
    if (envelope.type === "snapshot_complete" || envelope.type === "snapshot_error") {
      queueState.isSnapshotLocked = false;
      queueState.snapshotPhase = "idle";
      queueState.snapshotChunkIndex = 0;
      logSnapshotEvent("snapshot_lock_released", sessionId, envelope);
    }
  }

  function cleanupSessionQueue(sessionId, queueState) {
    if (!queueState.processing && !queueState.isSnapshotLocked && queueState.pending.length === 0) {
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
        const queuedEntry = dequeueProcessableEntry(queueState, sessionId);
        if (!queuedEntry) {
          if (queueState.isSnapshotLocked) {
            const deferredEntries = queueState.pending.filter(
              (pendingEntry) => !isSnapshotEnvelopeType(pendingEntry.envelope.type),
            );

            for (const deferredEntry of deferredEntries) {
              logSnapshotEvent(
                "message_deferred_due_to_snapshot",
                sessionId,
                deferredEntry.envelope,
              );
            }
          }

          break;
        }

        let guardState = { chunkIndex: null, lockAcquired: false };

        try {
          guardState = validateSnapshotEnvelope(queueState, queuedEntry.envelope, sessionId);

          if (guardState.skipProcessing) {
            queuedEntry.deferred.resolve(false);
            continue;
          }

          const routeResult = await forwardEnvelope(queuedEntry.envelope, queuedEntry.metadata);
          const chunkSuffix = guardState.chunkIndex === null ? "" : ` index=${guardState.chunkIndex}`;

          logger(
            `RELAY_QUEUE_PROCESS session=${sessionId} type=${queuedEntry.envelope.type} sequence=${queuedEntry.envelope.sequence}${chunkSuffix}`,
          );

          if (isSnapshotEnvelopeType(queuedEntry.envelope.type)) {
            logSnapshotEvent(
              "snapshot_message_processed",
              sessionId,
              queuedEntry.envelope,
              guardState.chunkIndex === null ? "" : `index=${guardState.chunkIndex}`,
            );
          }

          finalizeSnapshotEnvelope(queueState, queuedEntry.envelope, sessionId);

          queuedEntry.deferred.resolve(routeResult);
        } catch (error) {
          if (guardState.lockAcquired) {
            queueState.isSnapshotLocked = false;
            queueState.snapshotPhase = "idle";
            queueState.snapshotChunkIndex = 0;
          }

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
      rawFrame: metadata.rawFrame,
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
    validateSnapshotEnvelope,
  };
}

module.exports = {
  createSessionQueueManager,
};