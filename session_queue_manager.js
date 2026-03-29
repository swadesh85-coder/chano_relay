function assertSessionId(sessionId) {
  if (typeof sessionId !== "string" || sessionId.trim() === "") {
    throw new Error("sessionId is required");
  }
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
        tail: Promise.resolve(),
        pendingCount: 0,
      };
      sessionQueues.set(sessionId, queueState);
    }

    return queueState;
  }

  function cleanupSessionQueue(sessionId, queueState) {
    if (queueState.pendingCount === 0) {
      sessionQueues.delete(sessionId);
    }
  }

  async function processQueue(sessionId) {
    const queueState = sessionQueues.get(sessionId);
    return queueState ? queueState.tail : Promise.resolve();
  }

  function enqueueMessage(sessionId, envelope, metadata = {}) {
    const queueState = initializeSessionQueue(sessionId);
    queueState.pendingCount += 1;

    const resultPromise = queueState.tail
      .catch(() => {})
      .then(async () => {
        const routeResult = await forwardEnvelope(envelope, metadata);

        logger(
          `RELAY_QUEUE_PROCESS session=${sessionId} type=${envelope.type} sequence=${envelope.sequence}`,
        );

        return routeResult;
      });

    queueState.tail = resultPromise
      .catch(() => {})
      .finally(() => {
        queueState.pendingCount -= 1;
        cleanupSessionQueue(sessionId, queueState);
      });

    return resultPromise;
  }

  return {
    initializeSessionQueue,
    enqueueMessage,
    processQueue,
  };
}

module.exports = {
  createSessionQueueManager,
};