import type { AgentEventRuntimePayload } from "../infra/agent-events.types.js";
import { logError } from "../logger.js";
import { setSafeTimeout } from "../utils/timer-delay.js";
import { formatForLog } from "./ws-log.js";

export type ChatTerminalLifecycleOptions = {
  publishLifecycle?: boolean;
  skipChatErrorFinal?: boolean;
  suppressRestartRecoveryProjection?: boolean;
  restartRecoveryState?: { suppress: boolean };
  isCurrent?: () => boolean;
};

/** Retry start and disposal revoke a grace error even after its timer has fired. */
export function createChatErrorGrace(params: {
  delayMs: number;
  finalize: (
    event: AgentEventRuntimePayload,
    options: ChatTerminalLifecycleOptions,
  ) => void | Promise<void>;
}) {
  const pending = new Map<
    string,
    {
      timer: NodeJS.Timeout;
      event: AgentEventRuntimePayload;
      options?: ChatTerminalLifecycleOptions;
    }
  >();
  const publications = new Set<Promise<void>>();
  let closed = false;
  const clear = (runId: string, lifecycleGeneration?: string) => {
    const entry = pending.get(runId);
    if (
      !entry ||
      (lifecycleGeneration &&
        entry.event.lifecycleGeneration &&
        lifecycleGeneration !== entry.event.lifecycleGeneration)
    ) {
      return;
    }
    clearTimeout(entry.timer);
    pending.delete(runId);
  };
  return {
    clear,
    schedule(event: AgentEventRuntimePayload, options?: ChatTerminalLifecycleOptions) {
      if (closed) {
        return;
      }
      clear(event.runId);
      const timer = setSafeTimeout(() => {
        const entry = pending.get(event.runId);
        if (!entry || entry.timer !== timer) {
          return;
        }
        const isCurrent = () => pending.get(event.runId) === entry;
        const release = () => {
          if (isCurrent()) {
            pending.delete(event.runId);
          }
        };
        let completion: void | Promise<void> = undefined;
        try {
          completion = params.finalize(entry.event, { ...entry.options, isCurrent });
        } finally {
          if (!completion) {
            release();
          }
        }
        if (completion) {
          const publication = completion
            .catch((error: unknown) => {
              logError(`Deferred chat terminal publication failed: ${formatForLog(error)}`);
            })
            .finally(() => {
              publications.delete(publication);
              release();
            });
          publications.add(publication);
        }
      }, params.delayMs);
      timer.unref?.();
      pending.set(event.runId, { timer, event, options });
    },
    dispose: async () => {
      closed = true;
      for (const entry of pending.values()) {
        clearTimeout(entry.timer);
      }
      pending.clear();
      await Promise.allSettled(publications);
    },
  };
}
