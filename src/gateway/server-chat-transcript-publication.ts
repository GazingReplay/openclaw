import { getTranscriptMessageRole } from "../agents/embedded-agent-runner/message-visibility.js";
import { readAgentAssistantSource } from "../infra/agent-events.js";
import { getAgentRunContext } from "../infra/agent-run-registry.js";
import {
  readSessionTranscriptRunId,
  type InternalSessionTranscriptUpdate,
} from "../sessions/transcript-events.js";
import { isChatAbortMarkerCurrent, type ChatRunState } from "./server-chat-state.js";
import { readTranscriptMessageIdempotencyKey } from "./session-transcript-entry-message.js";

/** Joins occurrence retirement to publication without stalling subsequent live text. */
export function createChatTranscriptPublication({
  chatRunState,
  agentRunSeq,
  flushBufferedChatDeltaIfNeeded,
}: {
  chatRunState: ChatRunState;
  agentRunSeq: Map<string, number>;
  flushBufferedChatDeltaIfNeeded: (
    sessionKey: string,
    agentId: string | undefined,
    clientRunId: string,
    sourceRunId: string,
    seq: number,
    opts: { controlUiVisible?: boolean; isHeartbeat?: boolean },
  ) => void;
}) {
  const transcriptPublications = new WeakMap<
    NonNullable<ReturnType<ChatRunState["getOrCreate"]>["assistantItems"]>,
    { count: number; settled: Promise<void>; resolve: () => void }
  >();
  return {
    deferTerminal(sourceRunId: string, clientRunId: string, finish: () => void | Promise<void>) {
      const run = chatRunState.runs.get(clientRunId);
      const items = run?.assistantItems;
      const publication = items && transcriptPublications.get(items);
      if (!publication) {
        return undefined;
      }
      const link = chatRunState.registry.peek(sourceRunId);
      return publication.settled.then(() => {
        if (
          chatRunState.runs.get(clientRunId) === run &&
          run?.assistantItems === items &&
          chatRunState.registry.peek(sourceRunId) === link
        ) {
          return finish();
        }
      });
    },
    retireTranscript: async (
      event: InternalSessionTranscriptUpdate,
      publish: (published: () => void) => Promise<void>,
    ) => {
      const sourceRunId = readSessionTranscriptRunId(event.message);
      if (!sourceRunId || getTranscriptMessageRole(event.message) !== "assistant") {
        return publish(() => {});
      }
      const link = chatRunState.registry.peek(sourceRunId);
      const clientRunId = link?.clientRunId ?? sourceRunId;
      const context = getAgentRunContext(sourceRunId);
      const sessionKey = link?.sessionKey ?? context?.sessionKey;
      const source = readAgentAssistantSource(event.message);
      const mirrorKey = readTranscriptMessageIdempotencyKey(event.message);
      const itemIds = [...(event.assistantItemIds ?? []), ...(mirrorKey ? [mirrorKey] : [])];
      if (
        (!source && itemIds.length === 0) ||
        !sessionKey ||
        sessionKey !== event.sessionKey ||
        (context?.sessionId && context.sessionId !== event.sessionId)
      ) {
        return publish(() => {});
      }
      const run = chatRunState.getOrCreate(clientRunId);
      if (run.bufferIsCurrent?.() === false || isChatAbortMarkerCurrent(run.abortMarker, link)) {
        return publish(() => {});
      }
      const retired = source
        ? chatRunState.retireSource(clientRunId, source)
        : chatRunState.retireBuffer(clientRunId, itemIds);
      if (retired) {
        run.liveTextEpoch = {};
      }
      const items = run.assistantItems;
      if (!items) {
        return publish(() => {});
      }
      const publishedItemIds = source?.itemId ? [source.itemId] : itemIds;
      const isCurrent = () =>
        chatRunState.runs.get(clientRunId) === run &&
        run.assistantItems === items &&
        run.bufferIsCurrent?.() !== false &&
        chatRunState.registry.peek(sourceRunId) === link &&
        !isChatAbortMarkerCurrent(run.abortMarker, link);
      const flush = () =>
        flushBufferedChatDeltaIfNeeded(
          sessionKey,
          link?.agentId ?? context?.agentId,
          clientRunId,
          sourceRunId,
          agentRunSeq.get(sourceRunId) ?? 0,
          {
            controlUiVisible: context?.isControlUiVisible,
            isHeartbeat: context?.isHeartbeat,
          },
        );
      let publication = transcriptPublications.get(items);
      if (!publication) {
        const { promise, resolve } = Promise.withResolvers<void>();
        publication = { count: 0, settled: promise, resolve };
        transcriptPublications.set(items, publication);
      }
      publication.count += 1;
      const pending = publication;
      try {
        await publish(() => {
          if (isCurrent() && chatRunState.retireBuffer(clientRunId, publishedItemIds, true)) {
            flush();
          }
        });
      } finally {
        pending.count -= 1;
        if (pending.count === 0) {
          transcriptPublications.delete(items);
          pending.resolve();
        }
      }
    },
  };
}
