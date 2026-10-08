import { expect, it, vi, type Mock } from "vitest";
import {
  awaitGateBeforeSettlement,
  createDeferred,
  withinTest,
} from "../../test/helpers/promise.js";
import {
  type AgentEventPayload,
  type AgentAssistantSourceReceipt,
  bindAgentAssistantSource,
  emitAgentEvent,
  emitAgentEventWithAssistantSourceIfCurrent,
  getAgentEventLifecycleGeneration,
} from "../infra/agent-events.js";
import { claimAgentRunContext } from "../infra/agent-run-registry.js";
import {
  getActiveGatewayRootWorkCount,
  tryBeginGatewayRootWorkAdmission,
} from "../process/gateway-work-admission.js";
import { emitSessionTranscriptUpdate } from "../sessions/transcript-events.js";
import { AsyncWorkScope, getAsyncWorkSignal } from "../shared/async-work-scope.js";
import type { AgentEventHandlerOptions } from "./server-chat.js";
import type { startGatewayEventSubscriptions } from "./server-runtime-subscriptions.js";
import {
  type createSubscriptionTestFixture,
  registerSubscriptionChatRun,
} from "./server-runtime-subscriptions.test-support.js";
import type { readSessionMessageByIdAsync } from "./session-transcript-readers.js";

export function registerTranscriptPublicationTests(fixture: {
  installHandlerFactory: (
    factory: typeof import("./server-chat.js").createAgentEventHandler,
  ) => void;
  stop: () => Promise<void>;
  createParams: ReturnType<typeof createSubscriptionTestFixture>["createParams"];
  start: (
    params: Parameters<typeof startGatewayEventSubscriptions>[0],
  ) => ReturnType<typeof startGatewayEventSubscriptions>;
  transcriptBroadcastMocks: {
    useActualHandler: boolean;
    readMessageById: Mock<typeof readSessionMessageByIdAsync>;
  };
  warn: ReturnType<typeof createSubscriptionTestFixture>["warn"];
}): void {
  const { createParams, start, transcriptBroadcastMocks, warn, installHandlerFactory, stop } =
    fixture;
  it("publishes committed transcripts after the producer drains and before Gateway shutdown", async ({
    signal,
  }) => {
    transcriptBroadcastMocks.useActualHandler = true;
    const readEntered = createDeferred();
    const releaseRead = createDeferred();
    const published = createDeferred();
    const producer = new AsyncWorkScope();
    const storedMessage = {
      role: "assistant",
      content: [{ type: "text", text: "committed completion" }],
      __openclaw: { transcriptPosition: { source: "committed-generation", rawSeq: 1 } },
    };
    transcriptBroadcastMocks.readMessageById.mockImplementationOnce(async () => {
      const readSignal = getAsyncWorkSignal();
      readEntered.resolve();
      await releaseRead.promise;
      readSignal?.throwIfAborted();
      return { found: true, oversized: false, seq: 1, message: storedMessage };
    });
    const params = createParams();
    params.sessionEventSubscribers.subscribe("conn-transcript");
    params.broadcastToConnIds = vi.fn(() => published.resolve());
    const unsubs = start(params);
    const admission = tryBeginGatewayRootWorkAdmission("test:committed-transcript-publisher");
    if (!admission) {
      throw new Error("Transcript publisher admission was closed");
    }
    let shutdown: Promise<void> | undefined;
    try {
      await admission.run(async () =>
        producer.run(() =>
          emitSessionTranscriptUpdate({
            messageId: "committed-message",
            message: storedMessage,
            target: {
              agentId: "main",
              sessionId: "committed-session",
              sessionKey: "agent:main:main",
              storePath: "/tmp/openclaw-committed-transcript.sqlite",
            },
          }),
        ),
      );
      await withinTest(readEntered.promise, signal);
      admission.release();
      await producer.drain();
      expect(getActiveGatewayRootWorkCount()).toBe(1);
      shutdown = unsubs.agentUnsub();
      const publicationBeforeShutdown = awaitGateBeforeSettlement(
        published.promise,
        shutdown,
        "Gateway shutdown finished before the committed transcript was published",
      );
      releaseRead.resolve();
      await withinTest(publicationBeforeShutdown, signal);
      await shutdown;
      expect(params.broadcastToConnIds).toHaveBeenCalledExactlyOnceWith(
        "session.message",
        expect.objectContaining({
          sessionKey: "agent:main:main",
          messageId: "committed-message",
          message: expect.objectContaining({ content: storedMessage.content }),
        }),
        new Set(["conn-transcript"]),
        undefined,
      );
      expect(warn).not.toHaveBeenCalled();
      expect(getActiveGatewayRootWorkCount()).toBe(0);
    } finally {
      releaseRead.resolve();
      admission.release();
      await producer.drain();
      await shutdown;
    }
  });

  it("logs real asynchronous transcript failures and recovers the broadcast queue", async ({
    signal,
  }) => {
    transcriptBroadcastMocks.useActualHandler = true;
    const readEntered = createDeferred();
    const failedRead = createDeferred();
    const failureReported = createDeferred();
    const recoveredPublication = createDeferred();
    const persistenceFailure = new Error("session transcript read failed");
    const transcriptPosition = { source: "recovered-generation", rawSeq: 7 };
    const storedMessage = {
      role: "assistant",
      content: [{ type: "text", text: "visible answer" }],
      __openclaw: { transcriptPosition },
    };
    transcriptBroadcastMocks.readMessageById
      .mockImplementationOnce(async () => {
        readEntered.resolve();
        await failedRead.promise;
        throw persistenceFailure;
      })
      .mockResolvedValueOnce({ found: true, oversized: false, seq: 2, message: storedMessage });

    const params = createParams();
    params.sessionEventSubscribers.subscribe("conn-transcript");
    params.broadcastToConnIds = vi.fn(() => recoveredPublication.resolve());
    warn.mockImplementationOnce(() => failureReported.resolve());
    const unsubs = start(params);

    const emitMessage = (messageId: string) =>
      emitSessionTranscriptUpdate({
        sessionFile: "/tmp/openclaw-transcript-dispatch.sqlite",
        sessionKey: "agent:main:main",
        message: { role: "assistant", content: [{ type: "text", text: "stale queued answer" }] },
        messageId,
        target: {
          agentId: "main",
          sessionId: "sess-transcript",
          sessionKey: "agent:main:main",
          storePath: "/tmp/openclaw-transcript-dispatch-sessions.json",
        },
      });

    const admission = tryBeginGatewayRootWorkAdmission("test:transcript-publisher");
    if (!admission) {
      throw new Error("Transcript publisher admission was closed");
    }
    let shutdown: Promise<void> | undefined;
    try {
      await admission.run(async () => emitMessage("failed-message"));
      admission.release();
      await withinTest(readEntered.promise, signal);
      expect(transcriptBroadcastMocks.readMessageById).toHaveBeenCalledOnce();
      expect(getActiveGatewayRootWorkCount()).toBe(1);
      failedRead.resolve();
      await withinTest(failureReported.promise, signal);
      expect(warn).toHaveBeenCalledWith("Transcript update dispatch failed", {
        sessionKey: "agent:main:main",
        error: persistenceFailure,
      });
      expect(params.broadcastToConnIds).not.toHaveBeenCalled();

      emitMessage("recovered-message");
      await withinTest(recoveredPublication.promise, signal);
      expect(params.broadcastToConnIds).toHaveBeenCalledOnce();
      expect(params.broadcastToConnIds).toHaveBeenCalledWith(
        "session.message",
        expect.objectContaining({
          sessionKey: "agent:main:main",
          messageId: "recovered-message",
          messageSeq: 2,
          message: expect.objectContaining({
            content: storedMessage.content,
            __openclaw: expect.objectContaining({ transcriptPosition }),
          }),
        }),
        new Set(["conn-transcript"]),
        undefined,
      );
      expect(transcriptBroadcastMocks.readMessageById).toHaveBeenCalledTimes(2);
      expect(warn).toHaveBeenCalledOnce();
      shutdown = unsubs.agentUnsub();
      await shutdown;
      expect(getActiveGatewayRootWorkCount()).toBe(0);
    } finally {
      failedRead.resolve();
      admission.release();
      await (shutdown ?? unsubs.agentUnsub());
    }
  });
  it("publishes assistant receipts before later user transcript notifications", async () => {
    const actual = await vi.importActual<typeof import("./server-chat.js")>("./server-chat.js");
    transcriptBroadcastMocks.useActualHandler = true;
    installHandlerFactory(actual.createAgentEventHandler);
    const params = createParams();
    const sessionKey = "agent:main:main";
    const sessionId = "ordered-transcript-session";
    const runId = "ordered-transcript-run";
    const initialSent = createDeferred();
    const bothPublished = createDeferred();
    const published: unknown[] = [];
    params.broadcast = vi.fn(() => initialSent.resolve());
    params.broadcastToConnIds = vi.fn((event, payload) => {
      if (event === "session.message") {
        published.push(payload);
        if (published.length === 2) {
          bothPublished.resolve();
        }
      }
    });
    params.sessionEventSubscribers.subscribe("reader");
    const lifecycleGeneration = getAgentEventLifecycleGeneration();
    const registration = registerSubscriptionChatRun(params, {
      runId,
      sessionId,
      sessionKey,
      lifecycleGeneration,
    });
    claimAgentRunContext(runId, {
      lifecycleGeneration,
      sessionId,
      sessionKey,
      projectSessionLifecycle: false,
    });
    const messages = {
      assistant: {
        role: "assistant",
        content: [{ type: "text", text: "Reply" }],
        __openclaw: { runId },
      },
      user: { role: "user", content: [{ type: "text", text: "Next question" }] },
    };
    transcriptBroadcastMocks.readMessageById.mockImplementation(async (_scope, messageId) => ({
      found: true,
      oversized: false,
      seq: messageId === "assistant" ? 1 : 2,
      message: messageId === "assistant" ? messages.assistant : messages.user,
    }));
    start(params);
    try {
      emitAgentEvent({ runId, stream: "assistant", data: { itemId: "reply", text: "Reply" } });
      await initialSent.promise;
      for (const [messageId, message] of Object.entries(messages)) {
        emitSessionTranscriptUpdate({
          sessionKey,
          messageId,
          message,
          assistantItemIds: messageId === "assistant" ? ["reply"] : undefined,
          target: {
            agentId: "main",
            sessionId,
            sessionKey,
            storePath: "/tmp/ordered-transcript.sqlite",
          },
        });
      }
      await bothPublished.promise;
      await stop();
      expect(published).toEqual([
        expect.objectContaining({ messageId: "assistant" }),
        expect.objectContaining({ messageId: "user" }),
      ]);
    } finally {
      registration.cleanup();
    }
  });

  it.each(["published", "unavailable", "failed"] as const)(
    "preserves the reply until transcript publication settles (%s)",
    async (outcome) => {
      const actual = await vi.importActual<typeof import("./server-chat.js")>("./server-chat.js");
      transcriptBroadcastMocks.useActualHandler = true;
      const readEntered = createDeferred();
      const releaseRead = createDeferred();
      const initialSent = createDeferred();
      const continuationHandled = createDeferred();
      const terminalEntered = createDeferred();
      const terminalSent = createDeferred();
      const readFailure = new Error("held transcript read failed");
      const params = createParams();
      const runId = "publication-handoff";
      const sessionId = "publication-session";
      const sessionKey = "agent:main:main";
      const lifecycleGeneration = getAgentEventLifecycleGeneration();
      const registration = registerSubscriptionChatRun(params, {
        runId,
        sessionId,
        sessionKey,
        lifecycleGeneration,
      });
      claimAgentRunContext(runId, {
        lifecycleGeneration,
        sessionId,
        sessionKey,
        projectSessionLifecycle: false,
      });
      const frames: Array<{ event: string; payload: unknown }> = [];
      params.broadcast = vi.fn((event, payload) => {
        frames.push({ event, payload });
        if (event === "chat") {
          initialSent.resolve();
          if (frames.filter((frame) => frame.event === "chat").length > 1) {
            terminalSent.resolve();
          }
        }
      });
      params.broadcastToConnIds = vi.fn((event, payload) => frames.push({ event, payload }));
      params.sessionEventSubscribers.subscribe("reader");
      const message = {
        role: "assistant",
        idempotencyKey: "saved-reply",
        content: [{ type: "text", text: "Saved reply." }],
        __openclaw: { runId, idempotencyKey: "saved-reply" },
      };
      const source: AgentAssistantSourceReceipt = { itemId: "saved-reply" };
      bindAgentAssistantSource(message, source);
      transcriptBroadcastMocks.readMessageById.mockImplementation(async () => {
        readEntered.resolve();
        await releaseRead.promise;
        if (outcome === "failed") {
          throw readFailure;
        }
        return {
          found: outcome === "published",
          oversized: false,
          seq: 2,
          message: outcome === "published" ? message : undefined,
        };
      });
      installHandlerFactory((options: AgentEventHandlerOptions) => {
        const handler = actual.createAgentEventHandler(options);
        return Object.assign(async (event: AgentEventPayload) => {
          if (event.data.text === "Next reply.") {
            expect(params.chatRunState.resolveBuffer(runId).text).toBe("");
          }
          if (event.stream === "lifecycle" && event.data.phase === "end") {
            terminalEntered.resolve();
          }
          await handler(event);
          if (event.data.text === "Next reply.") {
            continuationHandled.resolve();
          }
        }, handler);
      });
      start(params);
      try {
        emitAgentEventWithAssistantSourceIfCurrent(
          {
            runId,
            stream: "assistant",
            data: { itemId: "saved-reply", text: "Saved reply." },
          },
          source,
        );
        await initialSent.promise;
        // The append receipt mutates before queued agent callbacks run. Neither
        // that callback nor the transcript notification may retract its wire text.
        source.committedMessageSeq = 2;
        emitAgentEventWithAssistantSourceIfCurrent(
          {
            runId,
            stream: "assistant",
            data: { text: "Saved reply.", replace: true },
          },
          source,
        );
        emitSessionTranscriptUpdate({
          sessionKey,
          messageId: "saved-reply",
          messageSeq: 2,
          message,
          target: {
            agentId: "main",
            sessionId,
            sessionKey,
            storePath: "/tmp/publication-sessions.json",
          },
        });
        // These arrive in the same turn as the receipt; reserving publication
        // after another promise turn would let the terminal clear the run first.
        emitAgentEvent({
          runId,
          stream: "assistant",
          data: { itemId: "next-reply", text: "Next reply.", replace: true },
        });
        emitAgentEvent({ runId, stream: "lifecycle", data: { phase: "end", endedAt: Date.now() } });
        await readEntered.promise;
        await continuationHandled.promise;
        await terminalEntered.promise;
        expect(frames).not.toContainEqual({
          event: "chat",
          payload: expect.objectContaining({ state: "final" }),
        });
        for (const frame of frames.filter((entry) => entry.event === "chat").slice(0, -1)) {
          expect(frame).toMatchObject({
            payload: {
              state: "delta",
              message: { content: [{ type: "text", text: "Saved reply." }] },
            },
          });
        }
        expect(params.chatRunState.resolveBuffer(runId).text).toBe("Next reply.");
        expect(frames.findLast((frame) => frame.event === "chat")).toMatchObject({
          payload: {
            message: { content: [{ type: "text", text: "Saved reply.\n\nNext reply." }] },
          },
        });
        releaseRead.resolve();
        await terminalSent.promise;
        await stop();
        const published = frames.filter(
          (frame) => frame.event === "chat" || frame.event === "session.message",
        );
        if (outcome === "published") {
          expect(published.slice(-3).map((frame) => frame.event)).toEqual([
            "session.message",
            "chat",
            "chat",
          ]);
          expect(published.at(-1)).toMatchObject({
            payload: {
              message: {
                openclawDisplayContent: [{ type: "text", text: "Next reply." }],
              },
            },
          });
        } else {
          expect(published.map((frame) => frame.event)).not.toContain("session.message");
          expect(published.at(-1)).toMatchObject({
            payload: {
              message: {
                content: [{ type: "text", text: "Saved reply.\n\nNext reply." }],
              },
            },
          });
          expect(published.at(-1)).not.toMatchObject({
            payload: {
              message: {
                openclawDisplayContent: expect.anything(),
              },
            },
          });
        }
        expect(published.at(-1)).toMatchObject({ event: "chat", payload: { state: "final" } });
        if (outcome === "failed") {
          expect(warn).toHaveBeenCalledWith("Transcript update dispatch failed", {
            sessionKey,
            error: readFailure,
          });
        } else {
          expect(warn).not.toHaveBeenCalled();
        }
      } finally {
        releaseRead.resolve();
        params.chatRunState.clear();
        registration.cleanup();
      }
    },
  );
}
