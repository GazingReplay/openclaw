import { EventEmitter } from "node:events";
import { vi } from "vitest";
import type { GatewayClientRegistry } from "./server/client-registry.js";
import type { GatewayWsClient } from "./server/ws-types.js";

export function connectChatWireClient(
  clients: GatewayClientRegistry,
  connId: string,
  completeWrite: (callback?: () => void) => void = (callback) => callback?.(),
  caps: string[] = [],
) {
  const frames: Array<{
    event: string;
    seq: number;
    payload: {
      stream?: string;
      message?: unknown;
      data?: { text?: string; delta?: string };
      state?: string;
      deltaText?: string;
    };
  }> = [];
  const socket = Object.assign(new EventEmitter(), {
    readyState: 1,
    bufferedAmount: 0,
    send: (
      wire: string | Buffer,
      options?: { binary: false } | (() => void),
      callback?: () => void,
    ) => {
      frames.push(JSON.parse(wire.toString()));
      completeWrite(typeof options === "function" ? options : callback);
    },
    close: vi.fn(),
    terminate: vi.fn(),
  });
  const client = {
    connId,
    socket,
    usesSharedGatewayAuth: false,
    connect: {
      minProtocol: 4,
      maxProtocol: 4,
      client: { id: "test", version: "test", platform: "test", mode: "test" },
      role: "operator",
      scopes: ["operator.read"],
      caps,
    },
  } satisfies GatewayWsClient;
  clients.add(client);
  return { frames, client, socket };
}
