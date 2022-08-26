import { Env } from "src/common";
import { ErrorCode, Int32 } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { PartitionResponseEncoder } from "src/protocol/encoder";
import {
  decodePartitionRequestHeader,
  encodePartitionProduceResponse,
} from "src/protocol/internal/partition";

export const partitionStubUrl = "https://partition.state";

export class Partition {
  private readonly state: DurableObjectState;
  private readonly env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  fetch(request: Request): Response {
    const upgradeHeader = request.headers.get("Upgrade");
    if (upgradeHeader !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();
    server.addEventListener("message", (event) => {
      if (typeof event.data === "string") {
        console.log("Received string data, but we want binary data!");
        return;
      }

      const decoder = new Decoder(event.data);
      const header = decodePartitionRequestHeader(decoder);

      const encoder = new PartitionResponseEncoder(header.correlationId);
      const response = encodePartitionProduceResponse(encoder, {
        errorCode: ErrorCode.None,
        baseOffset: BigInt(0),
      });
      server.send(response);
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }
}

export type PartitionId = string;

export const generatePartitonId = (topic: string, index: Int32): PartitionId =>
  `${topic}-${index}`;

export const parsePartitionId = (
  id: PartitionId
): { topic: string; index: Int32 } => {
  const sep = id.lastIndexOf("-");
  return { topic: id.slice(0, sep), index: parseInt(id.slice(sep + 1)) };
};
