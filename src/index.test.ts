import handler from "src/index";
import { ApiKey, Int32 } from "src/protocol/common";
import { KafkaDecoder, KafkaRequestEncoder } from "src/protocol/kafka/common";
import {
  KafkaMetadataResponse,
  decodeKafkaMetadataResponse,
  encodeKafkaMetadataRequest,
} from "src/protocol/kafka/metadata";
import { KafkaProduceResponse } from "src/protocol/kafka/produce";
import { base64 } from "src/protocol/test-utils";
import { Decoder } from "./protocol/decoder";

class GatewayConn {
  private readonly socket: WebSocket;
  private readonly unhandled: (ArrayBuffer | string)[];
  private handleMessage?: (message: ArrayBuffer) => void;

  constructor() {
    const env = getMiniflareBindings();

    const response = handler.fetch(
      new Request("https://fetch.handler", {
        headers: { Upgrade: "websocket" },
      }),
      env
    );
    if (!response.ok) {
      throw new Error(`Initial request failed with status ${response.status}`);
    }

    const socket = response.webSocket;
    if (socket === null) {
      throw new Error("Initial response does not include WebSocket");
    }

    socket.accept();
    this.socket = socket;
    this.unhandled = [];

    this.socket.addEventListener("message", (event) => {
      if (typeof event.data === "string" || this.handleMessage === undefined) {
        this.unhandled.push(event.data);
        return;
      }
      this.handleMessage(event.data);
    });
  }

  request(buffer: ArrayBuffer): Promise<ArrayBuffer> {
    if (this.unhandled.length > 0) {
      throw new Error(
        `Can't send next message when there are ${this.unhandled.length} unhandled messages`
      );
    }

    if (this.handleMessage !== undefined) {
      throw new Error(
        "Can't send next message when there is a request in flight"
      );
    }

    this.socket.send(buffer);

    return new Promise((resolve) => {
      this.handleMessage = (message: ArrayBuffer) => {
        this.handleMessage = undefined;
        resolve(message);
      };
    });
  }

  close() {
    if (this.unhandled.length > 0) {
      throw new Error(
        `Can't close connection when there are ${this.unhandled.length} unhandled messages`
      );
    }

    this.socket.close();
  }
}

type DecodeFunc<T> = (decoder: Decoder) => T;
type RequestResponse<T> = [Int32, ArrayBuffer, DecodeFunc<T>];
type TestCase<T> = [string, RequestResponse<T>[]];

const makeMetadataPair = (
  correlationId: Int32,
  topics: string[]
): RequestResponse<KafkaMetadataResponse> => {
  const encoder = new KafkaRequestEncoder({
    apiKey: ApiKey.Metadata,
    apiVersion: 0,
    correlationId,
    clientId: null,
  });
  const request = encodeKafkaMetadataRequest(encoder, {
    topics,
  });
  return [correlationId, request, decodeKafkaMetadataResponse];
};

describe("Metadata API", () => {
  const cases: TestCase<KafkaMetadataResponse | KafkaProduceResponse>[] = [
    ["fetch all topics", [makeMetadataPair(5, [])]],
    ["fetch specific topic", [makeMetadataPair(5, ["test-topic"])]],
    ["fetch nonexistent topic", [makeMetadataPair(5, ["other-topic"])]],
    [
      "fetch specific topic and nonexistent topic",
      [makeMetadataPair(5, ["test-topic", "other-topic"])],
    ],
  ];

  test.each(cases)("%s", async (_name, pairs) => {
    const gateway = new GatewayConn();

    for (const [correlationId, request, decodeResponse] of pairs) {
      const response = await gateway.request(request);
      expect(base64(response)).toMatchSnapshot();

      const decoder = new KafkaDecoder(response);
      expect(decoder.readInt32()).toEqual(correlationId);

      const decoded = decodeResponse(decoder);
      expect(decoded).toMatchSnapshot();
    }

    gateway.close();
  });
});
