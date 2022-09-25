import handler from "src/index";
import { Acks, ApiKey, Int32 } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { KafkaDecoder, KafkaRequestEncoder } from "src/protocol/kafka/common";
import {
  KafkaFetchRequest,
  KafkaFetchResponse,
  decodeKafkaFetchResponse,
  encodeKafkaFetchRequest,
} from "src/protocol/kafka/fetch";
import {
  KafkaListOffsetsRequest,
  KafkaListOffsetsResponse,
  decodeKafkaListOffsetsResponse,
  encodeKafkaListOffsetsRequest,
} from "src/protocol/kafka/list-offsets";
import {
  KafkaMetadataResponse,
  decodeKafkaMetadataResponse,
  encodeKafkaMetadataRequest,
} from "src/protocol/kafka/metadata";
import {
  KafkaProduceRequest,
  KafkaProduceResponse,
  decodeKafkaProduceResponse,
  encodeKafkaProduceRequest,
} from "src/protocol/kafka/produce";
import { base64, fillMessageSet } from "test/common";

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
type TestCase<T> = [string, string, RequestResponse<T>[]];

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

const makeProducePair = (
  correlationId: Int32,
  topics: KafkaProduceRequest["topics"]
): RequestResponse<KafkaProduceResponse> => {
  const encoder = new KafkaRequestEncoder({
    apiKey: ApiKey.Produce,
    apiVersion: 0,
    correlationId,
    clientId: null,
  });
  const request = encodeKafkaProduceRequest(encoder, {
    acks: Acks.Leader,
    timeoutMs: 10_000,
    topics,
  });
  return [correlationId, request, decodeKafkaProduceResponse];
};

const makeFetchPair = (
  correlationId: Int32,
  request: KafkaFetchRequest
): RequestResponse<KafkaFetchResponse> => {
  const encoder = new KafkaRequestEncoder({
    apiKey: ApiKey.Fetch,
    apiVersion: 0,
    correlationId,
    clientId: null,
  });
  const encoded = encodeKafkaFetchRequest(encoder, request);
  return [correlationId, encoded, decodeKafkaFetchResponse];
};

const makeListOffsetsPair = (
  correlationId: Int32,
  request: KafkaListOffsetsRequest
): RequestResponse<KafkaListOffsetsResponse> => {
  const encoder = new KafkaRequestEncoder({
    apiKey: ApiKey.ListOffsets,
    apiVersion: 0,
    correlationId,
    clientId: null,
  });
  const encoded = encodeKafkaListOffsetsRequest(encoder, request);
  return [correlationId, encoded, decodeKafkaListOffsetsResponse];
};

describe("Kafka API", () => {
  const cases: TestCase<
    | KafkaMetadataResponse
    | KafkaProduceResponse
    | KafkaListOffsetsResponse
    | KafkaFetchResponse
  >[] = [
    ["metadata", "fetch all topics", [makeMetadataPair(5, [])]],
    ["metadata", "fetch specific topic", [makeMetadataPair(5, ["test-topic"])]],
    [
      "metadata",
      "fetch nonexistent topic",
      [makeMetadataPair(5, ["other-topic"])],
    ],
    [
      "metadata",
      "fetch specific topic and nonexistent topic",
      [makeMetadataPair(5, ["test-topic", "other-topic"])],
    ],
    [
      "produce",
      "send one message to one partition",
      [
        makeProducePair(5, [
          {
            name: "test-topic",
            partitions: [{ index: 333, messageSet: fillMessageSet(1) }],
          },
        ]),
      ],
    ],
    [
      "produce",
      "send multiple message batches to one partition",
      [
        makeProducePair(5, [
          {
            name: "other-topic",
            partitions: [{ index: 999, messageSet: fillMessageSet(3) }],
          },
        ]),
        makeProducePair(6, [
          {
            name: "other-topic",
            partitions: [{ index: 999, messageSet: fillMessageSet(2) }],
          },
        ]),
      ],
    ],
    [
      "listOffsets",
      "initial offsets",
      [
        makeListOffsetsPair(0, {
          replicaId: -1,
          topics: [
            {
              name: "test-topic",
              partitions: [
                { index: 0, timestamp: BigInt(0), maxNumOffsets: 16 },
              ],
            },
          ],
        }),
      ],
    ],
    [
      "listOffsets",
      "full offset list after producing records",
      [
        makeProducePair(0, [
          {
            name: "test-topic",
            partitions: [{ index: 0, messageSet: fillMessageSet(10) }],
          },
        ]),
        makeListOffsetsPair(1, {
          replicaId: -1,
          topics: [
            {
              name: "test-topic",
              partitions: [
                { index: 0, timestamp: BigInt(0), maxNumOffsets: 16 },
              ],
            },
          ],
        }),
      ],
    ],
    [
      "listOffsets",
      "oldest offset after producing records",
      [
        makeProducePair(0, [
          {
            name: "test-topic",
            partitions: [{ index: 0, messageSet: fillMessageSet(10) }],
          },
        ]),
        makeListOffsetsPair(1, {
          replicaId: -1,
          topics: [
            {
              name: "test-topic",
              partitions: [
                { index: 0, timestamp: BigInt(-2), maxNumOffsets: 1 },
              ],
            },
          ],
        }),
      ],
    ],
    [
      "listOffsets",
      "maximum number of offsets is respected",
      [
        makeProducePair(0, [
          {
            name: "test-topic",
            partitions: [{ index: 0, messageSet: fillMessageSet(10) }],
          },
        ]),
        makeListOffsetsPair(1, {
          replicaId: -1,
          topics: [
            {
              name: "test-topic",
              partitions: [
                { index: 0, timestamp: BigInt(0), maxNumOffsets: 4 },
              ],
            },
          ],
        }),
      ],
    ],
    [
      "fetch",
      "fetch on empty partition",
      [
        makeFetchPair(0, {
          replicaId: -1,
          maxWaitMs: 500,
          minBytes: 256,
          topics: [
            {
              name: "test-topic",
              partitions: [
                { index: 0, fetchOffset: BigInt(0), maxBytes: 8192 },
              ],
            },
          ],
        }),
      ],
    ],
  ];

  test.each(cases)("%s: %s", async (_api, _name, pairs) => {
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

  test("produce message set then fetch message set", async () => {
    const gateway = new GatewayConn();
    const messageSet = fillMessageSet(5);

    const produceCorrelationId = 0;
    const produceEncoder = new KafkaRequestEncoder({
      apiKey: ApiKey.Produce,
      apiVersion: 0,
      correlationId: produceCorrelationId,
      clientId: null,
    });
    const produceRequest = encodeKafkaProduceRequest(produceEncoder, {
      acks: Acks.Leader,
      timeoutMs: 10_000,
      topics: [{ name: "test-topic", partitions: [{ index: 0, messageSet }] }],
    });
    const produceResponse = await gateway.request(produceRequest);
    const produceDecoder = new KafkaDecoder(produceResponse);
    expect(produceDecoder.readInt32()).toEqual(produceCorrelationId);
    expect(decodeKafkaProduceResponse(produceDecoder)).toMatchInlineSnapshot(`
      Object {
        "topics": Array [
          Object {
            "name": "test-topic",
            "partitions": Array [
              Object {
                "baseOffset": 0n,
                "errorCode": 0,
                "index": 0,
              },
            ],
          },
        ],
      }
    `);

    const fetchCorrelationId = 1;
    const fetchEncoder = new KafkaRequestEncoder({
      apiKey: ApiKey.Fetch,
      apiVersion: 0,
      correlationId: fetchCorrelationId,
      clientId: null,
    });
    const fetchRequest = encodeKafkaFetchRequest(fetchEncoder, {
      replicaId: -1,
      maxWaitMs: 1000,
      minBytes: 64,
      topics: [
        {
          name: "test-topic",
          partitions: [{ index: 0, fetchOffset: BigInt(0), maxBytes: 8192 }],
        },
      ],
    });
    const fetchResponse = await gateway.request(fetchRequest);
    const fetchDecoder = new KafkaDecoder(fetchResponse);
    expect(fetchDecoder.readInt32()).toEqual(fetchCorrelationId);
    const fetchResponseDecoded = decodeKafkaFetchResponse(fetchDecoder);
    expect(fetchResponseDecoded.topics[0].partitions[0].messageSet).toEqual(
      messageSet
    );
    // Exclude buffer data from inline snapshot (because it's huge)
    fetchResponseDecoded.topics[0].partitions[0].messageSet = new Uint8Array();
    expect(fetchResponseDecoded).toMatchInlineSnapshot(`
      Object {
        "topics": Array [
          Object {
            "name": "test-topic",
            "partitions": Array [
              Object {
                "errorCode": 0,
                "highWatermark": 5n,
                "index": 0,
                "messageSet": Uint8Array [],
              },
            ],
          },
        ],
      }
    `);
  });
});
