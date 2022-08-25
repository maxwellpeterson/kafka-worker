import {
  MetadataRequest,
  MetadataResponse,
  decodeMetadataRequest,
  encodeMetadataResponse,
} from "src/protocol/api/metadata";
import { ErrorCode } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { base64 } from "src/protocol/test-utils";

describe("decodeMetadataRequest", () => {
  type TestCase = [string, MetadataRequest];
  const cases: TestCase[] = [
    ["no topics", { topics: [] }],
    ["one topics", { topics: ["topic-one"] }],
    ["multiple topics", { topics: ["topic-one", "topic-two", "topic-three"] }],
  ];
  test.each(cases)("%s", (_name, request) => {
    const encoder = new Encoder();
    encoder.writeArray(request.topics, (topic) => encoder.writeString(topic));
    const buffer = encoder.buffer();

    const decoder = new Decoder(buffer);
    const decoded = decodeMetadataRequest(decoder);

    expect(request).toEqual(decoded);
  });
});

describe("encodeMetadataResponse", () => {
  type TestCase = [string, MetadataResponse];
  const cases: TestCase[] = [
    [
      "no topics",
      {
        brokers: [{ nodeId: 1, host: "example.com", port: 443 }],
        topics: [],
      },
    ],
    [
      "single-partition topic",
      {
        brokers: [{ nodeId: 1, host: "example.com", port: 443 }],
        topics: [
          {
            errorCode: ErrorCode.None,
            name: "topic-one",
            partitions: [
              {
                errorCode: ErrorCode.None,
                partitionIndex: 1,
                leaderId: 1,
                replicaNodes: [],
                isrNodes: [],
              },
            ],
          },
        ],
      },
    ],
    [
      "multi-partition topic",
      {
        brokers: [{ nodeId: 1, host: "example.com", port: 443 }],
        topics: [
          {
            errorCode: ErrorCode.None,
            name: "topic-one",
            partitions: [
              {
                errorCode: ErrorCode.None,
                partitionIndex: 1,
                leaderId: 1,
                replicaNodes: [],
                isrNodes: [],
              },
              {
                errorCode: ErrorCode.None,
                partitionIndex: 2,
                leaderId: 1,
                replicaNodes: [],
                isrNodes: [],
              },
            ],
          },
        ],
      },
    ],
    [
      "multiple topics",
      {
        brokers: [{ nodeId: 1, host: "example.com", port: 443 }],
        topics: [
          {
            errorCode: ErrorCode.None,
            name: "topic-one",
            partitions: [
              {
                errorCode: ErrorCode.None,
                partitionIndex: 1,
                leaderId: 1,
                replicaNodes: [],
                isrNodes: [],
              },
            ],
          },
          {
            errorCode: ErrorCode.None,
            name: "topic-two",
            partitions: [
              {
                errorCode: ErrorCode.None,
                partitionIndex: 1,
                leaderId: 1,
                replicaNodes: [],
                isrNodes: [],
              },
              {
                errorCode: ErrorCode.None,
                partitionIndex: 2,
                leaderId: 1,
                replicaNodes: [],
                isrNodes: [],
              },
            ],
          },
        ],
      },
    ],
    [
      "multiple topics with errors",
      {
        brokers: [{ nodeId: 1, host: "example.com", port: 443 }],
        topics: [
          {
            errorCode: ErrorCode.UnknownTopicOrPartition,
            name: "topic-one",
            partitions: [],
          },
          {
            errorCode: ErrorCode.UnknownTopicOrPartition,
            name: "topic-two",
            partitions: [],
          },
          {
            errorCode: ErrorCode.None,
            name: "topic-three",
            partitions: [
              {
                errorCode: ErrorCode.None,
                partitionIndex: 1,
                leaderId: 1,
                replicaNodes: [],
                isrNodes: [],
              },
              {
                errorCode: ErrorCode.None,
                partitionIndex: 2,
                leaderId: 1,
                replicaNodes: [],
                isrNodes: [],
              },
            ],
          },
        ],
      },
    ],
  ];

  test.each(cases)("%s", (_name, response) => {
    // TODO: Write response decoder and assert output matches original response,
    // and use snapshot as a hedge against encoder/decoder bugs
    const encoder = new Encoder();
    const buffer = encodeMetadataResponse(encoder, response);
    expect(base64(buffer)).toMatchSnapshot();
  });
});
