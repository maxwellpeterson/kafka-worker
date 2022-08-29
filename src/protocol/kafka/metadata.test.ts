import { ErrorCode } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import {
  KafkaMetadataRequest,
  KafkaMetadataResponse,
  decodeKafkaMetadataRequest,
  decodeKafkaMetadataResponse,
  encodeKafkaMetadataRequest,
  encodeKafkaMetadataResponse,
} from "src/protocol/kafka/metadata";
import { base64 } from "src/protocol/test-utils";

describe("KafkaMetadataRequest", () => {
  type TestCase = [string, KafkaMetadataRequest];
  const cases: TestCase[] = [
    ["no topics", { topics: [] }],
    ["one topic", { topics: ["topic-one"] }],
    ["multiple topics", { topics: ["topic-one", "topic-two", "topic-three"] }],
  ];
  test.each(cases)("%s", (_name, request) => {
    const encoder = new Encoder();
    const buffer = encodeKafkaMetadataRequest(encoder, request);

    expect(base64(buffer)).toMatchSnapshot();

    const decoder = new Decoder(buffer);
    const decoded = decodeKafkaMetadataRequest(decoder);

    expect(request).toEqual(decoded);
  });
});

describe("KafkaMetadataResponse", () => {
  type TestCase = [string, KafkaMetadataResponse];
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
    const encoder = new Encoder();
    const buffer = encodeKafkaMetadataResponse(encoder, response);

    expect(base64(buffer)).toMatchSnapshot();

    const decoder = new Decoder(buffer);
    const decoded = decodeKafkaMetadataResponse(decoder);

    expect(response).toEqual(decoded);
  });
});
