import { ErrorCode } from "src/protocol/common";
import {
  KafkaListOffsetsRequest,
  KafkaListOffsetsResponse,
  decodeKafkaListOffsetsRequest,
  decodeKafkaListOffsetsResponse,
  encodeKafkaListOffsetsRequest,
  encodeKafkaListOffsetsResponse,
} from "src/protocol/kafka/list-offsets";
import { testEncodeDecodeSnapshot } from "test/protocol/common";

describe("KafkaListOffsetsRequest", () => {
  type TestCase = [string, KafkaListOffsetsRequest];
  const cases: TestCase[] = [
    [
      "latest offset",
      {
        replicaId: 333,
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 0,
                timestamp: BigInt(-1),
                maxNumOffsets: 1,
              },
            ],
          },
        ],
      },
    ],
    [
      "earliest offset",
      {
        replicaId: 333,
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 0,
                timestamp: BigInt(-2),
                maxNumOffsets: 1,
              },
            ],
          },
        ],
      },
    ],
    [
      "multiple offsets",
      {
        replicaId: 333,
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 0,
                timestamp: BigInt(0),
                maxNumOffsets: 64,
              },
            ],
          },
        ],
      },
    ],
  ];

  test.each(cases)("%s", (_name, value) => {
    testEncodeDecodeSnapshot(
      value,
      encodeKafkaListOffsetsRequest,
      decodeKafkaListOffsetsRequest
    );
  });
});

describe("KafkaListOffsetsResponse", () => {
  type TestCase = [string, KafkaListOffsetsResponse];
  const cases: TestCase[] = [
    [
      "one offset",
      {
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 0,
                errorCode: ErrorCode.None,
                oldStyleOffsets: [BigInt(16)],
              },
            ],
          },
        ],
      },
    ],
    [
      "multiple offsets",
      {
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 0,
                errorCode: ErrorCode.None,
                oldStyleOffsets: [BigInt(3), BigInt(2), BigInt(1), BigInt(0)],
              },
            ],
          },
        ],
      },
    ],
    [
      "offset out of range",
      {
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 0,
                errorCode: ErrorCode.OffsetOutOfRange,
                oldStyleOffsets: [],
              },
            ],
          },
        ],
      },
    ],
  ];

  test.each(cases)("%s", (_name, value) => {
    testEncodeDecodeSnapshot(
      value,
      encodeKafkaListOffsetsResponse,
      decodeKafkaListOffsetsResponse
    );
  });
});
