import { Acks, ErrorCode } from "src/protocol/common";
import {
  KafkaProduceRequest,
  KafkaProduceResponse,
  decodeKafkaProduceRequest,
  decodeKafkaProduceResponse,
  encodeKafkaProduceRequest,
  encodeKafkaProduceResponse,
} from "src/protocol/kafka/produce";
import { fillMessageSet } from "test/common";
import { testEncodeDecodeSnapshot } from "test/protocol/common";

describe("KafkaProduceRequest", () => {
  type TestCase = [string, KafkaProduceRequest];
  const cases: TestCase[] = [
    [
      "one topic and parition",
      {
        acks: Acks.Leader,
        timeoutMs: 100,
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 1,
                messageSet: fillMessageSet(2),
              },
            ],
          },
        ],
      },
    ],
    [
      "multiple topics and partitions",
      {
        acks: Acks.None,
        timeoutMs: 50,
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 2,
                messageSet: fillMessageSet(3),
              },
              {
                index: 4,
                messageSet: fillMessageSet(1),
              },
            ],
          },
          {
            name: "topic-two",
            partitions: [
              {
                index: 5,
                messageSet: fillMessageSet(2),
              },
            ],
          },
        ],
      },
    ],
    [
      "full isr acks",
      {
        acks: Acks.FullISR,
        timeoutMs: 250,
        topics: [
          {
            name: "topic-two",
            partitions: [
              {
                index: 7,
                messageSet: fillMessageSet(2),
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
      encodeKafkaProduceRequest,
      decodeKafkaProduceRequest
    );
  });
});

describe("KafkaProduceResponse", () => {
  type TestCase = [string, KafkaProduceResponse];
  const cases: TestCase[] = [
    [
      "one topic and partition",
      {
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 1,
                errorCode: ErrorCode.None,
                baseOffset: BigInt(16),
              },
            ],
          },
        ],
      },
    ],
    [
      "multiple topics and partitions",
      {
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 3,
                errorCode: ErrorCode.None,
                baseOffset: BigInt(127),
              },
              {
                index: 5,
                errorCode: ErrorCode.None,
                baseOffset: BigInt(2),
              },
            ],
          },
          {
            name: "topic-two",
            partitions: [
              {
                index: 8,
                errorCode: ErrorCode.None,
                baseOffset: BigInt(1010),
              },
            ],
          },
        ],
      },
    ],
    [
      "multiple topics and partitions with errors",
      {
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 3,
                errorCode: ErrorCode.None,
                baseOffset: BigInt(127),
              },
              {
                index: 7,
                errorCode: ErrorCode.CorruptMessage,
                baseOffset: BigInt(26),
              },
            ],
          },
          {
            name: "topic-two",
            partitions: [
              {
                index: 15,
                errorCode: ErrorCode.CorruptMessage,
                baseOffset: BigInt(947),
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
      encodeKafkaProduceResponse,
      decodeKafkaProduceResponse
    );
  });
});
