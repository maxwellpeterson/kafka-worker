import { ErrorCode } from "src/protocol/common";
import {
  KafkaFetchRequest,
  KafkaFetchResponse,
  decodeKafkaFetchRequest,
  decodeKafkaFetchResponse,
  encodeKafkaFetchRequest,
  encodeKafkaFetchResponse,
} from "src/protocol/kafka/fetch";
import { fillMessageSet } from "test/common";
import { testEncodeDecodeSnapshot } from "test/protocol/common";

describe("KafkaFetchRequest", () => {
  type TestCase = [string, KafkaFetchRequest];
  const cases: TestCase[] = [
    [
      "one topic and partition",
      {
        replicaId: -1,
        maxWaitMs: 10_000,
        minBytes: 256,
        topics: [
          {
            name: "topic-one",
            partitions: [{ index: 0, fetchOffset: BigInt(16), maxBytes: 4096 }],
          },
        ],
      },
    ],
  ];

  test.each(cases)("%s", (_name, value) => {
    testEncodeDecodeSnapshot(
      value,
      encodeKafkaFetchRequest,
      decodeKafkaFetchRequest
    );
  });
});

describe("KafkaFetchResponse", () => {
  type TestCase = [string, KafkaFetchResponse];
  const cases: TestCase[] = [
    [
      "one topic and partition",
      {
        topics: [
          {
            name: "topic-one",
            partitions: [
              {
                index: 0,
                errorCode: ErrorCode.None,
                highWatermark: BigInt(32),
                messageSet: fillMessageSet(3),
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
      encodeKafkaFetchResponse,
      decodeKafkaFetchResponse
    );
  });
});
