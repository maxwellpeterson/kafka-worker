import { jest } from "@jest/globals";
import {
  IncrementalResponse,
  PartitionResponse,
} from "src/client/incremental-response";
import { ErrorCode } from "src/protocol/common";
import { PartitionInfo } from "src/state/partition";

describe("IncrementalResponse", () => {
  const done = jest.fn<(response: TestResponse) => void>();

  interface TestResponse {
    topics: {
      name: string;
      partitions: {
        index: number;
        errorCode: ErrorCode;
      }[];
    }[];
  }

  type TestCase = [
    string,
    TestResponse,
    [PartitionInfo, PartitionResponse<TestResponse>][],
    TestResponse
  ];
  const cases: TestCase[] = [
    [
      "fills in response with one subresponse",
      {
        topics: [
          {
            name: "topic-one",
            partitions: [{ index: 0, errorCode: ErrorCode.None }],
          },
        ],
      },
      [
        [
          new PartitionInfo("topic-one", 0),
          { errorCode: ErrorCode.CorruptMessage },
        ],
      ],
      {
        topics: [
          {
            name: "topic-one",
            partitions: [{ index: 0, errorCode: ErrorCode.CorruptMessage }],
          },
        ],
      },
    ],
    [
      "fills in response with multiple topics",
      {
        topics: [
          {
            name: "topic-one",
            partitions: [{ index: 0, errorCode: ErrorCode.None }],
          },
          {
            name: "topic-two",
            partitions: [{ index: 5, errorCode: ErrorCode.None }],
          },
        ],
      },
      [
        [new PartitionInfo("topic-one", 0), { errorCode: ErrorCode.None }],
        [
          new PartitionInfo("topic-two", 5),
          { errorCode: ErrorCode.CorruptMessage },
        ],
      ],
      {
        topics: [
          {
            name: "topic-one",
            partitions: [{ index: 0, errorCode: ErrorCode.None }],
          },
          {
            name: "topic-two",
            partitions: [{ index: 5, errorCode: ErrorCode.CorruptMessage }],
          },
        ],
      },
    ],
    [
      "fills in response with multiple partitions",
      {
        topics: [
          {
            name: "topic-one",
            partitions: [
              { index: 0, errorCode: ErrorCode.None },
              { index: 12, errorCode: ErrorCode.None },
            ],
          },
        ],
      },
      [
        [
          new PartitionInfo("topic-one", 0),
          { errorCode: ErrorCode.CorruptMessage },
        ],
        [new PartitionInfo("topic-one", 12), { errorCode: ErrorCode.None }],
      ],
      {
        topics: [
          {
            name: "topic-one",
            partitions: [
              { index: 0, errorCode: ErrorCode.CorruptMessage },
              { index: 12, errorCode: ErrorCode.None },
            ],
          },
        ],
      },
    ],
  ];

  test.each<TestCase>(cases)("%s", (_name, stub, subresponses, expected) => {
    const incremental = new IncrementalResponse(stub, done);

    subresponses.forEach((subresponse) => {
      expect(done).not.toHaveBeenCalled();
      incremental.addPartition(...subresponse);
    });
    expect(done).toHaveBeenCalledTimes(1);
    expect(done).toHaveBeenCalledWith(expected);

    // Make sure that done is not called again and response is not mutated, even
    // after receiving more subresponses

    const response = done.mock.calls[0][0];
    const snapshot = structuredClone(response) as TestResponse;

    subresponses.forEach((subresponse) => {
      incremental.addPartition(...subresponse);
      expect(done).toHaveBeenCalledTimes(1);
      expect(response).toEqual(snapshot);
    });
  });

  test("calls done immediately when stub response has no topics", () => {
    const stub = { topics: [] };

    new IncrementalResponse(stub, done);
    expect(done).toHaveBeenCalledTimes(1);
    expect(done).toHaveBeenCalledWith(stub);
  });

  test("calls done immediately when stub response has no partitions", () => {
    const stub = {
      topics: [{ name: "topic-one", partitions: [] }],
    };

    new IncrementalResponse(stub, done);
    expect(done).toHaveBeenCalledTimes(1);
    expect(done).toHaveBeenCalledWith(stub);
  });
});
