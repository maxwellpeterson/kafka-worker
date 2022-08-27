import { ErrorCode } from "src/protocol/common";
import { clone } from "src/protocol/test-utils";
import { PartitionInfo } from "src/state/broker/partition";
import {
  IncrementalResponse,
  PartitionResponse,
} from "src/state/client/incremental-response";

interface TestResponse {
  topics: {
    name: string;
    partitions: {
      index: number;
      errorCode: ErrorCode;
    }[];
  }[];
}

describe("IncrementalResponse", () => {
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
    const done = jest.fn<undefined, [TestResponse]>();
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
    const snapshot = clone(response);

    subresponses.forEach((subresponse) => {
      incremental.addPartition(...subresponse);
      expect(done).toHaveBeenCalledTimes(1);
      expect(response).toEqual(snapshot);
    });
  });

  test("calls done immediately when stub response has no topics", () => {
    const stub = { topics: [] };
    const done = jest.fn<undefined, [TestResponse]>();

    new IncrementalResponse(stub, done);
    expect(done).toHaveBeenCalledTimes(1);
    expect(done).toHaveBeenCalledWith(stub);
  });

  test("calls done immediately when stub response has no partitions", () => {
    const stub = {
      topics: [{ name: "topic-one", partitions: [] }],
    };
    const done = jest.fn<undefined, [TestResponse]>();

    new IncrementalResponse(stub, done);
    expect(done).toHaveBeenCalledTimes(1);
    expect(done).toHaveBeenCalledWith(stub);
  });
});
