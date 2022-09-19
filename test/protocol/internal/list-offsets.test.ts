import { ErrorCode } from "src/protocol/common";
import {
  InternalListOffsetsRequest,
  InternalListOffsetsResponse,
  decodeInternalListOffsetsRequest,
  decodeInternalListOffsetsResponse,
  encodeInternalListOffsetsRequest,
  encodeInternalListOffsetsResponse,
} from "src/protocol/internal/list-offsets";
import { testEncodeDecode } from "test/protocol/common";

describe("InternalListOffsetsRequest", () => {
  type TestCase = [string, InternalListOffsetsRequest];
  const cases: TestCase[] = [
    ["latest offset", { timestamp: BigInt(-1), maxNumOffsets: 1 }],
    ["multiple offsets", { timestamp: BigInt(0), maxNumOffsets: 64 }],
  ];

  test.each(cases)("%s", (_name, value) => {
    testEncodeDecode(
      value,
      encodeInternalListOffsetsRequest,
      decodeInternalListOffsetsRequest
    );
  });
});

describe("InternalListOffsetsResponse", () => {
  type TestCase = [string, InternalListOffsetsResponse];
  const cases: TestCase[] = [
    [
      "one offset",
      { errorCode: ErrorCode.None, oldStyleOffsets: [BigInt(16)] },
    ],
    [
      "multiple offsets",
      {
        errorCode: ErrorCode.None,
        oldStyleOffsets: [BigInt(3), BigInt(2), BigInt(1), BigInt(0)],
      },
    ],
  ];

  test.each(cases)("%s", (_name, value) => {
    testEncodeDecode(
      value,
      encodeInternalListOffsetsResponse,
      decodeInternalListOffsetsResponse
    );
  });
});
