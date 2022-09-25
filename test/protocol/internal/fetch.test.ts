import { ErrorCode } from "src/protocol/common";
import {
  InternalFetchRequest,
  InternalFetchResponse,
  decodeInternalFetchRequest,
  decodeInternalFetchResponse,
  encodeInternalFetchRequest,
  encodeInternalFetchResponse,
} from "src/protocol/internal/fetch";
import { fillMessageSet } from "test/common";
import { testEncodeDecode } from "test/protocol/common";

describe("InternalFetchRequest", () => {
  type TestCase = [string, InternalFetchRequest];
  const cases: TestCase[] = [
    [
      "min bytes set",
      {
        maxWaitMs: 10_000,
        fetchOffset: BigInt(17),
        minBytes: 512,
        maxBytes: 8192,
      },
    ],
    [
      "min bytes not set",
      {
        maxWaitMs: 5000,
        fetchOffset: BigInt(438),
        minBytes: 0,
        maxBytes: 10_000,
      },
    ],
  ];

  test.each(cases)("%s", (_name, value) => {
    testEncodeDecode(
      value,
      encodeInternalFetchRequest,
      decodeInternalFetchRequest
    );
  });
});

describe("InternalFetchResponse", () => {
  type TestCase = [string, InternalFetchResponse];
  const cases: TestCase[] = [
    [
      "baseline response",
      {
        errorCode: ErrorCode.None,
        highWatermark: BigInt(8932),
        messageSet: fillMessageSet(6),
      },
    ],
  ];

  test.each(cases)("%s", (_name, value) => {
    testEncodeDecode(
      value,
      encodeInternalFetchResponse,
      decodeInternalFetchResponse
    );
  });
});
