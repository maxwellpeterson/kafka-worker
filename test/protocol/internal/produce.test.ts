import { Acks, ErrorCode } from "src/protocol/common";
import {
  InternalProduceRequest,
  InternalProduceResponse,
  decodeInternalProduceRequest,
  decodeInternalProduceResponse,
  encodeInternalProduceRequest,
  encodeInternalProduceResponse,
} from "src/protocol/internal/produce";
import { fillMessageSet } from "test/common";
import { testEncodeDecode } from "test/protocol/common";

describe("InternalProduceRequest", () => {
  type TestCase = [string, InternalProduceRequest];
  const cases: TestCase[] = [
    [
      "baseline request",
      {
        acks: Acks.Leader,
        messageSet: fillMessageSet(1),
      },
    ],
    [
      "no acks",
      {
        acks: Acks.None,
        messageSet: fillMessageSet(2),
      },
    ],
    [
      "full isr acks",
      {
        acks: Acks.FullISR,
        messageSet: fillMessageSet(5),
      },
    ],
    [
      "empty message set",
      {
        acks: Acks.Leader,
        messageSet: fillMessageSet(0),
      },
    ],
  ];

  test.each(cases)("%s", (_name, value) => {
    testEncodeDecode(
      value,
      encodeInternalProduceRequest,
      decodeInternalProduceRequest
    );
  });
});

describe("InternalProduceResponse", () => {
  type TestCase = [string, InternalProduceResponse];
  const cases: TestCase[] = [
    [
      "baseline response",
      { errorCode: ErrorCode.None, baseOffset: BigInt(127) },
    ],
    ["offset zero", { errorCode: ErrorCode.None, baseOffset: BigInt(0) }],
  ];

  test.each(cases)("%s", (_name, value) => {
    testEncodeDecode(
      value,
      encodeInternalProduceResponse,
      decodeInternalProduceResponse
    );
  });
});
