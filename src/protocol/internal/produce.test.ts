import { Acks, ErrorCode } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import {
  InternalProduceRequest,
  InternalProduceResponse,
  decodeInternalProduceRequest,
  decodeInternalProduceResponse,
  encodeInternalProduceRequest,
  encodeInternalProduceResponse,
} from "src/protocol/internal/produce";
import { fillMessageSet } from "src/protocol/test-utils";

describe("InternalProduceRequest", () => {
  type TestCase = [string, InternalProduceRequest];
  const cases: TestCase[] = [
    [
      "baseline request",
      {
        acks: Acks.Leader,
        messageSet: fillMessageSet(64),
      },
    ],
    [
      "no acks",
      {
        acks: Acks.None,
        messageSet: fillMessageSet(100),
      },
    ],
    [
      "full isr acks",
      {
        acks: Acks.FullISR,
        messageSet: fillMessageSet(37),
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

  test.each(cases)("%s", (_name, request) => {
    const encoder = new Encoder();
    const buffer = encodeInternalProduceRequest(encoder, request);

    const decoder = new Decoder(buffer);
    const output = decodeInternalProduceRequest(decoder);

    expect(request).toEqual(output);
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

  test.each(cases)("%s", (_name, response) => {
    const encoder = new Encoder();
    const buffer = encodeInternalProduceResponse(encoder, response);

    const decoder = new Decoder(buffer);
    const output = decodeInternalProduceResponse(decoder);

    expect(response).toEqual(output);
  });
});
