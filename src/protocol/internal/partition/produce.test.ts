import { Acks, ErrorCode } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import {
  PartitionProduceRequest,
  PartitionProduceResponse,
  decodePartitionProduceRequest,
  decodePartitionProduceResponse,
  encodePartitionProduceRequest,
  encodePartitionProduceResponse,
} from "src/protocol/internal/partition/produce";
import { fillBuffer } from "src/protocol/test-utils";

describe("PartitionProduceRequest", () => {
  type TestCase = [string, PartitionProduceRequest];
  const cases: TestCase[] = [
    [
      "baseline request",
      {
        acks: Acks.Leader,
        messageSet: fillBuffer(64),
      },
    ],
    [
      "no acks",
      {
        acks: Acks.None,
        messageSet: fillBuffer(100),
      },
    ],
    [
      "full isr acks",
      {
        acks: Acks.FullISR,
        messageSet: fillBuffer(37),
      },
    ],
    [
      "empty message set",
      {
        acks: Acks.Leader,
        messageSet: new ArrayBuffer(0),
      },
    ],
  ];

  test.each(cases)("%s", (_name, request) => {
    const encoder = new Encoder();
    const buffer = encodePartitionProduceRequest(encoder, request);

    const decoder = new Decoder(buffer);
    const output = decodePartitionProduceRequest(decoder);

    expect(request).toEqual(output);
  });
});

describe("PartitionProduceResponse", () => {
  type TestCase = [string, PartitionProduceResponse];
  const cases: TestCase[] = [
    [
      "baseline response",
      { errorCode: ErrorCode.None, baseOffset: BigInt(127) },
    ],
    ["offset zero", { errorCode: ErrorCode.None, baseOffset: BigInt(0) }],
  ];

  test.each(cases)("%s", (_name, response) => {
    const encoder = new Encoder();
    const buffer = encodePartitionProduceResponse(encoder, response);

    const decoder = new Decoder(buffer);
    const output = decodePartitionProduceResponse(decoder);

    expect(response).toEqual(output);
  });
});
