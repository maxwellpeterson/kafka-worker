import { Acks, ErrorCode } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import {
  PartitionApiKey,
  PartitionProduceRequest,
  PartitionProduceResponse,
  PartitionRequestHeader,
  decodePartitionProduceRequest,
  decodePartitionProduceResponse,
  decodePartitionRequestHeader,
  encodePartitionProduceRequest,
  encodePartitionProduceResponse,
  encodePartitionRequestHeader,
} from "src/protocol/internal/partition";
import { fillBuffer } from "src/protocol/test-utils";

describe("PartitionRequestHeader", () => {
  type TestCase = [string, PartitionRequestHeader];
  const cases: TestCase[] = [
    [
      "baseline header",
      {
        apiKey: PartitionApiKey.Produce,
        apiVersion: 0,
        correlationId: 100,
        clientId: "max",
      },
    ],
    [
      "nonzero api version",
      {
        apiKey: PartitionApiKey.Produce,
        apiVersion: 5,
        correlationId: 25,
        clientId: "max",
      },
    ],
    [
      "empty client id",
      {
        apiKey: PartitionApiKey.Produce,
        apiVersion: 0,
        correlationId: 1024,
        clientId: "",
      },
    ],
  ];

  test.each(cases)("%s", (_name, header) => {
    const encoder = new Encoder();
    encodePartitionRequestHeader(encoder, header);
    const buffer = encoder.buffer();

    const decoder = new Decoder(buffer);
    const output = decodePartitionRequestHeader(decoder);

    expect(header).toEqual(output);
  });
});

describe("PartitionProduceRequest", () => {
  type TestCase = [string, PartitionProduceRequest];
  const cases: TestCase[] = [
    [
      "baseline request",
      {
        acks: Acks.Leader,
        messageSetSize: 64,
        messageSet: fillBuffer(64),
      },
    ],
    [
      "no acks",
      {
        acks: Acks.None,
        messageSetSize: 100,
        messageSet: fillBuffer(100),
      },
    ],
    [
      "full isr acks",
      {
        acks: Acks.FullISR,
        messageSetSize: 37,
        messageSet: fillBuffer(37),
      },
    ],
    [
      "empty message set",
      {
        acks: Acks.Leader,
        messageSetSize: 0,
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
    ["baseline response", { errorCode: ErrorCode.None, offset: BigInt(127) }],
    ["offset zero", { errorCode: ErrorCode.None, offset: BigInt(0) }],
  ];

  test.each(cases)("%s", (_name, response) => {
    const encoder = new Encoder();
    const buffer = encodePartitionProduceResponse(encoder, response);

    const decoder = new Decoder(buffer);
    const output = decodePartitionProduceResponse(decoder);

    expect(response).toEqual(output);
  });
});
