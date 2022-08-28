import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import {
  PartitionApiKey,
  PartitionRequestHeader,
  decodePartitionRequestHeader,
  encodePartitionRequestHeader,
} from "src/protocol/internal/partition/common";

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
