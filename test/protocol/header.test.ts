import { ApiKey, validApiKey } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import {
  RequestHeader,
  decodeRequestHeader,
  encodeRequestHeader,
} from "src/protocol/header";
import { testEncodeDecodeSnapshot } from "test/protocol/common";

describe("RequestHeader", () => {
  type TestCase = [string, RequestHeader<ApiKey>];
  const cases: TestCase[] = [
    [
      "baseline header",
      {
        apiKey: ApiKey.Metadata,
        apiVersion: 0,
        correlationId: 100,
        clientId: "max",
      },
    ],
    [
      "nonzero api version",
      {
        apiKey: ApiKey.Produce,
        apiVersion: 5,
        correlationId: 25,
        clientId: "max",
      },
    ],
    [
      "empty client id",
      {
        apiKey: ApiKey.Metadata,
        apiVersion: 0,
        correlationId: 1024,
        clientId: "",
      },
    ],
    [
      "null client id",
      {
        apiKey: ApiKey.Metadata,
        apiVersion: 0,
        correlationId: 1024,
        clientId: null,
      },
    ],
  ];

  test.each(cases)("%s", (_name, value) => {
    testEncodeDecodeSnapshot(
      value,
      (encoder: Encoder, header: RequestHeader<ApiKey>) =>
        encodeRequestHeader(encoder, header).buffer(),
      (decoder: Decoder) => decodeRequestHeader(decoder, validApiKey)
    );
  });
});
