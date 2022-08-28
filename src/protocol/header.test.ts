import { ApiKey, validApiKey } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import {
  RequestHeader,
  decodeRequestHeader,
  encodeRequestHeader,
} from "src/protocol/header";
import { base64 } from "./test-utils";

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

  test.each(cases)("%s", (_name, header) => {
    const encoder = new Encoder();
    const buffer = encodeRequestHeader(encoder, header).buffer();

    expect(base64(buffer)).toMatchSnapshot();

    const decoder = new Decoder(buffer);
    const output = decodeRequestHeader(decoder, validApiKey);

    expect(header).toEqual(output);
  });
});
