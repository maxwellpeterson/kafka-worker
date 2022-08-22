import { Env } from "src/common";
import { Decoder } from "src/protocol/decoder";
import { ApiKey, int32Size } from "src/protocol/common";
import {
  decodeMetadataRequest,
  handleMetadataRequest,
} from "src/protocol/api/metadata";

export const handleRequest = (env: Env, buffer: ArrayBuffer): ArrayBuffer => {
  const decoder = new Decoder(buffer);

  const expectedSize = decoder.readInt32();
  const actualSize = buffer.byteLength - int32Size;
  if (expectedSize !== actualSize) {
    throw new Error(
      `Message length does not match size field: expected ${expectedSize} but got ${actualSize}`
    );
  }

  const apiKey = decoder.readInt16();
  const request = {
    apiVersion: decoder.readInt16(),
    correlationId: decoder.readInt32(),
    clientId: decoder.readString(),
  };

  if (request.apiVersion !== 0) {
    throw new Error(
      `Unsupported version of metadata api: expected 0 but got ${request.apiVersion}`
    );
  }

  switch (apiKey) {
    case ApiKey.MetadataRequest:
      const message = decodeMetadataRequest(decoder);
      return handleMetadataRequest(env, { apiKey, ...request, message });
    default:
      throw new Error(`Unknown api key: ${apiKey}`);
  }
};
