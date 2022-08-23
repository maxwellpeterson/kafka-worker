import { Env } from "src/common";
import { Decoder } from "src/protocol/decoder";
import { ApiKey, int32Size } from "src/protocol/common";
import { handleMetadataRequest } from "src/protocol/api/metadata";
import { Encoder } from "src/protocol/encoder";

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
  const apiVersion = decoder.readInt16();
  const correlationId = decoder.readInt32();
  // Not currently used, but need to read to move decoder cursor
  const clientId = decoder.readString();

  const encoder = new Encoder();
  encoder.writeInt32(correlationId);

  switch (apiKey) {
    case ApiKey.MetadataRequest:
      return handleMetadataRequest(env, apiVersion, decoder, encoder);
    default:
      throw new Error(`Unknown api key: ${apiKey}`);
  }
};