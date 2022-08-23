import { Env } from "src/common";
import { Decoder } from "src/protocol/decoder";
import { ApiKey, int32Size } from "src/protocol/common";
import { metadataRequestEntrypoint } from "src/protocol/api/metadata/entrypoint";
import { Encoder } from "src/protocol/encoder";
import { produceRequestEntrypoint } from "src/protocol/api/produce/entrypoint";
import { EntrypointFn } from "src/protocol/api/common";

export const handleRequest = async (
  env: Env,
  buffer: ArrayBuffer
): Promise<ArrayBuffer | null> => {
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

  const entrypoint = findEntrypoint(apiKey);
  return await entrypoint(env, apiVersion, decoder, encoder);
};

const findEntrypoint = (apiKey: number): EntrypointFn<ArrayBuffer | null> => {
  switch (apiKey) {
    case ApiKey.Produce:
      return produceRequestEntrypoint;
    case ApiKey.Metadata:
      return metadataRequestEntrypoint;
    default:
      throw new Error(`Unknown api key: ${apiKey}`);
  }
};
