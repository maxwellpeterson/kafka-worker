import {
  MetadataRequest,
  MetadataResponse,
  Broker,
  TopicMetadata,
  PartitionMetadata,
} from "src/protocol/api/metadata/types";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { Env } from "src/common";
import { handleMetadataRequest } from "src/protocol/api/metadata/handler";

export const metadataRequestEntrypoint = async (
  env: Env,
  version: number,
  decoder: Decoder,
  encoder: Encoder
): Promise<ArrayBuffer> => {
  if (version !== 0) {
    throw new Error(
      `Unsupported version of metadata api: expected 0 but got ${version}`
    );
  }

  const request = decodeMetadataRequest(decoder);
  console.log(`Received metadata request: ${JSON.stringify(request, null, 2)}`);

  const response = await handleMetadataRequest(env, request);
  console.log(
    `Sending metadata response: ${JSON.stringify(response, null, 2)}`
  );
  return encodeMetadataResponse(encoder, response);
};

const decodeMetadataRequest = (decoder: Decoder): MetadataRequest => {
  return { topics: decoder.readArray(() => decoder.readString()) };
};

const encodeMetadataResponse = (
  encoder: Encoder,
  response: MetadataResponse
): ArrayBuffer => {
  encoder.writeArray(response.brokers, (broker) =>
    encodeBroker(encoder, broker)
  );
  encoder.writeArray(response.topics, (metadata) =>
    encodeTopicMetadata(encoder, metadata)
  );
  return encoder.sizedBuffer();
};

const encodeBroker = (encoder: Encoder, broker: Broker) => {
  encoder.writeInt32(broker.nodeId);
  encoder.writeString(broker.host);
  encoder.writeInt32(broker.port);
};

const encodeTopicMetadata = (encoder: Encoder, metadata: TopicMetadata) => {
  encoder.writeInt16(metadata.topicErrorCode);
  encoder.writeString(metadata.topicName);
  encoder.writeArray(metadata.partitions, (metadata) =>
    encodePartitionMetadata(encoder, metadata)
  );
};

const encodePartitionMetadata = (
  encoder: Encoder,
  metadata: PartitionMetadata
) => {
  encoder.writeInt16(metadata.partitionErrorCode);
  encoder.writeInt32(metadata.partitionId);
  encoder.writeInt32(metadata.leader);
  encoder.writeArray(metadata.replicas, (replica) =>
    encoder.writeInt32(replica)
  );
  encoder.writeArray(metadata.isr, (isr) => encoder.writeInt32(isr));
};
