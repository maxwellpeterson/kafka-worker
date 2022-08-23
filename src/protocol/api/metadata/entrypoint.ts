import {
  MetadataRequest,
  MetadataResponse,
  Broker,
  TopicMetadata,
  PartitionMetadata,
} from "src/protocol/api/metadata/types";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { handleMetadataRequest } from "src/protocol/api/metadata/handler";
import {
  generateCheckVersionFn,
  generateEntrypointFn,
} from "src/protocol/api/common";

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
  encoder.writeErrorCode(metadata.errorCode);
  encoder.writeString(metadata.name);
  encoder.writeArray(metadata.partitions, (metadata) =>
    encodePartitionMetadata(encoder, metadata)
  );
};

const encodePartitionMetadata = (
  encoder: Encoder,
  metadata: PartitionMetadata
) => {
  encoder.writeErrorCode(metadata.errorCode);
  encoder.writeInt32(metadata.partitionIndex);
  encoder.writeInt32(metadata.leaderId);
  encoder.writeArray(metadata.replicaNodes, (replica) =>
    encoder.writeInt32(replica)
  );
  encoder.writeArray(metadata.isrNodes, (isr) => encoder.writeInt32(isr));
};

export const metadataRequestEntrypoint = generateEntrypointFn(
  "metadata",
  generateCheckVersionFn("metadata", 0),
  decodeMetadataRequest,
  handleMetadataRequest,
  encodeMetadataResponse
);
