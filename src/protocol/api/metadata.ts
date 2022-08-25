import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { Int32, ErrorCode } from "src/protocol/common";

// Metadata Request (Version: 0) => [topics]
//   topics => name
//     name => STRING
//
// https://kafka.apache.org/protocol.html#The_Messages_Metadata

export interface MetadataRequest {
  topics: string[];
}

export const decodeMetadataRequest = (decoder: Decoder): MetadataRequest => {
  return { topics: decoder.readArray(() => decoder.readString()) };
};

// Metadata Response (Version: 0) => [brokers] [topics]
//   brokers => node_id host port
//     node_id => INT32
//     host => STRING
//     port => INT32
//   topics => error_code name [partitions]
//     error_code => INT16
//     name => STRING
//     partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes]
//       error_code => INT16
//       partition_index => INT32
//       leader_id => INT32
//       replica_nodes => INT32
//       isr_nodes => INT32
//
// https://kafka.apache.org/protocol.html#The_Messages_Metadata

export interface MetadataResponse {
  brokers: Broker[];
  topics: TopicMetadata[];
}

export const encodeMetadataResponse = (
  encoder: Encoder,
  response: MetadataResponse
): ArrayBuffer => {
  encoder.writeArray(response.brokers, (broker) =>
    encodeBroker(encoder, broker)
  );
  encoder.writeArray(response.topics, (metadata) =>
    encodeTopicMetadata(encoder, metadata)
  );
  return encoder.buffer();
};

export interface Broker {
  nodeId: Int32;
  host: string;
  port: Int32;
}

const encodeBroker = (encoder: Encoder, broker: Broker) => {
  encoder.writeInt32(broker.nodeId);
  encoder.writeString(broker.host);
  encoder.writeInt32(broker.port);
};

export interface TopicMetadata {
  errorCode: ErrorCode;
  name: string;
  partitions: PartitionMetadata[];
}

const encodeTopicMetadata = (encoder: Encoder, metadata: TopicMetadata) => {
  encoder.writeEnum(metadata.errorCode);
  encoder.writeString(metadata.name);
  encoder.writeArray(metadata.partitions, (metadata) =>
    encodePartitionMetadata(encoder, metadata)
  );
};

export interface PartitionMetadata {
  errorCode: ErrorCode;
  partitionIndex: Int32;
  leaderId: Int32;
  replicaNodes: Int32[];
  isrNodes: Int32[];
}

const encodePartitionMetadata = (
  encoder: Encoder,
  metadata: PartitionMetadata
) => {
  encoder.writeEnum(metadata.errorCode);
  encoder.writeInt32(metadata.partitionIndex);
  encoder.writeInt32(metadata.leaderId);
  encoder.writeArray(metadata.replicaNodes, (replica) =>
    encoder.writeInt32(replica)
  );
  encoder.writeArray(metadata.isrNodes, (isr) => encoder.writeInt32(isr));
};
