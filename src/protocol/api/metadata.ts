import { ErrorCode, Int32 } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

// Metadata Request (Version: 0) => [topics]
//   topics => name
//     name => STRING
//
// https://kafka.apache.org/protocol.html#The_Messages_Metadata

export interface MetadataRequest {
  topics: string[];
}

export const encodeMetadataRequest = (
  encoder: Encoder,
  request: MetadataRequest
): ArrayBuffer => {
  return encoder.writeStringArray(request.topics).buffer();
};

export const decodeMetadataRequest = (decoder: Decoder): MetadataRequest => {
  return { topics: decoder.readStringArray() };
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
  return encoder
    .writeArray(response.brokers, encodeBroker(encoder))
    .writeArray(response.topics, encodeTopicMetadata(encoder))
    .buffer();
};

export const decodeMetadataResponse = (decoder: Decoder): MetadataResponse => {
  return {
    brokers: decoder.readArray(decodeBroker(decoder)),
    topics: decoder.readArray(decodeTopicMetadata(decoder)),
  };
};

export interface Broker {
  nodeId: Int32;
  host: string;
  port: Int32;
}

const encodeBroker =
  (encoder: Encoder) =>
  (broker: Broker): Encoder => {
    return encoder
      .writeInt32(broker.nodeId)
      .writeString(broker.host)
      .writeInt32(broker.port);
  };

const decodeBroker = (decoder: Decoder) => (): Broker => {
  return {
    nodeId: decoder.readInt32(),
    host: decoder.readString(),
    port: decoder.readInt32(),
  };
};

export interface TopicMetadata {
  errorCode: ErrorCode;
  name: string;
  partitions: PartitionMetadata[];
}

const encodeTopicMetadata =
  (encoder: Encoder) =>
  (metadata: TopicMetadata): Encoder => {
    return encoder
      .writeEnum(metadata.errorCode)
      .writeString(metadata.name)
      .writeArray(metadata.partitions, encodePartitionMetadata(encoder));
  };

const decodeTopicMetadata = (decoder: Decoder) => (): TopicMetadata => {
  return {
    errorCode: decoder.readErrorCode(),
    name: decoder.readString(),
    partitions: decoder.readArray(decodePartitionMetadata(decoder)),
  };
};

export interface PartitionMetadata {
  errorCode: ErrorCode;
  partitionIndex: Int32;
  leaderId: Int32;
  replicaNodes: Int32[];
  isrNodes: Int32[];
}

const encodePartitionMetadata =
  (encoder: Encoder) =>
  (metadata: PartitionMetadata): Encoder => {
    return encoder
      .writeEnum(metadata.errorCode)
      .writeInt32(metadata.partitionIndex)
      .writeInt32(metadata.leaderId)
      .writeInt32Array(metadata.replicaNodes)
      .writeInt32Array(metadata.isrNodes);
  };

const decodePartitionMetadata = (decoder: Decoder) => (): PartitionMetadata => {
  return {
    errorCode: decoder.readErrorCode(),
    partitionIndex: decoder.readInt32(),
    leaderId: decoder.readInt32(),
    replicaNodes: decoder.readInt32Array(),
    isrNodes: decoder.readInt32Array(),
  };
};
