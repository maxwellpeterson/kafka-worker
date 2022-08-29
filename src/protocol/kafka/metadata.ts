import { ErrorCode, Int32 } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

// Metadata Request (Version: 0) => [topics]
//   topics => name
//     name => STRING
//
// https://kafka.apache.org/protocol.html#The_Messages_Metadata

export interface KafkaMetadataRequest {
  topics: string[];
}

export const encodeKafkaMetadataRequest = (
  encoder: Encoder,
  request: KafkaMetadataRequest
): ArrayBuffer => {
  return encoder.writeStringArray(request.topics).buffer();
};

export const decodeKafkaMetadataRequest = (
  decoder: Decoder
): KafkaMetadataRequest => {
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

export interface KafkaMetadataResponse {
  brokers: {
    nodeId: Int32;
    host: string;
    port: Int32;
  }[];
  topics: {
    errorCode: ErrorCode;
    name: string;
    partitions: {
      errorCode: ErrorCode;
      partitionIndex: Int32;
      leaderId: Int32;
      replicaNodes: Int32[];
      isrNodes: Int32[];
    }[];
  }[];
}

export const encodeKafkaMetadataResponse = (
  encoder: Encoder,
  response: KafkaMetadataResponse
): ArrayBuffer => {
  return encoder
    .writeArray(response.brokers, (broker) =>
      encoder
        .writeInt32(broker.nodeId)
        .writeString(broker.host)
        .writeInt32(broker.port)
    )
    .writeArray(response.topics, (topic) =>
      encoder
        .writeEnum(topic.errorCode)
        .writeString(topic.name)
        .writeArray(topic.partitions, (partition) =>
          encoder
            .writeEnum(partition.errorCode)
            .writeInt32(partition.partitionIndex)
            .writeInt32(partition.leaderId)
            .writeInt32Array(partition.replicaNodes)
            .writeInt32Array(partition.isrNodes)
        )
    )
    .buffer();
};

export const decodeKafkaMetadataResponse = (
  decoder: Decoder
): KafkaMetadataResponse => {
  return {
    brokers: decoder.readArray(() => ({
      nodeId: decoder.readInt32(),
      host: decoder.readString(),
      port: decoder.readInt32(),
    })),
    topics: decoder.readArray(() => ({
      errorCode: decoder.readErrorCode(),
      name: decoder.readString(),
      partitions: decoder.readArray(() => ({
        errorCode: decoder.readErrorCode(),
        partitionIndex: decoder.readInt32(),
        leaderId: decoder.readInt32(),
        replicaNodes: decoder.readInt32Array(),
        isrNodes: decoder.readInt32Array(),
      })),
    })),
  };
};
