import { Int32, KafkaString, KafkaArray, ErrorCode } from "src/protocol/common";

// Metadata Request (Version: 0) => [topics]
//   topics => name
//     name => STRING
//
// https://kafka.apache.org/protocol.html#The_Messages_Metadata

export interface MetadataRequest {
  topics: KafkaArray<KafkaString>;
}

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
  brokers: KafkaArray<Broker>;
  topics: KafkaArray<TopicMetadata>;
}

export interface Broker {
  nodeId: Int32;
  host: KafkaString;
  port: Int32;
}

export interface TopicMetadata {
  errorCode: ErrorCode;
  name: KafkaString;
  partitions: KafkaArray<PartitionMetadata>;
}

export interface PartitionMetadata {
  errorCode: ErrorCode;
  partitionIndex: Int32;
  leaderId: Int32;
  replicaNodes: KafkaArray<Int32>;
  isrNodes: KafkaArray<Int32>;
}
