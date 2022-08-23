import { Int32, KafkaString, KafkaArray, ErrorCode } from "src/protocol/common";

export interface MetadataRequest {
  topics: KafkaArray<KafkaString>;
}

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
  topicErrorCode: ErrorCode;
  topicName: KafkaString;
  partitions: KafkaArray<PartitionMetadata>;
}

export interface PartitionMetadata {
  partitionErrorCode: ErrorCode;
  partitionId: Int32;
  leader: Int32;
  replicas: KafkaArray<Int32>;
  isr: KafkaArray<Int32>;
}
