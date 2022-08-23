import {
  ErrorCode,
  Int16,
  Int32,
  Int64,
  KafkaArray,
  KafkaString,
} from "src/protocol/common";

// Produce Request (Version: 0) => acks timeout_ms [topic_data]
//   acks => INT16
//   timeout_ms => INT32
//   topic_data => name [partition_data]
//     name => STRING
//     partition_data => index records
//       index => INT32
//       records => RECORDS
//
// https://kafka.apache.org/protocol.html#The_Messages_Produce
//
// There some variation here between the current protocol spec and
// the older protocol guide:
//
// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
//
// Here we follow the older description of message set (now called record batch).

export interface ProduceRequest {
  acks: Int16;
  timeoutMs: Int32;
  topics: KafkaArray<TopicData>;
}

export interface TopicData {
  name: KafkaString;
  partitions: KafkaArray<PartitionData>;
}

export interface PartitionData {
  index: Int32;
  messageSetSize: Int32;
  messageSet: ArrayBuffer;
}

// Produce Response (Version: 0) => [responses]
//   responses => name [partition_responses]
//   name => STRING
//   partition_responses => index error_code base_offset
//     index => INT32
//     error_code => INT16
//     base_offset => INT64
//
// https://kafka.apache.org/protocol.html#The_Messages_Produce

export interface ProduceResponse {
  topics: KafkaArray<TopicResponse>;
}

export interface TopicResponse {
  name: KafkaString;
  partitions: KafkaArray<PartitionResponse>;
}

export interface PartitionResponse {
  index: Int32;
  errorCode: ErrorCode;
  baseOffset: Int64;
}
