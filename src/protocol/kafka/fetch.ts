import { ErrorCode, Int32, Int64, MessageSet } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

// Fetch Request (Version: 0) => replica_id max_wait_ms min_bytes [topics]
//   replica_id => INT32
//   max_wait_ms => INT32
//   min_bytes => INT32
//   topics => topic [partitions]
//     topic => STRING
//     partitions => partition fetch_offset partition_max_bytes
//       partition => INT32
//       fetch_offset => INT64
//       partition_max_bytes => INT32
//
// https://kafka.apache.org/protocol.html#The_Messages_Fetch

export interface KafkaFetchRequest {
  replicaId: Int32;
  maxWaitMs: Int32;
  minBytes: Int32;
  topics: {
    name: string;
    partitions: {
      index: Int32;
      fetchOffset: Int64;
      maxBytes: Int32;
    }[];
  }[];
}

export const encodeKafkaFetchRequest = (
  encoder: Encoder,
  request: KafkaFetchRequest
): ArrayBuffer => {
  return encoder
    .writeInt32(request.replicaId)
    .writeInt32(request.maxWaitMs)
    .writeInt32(request.minBytes)
    .writeArray(request.topics, (topic) =>
      encoder
        .writeString(topic.name)
        .writeArray(topic.partitions, (partition) =>
          encoder
            .writeInt32(partition.index)
            .writeInt64(partition.fetchOffset)
            .writeInt32(partition.maxBytes)
        )
    )
    .buffer();
};

export const decodeKafkaFetchRequest = (
  decoder: Decoder
): KafkaFetchRequest => {
  return {
    replicaId: decoder.readInt32(),
    maxWaitMs: decoder.readInt32(),
    minBytes: decoder.readInt32(),
    topics: decoder.readArray(() => ({
      name: decoder.readString(),
      partitions: decoder.readArray(() => ({
        index: decoder.readInt32(),
        fetchOffset: decoder.readInt64(),
        maxBytes: decoder.readInt32(),
      })),
    })),
  };
};

// Fetch Response (Version: 0) => [responses]
//   responses => topic [partitions]
//     topic => STRING
//     partitions => partition_index error_code high_watermark records
//       partition_index => INT32
//       error_code => INT16
//       high_watermark => INT64
//       records => RECORDS
//
// https://kafka.apache.org/protocol.html#The_Messages_Fetch
//
// There some variation here between the current protocol spec and
// the older protocol guide:
//
// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
//
// Here we follow the older description of message set (now called record batch).

export interface KafkaFetchResponse {
  topics: {
    name: string;
    partitions: {
      index: Int32;
      errorCode: ErrorCode;
      highWatermark: Int64;
      messageSet: MessageSet;
    }[];
  }[];
}

export const encodeKafkaFetchResponse = (
  encoder: Encoder,
  response: KafkaFetchResponse
): ArrayBuffer => {
  return encoder
    .writeArray(response.topics, (topic) =>
      encoder
        .writeString(topic.name)
        .writeArray(topic.partitions, (partition) =>
          encoder
            .writeInt32(partition.index)
            .writeEnum(partition.errorCode)
            .writeInt64(partition.highWatermark)
            .writeMessageSet(partition.messageSet)
        )
    )
    .buffer();
};

export const decodeKafkaFetchResponse = (
  decoder: Decoder
): KafkaFetchResponse => {
  return {
    topics: decoder.readArray(() => ({
      name: decoder.readString(),
      partitions: decoder.readArray(() => ({
        index: decoder.readInt32(),
        errorCode: decoder.readErrorCode(),
        highWatermark: decoder.readInt64(),
        messageSet: decoder.readMessageSet(),
      })),
    })),
  };
};
