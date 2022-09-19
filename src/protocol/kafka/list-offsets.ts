import { ErrorCode, Int32, Int64 } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

// ListOffsets Request (Version: 0) => replica_id [topics]
//   replica_id => INT32
//   topics => name  [partitions]
//     name => STRING
//     partitions => partition_index timestamp max_num_offsets
//       partition_index => INT32
//       timestamp => INT64
//       max_num_offsets => INT32
//
// https://kafka.apache.org/protocol.html#The_Messages_ListOffsets

export interface KafkaListOffsetsRequest {
  replicaId: Int32;
  topics: {
    name: string;
    partitions: {
      index: Int32;
      timestamp: Int64;
      maxNumOffsets: Int32;
    }[];
  }[];
}

export const encodeKafkaListOffsetsRequest = (
  encoder: Encoder,
  request: KafkaListOffsetsRequest
): ArrayBuffer => {
  return encoder
    .writeInt32(request.replicaId)
    .writeArray(request.topics, (topic) =>
      encoder
        .writeString(topic.name)
        .writeArray(topic.partitions, (partition) =>
          encoder
            .writeInt32(partition.index)
            .writeInt64(partition.timestamp)
            .writeInt32(partition.maxNumOffsets)
        )
    )
    .buffer();
};

export const decodeKafkaListOffsetsRequest = (
  decoder: Decoder
): KafkaListOffsetsRequest => {
  return {
    replicaId: decoder.readInt32(),
    topics: decoder.readArray(() => ({
      name: decoder.readString(),
      partitions: decoder.readArray(() => ({
        index: decoder.readInt32(),
        timestamp: decoder.readInt64(),
        maxNumOffsets: decoder.readInt32(),
      })),
    })),
  };
};

// ListOffsets Response (Version: 0) => [topics]
//   topics => name  [partitions]
//     name => STRING
//     partitions => partition_index error_code [old_style_offsets]
//       partition_index => INT32
//       error_code => INT16
//       old_style_offsets => INT64
//
// https://kafka.apache.org/protocol.html#The_Messages_ListOffsets

export interface KafkaListOffsetsResponse {
  topics: {
    name: string;
    partitions: {
      index: Int32;
      errorCode: ErrorCode;
      oldStyleOffsets: Int64[];
    }[];
  }[];
}

export const encodeKafkaListOffsetsResponse = (
  encoder: Encoder,
  response: KafkaListOffsetsResponse
): ArrayBuffer => {
  return encoder
    .writeArray(response.topics, (topic) =>
      encoder
        .writeString(topic.name)
        .writeArray(topic.partitions, (partition) =>
          encoder
            .writeInt32(partition.index)
            .writeEnum(partition.errorCode)
            .writeInt64Array(partition.oldStyleOffsets)
        )
    )
    .buffer();
};

export const decodeKafkaListOffsetsResponse = (
  decoder: Decoder
): KafkaListOffsetsResponse => {
  return {
    topics: decoder.readArray(() => ({
      name: decoder.readString(),
      partitions: decoder.readArray(() => ({
        index: decoder.readInt32(),
        errorCode: decoder.readErrorCode(),
        oldStyleOffsets: decoder.readInt64Array(),
      })),
    })),
  };
};
