import { Acks, ErrorCode, Int32, Int64 } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

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
  acks: Acks;
  timeoutMs: Int32;
  topics: TopicData[];
}

export const decodeProduceRequest = (decoder: Decoder): ProduceRequest => {
  return {
    acks: decoder.readAcks(),
    timeoutMs: decoder.readInt32(),
    topics: decoder.readArray(decodeTopicData(decoder)),
  };
};

export interface TopicData {
  name: string;
  partitions: PartitionData[];
}

const decodeTopicData = (decoder: Decoder) => (): TopicData => {
  return {
    name: decoder.readString(),
    partitions: decoder.readArray(decodePartitionData(decoder)),
  };
};

export interface PartitionData {
  index: Int32;
  messageSetSize: Int32;
  messageSet: ArrayBuffer;
}

const decodePartitionData = (decoder: Decoder) => (): PartitionData => {
  const data = {
    index: decoder.readInt32(),
    messageSetSize: decoder.readInt32(),
  };
  return { ...data, messageSet: decoder.readBuffer(data.messageSetSize) };
};

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
  topics: TopicResponse[];
}

export const encodeProduceResponse = (
  encoder: Encoder,
  response: ProduceResponse
): ArrayBuffer => {
  return encoder
    .writeArray(response.topics, encodeTopicResponse(encoder))
    .buffer();
};

export interface TopicResponse {
  name: string;
  partitions: PartitionResponse[];
}

const encodeTopicResponse =
  (encoder: Encoder) =>
  (topic: TopicResponse): Encoder => {
    return encoder
      .writeString(topic.name)
      .writeArray(topic.partitions, encodePartitionResponse(encoder));
  };

export interface PartitionResponse {
  index: Int32;
  errorCode: ErrorCode;
  baseOffset: Int64;
}

const encodePartitionResponse =
  (encoder: Encoder) =>
  (partition: PartitionResponse): Encoder => {
    return encoder
      .writeInt32(partition.index)
      .writeEnum(partition.errorCode)
      .writeInt64(partition.baseOffset);
  };
