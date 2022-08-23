import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import {
  PartitionData,
  PartitionResponse,
  ProduceRequest,
  ProduceResponse,
  TopicData,
  TopicResponse,
} from "src/protocol/api/produce/types";
import {
  generateCheckVersionFn,
  generateEntrypointFn,
} from "src/protocol/api/common";
import { handleProduceRequest } from "src/protocol/api/produce/handler";

const decodeProduceRequest = (decoder: Decoder): ProduceRequest => {
  return {
    acks: decoder.readInt16(),
    timeoutMs: decoder.readInt32(),
    topics: decoder.readArray(() => decodeTopicData(decoder)),
  };
};

const decodeTopicData = (decoder: Decoder): TopicData => {
  return {
    name: decoder.readString(),
    partitions: decoder.readArray(() => decodePartitionData(decoder)),
  };
};

const decodePartitionData = (decoder: Decoder): PartitionData => {
  const data = {
    index: decoder.readInt32(),
    messageSetSize: decoder.readInt32(),
  };
  return { ...data, messageSet: decoder.readMessageSet(data.messageSetSize) };
};

const encodeProduceResponse = (
  encoder: Encoder,
  response: ProduceResponse | null
): ArrayBuffer | null => {
  if (response === null) {
    return null;
  }
  encoder.writeArray(response.topics, (topic) =>
    encodeTopicResponse(encoder, topic)
  );
  return encoder.sizedBuffer();
};

const encodeTopicResponse = (encoder: Encoder, topic: TopicResponse) => {
  encoder.writeString(topic.name);
  encoder.writeArray(topic.partitions, (partition) =>
    encodePartitionResponse(encoder, partition)
  );
};

const encodePartitionResponse = (
  encoder: Encoder,
  partition: PartitionResponse
) => {
  encoder.writeInt32(partition.index);
  encoder.writeErrorCode(partition.errorCode);
  encoder.writeInt64(partition.baseOffset);
};

export const produceRequestEntrypoint = generateEntrypointFn(
  "produce",
  generateCheckVersionFn("produce", 0),
  decodeProduceRequest,
  handleProduceRequest,
  encodeProduceResponse
);
