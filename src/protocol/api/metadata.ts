import { Int32, KafkaString, KafkaArray, ErrorCode } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { Env } from "src/common";

// TODO: Come up with a real "cluster state"
const brokerNodeId = 333;
const workerHost = "kafka-worker.archmap.workers.dev";
const httpsPort = 443;
const stubTopicName = "test-topic";
const stubPartitionId = 999;

export const handleMetadataRequest = (
  env: Env,
  version: number,
  decoder: Decoder,
  encoder: Encoder
): ArrayBuffer => {
  if (version !== 0) {
    throw new Error(
      `Unsupported version of metadata api: expected 0 but got ${version}`
    );
  }

  const request = decodeMetadataRequest(decoder);
  console.log(`Received metadata request: ${JSON.stringify(request, null, 2)}`);

  const response = {
    brokers: [{ nodeId: brokerNodeId, host: workerHost, port: httpsPort }],
    topicMetadata: [
      {
        topicErrorCode: ErrorCode.None,
        topicName: stubTopicName,
        partitionMetadata: [
          {
            partitionErrorCode: ErrorCode.None,
            partitionId: stubPartitionId,
            leader: brokerNodeId,
            replicas: [brokerNodeId],
            isr: [brokerNodeId],
          },
        ],
      },
    ],
  };
  console.log(
    `Sending metadata response: ${JSON.stringify(response, null, 2)}`
  );
  return encodeMetadataResponse(encoder, response);
};

interface MetadataRequest {
  topics: KafkaArray<KafkaString>;
}

const decodeMetadataRequest = (decoder: Decoder): MetadataRequest => {
  return { topics: decoder.readArray(() => decoder.readString()) };
};

interface MetadataResponse {
  brokers: KafkaArray<Broker>;
  topicMetadata: KafkaArray<TopicMetadata>;
}

const encodeMetadataResponse = (
  encoder: Encoder,
  response: MetadataResponse
): ArrayBuffer => {
  encoder.writeArray(response.brokers, (broker) =>
    encodeBroker(encoder, broker)
  );
  encoder.writeArray(response.topicMetadata, (metadata) =>
    encodeTopicMetadata(encoder, metadata)
  );
  return encoder.sizedBuffer();
};

interface Broker {
  nodeId: Int32;
  host: KafkaString;
  port: Int32;
}

const encodeBroker = (encoder: Encoder, broker: Broker) => {
  encoder.writeInt32(broker.nodeId);
  encoder.writeString(broker.host);
  encoder.writeInt32(broker.port);
};

interface TopicMetadata {
  topicErrorCode: ErrorCode;
  topicName: KafkaString;
  partitionMetadata: KafkaArray<PartitionMetadata>;
}

const encodeTopicMetadata = (encoder: Encoder, metadata: TopicMetadata) => {
  encoder.writeInt16(metadata.topicErrorCode);
  encoder.writeString(metadata.topicName);
  encoder.writeArray(metadata.partitionMetadata, (metadata) =>
    encodePartitionMetadata(encoder, metadata)
  );
};

interface PartitionMetadata {
  partitionErrorCode: ErrorCode;
  partitionId: Int32;
  leader: Int32;
  replicas: KafkaArray<Int32>;
  isr: KafkaArray<Int32>;
}

const encodePartitionMetadata = (
  encoder: Encoder,
  metadata: PartitionMetadata
) => {
  encoder.writeInt16(metadata.partitionErrorCode);
  encoder.writeInt32(metadata.partitionId);
  encoder.writeInt32(metadata.leader);
  encoder.writeArray(metadata.replicas, (replica) =>
    encoder.writeInt32(replica)
  );
  encoder.writeArray(metadata.isr, (isr) => encoder.writeInt32(isr));
};
