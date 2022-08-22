import {
  Int32,
  KafkaString,
  KafkaArray,
  ApiKey,
  ErrorCode,
  BaseKafkaRequest,
} from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { Env, Message } from "src/common";

type MetadataRequest = BaseKafkaRequest & {
  apiKey: typeof ApiKey.MetadataRequest;
  message: {
    topics: KafkaArray<KafkaString>;
  };
};

interface MetadataResponse {
  brokers: KafkaArray<Broker>;
  topicMetadata: KafkaArray<TopicMetadata>;
}

interface Broker {
  nodeId: Int32;
  host: KafkaString;
  port: Int32;
}

interface TopicMetadata {
  topicErrorCode: ErrorCode;
  topicName: KafkaString;
  partitionMetadata: KafkaArray<PartitionMetadata>;
}

interface PartitionMetadata {
  partitionErrorCode: ErrorCode;
  partitionId: Int32;
  leader: Int32;
  replicas: KafkaArray<Int32>;
  isr: KafkaArray<Int32>;
}

export const decodeMetadataRequest = (
  decoder: Decoder
): Message<MetadataRequest> => {
  return { topics: decoder.readArray(() => decoder.readString()) };
};

const brokerNodeId = 100;
const workerHost = "kafka-worker.archmap.workers.dev";
const httpsPort = 443;
const stubTopicName = "test-topic";
const stubPartitionId = 1000;

export const handleMetadataRequest = (
  env: Env,
  request: MetadataRequest
): ArrayBuffer => {
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
  return encodeMetadataResponse(request.correlationId, response);
};

const initialEncodeBufferSize = 64;

const encodeMetadataResponse = (
  correlationId: Int32,
  response: MetadataResponse
): ArrayBuffer => {
  const buffer = new ArrayBuffer(initialEncodeBufferSize);
  const encoder = new Encoder(buffer);

  // TODO: This doesn't belong in this function
  encoder.writeInt32(correlationId);

  encoder.writeArray(response.brokers, (broker) => {
    encoder.writeInt32(broker.nodeId);
    encoder.writeString(broker.host);
    encoder.writeInt32(broker.port);
  });

  encoder.writeArray(response.topicMetadata, (topicMetadata) => {
    encoder.writeInt16(topicMetadata.topicErrorCode);
    encoder.writeString(topicMetadata.topicName);
    encoder.writeArray(topicMetadata.partitionMetadata, (partitionMetadata) => {
      encoder.writeInt16(partitionMetadata.partitionErrorCode);
      encoder.writeInt32(partitionMetadata.partitionId);
      encoder.writeInt32(partitionMetadata.leader);
      encoder.writeArray(partitionMetadata.replicas, (replica) =>
        encoder.writeInt32(replica)
      );
      encoder.writeArray(partitionMetadata.isr, (isr) =>
        encoder.writeInt32(isr)
      );
    });
  });

  return encoder.sizedBuffer();
};
