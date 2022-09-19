import { RequestManager } from "src/client/request-manager";
import { Env, stringify } from "src/common";
import { ApiKey, ErrorCode, validApiKey } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { RequestMetadata, decodeRequestHeader } from "src/protocol/header";
import { KafkaDecoder, KafkaResponseEncoder } from "src/protocol/kafka/common";
import {
  decodeKafkaListOffsetsRequest,
  encodeKafkaListOffsetsResponse,
} from "src/protocol/kafka/list-offsets";
import {
  decodeKafkaMetadataRequest,
  encodeKafkaMetadataResponse,
} from "src/protocol/kafka/metadata";
import {
  decodeKafkaProduceRequest,
  encodeKafkaProduceResponse,
} from "src/protocol/kafka/produce";
import { fetchClusterMetadata, globalBrokerId } from "src/state/cluster";

// Coordinator class that handles one client connection and forwards incoming
// requests to Partition DOs and the global Cluster DO
export class Session {
  private readonly env: Env;

  // Client for making internal requests to DOs
  private readonly internal: RequestManager;

  constructor(env: Env) {
    this.env = env;

    this.internal = new RequestManager(env);
  }

  async handleRequest(buffer: ArrayBuffer): Promise<ArrayBuffer | null> {
    const decoder = new KafkaDecoder(buffer);
    const header = decodeRequestHeader(decoder, validApiKey);

    if (header.apiVersion !== 0) {
      throw new Error(
        `Unsupported version of api ${header.apiKey}: expected 0 but got ${header.apiVersion}`
      );
    }

    const encoder = new KafkaResponseEncoder(header.correlationId);

    switch (header.apiKey) {
      case ApiKey.Produce:
        return this.handleProduceRequest(header, decoder, encoder);
      case ApiKey.ListOffsets:
        return this.handleListOffsetsRequest(header, decoder, encoder);
      case ApiKey.Metadata:
        return this.handleMetadataRequest(header, decoder, encoder);
    }
  }

  private async handleProduceRequest(
    metadata: RequestMetadata,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer | null> {
    const request = decodeKafkaProduceRequest(decoder);
    console.log(`Produce request: ${stringify(request)}`);

    const response = await this.internal.produceRequest(metadata, request);
    console.log(`Produce response: ${stringify(response)}`);

    if (response === null) {
      return null;
    }
    return encodeKafkaProduceResponse(encoder, response);
  }

  private async handleListOffsetsRequest(
    metadata: RequestMetadata,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer | null> {
    const request = decodeKafkaListOffsetsRequest(decoder);
    console.log(`ListOffsets request: ${stringify(request)}`);

    if (request.replicaId !== globalBrokerId) {
      return encodeKafkaListOffsetsResponse(encoder, {
        topics: request.topics.map((topic) => ({
          name: topic.name,
          partitions: topic.partitions.map((partition) => ({
            index: partition.index,
            errorCode: ErrorCode.NotLeaderForPartition,
            oldStyleOffsets: [],
          })),
        })),
      });
    }

    const response = await this.internal.listOffsetsRequest(metadata, request);
    console.log(`ListOffsets response: ${stringify(response)}`);

    return encodeKafkaListOffsetsResponse(encoder, response);
  }

  private async handleMetadataRequest(
    metadata: RequestMetadata,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer> {
    const request = decodeKafkaMetadataRequest(decoder);
    console.log(`Metadata request: ${stringify(request)}`);

    const response = await fetchClusterMetadata(this.env, request.topics);
    console.log(`Metadata response: ${stringify(response)}`);

    return encodeKafkaMetadataResponse(encoder, response);
  }
}
