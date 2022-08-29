import { Env, stringify } from "src/common";
import { ApiKey, validApiKey } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { RequestMetadata, decodeRequestHeader } from "src/protocol/header";
import {
  KafkaRequestDecoder,
  KafkaResponseEncoder,
} from "src/protocol/kafka/common";
import {
  decodeKafkaMetadataRequest,
  encodeKafkaMetadataResponse,
} from "src/protocol/kafka/metadata";
import {
  decodeKafkaProduceRequest,
  encodeKafkaProduceResponse,
} from "src/protocol/kafka/produce";
import { RequestManager } from "src/state/client/request-manager";
import { fetchClusterMetadata } from "src/state/cluster";

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
    const decoder = new KafkaRequestDecoder(buffer);
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
