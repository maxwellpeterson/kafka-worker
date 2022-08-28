import { Env, stringify } from "src/common";
import {
  KafkaRequestDecoder,
  KafkaResponseEncoder,
} from "src/protocol/api/common";
import {
  decodeMetadataRequest,
  encodeMetadataResponse,
} from "src/protocol/api/metadata";
import {
  decodeProduceRequest,
  encodeProduceResponse,
} from "src/protocol/api/produce";
import { ApiKey, RequestMetadata, validApiKey } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
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
    const apiKey = decoder.readEnum(validApiKey);
    const metadata = {
      apiVersion: decoder.readInt16(),
      correlationId: decoder.readInt32(),
      clientId: decoder.readString(),
    };

    if (metadata.apiVersion !== 0) {
      throw new Error(
        `Unsupported version of api ${apiKey}: expected 0 but got ${metadata.apiVersion}`
      );
    }

    const encoder = new KafkaResponseEncoder(metadata.correlationId);

    switch (apiKey) {
      case ApiKey.Produce:
        return this.handleProduceRequest(metadata, decoder, encoder);
      case ApiKey.Metadata:
        return this.handleMetadataRequest(metadata, decoder, encoder);
    }
  }

  private async handleProduceRequest(
    metadata: RequestMetadata,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer | null> {
    const request = decodeProduceRequest(decoder);
    console.log(`Produce request: ${stringify(request)}`);

    const response = await this.internal.produceRequest(metadata, request);

    console.log(`Produce response: ${stringify(response)}`);

    if (response === null) {
      return null;
    }
    return encodeProduceResponse(encoder, response);
  }

  private async handleMetadataRequest(
    metadata: RequestMetadata,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer> {
    const request = decodeMetadataRequest(decoder);
    console.log(`Metadata request: ${stringify(request)}`);

    const response = await fetchClusterMetadata(this.env, request.topics);
    console.log(`Metadata response: ${stringify(response)}`);

    return encodeMetadataResponse(encoder, response);
  }
}
