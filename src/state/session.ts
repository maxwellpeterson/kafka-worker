import { Env } from "src/common";
import {
  decodeMetadataRequest,
  encodeMetadataResponse,
} from "src/protocol/api/metadata";
import {
  decodeProduceRequest,
  encodeProduceResponse,
  ProduceResponse,
} from "src/protocol/api/produce";
import {
  ApiKey,
  cleanKafkaStringArray,
  int32Size,
  KafkaArray,
  KafkaString,
} from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { fetchClusterMetadata } from "src/state/cluster";

export class Session {
  private readonly state: DurableObjectState;
  private readonly env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();
    server.addEventListener("message", (event) => {
      if (typeof event.data === "string") {
        console.log("Received string data, but we want binary data!");
        return;
      }

      this.handleRequest(event.data)
        .then((response) => {
          if (response !== null) {
            server.send(response);
          }
        })
        .catch((error) =>
          console.log(`Error while processing request: ${error}`)
        );
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  private async handleRequest(
    buffer: ArrayBuffer
  ): Promise<ArrayBuffer | null> {
    const decoder = new Decoder(buffer);

    const expectedSize = decoder.readInt32();
    const actualSize = buffer.byteLength - int32Size;
    if (expectedSize !== actualSize) {
      throw new Error(
        `Message length does not match size field: expected ${expectedSize} but got ${actualSize}`
      );
    }

    const apiKey = decoder.readInt16();
    const apiVersion = decoder.readInt16();

    if (apiVersion !== 0) {
      throw new Error(
        `Unsupported version of api ${apiKey}: expected 0 but got ${apiVersion}`
      );
    }

    const correlationId = decoder.readInt32();
    // Not currently used, but need to read to move decoder cursor
    const clientId = decoder.readString();

    const encoder = new Encoder();
    encoder.writeInt32(correlationId);

    switch (apiKey) {
      case ApiKey.Produce:
        return this.handleProduceRequest(
          apiVersion,
          correlationId,
          decoder,
          encoder
        );
      case ApiKey.Metadata:
        return this.handleMetadataRequest(
          apiVersion,
          correlationId,
          decoder,
          encoder
        );
      default:
        throw new Error(`Unknown api key: ${apiKey}`);
    }
  }

  private async handleProduceRequest(
    apiVersion: number,
    correlationId: number,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer | null> {
    const request = decodeProduceRequest(decoder);
    console.log(`Produce request: ${JSON.stringify(request, null, 2)}`);

    // TODO: Temp
    const response = {} as unknown as ProduceResponse;
    console.log(`Produce response: ${JSON.stringify(response, null, 2)}`);

    if (request.acks === 0) {
      return null;
    }
    return encodeProduceResponse(encoder, response);
  }

  private async handleMetadataRequest(
    apiVersion: number,
    correlationId: number,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer> {
    const request = decodeMetadataRequest(decoder);
    console.log(`Metadata request: ${JSON.stringify(request, null, 2)}`);

    const response = await fetchClusterMetadata(
      this.env,
      cleanKafkaStringArray(request.topics)
    );
    console.log(`Metadata response: ${JSON.stringify(response, null, 2)}`);

    return encodeMetadataResponse(encoder, response);
  }
}
