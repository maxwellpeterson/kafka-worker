import { Env } from "src/common";
import {
  decodeMetadataRequest,
  encodeMetadataResponse,
} from "src/protocol/api/metadata";
import {
  decodeProduceRequest,
  encodeProduceResponse,
} from "src/protocol/api/produce";
import { ApiKey, RequestMetadata, validApiKey } from "src/protocol/common";
import { Decoder, KafkaRequestDecoder } from "src/protocol/decoder";
import { Encoder, KafkaResponseEncoder } from "src/protocol/encoder";
import { fetchClusterMetadata } from "src/state/cluster";
import { RequestManager } from "src/state/request-manager";

// Coordinator object that handles one client connection and forwards incoming
// requests to Partition DOs and the global Cluster DO. Lives as long as the
// client connection, with no persistent state
export class Session {
  private readonly env: Env;

  // Client for making internal requests to DOs
  private readonly internal: RequestManager;

  constructor(_state: DurableObjectState, env: Env) {
    this.env = env;

    this.internal = new RequestManager(env);
  }

  fetch(request: Request): Response {
    const upgradeHeader = request.headers.get("Upgrade");
    if (upgradeHeader !== "websocket") {
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
        .catch((error: Error) =>
          console.log(`Error while handling request: ${error.message}`)
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
    console.log(
      `[Session DO] Produce request: ${JSON.stringify(request, null, 2)}`
    );

    const response = await this.internal.produceRequest(metadata, request);

    console.log(
      `[Session DO] Produce response: ${JSON.stringify(
        response,
        // TODO: Make this take effect globally
        // Taken from https://github.com/GoogleChromeLabs/jsbi/issues/30#issuecomment-521460510
        (key, value) => (typeof value === "bigint" ? value.toString() : value),
        2
      )}`
    );

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
    console.log(
      `[Session DO] Metadata request: ${JSON.stringify(request, null, 2)}`
    );

    const response = await fetchClusterMetadata(this.env, request.topics);
    console.log(
      `[Session DO] Metadata response: ${JSON.stringify(response, null, 2)}`
    );

    return encodeMetadataResponse(encoder, response);
  }
}
