import { Env, stringify } from "src/common";
import { Acks, ErrorCode, Int64, MessageSet } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { RequestMetadata, decodeRequestHeader } from "src/protocol/header";
import {
  InternalResponseEncoder,
  PartitionApiKey,
  validPartitionApiKey,
} from "src/protocol/internal/common";
import {
  InternalListOffsetsRequest,
  InternalListOffsetsResponse,
  decodeInternalListOffsetsRequest,
  encodeInternalListOffsetsResponse,
} from "src/protocol/internal/list-offsets";
import {
  InternalProduceResponse,
  decodeInternalProduceRequest,
  encodeInternalProduceResponse,
} from "src/protocol/internal/produce";
import { Chunk, prepareMessageSet } from "src/state/chunk";

export const partitionStubUrl = "https://partition.state";

interface OffsetInfo {
  nextOffset: Int64;
  currentChunk: string | null;
}
const offsetInfoKey = "offset-info";
const initialOffsetInfo = (): OffsetInfo => ({
  nextOffset: BigInt(0),
  currentChunk: null,
});

export class Partition {
  private readonly state: DurableObjectState;
  private readonly env: Env;

  private readonly chunkSize: number;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;

    this.chunkSize = parseInt(env.PARTITION_CHUNK_SIZE);
  }

  fetch(request: Request): Response {
    if (request.headers.get("Upgrade") !== "websocket") {
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
          console.log(
            `[Partition DO] Error while handling request: ${
              error.stack ?? "no stack"
            }`
          )
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
    const header = decodeRequestHeader(decoder, validPartitionApiKey);
    const encoder = new InternalResponseEncoder(header.correlationId);

    switch (header.apiKey) {
      case PartitionApiKey.Produce:
        return this.handleProduceRequest(header, decoder, encoder);
      case PartitionApiKey.ListOffsets:
        return this.handleListOffsetsRequest(header, decoder, encoder);
    }
  }

  private async handleProduceRequest(
    metadata: RequestMetadata,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer | null> {
    const request = decodeInternalProduceRequest(decoder);
    const response = await this.appendMessageSet(request.messageSet);

    if (request.acks === Acks.None) {
      return null;
    }
    return encodeInternalProduceResponse(encoder, response);
  }

  private async appendMessageSet(
    messageSet: MessageSet
  ): Promise<InternalProduceResponse> {
    const cursor = await this.getCursor();
    console.log(`[Partition DO] Cursor: ${stringify(cursor)}`);
    const baseOffset = cursor.nextOffset;

    const filler = prepareMessageSet(messageSet, cursor.nextOffset);
    const currentChunk = await this.getCurrentChunk(cursor);
    console.log(`[Partition DO] Chunk: ${stringify(currentChunk)}`);

    for (
      let chunk = currentChunk;
      !filler.done();
      chunk = this.makeChunk(cursor.nextOffset)
    ) {
      cursor.nextOffset += BigInt(filler.fillChunk(chunk));
      cursor.currentChunk = chunkKey(chunk);
      await this.state.storage.put(offsetInfoKey, cursor);
      console.log(`[Partition DO] Updated cursor: ${stringify(cursor)}`);
      await this.state.storage.put(chunkKey(chunk), chunk);
      console.log(`[Partition DO] Updated chunk: ${stringify(chunk)}`);
    }

    return { errorCode: ErrorCode.None, baseOffset };
  }

  private async getCursor(): Promise<OffsetInfo> {
    return (
      (await this.state.storage.get<OffsetInfo>(offsetInfoKey)) ??
      initialOffsetInfo()
    );
  }

  private async getCurrentChunk(cursor: OffsetInfo): Promise<Chunk> {
    if (!cursor.currentChunk) {
      return this.makeChunk(cursor.nextOffset);
    }
    // Chunk must exist, because offset and chunk are updated atomically
    return this.state.storage.get<Chunk>(cursor.currentChunk) as Promise<Chunk>;
  }

  private makeChunk(offsetStart: Int64): Chunk {
    return {
      offsetStart,
      buffer: new ArrayBuffer(this.chunkSize),
      frames: [],
      nextIndex: 0,
    };
  }

  private async handleListOffsetsRequest(
    metadata: RequestMetadata,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer> {
    const request = decodeInternalListOffsetsRequest(decoder);
    const response = await this.listOffsets(request);
    return encodeInternalListOffsetsResponse(encoder, response);
  }

  private async listOffsets(
    request: InternalListOffsetsRequest
  ): Promise<InternalListOffsetsResponse> {
    if (request.timestamp === BigInt(-2)) {
      // Only send earliest available offset
      return {
        errorCode: ErrorCode.None,
        oldStyleOffsets: [initialOffsetInfo().nextOffset],
      };
    }

    const cursor = await this.getCursor();

    // Send requested number of offsets
    const stopOffset = cursor.nextOffset - BigInt(request.maxNumOffsets);
    const clampedStopOffset = stopOffset < 0 ? -1 : stopOffset;
    const offsets: Int64[] = [];
    for (let i = cursor.nextOffset; i > clampedStopOffset; i--) {
      // TODO: More efficient approach here?
      offsets.push(i);
    }
    return {
      errorCode: ErrorCode.None,
      oldStyleOffsets: offsets,
    };
  }
}

const chunkKey = (chunk: Chunk): string =>
  `chunk-${chunk.offsetStart.toString()}`;

export class PartitionInfo {
  readonly topic: string;
  readonly index: number;
  readonly id: string;

  constructor(topic: string, index: number) {
    this.topic = topic;
    this.index = index;
    this.id = `${topic}-${index}`;
  }
}
