import { Env, stringify } from "src/common";
import { Acks, ErrorCode, Int64 } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { RequestMetadata, decodeRequestHeader } from "src/protocol/header";
import {
  InternalResponseEncoder,
  PartitionApiKey,
  validPartitionApiKey,
} from "src/protocol/internal/common";
import {
  InternalProduceResponse,
  decodeInternalProduceRequest,
  encodeInternalProduceResponse,
} from "src/protocol/internal/produce";
import { Chunk, prepareMessageSet } from "src/state/chunk";

export const partitionStubUrl = "https://partition.state";

interface OffsetInfo {
  nextOffset: Int64;
  currentChunkStart: Int64;
}
const offsetInfoKey = "offset-info";
const initialOffsetInfo: OffsetInfo = {
  nextOffset: BigInt(0),
  currentChunkStart: BigInt(-1),
};

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
            `[Partition DO] Error while handling request: ${error.message}`
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
    buffer: ArrayBuffer
  ): Promise<InternalProduceResponse> {
    const cursor =
      (await this.state.storage.get<OffsetInfo>(offsetInfoKey)) ??
      initialOffsetInfo;
    console.log(`[Partition DO] Cursor: ${stringify(cursor)}`);
    const baseOffset = cursor.nextOffset;

    const filler = prepareMessageSet(buffer, cursor.nextOffset);
    const currentChunk = await this.getCurrentChunk(cursor);
    console.log(`[Partition DO] Chunk: ${stringify(currentChunk)}`);

    for (
      let chunk = currentChunk;
      !filler.done();
      chunk = this.makeChunk(cursor.nextOffset)
    ) {
      cursor.nextOffset += BigInt(filler.fillChunk(chunk));
      cursor.currentChunkStart = chunk.offsetStart;
      await this.state.storage.put(offsetInfoKey, cursor);
      console.log(`[Partition DO] Updated cursor: ${stringify(cursor)}`);
      await this.state.storage.put(chunk.offsetStart.toString(), chunk);
      console.log(`[Partition DO] Updated chunk: ${stringify(chunk)}`);
    }

    return { errorCode: ErrorCode.None, baseOffset };
  }

  private async getCurrentChunk(cursor: OffsetInfo): Promise<Chunk> {
    if (cursor.currentChunkStart === BigInt(-1)) {
      return this.makeChunk(cursor.nextOffset);
    }
    // Chunk must exist, because offset and chunk are updated atomically
    return this.state.storage.get<Chunk>(
      cursor.currentChunkStart.toString()
    ) as Promise<Chunk>;
  }

  private makeChunk(offsetStart: Int64): Chunk {
    return {
      offsetStart,
      buffer: new ArrayBuffer(this.chunkSize),
      frames: [],
      nextIndex: 0,
    };
  }
}

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
