import { Env, stringify } from "src/common";
import {
  Acks,
  ErrorCode,
  Int64,
  int32Size,
  int64Size,
} from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder, PartitionResponseEncoder } from "src/protocol/encoder";
import {
  PartitionApiKey,
  PartitionProduceResponse,
  PartitionRequestHeader,
  decodePartitionProduceRequest,
  decodePartitionRequestHeader,
  encodePartitionProduceResponse,
} from "src/protocol/internal/partition";

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

interface Chunk {
  offsetStart: Int64;
  buffer: ArrayBuffer;
  frames: MessageFrame[];
  nextIndex: number;
}
// Tuple of [startIndex, size]
type MessageFrame = [number, number];

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
    const header = decodePartitionRequestHeader(decoder);
    const encoder = new PartitionResponseEncoder(header.correlationId);

    switch (header.apiKey) {
      case PartitionApiKey.Produce:
        return this.handleProduceRequest(header, decoder, encoder);
    }
  }

  // TODO: Check acks
  private async handleProduceRequest(
    // TODO: make this metadata, omit correlation id and api key
    header: PartitionRequestHeader,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer | null> {
    const request = decodePartitionProduceRequest(decoder);

    const response = await this.appendMessageSet(request.messageSet);

    if (request.acks === Acks.None) {
      return null;
    }
    return encodePartitionProduceResponse(encoder, response);
  }

  private async appendMessageSet(
    buffer: ArrayBuffer
  ): Promise<PartitionProduceResponse> {
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
  makeChunk(offsetStart: Int64): Chunk {
    return {
      offsetStart,
      buffer: new ArrayBuffer(this.chunkSize),
      frames: [],
      nextIndex: 0,
    };
  }
}

const prepareMessageSet = (
  buffer: ArrayBuffer,
  initialOffset: Int64
): ChunkFiller => {
  const view = new DataView(buffer);
  const frames: MessageFrame[] = [];

  let nextOffset = initialOffset;
  let viewIndex = 0;
  while (viewIndex < view.buffer.byteLength) {
    const frameStart = viewIndex;

    // Set message offset field
    view.setBigInt64(viewIndex, nextOffset);
    viewIndex += int64Size;
    nextOffset++;

    // Read message size field
    const frameSize = view.getInt32(viewIndex);
    viewIndex += int32Size + frameSize;

    // Record starting index and size of message
    frames.push([frameStart, frameSize]);

    // TODO: CRC check!
  }

  return new ChunkFiller(buffer, frames);
};

class ChunkFiller {
  private buffer: ArrayBuffer;
  private frames: MessageFrame[];

  constructor(buffer: ArrayBuffer, frames: MessageFrame[]) {
    this.buffer = buffer;
    this.frames = frames;
  }

  // Returns the number of messages written to chunk
  fillChunk(chunk: Chunk): number {
    // Number of available bytes remaining in chunk
    const chunkSpace = chunk.buffer.byteLength - chunk.nextIndex;
    const stopIndex = this.frames.findIndex(
      ([start, size]) => start + size > chunkSpace
    );

    // Number of messages that will be copied into the chunk
    const frameCount = stopIndex === -1 ? this.frames.length : stopIndex;
    if (frameCount === 0) {
      return 0;
    }
    const [finalFrameStart, finalFrameSize] = this.frames[frameCount - 1];
    const copySize = finalFrameStart + finalFrameSize;

    // Copy message data into chunk, including framing information
    new Uint8Array(chunk.buffer).set(
      new Uint8Array(this.buffer.slice(0, copySize)),
      chunk.nextIndex
    );
    chunk.frames.push(
      ...this.frames
        .slice(0, frameCount)
        // Reindex message frames based on chunk buffer
        .map(([start, size]) => [start + chunk.nextIndex, size] as MessageFrame)
    );
    chunk.nextIndex += copySize;

    // Delete message data and framing information that was copied into chunk
    this.buffer = this.buffer.slice(copySize);
    this.frames = this.frames
      .slice(frameCount)
      // Reindex message frames based on trimmed buffer
      .map(([start, size]) => [start - copySize, size]);

    return frameCount;
  }

  done(): boolean {
    return this.frames.length === 0;
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
