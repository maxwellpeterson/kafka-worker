import { AbortedRequestError, Env, stringify } from "src/common";
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
  InternalFetchRequest,
  InternalFetchResponse,
  decodeInternalFetchRequest,
  encodeInternalFetchResponse,
  stubInternalFetchResponse,
} from "src/protocol/internal/fetch";
import {
  InternalListOffsetsRequest,
  InternalListOffsetsResponse,
  decodeInternalListOffsetsRequest,
  encodeInternalListOffsetsResponse,
  stubInternalListOffsetsResponse,
} from "src/protocol/internal/list-offsets";
import {
  InternalProduceResponse,
  decodeInternalProduceRequest,
  encodeInternalProduceResponse,
  stubInternalProduceResponse,
} from "src/protocol/internal/produce";
import { Chunk, prepareMessageSet } from "src/state/chunk";
import { PendingFetch } from "src/state/pending-fetch";

export const partitionStubUrl = "https://partition.state";

interface OffsetInfo {
  nextOffset: Int64;
  chunkOffsets: Int64[];
}
const offsetInfoKey = "offset-info";
const initialOffsetInfo = (): OffsetInfo => ({
  nextOffset: BigInt(0),
  chunkOffsets: [],
});

type ChunkId = string;
const chunkIdPrefix = "chunk";

type RequestId = string;
interface PartitionRequestMetadata extends RequestMetadata {
  requestId: RequestId;
}

export class Partition {
  private readonly state: DurableObjectState;
  private readonly chunkSize: number;

  private readonly pending = new Map<RequestId, PendingFetch>();
  private connCount = 0;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;

    this.chunkSize = parseInt(env.PARTITION_CHUNK_SIZE);
  }

  fetch(request: Request): Response {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    const requestPrefix = this.connCount;
    this.connCount++;

    const requestIdDelim = "_";
    let requestCount = 0;

    server.accept();
    server.addEventListener("message", (event) => {
      if (typeof event.data === "string") {
        console.log("Received string data, but we want binary data!");
        return;
      }

      // Every internal request sent to this DO is uniquely identified (within
      // each in-memory lifetime) by the tuple (connCount, requestCount), where
      // connCount identifies the socket connection that the request belongs to,
      // and requestCount identifies the request within the scope of its socket
      // connection
      const requestId = `${requestPrefix}${requestIdDelim}${requestCount}`;
      requestCount++;

      this.handleRequest(requestId, event.data)
        .then((response) => {
          if (
            response !== null &&
            // TODO: Needed? Added because of issues related to:
            // https://github.com/cloudflare/miniflare/issues/339
            server.readyState === WebSocket.READY_STATE_OPEN
          ) {
            server.send(response);
          }
        })
        .catch((error) =>
          console.log(
            `[Partition DO] Uncaught error while handling request: ${stringify(
              error
            )}`
          )
        );
    });
    server.addEventListener("close", () => {
      this.pending.forEach((request, id) => {
        // eslint-disable-next-line @typescript-eslint/prefer-string-starts-ends-with
        if (id.slice(0, id.indexOf(requestIdDelim)) === `${requestPrefix}`) {
          // Abort pending requests tied to this connection
          request.abort();
        }
      });
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  private async handleRequest(
    requestId: RequestId,
    buffer: ArrayBuffer
  ): Promise<ArrayBuffer | null> {
    const decoder = new Decoder(buffer);
    const header = decodeRequestHeader(decoder, validPartitionApiKey);
    const encoder = new InternalResponseEncoder(header.correlationId);

    const metadata = { ...header, requestId };

    switch (header.apiKey) {
      case PartitionApiKey.Produce:
        return this.handleProduceRequest(metadata, decoder, encoder);
      case PartitionApiKey.Fetch:
        return this.handleFetchRequest(metadata, decoder, encoder);
      case PartitionApiKey.ListOffsets:
        return this.handleListOffsetsRequest(metadata, decoder, encoder);
    }
  }

  private async handleProduceRequest(
    metadata: PartitionRequestMetadata,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer | null> {
    try {
      const request = decodeInternalProduceRequest(decoder);
      const response = await this.appendMessageSet(request.messageSet);

      if (request.acks === Acks.None) {
        return null;
      }
      return encodeInternalProduceResponse(encoder, response);
    } catch (e) {
      console.log(
        `[Partition DO] Error while handling Produce request: ${stringify(e)}`
      );
      return encodeInternalProduceResponse(
        encoder,
        stubInternalProduceResponse(ErrorCode.UnknownServerError)
      );
    }
  }

  private async appendMessageSet(
    messageSet: MessageSet
  ): Promise<InternalProduceResponse> {
    const cursor = await this.getCursor();
    const baseOffset = cursor.nextOffset;

    const prepared = prepareMessageSet(messageSet, cursor.nextOffset);
    if ("error" in prepared) {
      return {
        errorCode: prepared.error,
        baseOffset: BigInt(0),
      };
    }
    const currentChunk = await this.getCurrentChunk(cursor);

    const chunks: Record<string, Chunk> = {};

    for (
      let chunk = currentChunk;
      !prepared.filler.done();
      chunk = this.nextChunk(cursor)
    ) {
      chunks[chunkId(chunk.offsetStart)] = chunk;
      cursor.nextOffset += BigInt(prepared.filler.fillChunk(chunk));
      // Add freshly created chunks to chunk list (chunks that existed before
      // this request will already have been added to the chunk list)
      if (cursor.chunkOffsets.at(-1) !== chunk.offsetStart) {
        cursor.chunkOffsets.push(chunk.offsetStart);
      }
    }

    await this.state.storage.put<Chunk | OffsetInfo>({
      ...chunks,
      [offsetInfoKey]: cursor,
    });
    this.pending.forEach((pending) =>
      pending.addChunks(cursor.nextOffset, Object.values(chunks))
    );

    return { errorCode: ErrorCode.None, baseOffset };
  }

  private async getCursor(): Promise<OffsetInfo> {
    return (
      (await this.state.storage.get<OffsetInfo>(offsetInfoKey)) ??
      initialOffsetInfo()
    );
  }

  private async getCurrentChunk(cursor: OffsetInfo): Promise<Chunk> {
    const currentChunkStart = cursor.chunkOffsets.at(-1);
    if (currentChunkStart === undefined) {
      return this.nextChunk(cursor);
    }
    // Chunk must exist, because offset and chunk are updated together
    return this.state.storage.get<Chunk>(
      chunkId(currentChunkStart)
    ) as Promise<Chunk>;
  }

  private nextChunk(cursor: OffsetInfo): Chunk {
    return {
      offsetStart: cursor.nextOffset,
      buffer: new ArrayBuffer(this.chunkSize),
      frames: [],
      nextIndex: 0,
    };
  }

  private async handleFetchRequest(
    metadata: PartitionRequestMetadata,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer | null> {
    try {
      const request = decodeInternalFetchRequest(decoder);
      const response = await this.fillMessageSet(metadata, request);
      return encodeInternalFetchResponse(encoder, response);
    } catch (e) {
      if (e instanceof AbortedRequestError) {
        return null;
      }
      console.log(
        `[Partition DO] Error while handling Fetch request: ${stringify(e)}`
      );
      return encodeInternalFetchResponse(
        encoder,
        stubInternalFetchResponse(ErrorCode.UnknownServerError)
      );
    }
  }

  private async fillMessageSet(
    metadata: PartitionRequestMetadata,
    request: InternalFetchRequest
  ): Promise<InternalFetchResponse> {
    const cursor = await this.getCursor();
    if (request.fetchOffset < 0 || request.fetchOffset > cursor.nextOffset) {
      return {
        errorCode: ErrorCode.OffsetOutOfRange,
        highWatermark: cursor.nextOffset,
        messageSet: new Uint8Array(),
      };
    }

    // The index of the chunk one position to the right of the chunk that the
    // fetch request should start from (could be a binary search)
    const startChunkRight = cursor.chunkOffsets.findIndex(
      (chunkOffset) => request.fetchOffset < chunkOffset
    );
    // The index of the chunk that the fetch request should start from
    const startChunk =
      startChunkRight === -1
        ? // Start from the most recent chunk
          cursor.chunkOffsets.length - 1
        : startChunkRight - 1;
    const maxChunks = Math.ceil(request.maxBytes / this.chunkSize) + 1;

    // Load the subset of chunks we need to read from storage
    const chunks = await this.state.storage.get<Chunk>(
      cursor.chunkOffsets
        .slice(startChunk, startChunk + maxChunks)
        .map((chunkOffset) => chunkId(chunkOffset))
    );

    return new Promise<InternalFetchResponse>((resolve, reject) => {
      const done = (response: InternalFetchResponse) => {
        this.pending.delete(metadata.requestId);
        resolve(response);
      };
      const abort = () => {
        this.pending.delete(metadata.requestId);
        reject(new AbortedRequestError());
      };
      const pending = new PendingFetch(request, cursor.nextOffset, done, abort);
      this.pending.set(metadata.requestId, pending);
      pending.addChunks(cursor.nextOffset, chunks.values());
    });
  }

  private async handleListOffsetsRequest(
    metadata: PartitionRequestMetadata,
    decoder: Decoder,
    encoder: Encoder
  ): Promise<ArrayBuffer> {
    try {
      const request = decodeInternalListOffsetsRequest(decoder);
      const response = await this.listOffsets(request);
      return encodeInternalListOffsetsResponse(encoder, response);
    } catch (e) {
      console.log(
        `[Partition DO] Error while handling ListOffsets request: ${stringify(
          e
        )}`
      );
      return encodeInternalListOffsetsResponse(
        encoder,
        stubInternalListOffsetsResponse(ErrorCode.UnknownServerError)
      );
    }
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

const chunkId = (offsetStart: Chunk["offsetStart"]): ChunkId =>
  `${chunkIdPrefix}-${offsetStart.toString()}`;

export class PartitionInfo {
  private static readonly delim = "_";
  readonly topic: string;
  readonly index: number;
  readonly id: string;

  constructor(topic: string, index: number) {
    this.topic = topic;
    this.index = index;
    this.id = `${topic}${PartitionInfo.delim}${index}`;
  }

  static fromId(id: string): PartitionInfo {
    const delimIndex = id.lastIndexOf(this.delim);
    return new PartitionInfo(
      id.slice(0, delimIndex),
      parseInt(id.slice(delimIndex + 1))
    );
  }
}
