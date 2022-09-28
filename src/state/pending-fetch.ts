import { ErrorCode, Int64, MessageSet } from "src/protocol/common";
import {
  InternalFetchRequest,
  InternalFetchResponse,
} from "src/protocol/internal/fetch";
import { Chunk } from "src/state/chunk";

export class PendingFetch {
  private readonly request: InternalFetchRequest;

  private highWatermark: Int64;
  private readonly messageSet: MessageSet;
  private nextOffset: Int64;
  private bytesWritten = 0;

  private readonly done: () => void;
  readonly abort: () => void;

  constructor(
    request: InternalFetchRequest,
    highWatermark: Int64,
    done: (response: InternalFetchResponse) => void,
    abort: () => void
  ) {
    this.request = request;

    this.highWatermark = highWatermark;
    this.messageSet = new Uint8Array(new ArrayBuffer(request.maxBytes));
    this.nextOffset = request.fetchOffset;

    const timeoutId = setTimeout(() => {
      this.done();
    }, request.maxWaitMs);

    this.done = () => {
      clearTimeout(timeoutId);
      done({
        errorCode: ErrorCode.None,
        highWatermark: this.highWatermark,
        messageSet: this.messageSet.subarray(0, this.bytesWritten),
      });
    };
    this.abort = () => {
      clearTimeout(timeoutId);
      abort();
    };
  }

  addChunks(highWatermark: Int64, chunks: Iterable<Chunk>) {
    this.highWatermark = highWatermark;

    for (const chunk of chunks) {
      const messageSetFull = this.readChunk(chunk);
      // This covers a weird case where individual messages are larger than the
      // difference between the upper and lower response byte limits, the
      // response is below the lower limit, and another message can't be read
      // without exceeding the upper limit. Note that the protocol allows for
      // partial messages to be returned (which would cover this case), but as
      // implemented we only return full messages.
      if (messageSetFull || this.bytesWritten >= this.request.minBytes) {
        this.done();
      }
    }
  }

  // Copies messages from the chunk into the response message set buffer, and
  // returns true if no more messages can be copied into the message set buffer
  private readChunk(chunk: Chunk): boolean {
    if (
      chunk.frames.length === 0 ||
      this.nextOffset < chunk.offsetStart ||
      this.nextOffset >= chunk.offsetStart + BigInt(chunk.frames.length)
    ) {
      // This chunk doesn't contain the data we're looking for
      return false;
    }

    // The offset we want to read from might be in the middle of the chunk,
    // so we need to skip over the messages with smaller offsets
    const frameOffset = Math.max(
      0,
      Number(this.nextOffset - chunk.offsetStart)
    );
    // The buffer index that we want to start reading message data from
    const bufferOffset = chunk.frames[frameOffset][0];

    const frames = chunk.frames.slice(frameOffset);
    // Find the index of the message that would put us over the response byte
    // limit. We want to read all messages up to but not including this message.
    const stopFrameIndex = frames.findIndex(([startIndex, size]) => {
      const writeSize = startIndex - bufferOffset + size;
      return this.bytesWritten + writeSize > this.request.maxBytes;
    });

    if (stopFrameIndex !== -1) {
      // We can't read the remainder of the chunk without exceeding the byte
      // limit, so instead we only read part of the chunk. This read fills up
      // the remainder of the buffer, and is the last read that we can do.
      const copySize = frames[stopFrameIndex][0] - bufferOffset;
      this.messageSet.set(
        new Uint8Array(chunk.buffer, bufferOffset, copySize),
        this.bytesWritten
      );
      this.bytesWritten += copySize;
      this.nextOffset += BigInt(stopFrameIndex);
      return true;
    }

    // We can read the remainder of the chunk without exceeding the byte limit
    const copySize = chunk.nextIndex - bufferOffset;
    this.messageSet.set(
      new Uint8Array(chunk.buffer, bufferOffset, copySize),
      this.bytesWritten
    );
    this.bytesWritten += copySize;
    this.nextOffset += BigInt(frames.length);
    return false;
  }
}
