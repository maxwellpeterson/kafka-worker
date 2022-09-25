import { Int64, MessageSet, int32Size, int64Size } from "src/protocol/common";

export interface Chunk {
  offsetStart: Int64;
  buffer: ArrayBuffer;
  frames: MessageFrame[];
  nextIndex: number;
}
// Tuple of [startIndex, size]
export type MessageFrame = [number, number];

// TODO: This should be able to return an error code
export const prepareMessageSet = (
  messageSet: MessageSet,
  initialOffset: Int64
): ChunkFiller => {
  // TODO: What is going on here? Node-specific weirdness?
  const view = new DataView(
    messageSet.buffer,
    messageSet.byteOffset,
    messageSet.byteLength
  );
  const frames: MessageFrame[] = [];

  let nextOffset = initialOffset;
  let viewIndex = 0;
  while (viewIndex < view.byteLength) {
    const frameStart = viewIndex;

    // Set message offset field
    view.setBigInt64(viewIndex, nextOffset);
    viewIndex += int64Size;
    nextOffset++;

    // Read message size field
    const messageSize = view.getInt32(viewIndex);
    viewIndex += int32Size + messageSize;

    // Record starting index and size of message
    frames.push([frameStart, int64Size + int32Size + messageSize]);

    // TODO: CRC check!
  }

  return new ChunkFiller(messageSet, frames);
};

class ChunkFiller {
  private messageSet: MessageSet;
  private frames: MessageFrame[];

  constructor(messageSet: MessageSet, frames: MessageFrame[]) {
    this.messageSet = messageSet;
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
      this.messageSet.subarray(0, copySize),
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
    this.messageSet = this.messageSet.subarray(copySize);
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
