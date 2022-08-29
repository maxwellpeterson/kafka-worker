import { Int64, int32Size, int64Size } from "src/protocol/common";

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

// TODO: Might want to wrap the ArrayBuffer in a Uint8Array and use subarray()
// instead of slice() to avoid a bunch of copies/allocations?
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
