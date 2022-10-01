// eslint-disable-next-line import/default
import crc32 from "crc-32";
import {
  ErrorCode,
  Int64,
  MessageSet,
  int32Size,
  int64Size,
  int8Size,
} from "src/protocol/common";

export interface Chunk {
  // The offset of the first message in the chunk
  offsetStart: Int64;
  buffer: ArrayBuffer;
  // Framing information about messages stored in the buffer
  frames: MessageFrame[];
  // The next available index in the buffer (the next message should be written
  // to the buffer starting at this index)
  nextIndex: number;
}
// TODO: Change to interface/object type for better readability
// export interface MessageFrame {
//   startIndex: number;
//   size: number
// }
// Tuple of [startIndex, size]
export type MessageFrame = [number, number];

// Performs required preprocessing and validation on a message set from a
// Produce request before it can be written to chunks
export const prepareMessageSet = (
  messageSet: MessageSet,
  initialOffset: Int64
): { filler: ChunkFiller } | { error: ErrorCode } => {
  // TODO: What is going on here? Node-specific weirdness? Instantiating this
  // with `new DataView(messageSet)` type-checks, but crashes under test with
  // the error "First argument to DataView constructor must be an ArrayBuffer"
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
    if (messageSize < 0) {
      return { error: ErrorCode.InvalidMessageSize };
    }
    viewIndex += int32Size;

    // Verify crc of remainder of message
    const crcExpected = view.getInt32(viewIndex);
    // eslint-disable-next-line import/no-named-as-default-member
    const crcActual = crc32.buf(
      new Uint8Array(
        view.buffer,
        view.byteOffset + viewIndex + int32Size,
        messageSize - int32Size
      )
    );
    if (crcActual !== crcExpected) {
      return { error: ErrorCode.CorruptMessage };
    }

    // If the magic byte is one, there is an additional attributes byte right
    // after the magic byte, and if the magic byte is zero then there is no
    // attributes byte
    // https://kafka.apache.org/08/documentation/#messageformat
    const magicByte = view.getInt8(viewIndex + int32Size);
    if (magicByte === 1) {
      // Check attributes byte to make sure that message set is not compressed,
      // since we don't support compression
      const attributes = view.getInt8(viewIndex + int32Size + int8Size);
      // If message set is compressed, attributes byte will be nonzero
      if (attributes !== 0) {
        console.log(`Attributes check failed, attributes: ${attributes}`);
        // There isn't a good error code for this that dates back to 0.8.0
        return { error: ErrorCode.UnknownServerError };
      }
    } else if (magicByte !== 0) {
      // The magic byte can only be 0 or 1 (for this version of Kafka)
      console.log(`Magic byte check failed, magic byte: ${magicByte}`);
      // There isn't a good error code for this that dates back to 0.8.0
      return { error: ErrorCode.UnknownServerError };
    }

    // Save framing information about message
    frames.push([frameStart, int64Size + int32Size + messageSize]);
    viewIndex += messageSize;
  }

  return { filler: new ChunkFiller(messageSet, frames) };
};

// Stores a preprocessed message set that can be written incrementally to
// multiple chunks
export class ChunkFiller {
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

    // Copy message data into chunk
    new Uint8Array(chunk.buffer).set(
      this.messageSet.subarray(0, copySize),
      chunk.nextIndex
    );
    // Copy message framing information into chunk
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
      // Reindex message frames after copied data is discarded
      .map(([start, size]) => [start - copySize, size]);

    return frameCount;
  }

  // Returns true if there are no more messages that need be written
  done(): boolean {
    return this.frames.length === 0;
  }
}
