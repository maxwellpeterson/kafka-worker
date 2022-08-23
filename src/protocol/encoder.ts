import {
  Int16,
  Int32,
  int16Size,
  int32Size,
  KafkaString,
  KafkaArray,
  ErrorCode,
  Int64,
  int64Size,
} from "src/protocol/common";

const initialBufferSize = 64;

export class Encoder {
  private view: DataView;
  private offset: number;

  constructor() {
    const buffer = new ArrayBuffer(initialBufferSize);
    this.view = new DataView(buffer);
    this.offset = int32Size;
  }

  private checkCapacity(size: number) {
    const nextLength = this.offset + size;
    if (nextLength > this.view.byteLength) {
      const newCapacity = 2 * nextLength;
      const newBuffer = new ArrayBuffer(newCapacity);
      // Reserved header space is preserved after copy
      new Uint8Array(newBuffer).set(new Uint8Array(this.view.buffer));
      this.view = new DataView(newBuffer);
    }
  }

  writeInt16(value: Int16) {
    this.checkCapacity(int16Size);
    this.view.setInt16(this.offset, value);
    this.offset += int16Size;
  }

  writeErrorCode(errorCode: ErrorCode) {
    this.writeInt16(errorCode);
  }

  writeInt32(value: Int32) {
    this.checkCapacity(int32Size);
    this.view.setInt32(this.offset, value);
    this.offset += int32Size;
  }

  writeInt64(value: bigint) {
    this.checkCapacity(int64Size);
    this.view.setBigInt64(this.offset, value);
    this.offset += int64Size;
  }

  writeString(value: KafkaString) {
    if (value === null) {
      this.writeInt16(-1);
      return;
    }
    const bytes = new TextEncoder().encode(value);
    this.writeInt16(bytes.length);
    this.checkCapacity(bytes.length);
    new Uint8Array(this.view.buffer).set(bytes, this.offset);
    this.offset += bytes.length;
  }

  writeArray<T>(values: KafkaArray<T>, writeElement: (value: T) => void) {
    if (values === null) {
      this.writeInt32(-1);
      return;
    }
    this.writeInt32(values.length);
    values.forEach(writeElement);
  }

  sizedBuffer(): ArrayBuffer {
    // Write size header to reserved space at the front of the array
    this.view.setInt32(0, this.offset - int32Size);
    return this.view.buffer.slice(0, this.offset);
  }
}
