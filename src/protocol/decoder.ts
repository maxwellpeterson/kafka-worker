import {
  ErrorCode,
  Int16,
  Int32,
  Int64,
  KafkaArray,
  int16Size,
  int32Size,
  int64Size,
  validErrorCode,
} from "src/protocol/common";

// This implementation borrows heavily from the kafkajs Node library:
// https://github.com/tulios/kafkajs/blob/master/src/protocol/decoder.js

export class Decoder {
  private readonly view: DataView;
  private offset: number;

  constructor(buffer: ArrayBuffer) {
    this.view = new DataView(buffer);
    this.offset = 0;
  }

  readInt16(): Int16 {
    const value = this.view.getInt16(this.offset);
    this.offset += int16Size;
    return value;
  }

  readEnum<T extends Int16>(predicate: (value: Int16) => value is T): T {
    const value = this.readInt16();
    if (!predicate(value)) {
      throw new Error(`Invalid enum value: ${value}`);
    }
    return value;
  }

  readErrorCode(): ErrorCode {
    return this.readEnum(validErrorCode);
  }

  readInt32(): Int32 {
    const value = this.view.getInt32(this.offset);
    this.offset += int32Size;
    return value;
  }

  readInt64(): Int64 {
    const value = this.view.getBigInt64(this.offset);
    this.offset += int64Size;
    return value;
  }

  readString(): string {
    const size = this.readInt16();
    if (size === -1) {
      // I don't think this should ever happen...
      throw new Error("Unexpected null string!");
    }
    const stringBuffer = this.view.buffer.slice(
      this.offset,
      this.offset + size
    );
    const value = new TextDecoder().decode(stringBuffer);
    this.offset += size;
    return value;
  }

  readKafkaArray<T>(readElement: (index: number) => T): KafkaArray<T> {
    const size = this.readInt32();
    if (size === -1) {
      return null;
    }
    const values = new Array<T>(size);
    for (let i = 0; i < size; i++) {
      values[i] = readElement(i);
    }
    return values;
  }

  // Convenience method for converting null array to empty array in cases where
  // there is no semantic difference between them
  readArray<T>(readElement: (index: number) => T): T[] {
    return this.readKafkaArray(readElement) ?? [];
  }

  readInt32Array(): Int32[] {
    return this.readArray(() => this.readInt32());
  }

  readStringArray(): string[] {
    return this.readArray(() => this.readString());
  }

  readBuffer(size: Int32): ArrayBuffer {
    const buffer = this.view.buffer.slice(this.offset, this.offset + size);
    this.offset += size;
    return buffer;
  }
}
