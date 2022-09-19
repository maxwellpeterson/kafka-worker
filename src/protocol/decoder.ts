import {
  Acks,
  ErrorCode,
  Int16,
  Int32,
  Int64,
  KafkaArray,
  MessageSet,
  NullableString,
  int16Size,
  int32Size,
  int64Size,
  validAcks,
  validErrorCode,
} from "src/protocol/common";

// This implementation borrows heavily from the kafkajs Node library:
// https://github.com/tulios/kafkajs/blob/master/src/protocol/decoder.js

export type EnumPredicate<T extends Int16> = (value: Int16) => value is T;

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

  readEnum<T extends Int16>(predicate: EnumPredicate<T>): T {
    const value = this.readInt16();
    if (!predicate(value)) {
      throw new Error(`Invalid enum value: ${value}`);
    }
    return value;
  }

  readErrorCode(): ErrorCode {
    return this.readEnum(validErrorCode);
  }

  readAcks(): Acks {
    return this.readEnum(validAcks);
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

  private readStringSize(size: number): string {
    const stringBuffer = this.view.buffer.slice(
      this.offset,
      this.offset + size
    );
    const value = new TextDecoder().decode(stringBuffer);
    this.offset += size;
    return value;
  }

  readString(): string {
    const size = this.readInt16();
    if (size === -1) {
      throw new Error("Unexpected null string!");
    }
    return this.readStringSize(size);
  }

  readNullableString(): NullableString {
    const size = this.readInt16();
    if (size === -1) {
      return null;
    }
    return this.readStringSize(size);
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

  readInt64Array(): Int64[] {
    return this.readArray(() => this.readInt64());
  }

  readStringArray(): string[] {
    return this.readArray(() => this.readString());
  }

  private readSlice(size: Int32): Uint8Array {
    const slice = new Uint8Array(this.view.buffer, this.offset, size);
    this.offset += size;
    return slice;
  }

  readMessageSet(): MessageSet {
    const size = this.readInt32();
    return this.readSlice(size);
  }
}
