import {
  Int16,
  Int32,
  int16Size,
  int32Size,
  String,
  KafkaArray,
  ErrorCode,
  validErrorCode,
  Int64,
  int64Size,
} from "src/protocol/common";

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

  readString(): String {
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

  // In several apis, there is no semantic difference between empty and null
  // arrays, and converting null arrays to empty arrays eliminates unnecessary
  // code pollution from null checking
  readArray<T>(readElement: (index: number) => T): Array<T> {
    return this.readKafkaArray(readElement) ?? [];
  }

  readMessageSet(size: Int32): ArrayBuffer {
    const buffer = this.view.buffer.slice(this.offset, this.offset + size);
    this.offset += size;
    return buffer;
  }
}

export class KafkaRequestDecoder extends Decoder {
  constructor(buffer: ArrayBuffer) {
    super(buffer);

    const expectedSize = this.readInt32();
    const actualSize = buffer.byteLength - int32Size;

    if (expectedSize !== actualSize) {
      throw new Error(
        `Message length does not match size header: expected ${expectedSize} but got ${actualSize}`
      );
    }
  }
}
