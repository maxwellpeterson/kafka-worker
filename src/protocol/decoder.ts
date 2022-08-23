import {
  Int16,
  Int32,
  int16Size,
  int32Size,
  KafkaString,
  KafkaArray,
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

  readInt32(): Int32 {
    const value = this.view.getInt32(this.offset);
    this.offset += int32Size;
    return value;
  }

  readString(): KafkaString {
    const size = this.readInt16();
    if (size === -1) {
      return null;
    }
    const stringBuffer = this.view.buffer.slice(
      this.offset,
      this.offset + size
    );
    const value = new TextDecoder().decode(stringBuffer);
    this.offset += size;
    return value;
  }

  readArray<T>(readElement: (index: number) => T): KafkaArray<T> {
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

  readMessageSet(size: Int32): ArrayBuffer {
    const buffer = this.view.buffer.slice(this.offset, this.offset + size);
    this.offset += size;
    return buffer;
  }
}
