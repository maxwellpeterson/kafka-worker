import {
  Int16,
  Int32,
  Int64,
  KafkaArray,
  NullableString,
  int16Size,
  int32Size,
  int64Size,
} from "src/protocol/common";

// This implementation borrows heavily from the kafkajs Node library:
// https://github.com/tulios/kafkajs/blob/master/src/protocol/encoder.js

export class Encoder {
  protected view: DataView;
  protected offset: number;

  constructor(initialBufferSize = 64) {
    const buffer = new ArrayBuffer(initialBufferSize);
    this.view = new DataView(buffer);
    this.offset = 0;
  }

  private checkCapacity(size: number) {
    const nextLength = this.offset + size;
    if (nextLength > this.view.byteLength) {
      // If we run out of space, reallocate the backing array
      const newCapacity = 2 * nextLength;
      const newBuffer = new ArrayBuffer(newCapacity);
      new Uint8Array(newBuffer).set(new Uint8Array(this.view.buffer));
      this.view = new DataView(newBuffer);
    }
  }

  writeInt16(value: Int16): this {
    this.checkCapacity(int16Size);
    this.view.setInt16(this.offset, value);
    this.offset += int16Size;
    return this;
  }

  // Convenience method that makes sure "enum" values are encoded as Int16
  writeEnum<T extends Int16>(value: T): this {
    return this.writeInt16(value);
  }

  writeInt32(value: Int32): this {
    this.checkCapacity(int32Size);
    this.view.setInt32(this.offset, value);
    this.offset += int32Size;
    return this;
  }

  writeInt64(value: Int64): this {
    this.checkCapacity(int64Size);
    this.view.setBigInt64(this.offset, value);
    this.offset += int64Size;
    return this;
  }

  writeString(value: string): this {
    const bytes = new TextEncoder().encode(value);
    this.writeInt16(bytes.length);
    this.checkCapacity(bytes.length);
    new Uint8Array(this.view.buffer).set(bytes, this.offset);
    this.offset += bytes.length;
    return this;
  }

  writeNullableString(value: NullableString): this {
    if (value === null) {
      this.writeInt16(-1);
      return this;
    }
    return this.writeString(value);
  }

  writeArray<T>(values: KafkaArray<T>, writeElement: (value: T) => void): this {
    if (values === null) {
      this.writeInt32(-1);
      return this;
    }
    this.writeInt32(values.length);
    values.forEach(writeElement);
    return this;
  }

  writeInt32Array(values: Int32[]): this {
    return this.writeArray(values, (value) => this.writeInt32(value));
  }

  writeStringArray(values: KafkaArray<string>): this {
    return this.writeArray(values, (value) => this.writeString(value));
  }

  writeBuffer(buffer: ArrayBuffer): this {
    this.checkCapacity(buffer.byteLength);
    new Uint8Array(this.view.buffer).set(new Uint8Array(buffer), this.offset);
    this.offset += buffer.byteLength;
    return this;
  }

  buffer(): ArrayBuffer {
    if (this.view.buffer.byteLength === this.offset) {
      // Avoid an allocation/copy if the underlying buffer does not need to be
      // resized (probably because initialBufferSize was set optimally)
      return this.view.buffer;
    }
    return this.view.buffer.slice(0, this.offset);
  }
}
