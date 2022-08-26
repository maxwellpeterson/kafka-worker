import {
  Int16,
  Int32,
  Int64,
  KafkaArray,
  int16Size,
  int32Size,
  int64Size,
} from "src/protocol/common";

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
      const newCapacity = 2 * nextLength;
      const newBuffer = new ArrayBuffer(newCapacity);
      new Uint8Array(newBuffer).set(new Uint8Array(this.view.buffer));
      this.view = new DataView(newBuffer);
    }
  }

  writeInt16(value: Int16) {
    this.checkCapacity(int16Size);
    this.view.setInt16(this.offset, value);
    this.offset += int16Size;
  }

  writeEnum<T extends Int16>(value: T) {
    this.writeInt16(value);
  }

  writeInt32(value: Int32) {
    this.checkCapacity(int32Size);
    this.view.setInt32(this.offset, value);
    this.offset += int32Size;
  }

  writeInt64(value: Int64) {
    this.checkCapacity(int64Size);
    this.view.setBigInt64(this.offset, value);
    this.offset += int64Size;
  }

  writeString(value: string) {
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

  writeMessageSet(buffer: ArrayBuffer) {
    this.checkCapacity(buffer.byteLength);
    new Uint8Array(this.view.buffer).set(new Uint8Array(buffer), this.offset);
    this.offset += buffer.byteLength;
  }

  buffer(): ArrayBuffer {
    return this.view.buffer.slice(0, this.offset);
  }
}

export class KafkaResponseEncoder extends Encoder {
  constructor(correlationId: Int32, initialBufferSize = 64) {
    super(initialBufferSize + 2 * int32Size);
    // Reserve space for size header at front of buffer
    this.offset += int32Size;
    this.writeInt32(correlationId);
  }

  // Overrides parent method, adds size header to buffer
  buffer(): ArrayBuffer {
    // Write size header to reserved space at front of buffer
    this.view.setInt32(0, this.offset - int32Size);
    return super.buffer();
  }
}

export class PartitionResponseEncoder extends Encoder {
  constructor(correlationId: Int32, initialBufferSize = 64) {
    super(initialBufferSize + int32Size);
    this.writeInt32(correlationId);
  }
}
