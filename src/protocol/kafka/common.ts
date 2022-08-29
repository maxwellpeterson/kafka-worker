import { Int32, int32Size } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

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
