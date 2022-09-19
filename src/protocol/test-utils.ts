// eslint-disable-next-line import/default
import crc32 from "crc-32";
import { MessageSet, int32Size, int8Size } from "src/protocol/common";
import { Encoder } from "src/protocol/encoder";

// These functions are only intended to be used in test files!

// Convert an ArrayBuffer to a string that can be snapshotted
// Taken from https://stackoverflow.com/a/9458996
export const base64 = (buffer: ArrayBuffer): string => {
  let binary = "";
  const len = buffer.byteLength;
  const bytes = new Uint8Array(buffer);
  for (let i = 0; i < len; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
};

// Generate an arbitrary message set for testing
export const fillMessageSet = (
  numMessages: number,
  keySizes = [5, 17, 76, 32, 57],
  valueSizes = [48, 128, 155, 96, 67]
): MessageSet => {
  const encoder = new Encoder();
  for (let i = 0; i < numMessages; i++) {
    const keySize = keySizes[i % keySizes.length];
    const valueSize = valueSizes[i % valueSizes.length];

    // Write message offset (discarded by broker)
    encoder.writeInt64(BigInt(i));
    // Write message size (crc + magic byte + attributes + (size +) key + (size +) value)
    encoder.writeInt32(
      int32Size +
        int8Size +
        int8Size +
        int32Size +
        keySize +
        int32Size +
        valueSize
    );

    const remainder = new Encoder();
    // Write magic byte (with magic value of 1)
    remainder.writeInt8(1);
    // Write attributes (no compression)
    remainder.writeInt8(0);
    // Write key and value
    remainder.writeBytes(fillBytes(keySize));
    remainder.writeBytes(fillBytes(valueSize));

    // Calculate and write crc
    const crcInput = remainder.slice();
    // eslint-disable-next-line import/no-named-as-default-member
    encoder.writeInt32(crc32.buf(crcInput));
    // Write remainder of message
    encoder.writeSlice(crcInput);
  }
  return encoder.slice();
};

const fillBytes = (length: number): Uint8Array => {
  const view = new DataView(new ArrayBuffer(length));
  for (let i = 0; i < length; i++) {
    // This will wrap around when i > 127 but that's fine
    view.setInt8(i, i);
  }
  return new Uint8Array(view.buffer);
};
