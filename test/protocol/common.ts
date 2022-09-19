import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { base64 } from "test/common";

type EncodeFunc<T> = (encoder: Encoder, value: T) => ArrayBuffer;
type DecodeFunc<T> = (decoder: Decoder) => T;

export const testEncodeDecode = <T>(
  value: T,
  encode: EncodeFunc<T>,
  decode: DecodeFunc<T>
) => {
  const encoder = new Encoder();
  const buffer = encode(encoder, value);

  const decoder = new Decoder(buffer);
  const decoded = decode(decoder);

  expect(decoded).toEqual(value);
};

// Snapshot wire format of encoded message
export const testEncodeDecodeSnapshot = <T>(
  value: T,
  encode: EncodeFunc<T>,
  decode: DecodeFunc<T>
) => {
  const encoder = new Encoder();
  const buffer = encode(encoder, value);

  expect(base64(buffer)).toMatchSnapshot();

  const decoder = new Decoder(buffer);
  const decoded = decode(decoder);

  expect(decoded).toEqual(value);
};
