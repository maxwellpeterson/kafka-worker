import { Acks, ErrorCode, Int64 } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

// Internal Produce Request (Version: 0) => acks message_set_size message_set
//   acks => INT16
//   message_set_size => INT32
//   message_set => BYTES

export interface InternalProduceRequest {
  acks: Acks;
  messageSet: ArrayBuffer;
}

export const encodeInternalProduceRequest = (
  encoder: Encoder,
  request: InternalProduceRequest
): ArrayBuffer => {
  return encoder
    .writeEnum(request.acks)
    .writeMessageSet(request.messageSet)
    .buffer();
};

export const decodeInternalProduceRequest = (
  decoder: Decoder
): InternalProduceRequest => ({
  acks: decoder.readAcks(),
  messageSet: decoder.readMessageSet(),
});

// Internal Produce Response (Version: 0) => error_code base_offset
//   error_code => INT16
//   base_offset => INT64

export interface InternalProduceResponse {
  errorCode: ErrorCode;
  baseOffset: Int64;
}

export const encodeInternalProduceResponse = (
  encoder: Encoder,
  response: InternalProduceResponse
): ArrayBuffer => {
  return encoder
    .writeEnum(response.errorCode)
    .writeInt64(response.baseOffset)
    .buffer();
};

export const decodeInternalProduceResponse = (
  decoder: Decoder
): InternalProduceResponse => {
  return {
    errorCode: decoder.readErrorCode(),
    baseOffset: decoder.readInt64(),
  };
};
