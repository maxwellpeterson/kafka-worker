import { ErrorCode, Int32, Int64, MessageSet } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

// Internal Fetch Request (Version: 0) => max_wait_ms fetch_offset min_bytes max_bytes
//   max_wait_ms => INT32
//   fetch_offset => INT64
//   min_bytes => INT32
//   max_bytes => INT32

export interface InternalFetchRequest {
  maxWaitMs: Int32;
  fetchOffset: Int64;
  minBytes: Int32;
  maxBytes: Int32;
}

export const encodeInternalFetchRequest = (
  encoder: Encoder,
  request: InternalFetchRequest
): ArrayBuffer => {
  return encoder
    .writeInt32(request.maxWaitMs)
    .writeInt64(request.fetchOffset)
    .writeInt32(request.minBytes)
    .writeInt32(request.maxBytes)
    .buffer();
};

export const decodeInternalFetchRequest = (
  decoder: Decoder
): InternalFetchRequest => {
  return {
    maxWaitMs: decoder.readInt32(),
    fetchOffset: decoder.readInt64(),
    minBytes: decoder.readInt32(),
    maxBytes: decoder.readInt32(),
  };
};

// Internal Fetch Response (Version: 0) => error_code high_watermark message_set_size message_set
//   error_code => INT16
//   high_watermark => INT64
//   message_set_size => INT32
//   message_set => BYTES

export interface InternalFetchResponse {
  errorCode: ErrorCode;
  highWatermark: Int64;
  messageSet: MessageSet;
}

export const encodeInternalFetchResponse = (
  encoder: Encoder,
  response: InternalFetchResponse
): ArrayBuffer => {
  return encoder
    .writeEnum(response.errorCode)
    .writeInt64(response.highWatermark)
    .writeMessageSet(response.messageSet)
    .buffer();
};

export const decodeInternalFetchResponse = (
  decoder: Decoder
): InternalFetchResponse => {
  return {
    errorCode: decoder.readErrorCode(),
    highWatermark: decoder.readInt64(),
    messageSet: decoder.readMessageSet(),
  };
};
