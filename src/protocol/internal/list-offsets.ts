import { ErrorCode, Int32, Int64 } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

// Internal ListOffsets Request (Version: 0) => timestamp max_num_offsets
//   timestamp => INT64
//   max_num_offsets => INT32

export interface InternalListOffsetsRequest {
  timestamp: Int64;
  maxNumOffsets: Int32;
}

export const encodeInternalListOffsetsRequest = (
  encoder: Encoder,
  request: InternalListOffsetsRequest
): ArrayBuffer => {
  return encoder
    .writeInt64(request.timestamp)
    .writeInt32(request.maxNumOffsets)
    .buffer();
};

export const decodeInternalListOffsetsRequest = (
  decoder: Decoder
): InternalListOffsetsRequest => {
  return {
    timestamp: decoder.readInt64(),
    maxNumOffsets: decoder.readInt32(),
  };
};

// Internal ListOffsets Response (Version: 0) => error_code [old_style_offsets]
//   error_code => INT16
//   old_style_offsets => INT64

export interface InternalListOffsetsResponse {
  errorCode: ErrorCode;
  oldStyleOffsets: Int64[];
}

export const encodeInternalListOffsetsResponse = (
  encoder: Encoder,
  response: InternalListOffsetsResponse
): ArrayBuffer => {
  return encoder
    .writeEnum(response.errorCode)
    .writeInt64Array(response.oldStyleOffsets)
    .buffer();
};

export const decodeInternalListOffsetsResponse = (
  decoder: Decoder
): InternalListOffsetsResponse => {
  return {
    errorCode: decoder.readErrorCode(),
    oldStyleOffsets: decoder.readInt64Array(),
  };
};
