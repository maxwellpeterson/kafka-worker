import { Acks, ErrorCode, Int64 } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

// Partition Produce Request (Version: 0) => acks message_set_size message_set
//   acks => INT16
//   message_set_size => INT32
//   message_set => BYTES

export interface PartitionProduceRequest {
  acks: Acks;
  messageSet: ArrayBuffer;
}

export const encodePartitionProduceRequest = (
  encoder: Encoder,
  request: PartitionProduceRequest
): ArrayBuffer => {
  return encoder
    .writeEnum(request.acks)
    .writeMessageSet(request.messageSet)
    .buffer();
};

export const decodePartitionProduceRequest = (
  decoder: Decoder
): PartitionProduceRequest => ({
  acks: decoder.readAcks(),
  messageSet: decoder.readMessageSet(),
});

// Partition Produce Response (Version: 0) => error_code base_offset
//   error_code => INT16
//   base_offset => INT64

export interface PartitionProduceResponse {
  errorCode: ErrorCode;
  baseOffset: Int64;
}

export const encodePartitionProduceResponse = (
  encoder: Encoder,
  response: PartitionProduceResponse
): ArrayBuffer => {
  return encoder
    .writeEnum(response.errorCode)
    .writeInt64(response.baseOffset)
    .buffer();
};

export const decodePartitionProduceResponse = (
  decoder: Decoder
): PartitionProduceResponse => {
  return {
    errorCode: decoder.readErrorCode(),
    baseOffset: decoder.readInt64(),
  };
};
