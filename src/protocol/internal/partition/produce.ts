import {
  Acks,
  ErrorCode,
  Int32,
  Int64,
  validAcks,
  validErrorCode,
} from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

// Partition Produce Request (Version: 0) => acks message_set_size message_set
//   acks => INT16
//   message_set_size => INT32
//   message_set => BYTES

export interface PartitionProduceRequest {
  acks: Acks;
  messageSetSize: Int32;
  messageSet: ArrayBuffer;
}

export const encodePartitionProduceRequest = (
  encoder: Encoder,
  request: PartitionProduceRequest
): ArrayBuffer => {
  encoder.writeInt16(request.acks);
  encoder.writeInt32(request.messageSetSize);
  encoder.writeBuffer(request.messageSet);
  return encoder.buffer();
};

export const decodePartitionProduceRequest = (
  decoder: Decoder
): PartitionProduceRequest => {
  const request = {
    acks: decoder.readEnum(validAcks),
    messageSetSize: decoder.readInt32(),
  };
  return {
    ...request,
    messageSet: decoder.readBuffer(request.messageSetSize),
  };
};

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
  encoder.writeEnum(response.errorCode);
  encoder.writeInt64(response.baseOffset);
  return encoder.buffer();
};

export const decodePartitionProduceResponse = (
  decoder: Decoder
): PartitionProduceResponse => {
  return {
    errorCode: decoder.readEnum(validErrorCode),
    baseOffset: decoder.readInt64(),
  };
};
