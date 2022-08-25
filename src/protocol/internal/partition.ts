import { ValueOf } from "src/common";
import {
  ErrorCode,
  generateEnumPredicate,
  Int16,
  Int32,
  Int64,
  String,
  validErrorCode,
} from "src/protocol/common";
import { Encoder } from "src/protocol/encoder";
import { Decoder } from "src/protocol/decoder";

// We define our own grammar for the request and response format that follows
// closely from the Kafka protocol.

// Partition Request (Version: 0) => header request_message
//   header => api_key api_version correlation_id client_id
//     api_key => INT16
//     api_version => INT16
//     correlation_id => INT32
//     client_id => STRING
//   request_message => PARTITION_PRODUCE_REQUEST

export interface PartitonRequestHeader {
  apiKey: PartitionApiKey;
  apiVersion: Int16;
  correlationId: Int32;
  clientId: String;
}

export const encodePartitonRequestHeader = (
  encoder: Encoder,
  header: PartitonRequestHeader
) => {
  encoder.writeEnum(header.apiKey);
  encoder.writeInt16(header.apiVersion);
  encoder.writeInt32(header.correlationId);
  encoder.writeString(header.clientId);
};

export const decodePartitonRequestHeader = (
  decoder: Decoder
): PartitonRequestHeader => {
  return {
    apiKey: decoder.readEnum(validPartitionApiKey),
    apiVersion: decoder.readInt16(),
    correlationId: decoder.readInt32(),
    clientId: decoder.readString(),
  };
};

// PartitionApiKey is an Int16
export const PartitionApiKey = {
  Produce: 0,
} as const;
export type PartitionApiKey = ValueOf<typeof PartitionApiKey>;
const validPartitionApiKey = generateEnumPredicate(PartitionApiKey);

// Partition Produce Request (Version: 0) => acks message_set_size message_set
//   acks => INT16
//   message_set_size => INT32
//   message_set => BYTES

export interface PartitionProduceRequest {
  acks: Int16;
  messageSetSize: Int32;
  messageSet: ArrayBuffer;
}

export const encodePartitonProduceRequest = (
  encoder: Encoder,
  request: PartitionProduceRequest
): ArrayBuffer => {
  encoder.writeInt16(request.acks);
  encoder.writeInt32(request.messageSetSize);
  encoder.writeMessageSet(request.messageSet);
  return encoder.buffer();
};

export const decodePartitonProduceRequest = (
  decoder: Decoder
): PartitionProduceRequest => {
  const request = {
    acks: decoder.readInt16(),
    messageSetSize: decoder.readInt32(),
  };
  return {
    ...request,
    messageSet: decoder.readMessageSet(request.messageSetSize),
  };
};

// Partition Response (Version: 0) => correlation_id response_message
//   correlation_id => INT32
//   response_message => PARTITION_PRODUCE_RESPONSE

// Partition Produce Response (Version: 0) => error_code offset
//   error_code => INT16
//   offset => INT64

export interface PartitionProduceResponse {
  errorCode: ErrorCode;
  offset: Int64;
}

export const encodePartitionProduceResponse = (
  encoder: Encoder,
  response: PartitionProduceResponse
): ArrayBuffer => {
  encoder.writeEnum(response.errorCode);
  encoder.writeInt64(response.offset);
  return encoder.buffer();
};

export const decodePartitionProduceResponse = (
  decoder: Decoder
): PartitionProduceResponse => {
  return {
    errorCode: decoder.readEnum(validErrorCode),
    offset: decoder.readInt64(),
  };
};
