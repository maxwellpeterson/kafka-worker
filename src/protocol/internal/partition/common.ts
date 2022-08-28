import { ValueOf } from "src/common";
import {
  ApiKey,
  Int16,
  Int32,
  generateEnumPredicate,
  int32Size,
} from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

// Partition Request (Version: 0) => header request_message
//   header => api_key api_version correlation_id client_id
//     api_key => INT16
//     api_version => INT16
//     correlation_id => INT32
//     client_id => STRING
//   request_message => PARTITION_PRODUCE_REQUEST

export interface PartitionRequestHeader {
  apiKey: PartitionApiKey;
  apiVersion: Int16;
  correlationId: Int32;
  clientId: string;
}

export const encodePartitionRequestHeader = (
  encoder: Encoder,
  header: PartitionRequestHeader
) => {
  encoder.writeEnum(header.apiKey);
  encoder.writeInt16(header.apiVersion);
  encoder.writeInt32(header.correlationId);
  encoder.writeString(header.clientId);
};

export const decodePartitionRequestHeader = (
  decoder: Decoder
): PartitionRequestHeader => {
  return {
    apiKey: decoder.readEnum(validPartitionApiKey),
    apiVersion: decoder.readInt16(),
    correlationId: decoder.readInt32(),
    clientId: decoder.readString(),
  };
};

// PartitionApiKey is an Int16
export const PartitionApiKey = {
  Produce: ApiKey.Produce,
} as const;
export type PartitionApiKey = ValueOf<typeof PartitionApiKey>;
const validPartitionApiKey = generateEnumPredicate(PartitionApiKey);

// Partition Response (Version: 0) => correlation_id response_message
//   correlation_id => INT32
//   response_message => PARTITION_PRODUCE_RESPONSE

export class PartitionResponseEncoder extends Encoder {
  constructor(correlationId: Int32, initialBufferSize = 64) {
    super(initialBufferSize + int32Size);
    this.writeInt32(correlationId);
  }
}
