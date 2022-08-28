import { Int16, Int32, NullableString } from "src/protocol/common";
import { Decoder, EnumPredicate } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";

// Request Header (Version: 1) => api_key api_version correlation_id client_id
//   api_key => INT16
//   api_version => INT16
//   correlation_id => INT32
//   client_id => NULLABLE_STRING
//
// https://kafka.apache.org/protocol.html#protocol_messages
//
// This header format is also used by internal requests.

export interface RequestHeader<T> {
  apiKey: T;
  apiVersion: Int16;
  correlationId: Int32;
  clientId: NullableString;
}

export const encodeRequestHeader = <T extends Int16>(
  encoder: Encoder,
  header: RequestHeader<T>
): Encoder => {
  return encoder
    .writeEnum(header.apiKey)
    .writeInt16(header.apiVersion)
    .writeInt32(header.correlationId)
    .writeNullableString(header.clientId);
};

export const decodeRequestHeader = <T extends Int16>(
  decoder: Decoder,
  predicate: EnumPredicate<T>
): RequestHeader<T> => {
  return {
    apiKey: decoder.readEnum(predicate),
    apiVersion: decoder.readInt16(),
    correlationId: decoder.readInt32(),
    clientId: decoder.readNullableString(),
  };
};

export type RequestMetadata = Omit<RequestHeader<unknown>, "apiKey">;
