import { ValueOf } from "src/common";

// These don't add any type safety, but help with labeling
export type Int16 = number;
export type Int32 = number;

export const int16Size = 2;
export const int32Size = 4;

export type KafkaString = string | null;
export type KafkaArray<T> = T[] | null;

// ApiKey is an Int16
export const ApiKey = {
  MetadataRequest: 3,
} as const;
export type ApiKey = ValueOf<typeof ApiKey>;

// ErrorCode is an Int16
export const ErrorCode = {
  None: 0,
} as const;
export type ErrorCode = ValueOf<typeof ErrorCode>;

export interface BaseKafkaRequest {
  apiVersion: Int16;
  correlationId: Int32;
  clientId: KafkaString;
}
