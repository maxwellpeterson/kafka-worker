import { ValueOf } from "src/common";

// These don't add any type safety, but help with labeling
export type Int16 = number;
export type Int32 = number;
export type Int64 = bigint;

export const int16Size = 2;
export const int32Size = 4;
export const int64Size = 8;

export type KafkaString = string | null;
export type KafkaArray<T> = T[] | null;

export const cleanKafkaStringArray = (
  strs: KafkaArray<KafkaString>
): string[] => {
  if (strs === null) {
    return [];
  }
  return strs.filter((str): str is string => str !== null);
};

// ApiKey is an Int16
export const ApiKey = {
  Produce: 0,
  Fetch: 1,
  ListOffsets: 2,
  Metadata: 3,
} as const;
export type ApiKey = ValueOf<typeof ApiKey>;

// ErrorCode is an Int16
export const ErrorCode = {
  None: 0,
  UnknownTopicOrPartition: 3,
} as const;
export type ErrorCode = ValueOf<typeof ErrorCode>;
