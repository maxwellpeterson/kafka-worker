import { ValueOf } from "src/common";

// These don't add type safety, but help with labeling
export type Int16 = number;
export type Int32 = number;
// This one does add some type safety
export type Int64 = bigint;

export const int16Size = 2;
export const int32Size = 4;
export const int64Size = 8;

// In the Kafka protocol, all arrays are nullable. In most cases, there is no
// semantic difference between null and empty arrays, and in these cases we
// replace null arrays with empty arrays to avoid code pollution from null checks
export type KafkaArray<T> = T[] | null;

// Fields that appear in the header of every Kafka protocol request
export interface RequestMetadata {
  apiVersion: Int16;
  correlationId: Int32;
  clientId: string;
}

// Returns a type predicate for the given "enum" object (this project doesn't
// use TypeScript enums). These predicates are useful for decoding arbitrary
// numbers into "enum" values in a type-safe way (see usage in Decoder class)
export const generateEnumPredicate =
  <E extends Record<string, V>, V>(enumObj: E) =>
  (value: V): value is ValueOf<E> =>
    Object.values<V>(enumObj).includes(value);

// ApiKey is an Int16
export const ApiKey = {
  Produce: 0,
  // Fetch: 1,
  // ListOffsets: 2,
  Metadata: 3,
} as const;
export type ApiKey = ValueOf<typeof ApiKey>;
export const validApiKey = generateEnumPredicate(ApiKey);

// ErrorCode is an Int16
export const ErrorCode = {
  None: 0,
  UnknownTopicOrPartition: 3,
} as const;
export type ErrorCode = ValueOf<typeof ErrorCode>;
export const validErrorCode = generateEnumPredicate(ErrorCode);

// Acks is an Int16
export const Acks = {
  None: 0,
  Leader: 1,
  FullISR: -1,
} as const;
export type Acks = ValueOf<typeof Acks>;
export const validAcks = generateEnumPredicate(Acks);
