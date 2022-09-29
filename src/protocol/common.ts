import { ValueOf } from "src/common";

// These don't add type safety, but help with labeling
export type Int8 = number;
export type Int16 = number;
export type Int32 = number;
// This one does add some type safety
export type Int64 = bigint;

export const int8Size = 1;
export const int16Size = 2;
export const int32Size = 4;
export const int64Size = 8;

// In the Kafka protocol, all arrays are nullable. In most cases, there is no
// semantic difference between null and empty arrays, and in these cases we
// replace null arrays with empty arrays to avoid code pollution from null checks
export type KafkaArray<T> = T[] | null;
export type NullableString = string | null;
export type MessageSet = Uint8Array;

// Returns a type predicate for the given "enum" object (this project doesn't
// use TypeScript enums). These predicates are useful for decoding arbitrary
// numbers into "enum" values in a type-safe way (see usage in Decoder class)
export const generateEnumPredicate =
  <E extends Record<string, V>, V>(enumObj: E) =>
  (value: V): value is ValueOf<E> =>
    Object.values<V>(enumObj).includes(value);

// ApiKey is an Int16
// https://kafka.apache.org/protocol.html#protocol_api_keys
export const ApiKey = {
  Produce: 0,
  Fetch: 1,
  ListOffsets: 2,
  Metadata: 3,
} as const;
export type ApiKey = ValueOf<typeof ApiKey>;
export const validApiKey = generateEnumPredicate(ApiKey);

// ErrorCode is an Int16
// https://kafka.apache.org/protocol.html#protocol_error_codes
export const ErrorCode = {
  UnknownServerError: -1,
  None: 0,
  OffsetOutOfRange: 1,
  CorruptMessage: 2,
  UnknownTopicOrPartition: 3,
  InvalidMessageSize: 4,
  NotLeaderForPartition: 6,
  RequestTimedOut: 7,
  NetworkException: 13,
} as const;
export type ErrorCode = ValueOf<typeof ErrorCode>;
export const validErrorCode = generateEnumPredicate(ErrorCode);

// Acks is an Int16
// https://kafka.apache.org/protocol.html#The_Messages_Produce
export const Acks = {
  None: 0,
  Leader: 1,
  FullISR: -1,
} as const;
export type Acks = ValueOf<typeof Acks>;
export const validAcks = generateEnumPredicate(Acks);
