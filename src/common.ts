// We make this a read-only object to prevent any accidental funny business
export type Env = Readonly<{
  // The hostname of this worker (required)
  HOSTNAME: string;
  // The port of this worker (required)
  PORT: string;
  // The size of partition storage chunks in bytes (required)
  PARTITION_CHUNK_SIZE: string;
  CLUSTER: DurableObjectNamespace;
  PARTITION: DurableObjectNamespace;
}>;

// Generic utility types used in many places
export type ValueOf<T> = T[keyof T];
export type ElemOf<T extends E[], E = T[number]> = E;

// Wrapper around JSON.stringify that converts BigInts to strings
export const stringify = <T>(value: T): string =>
  JSON.stringify(
    value,
    // Make BigInt serializable into JSON for logging, taken from:
    // https://github.com/GoogleChromeLabs/jsbi/issues/30#issuecomment-521460510
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    (_key, value) => (typeof value === "bigint" ? value.toString() : value),
    2
  );

export class AbortedRequestError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = this.constructor.name;
  }
}
