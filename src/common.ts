// We make this a read-only object to prevent any accidental funny business
export type Env = Readonly<{
  // The hostname of this worker (required)
  HOSTNAME: string;
  // The port of this worker (required)
  PORT: string;
  SESSION: DurableObjectNamespace;
  CLUSTER: DurableObjectNamespace;
  PARTITION: DurableObjectNamespace;
}>;

// Generic utility types used in many places
export type ValueOf<T> = T[keyof T];
export type ElemOf<T extends E[], E = T[number]> = E;
