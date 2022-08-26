export type Env = Readonly<{
  // The hostname of this worker
  HOSTNAME: string;
  // The port of this worker
  PORT: string;
  SESSION: DurableObjectNamespace;
  CLUSTER: DurableObjectNamespace;
  PARTITION: DurableObjectNamespace;
  // Example binding to KV. Learn more at https://developers.cloudflare.com/workers/runtime-apis/kv/
  // MY_KV_NAMESPACE: KVNamespace;
  //
  // Example binding to R2. Learn more at https://developers.cloudflare.com/workers/runtime-apis/r2/
  // MY_BUCKET: R2Bucket;
}>;

export type ValueOf<T> = T[keyof T];
