import { Env } from "src/common";

export { Session } from "src/state/client/session";
export { Cluster } from "src/state/cluster";
export { Partition } from "src/state/broker/partition";

export default {
  // All the gateway worker does is forward the request to the Session DO, which
  // manages the client connection
  async fetch(request: Request, env: Env): Promise<Response> {
    const id = env.SESSION.newUniqueId();
    const obj = env.SESSION.get(id);
    return await obj.fetch(request);
  },
};
