import { Env } from "src/common";

export { Session } from "src/state/session";
export { Cluster } from "src/state/cluster";

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    const id = env.SESSION.newUniqueId();
    const obj = env.SESSION.get(id);
    return await obj.fetch(request);
  },
};
