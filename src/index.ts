import { Env } from "src/common";
import { handleRequest } from "src/protocol/request";

export { Cluster } from "src/state/cluster";

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();
    server.addEventListener("message", (event) => {
      if (typeof event.data === "string") {
        console.log("Received string data, but we want binary data!");
        return;
      }

      handleRequest(env, event.data)
        .then((response) => {
          if (response !== null) {
            server.send(response);
          }
        })
        .catch((error) =>
          console.log(`Error while processing request: ${error}`)
        );
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};
