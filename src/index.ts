import { Env } from "src/common";
import { handleRequest } from "src/protocol/request";

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
      try {
        const response = handleRequest(env, event.data);
        server.send(response);
      } catch (e) {
        console.log(`Error while processing request: ${e}`);
      }
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};
