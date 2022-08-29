import { Session } from "src/client/session";
import { Env } from "src/common";

export { Cluster } from "src/state/cluster";
export { Partition } from "src/state/partition";

export default {
  fetch(request: Request, env: Env): Response {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const session = new Session(env);
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();
    server.addEventListener("message", (event) => {
      if (typeof event.data === "string") {
        console.log("Received string data, but we want binary data!");
        return;
      }

      session
        .handleRequest(event.data)
        .then((response) => {
          if (response !== null) {
            server.send(response);
          }
        })
        .catch((error: Error) =>
          console.log(`Error while handling request: ${error.message}`)
        );
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};
