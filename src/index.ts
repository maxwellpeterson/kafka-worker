import { Session } from "src/client/session";
import { Env, stringify } from "src/common";

export { Cluster } from "src/state/cluster";
export { Partition } from "src/state/partition";

export default {
  fetch(request: Request, env: Env): Response {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }
    console.log(`Opening new client connection!`);

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
        .catch((error) =>
          console.log(`Error while handling request: ${stringify(error)}`)
        );
    });
    server.addEventListener("close", () => {
      session.close();
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};
