import { Env } from "src/common";
import { handleRequest } from "src/protocol/request";

export class Session {
  private readonly state: DurableObjectState;
  private readonly env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
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

      handleRequest(this.env, event.data)
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
  }

  // private async handleRequest(
  //   buffer: ArrayBuffer
  // ): Promise<ArrayBuffer | null> {}
}
