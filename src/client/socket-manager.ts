import { Env } from "src/common";
import { PartitionInfo, partitionStubUrl } from "src/state/partition";

interface PartitionHandler {
  handlePartitionMessage(partition: PartitionInfo, message: ArrayBuffer): void;
  handlePartitionClose(partition: PartitionInfo): void;
}

interface SocketState {
  active: Map<SocketId, WebSocket>;
  pending: Map<SocketId, Promise<WebSocket>>;
}
type SocketId = string;

// WebSocket connections to DOs are created asynchronously using fetch(). This
// means connection creation can overlap, and always creating a new connection
// when there is no existing connection can result in multiple connections to
// the same DO. The SocketManager class controls all DO WebSocket connections
// and makes sure there are no duplicates.
export class SocketManager {
  private readonly env: Env;
  private readonly partitionHandler: PartitionHandler;

  private readonly sockets: SocketState;
  private readonly controller: AbortController;

  constructor(env: Env, partitionHandler: PartitionHandler) {
    this.env = env;
    this.partitionHandler = partitionHandler;

    this.sockets = {
      active: new Map<SocketId, WebSocket>(),
      pending: new Map<SocketId, Promise<WebSocket>>(),
    };
    this.controller = new AbortController();
  }

  // Send a WebSocket message to the given partition
  async sendPartition(
    partition: PartitionInfo,
    message: ArrayBuffer
  ): Promise<void> {
    // If there is already an active connection, just use that
    const existing = this.sockets.active.get(partition.id);
    if (existing !== undefined) {
      existing.send(message);
      return;
    }

    // If there is already a pending connection, wait for it to be ready to use
    const pending = this.sockets.pending.get(partition.id);
    if (pending !== undefined) {
      const socket = await pending;
      socket.send(message);
      return;
    }

    // Otherwise, we'll need to create a new connection ourselves
    const socket = await this.dialPartition(partition);
    socket.send(message);
  }

  // Enforces the invariant that a maximum of one socket connection per
  // partition can be open at the same time. Prevents the case where a second
  // connection is initiated after the first connection has been initiated but
  // before the first connection is ready to use
  private async dialPartition(partition: PartitionInfo): Promise<WebSocket> {
    const objId = this.env.PARTITION.idFromName(partition.id);
    const obj = this.env.PARTITION.get(objId);

    const socketPromise = obj
      .fetch(partitionStubUrl, {
        headers: {
          Upgrade: "websocket",
        },
        signal: this.controller.signal,
      })
      .then((response) => {
        // Unmark the connection as pending. If there is no WebSocket in the
        // response, an error is thrown and the caller can start over with
        // creating a new connection (but this is a bad sign)
        this.sockets.pending.delete(partition.id);
        const socket = response.webSocket;

        if (socket === null) {
          throw new Error("Expected websocket in response from Partition");
        }

        socket.accept();
        this.sockets.active.set(partition.id, socket);

        socket.addEventListener("message", (event) => {
          if (typeof event.data === "string") {
            console.log("Received string data, but we want binary data!");
            return;
          }

          this.partitionHandler.handlePartitionMessage(partition, event.data);
        });

        socket.addEventListener("close", () => {
          this.sockets.active.delete(partition.id);
          this.partitionHandler.handlePartitionClose(partition);
        });

        // Once this.partitions.pending and this.partitions.active have been
        // updated, and the socket event handlers have been registered, the
        // connection is ready for sending
        return socket;
      });

    // Mark connection as pending to prevent duplicate connections from being
    // created before this connection is ready to use
    this.sockets.pending.set(partition.id, socketPromise);
    return socketPromise;
  }

  close() {
    // Close all open conections
    this.sockets.active.forEach((socket) => socket.close());
    // Abort pending connections
    this.controller.abort();
  }
}
