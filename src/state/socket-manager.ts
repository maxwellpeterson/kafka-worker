import { Env } from "src/common";
import { PartitionInfo, partitionStubUrl } from "src/state/partition";

interface PartitionHandler {
  handlePartitionMessage(partition: PartitionInfo, message: ArrayBuffer): void;
  // TODO: Add error and close(?) handlers
}

interface SocketState {
  active: Map<SocketId, WebSocket>;
  pending: Map<SocketId, Promise<WebSocket>>;
}
type SocketId = string;

export class SocketManager {
  private readonly env: Env;
  private readonly partitionHandler: PartitionHandler;

  private readonly sockets: SocketState;

  constructor(env: Env, partitionHandler: PartitionHandler) {
    this.env = env;
    this.partitionHandler = partitionHandler;

    this.sockets = {
      active: new Map<SocketId, WebSocket>(),
      pending: new Map<SocketId, Promise<WebSocket>>(),
    };
  }

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

        // TODO: Add error and close(?) handlers

        // Once this.partitions.pending and this.partitions.active have been
        // updated, and the socket event handlers have been registered, the
        // connection is ready for use
        return socket;
      });

    // Mark connection as pending to prevent duplicate connections from being
    // created before this connection is ready to use
    this.sockets.pending.set(partition.id, socketPromise);
    return socketPromise;
  }
}
