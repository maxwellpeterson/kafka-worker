import { Env } from "src/common";
import { ProduceRequest, ProduceResponse } from "src/protocol/api/produce";
import { Int32, RequestMetadata } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import {
  PartitionApiKey,
  decodePartitionProduceResponse,
  encodePartitionProduceRequest,
  encodePartitionRequestHeader,
} from "src/protocol/internal/partition";
import {
  PartitionId,
  generatePartitonId,
  parsePartitionId,
  partitionStubUrl,
} from "src/state/partition";

interface PartitionState {
  active: Map<PartitionId, WebSocket>;
  pending: Map<PartitionId, Promise<WebSocket>>;
}

// TODO: One ProduceRequest can correspond to multiple
// PartititionProduceRequests, and therefore multiple PartitionProduceResponses.
// We need a way to aggregate all of these responses, and once we have them all
// combine them into a single response to send back to the client
//
// Solution: Every outbound WebSocket message is uniquely identified by the tuple
// (correlationId, partitionId). Before sending these messages, the request
// handler creates a stub response (topic array is filled in, but partition
// arrays for each topic are empty) as well as a set of the partitions it will
// send messages to. When a response is received from a partition, that parition
// is removed from the set and the stub response is filled in with the response
// data. When the set is empty, the stub response has been fully filled in and
// can be sent back to the client.
//
// TODO: Verify uniqueness assumptions (in produce request, are topics in topic
// list and partitions in partition lists guaranteed to be unique? If not we
// might need to add a third element to the identifier, such as index in the
// request array, but the same approach should work)
interface RequestState {
  produce: {
    pending: Map<CorrelationId, ProduceRequestState>;
  };
}
type CorrelationId = number;
interface ProduceRequestState {
  correlationId: Int32;
  response: ProduceResponse;
  pendingPartitions: Set<PartitionId>;
  done: () => void;
}

type PartitionSocket =
  | { state: typeof SocketState.Active; socket: WebSocket }
  | {
      state: typeof SocketState.Pending;
      socket: Promise<WebSocket>;
    };
const SocketState = {
  Active: 0,
  Pending: 1,
} as const;

export class RequestManager {
  private readonly env: Env;

  // The getSocketPartition method should be used to read and update this field,
  // and no other methods should access it directly
  private readonly partitions: PartitionState;
  private readonly requests: RequestState;

  constructor(env: Env) {
    this.env = env;

    this.partitions = {
      active: new Map<PartitionId, WebSocket>(),
      pending: new Map<PartitionId, Promise<WebSocket>>(),
    };
    this.requests = {
      produce: { pending: new Map<CorrelationId, ProduceRequestState>() },
    };
  }

  internalProduceRequest(
    metadata: RequestMetadata,
    request: ProduceRequest
  ): Promise<ProduceResponse | null> {
    const stubResponse: ProduceResponse = {
      topics: request.topics.map((topic) => ({
        name: topic.name,
        partitions: [],
      })),
    };

    const partitionIds = request.topics.flatMap((topic) =>
      topic.partitions.map((partition) =>
        generatePartitonId(topic.name, partition.index)
      )
    );

    return new Promise<ProduceResponse | null>((resolve, reject) => {
      this.requests.produce.pending.set(metadata.correlationId, {
        correlationId: metadata.correlationId,
        response: stubResponse,
        pendingPartitions: new Set(partitionIds),
        done: () => {
          this.requests.produce.pending.delete(metadata.correlationId);
          resolve(stubResponse);
        },
      });

      Promise.all(
        request.topics.flatMap((topic) =>
          topic.partitions.map((partition) => {
            const encoder = new Encoder();
            encodePartitionRequestHeader(encoder, {
              apiKey: PartitionApiKey.Produce,
              apiVersion: 0,
              correlationId: metadata.correlationId,
              clientId: metadata.clientId,
            });

            const partitionRequest = encodePartitionProduceRequest(encoder, {
              acks: request.acks,
              messageSetSize: partition.messageSetSize,
              messageSet: partition.messageSet,
            });
            const connection = this.getPartitionSocket(
              topic.name,
              partition.index
            );

            const handleSocket = (socket: WebSocket) =>
              socket.send(partitionRequest);

            if (connection.state === SocketState.Active) {
              handleSocket(connection.socket);
              return;
            }
            return connection.socket.then(handleSocket);
          })
        )
      ).catch(reject);
    });
  }

  // Enforces the invariant that a maximum of one socket connection per
  // partition can be open at the same time. Prevents the case where a second
  // connection is initiated after the first connection has been initiated but
  // before the first connection is ready to use
  private getPartitionSocket(topic: string, index: number): PartitionSocket {
    const partitionId = generatePartitonId(topic, index);

    // If there is already an open connection, just use that
    const existing = this.partitions.active.get(partitionId);
    if (existing !== undefined) {
      return { state: SocketState.Active, socket: existing };
    }

    // If there is already a pending connection, the caller can wait for it
    // to become ready to use
    const pending = this.partitions.pending.get(partitionId);
    if (pending !== undefined) {
      return { state: SocketState.Pending, socket: pending };
    }

    // Otherwise, we'll need to create a new connection ourselves
    const objId = this.env.PARTITION.idFromName(partitionId);
    const obj = this.env.PARTITION.get(objId);

    const promise = obj
      .fetch(partitionStubUrl, {
        headers: {
          Upgrade: "websocket",
        },
      })
      .then((response) => {
        // Unmark the connection as pending. If there is no WebSocket in the
        // response, an error is thrown and the caller can start over with
        // creating a new connection. But no WebSocket in the response probably
        // means there is a issue with the Partition fetch method
        this.partitions.pending.delete(partitionId);
        const socket = response.webSocket;

        if (socket === null) {
          throw new Error("Expected websocket in response from Partition");
        }

        socket.accept();
        this.partitions.active.set(partitionId, socket);

        socket.addEventListener("message", (event) => {
          if (typeof event.data === "string") {
            console.log("Received string data, but we want binary data!");
            return;
          }

          const decoder = new Decoder(event.data);
          const correlationId = decoder.readInt32();

          const produceRequest =
            this.requests.produce.pending.get(correlationId);
          if (produceRequest !== undefined) {
            this.handlePartitionProduceResponse(
              produceRequest,
              partitionId,
              decoder
            );
            return;
          }

          console.log("Couldn't match Partition response to pending request");
        });

        // TODO: Add error and close handlers (delete requests that depend on
        // closed/failed socket)

        // Once this.partitions.pending and this.partitions.active have been
        // updated, and the socket event handlers have been registered, the
        // connection is ready for use. Note that callers should not register
        // any event handlers themselves, all they should do is call
        // socket.send()
        return socket;
      });

    // Mark connection as pending to prevent duplicate connections from being
    // created before this connection is ready to use
    this.partitions.pending.set(partitionId, promise);
    return { state: SocketState.Pending, socket: promise };
  }

  private handlePartitionProduceResponse(
    clientRequest: ProduceRequestState,
    partitionId: PartitionId,
    decoder: Decoder
  ) {
    if (!clientRequest.pendingPartitions.has(partitionId)) {
      console.log("Received Partition response from non-pending partition");
      return;
    }

    const response = decodePartitionProduceResponse(decoder);
    const { topic: topicName, index } = parsePartitionId(partitionId);
    const topic = clientRequest.response.topics.find(
      (topic) => topic.name === topicName
    );

    if (topic === undefined) {
      // This should be an unreachable state
      return;
    }

    topic.partitions.push({ ...response, index });
    clientRequest.pendingPartitions.delete(partitionId);

    if (clientRequest.pendingPartitions.size === 0) {
      clientRequest.done();
    }
  }
}
