import { Env } from "src/common";
import { ProduceRequest, ProduceResponse } from "src/protocol/api/produce";
import { RequestMetadata } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import {
  PartitionApiKey,
  decodePartitionProduceResponse,
  encodePartitionProduceRequest,
  encodePartitionRequestHeader,
} from "src/protocol/internal/partition";
import { PartitionInfo } from "src/state/partition";
import { SocketManager } from "src/state/socket-manager";

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
  response: ProduceResponse;
  pendingPartitions: Set<PartitionId>;
  done: () => void;
}
type PartitionId = string;

export class RequestManager {
  private readonly env: Env;

  private readonly requests: RequestState;
  private readonly socket: SocketManager;

  constructor(env: Env) {
    this.env = env;

    this.requests = {
      produce: { pending: new Map<CorrelationId, ProduceRequestState>() },
    };
    this.socket = new SocketManager(env, this);
  }

  // TODO: Handle acks = 0 case
  produceRequest(
    metadata: RequestMetadata,
    request: ProduceRequest
  ): Promise<ProduceResponse | null> {
    const stubResponse: ProduceResponse = {
      topics: request.topics.map((topic) => ({
        name: topic.name,
        partitions: [],
      })),
    };

    const partitions = request.topics.flatMap((topic) =>
      topic.partitions.map(
        (partition) => new PartitionInfo(topic.name, partition.index).id
      )
    );

    return new Promise<ProduceResponse | null>((resolve, reject) => {
      this.requests.produce.pending.set(metadata.correlationId, {
        response: stubResponse,
        pendingPartitions: new Set(partitions),
        done: () => {
          this.requests.produce.pending.delete(metadata.correlationId);
          resolve(stubResponse);
        },
      });

      Promise.all(
        request.topics.flatMap((topic) =>
          topic.partitions.map(async (partition) => {
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

            await this.socket.sendPartition(
              new PartitionInfo(topic.name, partition.index),
              partitionRequest
            );
          })
        )
      ).catch(reject);
    });
  }

  handlePartitionMessage(partition: PartitionInfo, message: ArrayBuffer): void {
    const decoder = new Decoder(message);
    const correlationId = decoder.readInt32();

    const produceRequest = this.requests.produce.pending.get(correlationId);
    if (produceRequest !== undefined) {
      this.handlePartitionProduceResponse(produceRequest, partition, decoder);
      return;
    }

    console.log("Couldn't match Partition response to pending request");
  }

  private handlePartitionProduceResponse(
    clientRequest: ProduceRequestState,
    partition: PartitionInfo,
    decoder: Decoder
  ) {
    if (!clientRequest.pendingPartitions.has(partition.id)) {
      console.log("Received Partition response from non-pending partition");
      return;
    }

    const response = decodePartitionProduceResponse(decoder);
    const topic = clientRequest.response.topics.find(
      (topic) => topic.name === partition.topic
    );

    if (topic === undefined) {
      // This should be an unreachable state
      return;
    }

    topic.partitions.push({ ...response, index: partition.index });
    clientRequest.pendingPartitions.delete(partition.id);

    if (clientRequest.pendingPartitions.size === 0) {
      clientRequest.done();
    }
  }
}
