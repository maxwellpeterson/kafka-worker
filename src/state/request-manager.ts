import { Env } from "src/common";
import { ProduceRequest, ProduceResponse } from "src/protocol/api/produce";
import { RequestMetadata } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import {
  PartitionApiKey,
  encodePartitionProduceRequest,
  encodePartitionRequestHeader,
} from "src/protocol/internal/partition";
import { PartitionInfo } from "src/state/partition";
import {
  PendingProduceRequest,
  PendingRequest,
} from "src/state/pending-request";
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
type CorrelationId = number;

export class RequestManager {
  private readonly pending: Map<CorrelationId, PendingRequest>;
  private readonly socket: SocketManager;

  constructor(env: Env) {
    this.pending = new Map<CorrelationId, PendingRequest>();
    this.socket = new SocketManager(env, this);
  }

  produceRequest(
    metadata: RequestMetadata,
    request: ProduceRequest
  ): Promise<ProduceResponse | null> {
    return new Promise<ProduceResponse | null>((resolve, reject) => {
      if (request.acks !== 0) {
        // If the request will return a response, resolve the promise when the
        // response is complete
        const done = (response: ProduceResponse) => {
          this.pending.delete(metadata.correlationId);
          resolve(response);
        };
        this.pending.set(
          metadata.correlationId,
          new PendingProduceRequest(request, done)
        );
      }

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
      )
        .then(() => {
          if (request.acks === 0) {
            // If the request will not return a response, resolve the promise as
            // soon as the subrequests are sent
            resolve(null);
          }
        })
        .catch(reject);
    });
  }

  handlePartitionMessage(partition: PartitionInfo, message: ArrayBuffer): void {
    const decoder = new Decoder(message);
    const correlationId = decoder.readInt32();

    const request = this.pending.get(correlationId);
    if (request !== undefined) {
      request.handlePartitionMessage(partition, decoder);
      return;
    }
    console.log("Couldn't match Partition response to pending request");
  }
}
