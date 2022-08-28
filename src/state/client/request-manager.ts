import { Env } from "src/common";
import { ProduceRequest, ProduceResponse } from "src/protocol/api/produce";
import { Acks } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { RequestMetadata, encodeRequestHeader } from "src/protocol/header";
import { PartitionApiKey } from "src/protocol/internal/partition/common";
import { encodePartitionProduceRequest } from "src/protocol/internal/partition/produce";
import { PartitionInfo } from "src/state/broker/partition";
import {
  PendingProduceRequest,
  PendingRequest,
} from "src/state/client/pending-request";
import { SocketManager } from "src/state/client/socket-manager";

type CorrelationId = number;

// One Kafka protocol request can operate on multiple partitions, which means
// multiple subrequests to Partition DOs that fan out, and multiple subresponses
// that need to be fanned in to complete the final response sent back to the
// client. The RequestManager class handles the fan out and fan in process.
//
// Public RequestManager methods wrap the fan out and fan in process in a
// "super-promise" that resolves when all subrequests have completed
// successfully, failed, or timed out. All the caller needs to do is await this
// promise to get the complete response.
//
// Note that error handling and timeouts have not been implemented, but should
// be a simple extension of this existing structure (TODO).
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
      if (request.acks !== Acks.None) {
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
            encodeRequestHeader(encoder, {
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
          if (request.acks === Acks.None) {
            // If the request will not return a response, resolve the promise as
            // soon as the subrequests are sent
            resolve(null);
          }
        })
        .catch(reject);
    });
  }

  // Matches WebSocket messages received from Partition DOs to pending client
  // requests
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
