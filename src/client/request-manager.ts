import {
  PendingListOffsetsRequest,
  PendingProduceRequest,
  PendingRequest,
} from "src/client/pending-request";
import { SocketManager } from "src/client/socket-manager";
import { Env } from "src/common";
import { Acks } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { Encoder } from "src/protocol/encoder";
import { RequestMetadata, encodeRequestHeader } from "src/protocol/header";
import { PartitionApiKey } from "src/protocol/internal/common";
import { encodeInternalListOffsetsRequest } from "src/protocol/internal/list-offsets";
import { encodeInternalProduceRequest } from "src/protocol/internal/produce";
import {
  KafkaListOffsetsRequest,
  KafkaListOffsetsResponse,
} from "src/protocol/kafka/list-offsets";
import {
  KafkaProduceRequest,
  KafkaProduceResponse,
} from "src/protocol/kafka/produce";
import { PartitionInfo } from "src/state/partition";

type CorrelationId = number;

const abortedRequestError = new Error("Request aborted");

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
    request: KafkaProduceRequest
  ): Promise<KafkaProduceResponse | null> {
    return new Promise<KafkaProduceResponse | null>((resolve, reject) => {
      if (request.acks !== Acks.None) {
        // If the request will return a response, resolve the promise when the
        // response is complete
        const done = (response: KafkaProduceResponse) => {
          this.pending.delete(metadata.correlationId);
          resolve(response);
        };
        const abort = () => {
          this.pending.delete(metadata.correlationId);
          reject(abortedRequestError);
        };
        this.pending.set(
          metadata.correlationId,
          new PendingProduceRequest(request, done, abort)
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

            const partitionRequest = encodeInternalProduceRequest(encoder, {
              acks: request.acks,
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

  listOffsetsRequest(
    metadata: RequestMetadata,
    request: KafkaListOffsetsRequest
  ): Promise<KafkaListOffsetsResponse> {
    return new Promise<KafkaListOffsetsResponse>((resolve, reject) => {
      const done = (response: KafkaListOffsetsResponse) => {
        this.pending.delete(metadata.correlationId);
        resolve(response);
      };
      const abort = () => {
        this.pending.delete(metadata.correlationId);
        reject(abortedRequestError);
      };
      this.pending.set(
        metadata.correlationId,
        new PendingListOffsetsRequest(request, done, abort)
      );

      Promise.all(
        request.topics.flatMap((topic) =>
          topic.partitions.map(async (partition) => {
            const encoder = new Encoder();
            encodeRequestHeader(encoder, {
              apiKey: PartitionApiKey.ListOffsets,
              apiVersion: 0,
              correlationId: metadata.correlationId,
              clientId: metadata.clientId,
            });

            const partitionRequest = encodeInternalListOffsetsRequest(encoder, {
              timestamp: partition.timestamp,
              maxNumOffsets: partition.maxNumOffsets,
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

  handlePartitionClose(partition: PartitionInfo): void {
    this.pending.forEach((request) => request.handlePartitionClose(partition));
  }

  close() {
    this.pending.forEach((request) => request.abort());
    this.socket.close();
  }
}
