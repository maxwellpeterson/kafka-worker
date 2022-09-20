import {
  DoneHandler,
  IncrementalResponse,
} from "src/client/incremental-response";
import { ErrorCode } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { decodeInternalListOffsetsResponse } from "src/protocol/internal/list-offsets";
import { decodeInternalProduceResponse } from "src/protocol/internal/produce";
import {
  KafkaListOffsetsRequest,
  KafkaListOffsetsResponse,
} from "src/protocol/kafka/list-offsets";
import {
  KafkaProduceRequest,
  KafkaProduceResponse,
} from "src/protocol/kafka/produce";
import { PartitionInfo } from "src/state/partition";

// Represents a client request that is waiting on subresponses from Partition
// DOs. These subresponses arrive asynchronously as WebSocket messages, and need
// to be pieced together into a complete response that is sent back to the client.
export interface PendingRequest {
  handlePartitionMessage(partition: PartitionInfo, decoder: Decoder): void;
  // If request does not depend on given partition, this should be a no-op
  handlePartitionClose(partition: PartitionInfo): void;
}

export class PendingProduceRequest {
  private readonly response: IncrementalResponse<KafkaProduceResponse>;

  constructor(
    request: KafkaProduceRequest,
    done: DoneHandler<KafkaProduceResponse>
  ) {
    const stubResponse: KafkaProduceResponse = {
      topics: request.topics.map((topic) => ({
        name: topic.name,
        partitions: topic.partitions.map((partition) => ({
          index: partition.index,
          errorCode: ErrorCode.None,
          baseOffset: BigInt(0),
        })),
      })),
    };

    const timeoutId = setTimeout(() => {
      this.response.cancel({
        errorCode: ErrorCode.RequestTimedOut,
        baseOffset: BigInt(0),
      });
    }, request.timeoutMs);

    this.response = new IncrementalResponse<KafkaProduceResponse>(
      stubResponse,
      (response) => {
        clearTimeout(timeoutId);
        done(response);
      }
    );
  }

  handlePartitionMessage(partition: PartitionInfo, decoder: Decoder): void {
    const response = decodeInternalProduceResponse(decoder);
    this.response.addPartition(partition, response);
  }

  handlePartitionClose(partition: PartitionInfo): void {
    this.response.addPartition(partition, {
      errorCode: ErrorCode.UnknownServerError,
      baseOffset: BigInt(0),
    });
  }
}

export class PendingListOffsetsRequest {
  private readonly response: IncrementalResponse<KafkaListOffsetsResponse>;

  constructor(
    request: KafkaListOffsetsRequest,
    done: DoneHandler<KafkaListOffsetsResponse>
  ) {
    const stubResponse: KafkaListOffsetsResponse = {
      topics: request.topics.map((topic) => ({
        name: topic.name,
        partitions: topic.partitions.map((partition) => ({
          index: partition.index,
          errorCode: ErrorCode.None,
          oldStyleOffsets: [],
        })),
      })),
    };

    this.response = new IncrementalResponse<KafkaListOffsetsResponse>(
      stubResponse,
      done
    );
  }

  handlePartitionMessage(partition: PartitionInfo, decoder: Decoder): void {
    const response = decodeInternalListOffsetsResponse(decoder);
    this.response.addPartition(partition, response);
  }

  handlePartitionClose(partition: PartitionInfo): void {
    this.response.addPartition(partition, {
      errorCode: ErrorCode.UnknownServerError,
      oldStyleOffsets: [],
    });
  }
}
