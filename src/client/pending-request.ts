import {
  DoneHandler,
  IncrementalResponse,
} from "src/client/incremental-response";
import { ErrorCode } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { decodeInternalProduceResponse } from "src/protocol/internal/produce";
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

    this.response = new IncrementalResponse<KafkaProduceResponse>(
      stubResponse,
      done
    );
  }

  handlePartitionMessage(partition: PartitionInfo, decoder: Decoder): void {
    const response = decodeInternalProduceResponse(decoder);
    this.response.addPartition(partition, response);
  }
}
