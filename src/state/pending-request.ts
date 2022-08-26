import { ProduceRequest, ProduceResponse } from "src/protocol/api/produce";
import { ErrorCode } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { decodePartitionProduceResponse } from "src/protocol/internal/partition";
import {
  DoneHandler,
  IncrementalResponse,
} from "src/state/incremental-response";
import { PartitionInfo } from "src/state/partition";

export interface PendingRequest {
  handlePartitionMessage(partition: PartitionInfo, decoder: Decoder): void;
}

export class PendingProduceRequest {
  private readonly response: IncrementalResponse<ProduceResponse>;

  constructor(request: ProduceRequest, done: DoneHandler<ProduceResponse>) {
    const stubResponse: ProduceResponse = {
      topics: request.topics.map((topic) => ({
        name: topic.name,
        partitions: topic.partitions.map((partition) => ({
          index: partition.index,
          errorCode: ErrorCode.None,
          baseOffset: BigInt(0),
        })),
      })),
    };

    this.response = new IncrementalResponse(stubResponse, done);
  }

  handlePartitionMessage(partition: PartitionInfo, decoder: Decoder): void {
    const response = decodePartitionProduceResponse(decoder);
    this.response.addPartition(partition, response);
  }
}
