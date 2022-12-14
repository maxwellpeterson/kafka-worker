import {
  DoneHandler,
  IncrementalResponse,
} from "src/client/incremental-response";
import { ErrorCode } from "src/protocol/common";
import { Decoder } from "src/protocol/decoder";
import { decodeInternalFetchResponse } from "src/protocol/internal/fetch";
import { decodeInternalListOffsetsResponse } from "src/protocol/internal/list-offsets";
import { decodeInternalProduceResponse } from "src/protocol/internal/produce";
import {
  KafkaFetchRequest,
  KafkaFetchResponse,
  stubKafkaFetchResponse,
} from "src/protocol/kafka/fetch";
import {
  KafkaListOffsetsRequest,
  KafkaListOffsetsResponse,
  stubKafkaListOffsetsResponse,
} from "src/protocol/kafka/list-offsets";
import {
  KafkaProduceRequest,
  KafkaProduceResponse,
  stubKafkaProduceResponse,
} from "src/protocol/kafka/produce";
import { PartitionInfo } from "src/state/partition";

// Represents a client request that is waiting on subresponses from Partition
// DOs. These subresponses arrive asynchronously as WebSocket messages, and need
// to be pieced together into a complete response that is sent back to the client.
export interface PendingRequest {
  handlePartitionMessage(partition: PartitionInfo, decoder: Decoder): void;
  // If request does not depend on given partition, this should be a no-op
  handlePartitionClose(partition: PartitionInfo): void;
  abort(): void;
}

export class PendingProduceRequest {
  private readonly response: IncrementalResponse<KafkaProduceResponse>;
  readonly abort: () => void;

  constructor(
    request: KafkaProduceRequest,
    done: DoneHandler<KafkaProduceResponse>,
    abort: () => void
  ) {
    const timeoutId = setTimeout(() => {
      this.response.cancel({
        errorCode: ErrorCode.RequestTimedOut,
        baseOffset: BigInt(0),
      });
    }, request.timeoutMs);

    this.response = new IncrementalResponse<KafkaProduceResponse>(
      stubKafkaProduceResponse(request, ErrorCode.None),
      (response) => {
        clearTimeout(timeoutId);
        done(response);
      }
    );
    this.abort = () => {
      clearTimeout(timeoutId);
      abort();
    };
  }

  handlePartitionMessage(partition: PartitionInfo, decoder: Decoder): void {
    const response = decodeInternalProduceResponse(decoder);
    this.response.addPartition(partition, response);
  }

  handlePartitionClose(partition: PartitionInfo): void {
    this.response.addPartition(partition, {
      errorCode: ErrorCode.NetworkException,
      baseOffset: BigInt(0),
    });
  }
}

export class PendingFetchRequest {
  private readonly response: IncrementalResponse<KafkaFetchResponse>;
  readonly abort: () => void;

  constructor(
    request: KafkaFetchRequest,
    done: DoneHandler<KafkaFetchResponse>,
    abort: () => void
  ) {
    this.response = new IncrementalResponse<KafkaFetchResponse>(
      stubKafkaFetchResponse(request, ErrorCode.None),
      done
    );
    this.abort = abort;
  }

  handlePartitionMessage(partition: PartitionInfo, decoder: Decoder): void {
    const response = decodeInternalFetchResponse(decoder);
    this.response.addPartition(partition, response);
  }

  handlePartitionClose(partition: PartitionInfo): void {
    this.response.addPartition(partition, {
      errorCode: ErrorCode.NetworkException,
      highWatermark: BigInt(0),
      messageSet: new Uint8Array(),
    });
  }
}

export class PendingListOffsetsRequest {
  private readonly response: IncrementalResponse<KafkaListOffsetsResponse>;
  readonly abort: () => void;

  constructor(
    request: KafkaListOffsetsRequest,
    done: DoneHandler<KafkaListOffsetsResponse>,
    abort: () => void
  ) {
    this.response = new IncrementalResponse<KafkaListOffsetsResponse>(
      stubKafkaListOffsetsResponse(request, ErrorCode.None),
      done
    );
    this.abort = abort;
  }

  handlePartitionMessage(partition: PartitionInfo, decoder: Decoder): void {
    const response = decodeInternalListOffsetsResponse(decoder);
    this.response.addPartition(partition, response);
  }

  handlePartitionClose(partition: PartitionInfo): void {
    this.response.addPartition(partition, {
      errorCode: ErrorCode.NetworkException,
      oldStyleOffsets: [],
    });
  }
}
