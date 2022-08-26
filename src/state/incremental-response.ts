import { ElemOf } from "src/common";
import { PartitionInfo } from "src/state/partition";

interface BaseResponse {
  topics: {
    name: string;
    partitions: {
      index: number;
    }[];
  }[];
}

type DoneHandler<T> = (response: T) => void;
type PartitionId = string;

type PartitionResponse<T extends BaseResponse> = Omit<
  ElemOf<ElemOf<T["topics"]>["partitions"]>,
  "index"
>;

export class IncrementalResponse<T extends BaseResponse> {
  private readonly response: T;
  private readonly done: DoneHandler<T>;

  private readonly pendingPartitions: Set<PartitionId>;

  constructor(stubResponse: T, done: DoneHandler<T>) {
    this.response = stubResponse;
    this.done = done;

    const partitionIds = stubResponse.topics.flatMap((topic) =>
      topic.partitions.map(
        (partition) => new PartitionInfo(topic.name, partition.index).id
      )
    );
    this.pendingPartitions = new Set<PartitionId>(partitionIds);
  }

  addPartition(partition: PartitionInfo, response: PartitionResponse<T>) {
    if (!this.pendingPartitions.has(partition.id)) {
      console.log("Received Partition response from non-pending partition");
      return;
    }

    const topic = this.response.topics.find(
      (topic) => topic.name === partition.topic
    );
    if (topic === undefined) {
      // This should be an unreachable state
      return;
    }

    const target = topic.partitions.findIndex(
      ({ index }) => index === partition.index
    );
    if (target === -1) {
      // This should be an unreachable state
      return;
    }

    topic.partitions[target] = { ...response, index: partition.index };
    this.pendingPartitions.delete(partition.id);

    if (this.pendingPartitions.size === 0) {
      this.done(this.response);
    }
  }
}
