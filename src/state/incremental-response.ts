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

export type DoneHandler<T> = (response: T) => void;
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

    // Find the placeholder response that we want to fill in
    const stubResponse = this.response.topics
      .find((topic) => topic.name === partition.topic)
      ?.partitions.find(({ index }) => index === partition.index);

    if (stubResponse === undefined) {
      // This should be an unreachable state
      return;
    }

    // Fill in the matching placeholder with the actual response, and remove the
    // partition from the pending set
    Object.assign(stubResponse, { ...response, index: partition.index });
    this.pendingPartitions.delete(partition.id);

    // If all partitions are settled (this was the last one), then the response
    // is complete. Done cannot be called again since the pending set is empty
    if (this.pendingPartitions.size === 0) {
      this.done(this.response);
    }
  }
}
