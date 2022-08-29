import { ElemOf } from "src/common";
import { PartitionInfo } from "src/state/partition";

// This is an extremely common response structure in the Kafka protocol
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

export type PartitionResponse<T extends BaseResponse> = Omit<
  ElemOf<ElemOf<T["topics"]>["partitions"]>,
  "index"
>;

// Represents a response that is filled in asynchronously from multiple
// Partition DO subresponses. Keeps track of the partitions that haven't
// responded yet, and executes a callback when the response is complete (this
// callback usually resolves a Promise<T>)
//
// The constructor takes a "template" response that is filled in one "template
// slot" at a time by each subresponse. You can think of this template response
// as a tree with blank leaves, and each subresponse fills in a new leaf that
// was previously blank. When all the leaves have been filled in, the tree is
// complete and can be returned. Note that no leaves are created or removed as
// part of this process, only updated in place.
export class IncrementalResponse<T extends BaseResponse> {
  private readonly response: T;
  private readonly done: DoneHandler<T>;

  private readonly pendingPartitions: Set<PartitionId>;

  constructor(stubResponse: T, done: DoneHandler<T>) {
    this.response = stubResponse;
    this.done = done;

    // Assumes that all (topic.name, partition.index) pairs are unique. I'm not
    // sure if this uniqueness constraint is officially part of the Kafka
    // protocol, but it doesn't make sense for duplicate pairs to appear in the
    // same request (unless you wanted to consume the same partition at different
    // offsets, which I don't think is possible)
    const partitionIds = stubResponse.topics.flatMap((topic) =>
      topic.partitions.map(
        (partition) => new PartitionInfo(topic.name, partition.index).id
      )
    );
    this.pendingPartitions = new Set<PartitionId>(partitionIds);

    // Make sure we don't block forever if the template response is already complete
    this.checkDone();
  }

  addPartition(partition: PartitionInfo, response: PartitionResponse<T>) {
    if (!this.pendingPartitions.has(partition.id)) {
      // Partition is no longer pending, discard response
      return;
    }

    // Find the placeholder subresponse that we want to fill in
    const stubResponse = this.response.topics
      .find((topic) => topic.name === partition.topic)
      ?.partitions.find(({ index }) => index === partition.index);

    if (stubResponse === undefined) {
      // This should be an unreachable state
      return;
    }

    // Fill in the matching placeholder with the actual subresponse, remove
    // the partition from the pending set, and check if this was the last one
    Object.assign(stubResponse, { ...response, index: partition.index });
    this.pendingPartitions.delete(partition.id);
    this.checkDone();
  }

  private checkDone() {
    // If all partitions are settled, then the response is complete. Done cannot
    // be called again because the pending set only decreases in size over time
    if (this.pendingPartitions.size === 0) {
      this.done(this.response);
    }
  }
}
