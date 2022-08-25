import { Env } from "src/common";
import { MetadataResponse, TopicMetadata } from "src/protocol/api/metadata";
import { ErrorCode, Int32 } from "src/protocol/common";

const globalClusterName = "global";
const globalBroker = {
  // This is a made up number
  nodeId: 333,
  port: 443,
};

const searchParam = "topics";
const sep = ",";

export const fetchClusterMetadata = async (
  env: Env,
  topics: string[]
): Promise<MetadataResponse> => {
  const id = env.CLUSTER.idFromName(globalClusterName);
  const obj = env.CLUSTER.get(id);

  const request = new Request(
    `https://cluster.state/metadata?${searchParam}=${topics.join(sep)}`
  );
  const response = await obj.fetch(request);

  if (!response.ok) {
    const message = await response.text();
    throw new Error(`Error from Cluster DO: ${message}`);
  }

  return await response.json<MetadataResponse>();
};

// TODO: Topics could be individually keyed?
const topicStateKey = "TopicState";
const initialTopicState: TopicState = {
  topics: [{ name: "test-topic", partitions: [{ index: 999 }] }],
};

export class Cluster {
  private readonly state: DurableObjectState;
  private readonly env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const topicQuery = new URL(request.url).searchParams.get(searchParam);
    if (topicQuery === null) {
      return new Response(`Missing search param: ${searchParam}`, {
        status: 400,
      });
    }

    const topicNames = topicQuery === "" ? [] : topicQuery.split(",");
    const state =
      (await this.state.storage.get<TopicState>(topicStateKey)) ??
      initialTopicState;

    const brokers = [{ ...globalBroker, host: this.env.HOSTNAME }];

    // Empty list means return metadata for all topics
    if (topicNames.length === 0) {
      return new Response(
        JSON.stringify({ brokers, topics: state.topics.map(generateMetadata) })
      );
    }

    return new Response(
      JSON.stringify({
        brokers,
        topics: topicNames.map((topicName) => {
          const topic = state.topics.find(({ name }) => name === topicName);
          if (topic === undefined) {
            return {
              errorCode: ErrorCode.UnknownTopicOrPartition,
              name: topicName,
              partitions: null,
            };
          }
          return generateMetadata(topic);
        }),
      })
    );
  }
}

const generateMetadata = (topic: Topic): TopicMetadata => ({
  errorCode: ErrorCode.None,
  name: topic.name,
  partitions: topic.partitions.map((partition) => ({
    errorCode: ErrorCode.None,
    partitionIndex: partition.index,
    leaderId: globalBroker.nodeId,
    replicaNodes: [],
    isrNodes: [],
  })),
});

interface TopicState {
  topics: Topic[];
}

interface Topic {
  name: string;
  partitions: Partition[];
}

interface Partition {
  index: Int32;
}
