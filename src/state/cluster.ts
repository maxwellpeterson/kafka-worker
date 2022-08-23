import { Env } from "src/common";
import { Broker, MetadataResponse } from "src/protocol/api/metadata/types";
import { ErrorCode, Int32 } from "src/protocol/common";

const globalClusterName = "global";
const globalBroker: Broker = {
  nodeId: 333,
  // TODO: Make this an env var
  host: "kafka-worker.archmap.workers.dev",
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
    // Note: Fake url scheme must be http or https?
    `https://cluster.state/metadata?${searchParam}=${topics.join(sep)}`
  );
  const response = await obj.fetch(request);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(`Error from Cluster DO: ${message}`);
  }
  const state = await response.json<TopicState>();

  return {
    brokers: [globalBroker],
    topics: topics.map((topic) => {
      const topicState = state.topics.find(({ name }) => name === topic);
      if (topicState === undefined) {
        return {
          topicErrorCode: ErrorCode.UnknownTopicOrPartition,
          topicName: topic,
          partitions: null,
        };
      }
      return {
        topicErrorCode: ErrorCode.None,
        topicName: topic,
        partitions: topicState.partitions.map((partition) => ({
          partitionErrorCode: ErrorCode.None,
          partitionId: partition.id,
          leader: globalBroker.nodeId,
          replicas: [],
          isr: [],
        })),
      };
    }),
  };
};

const topicStateKey = "TopicState";
const initialTopicState: TopicState = {
  topics: [{ name: "test-topic", partitions: [{ id: 999 }] }],
};

export class Cluster {
  private readonly state: DurableObjectState;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.state.blockConcurrencyWhile(async () =>
      this.state.storage.put<TopicState>(topicStateKey, initialTopicState)
    );
  }

  async fetch(request: Request): Promise<Response> {
    const topicQuery = new URL(request.url).searchParams.get(searchParam);
    if (topicQuery === null) {
      return new Response(`Missing search param: ${searchParam}`, {
        status: 400,
      });
    }

    const topics = topicQuery === "" ? [] : topicQuery.split(",");
    const topicState = await this.state.storage.get<TopicState>(topicStateKey);
    if (topicState === undefined) {
      return new Response("Couldn't load topic state", { status: 500 });
    }

    // Empty list means return info on all topics
    if (topics.length === 0) {
      return new Response(JSON.stringify(topicState));
    }
    return new Response(
      JSON.stringify({
        topics: topicState.topics.filter(({ name }) => topics.includes(name)),
      })
    );
  }
}

interface TopicState {
  topics: Topic[];
}

interface Topic {
  name: string;
  partitions: Partition[];
}

interface Partition {
  id: Int32;
}
