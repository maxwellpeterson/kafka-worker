import { ElemOf, Env } from "src/common";
import { ErrorCode, Int32 } from "src/protocol/common";
import { KafkaMetadataResponse } from "src/protocol/kafka/metadata";

// These are made up values
const globalClusterName = "global";
export const globalBrokerId = 333;

const searchParam = "topics";
const sep = ",";

export const fetchClusterMetadata = async (
  env: Env,
  topics: string[]
): Promise<KafkaMetadataResponse> => {
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

  return await response.json<KafkaMetadataResponse>();
};

interface ClusterState {
  topics: {
    name: string;
    partitions: {
      index: Int32;
    }[];
  }[];
}
const initialClusterState: ClusterState = {
  topics: [{ name: "test-topic", partitions: [{ index: 0 }] }],
};
const clusterStateKey = "cluster-state";

export class Cluster {
  private readonly state: DurableObjectState;
  private readonly env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  // TODO: This should be converted to a WebSocket protocol
  async fetch(request: Request): Promise<Response> {
    const topicQuery = new URL(request.url).searchParams.get(searchParam);
    if (topicQuery === null) {
      return new Response(`Missing search param: ${searchParam}`, {
        status: 400,
      });
    }

    const topicNames = topicQuery === "" ? [] : topicQuery.split(",");
    const state =
      (await this.state.storage.get<ClusterState>(clusterStateKey)) ??
      initialClusterState;

    const brokers = [
      {
        nodeId: globalBrokerId,
        host: this.env.HOSTNAME,
        port: parseInt(this.env.PORT),
      },
    ];

    // Empty topic list means return metadata for all topics
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
              partitions: [],
            };
          }
          return generateMetadata(topic);
        }),
      })
    );
  }
}

type TopicState = ElemOf<ClusterState["topics"]>;
type TopicMetadata = ElemOf<KafkaMetadataResponse["topics"]>;

const generateMetadata = (topic: TopicState): TopicMetadata => ({
  errorCode: ErrorCode.None,
  name: topic.name,
  partitions: topic.partitions.map((partition) => ({
    errorCode: ErrorCode.None,
    partitionIndex: partition.index,
    leaderId: globalBrokerId,
    replicaNodes: [],
    isrNodes: [],
  })),
});
