import { Env } from "src/common";
import {
  MetadataRequest,
  MetadataResponse,
} from "src/protocol/api/metadata/types";
import { KafkaArray, KafkaString } from "src/protocol/common";
import { fetchClusterMetadata } from "src/state/cluster";

export const handleMetadataRequest = async (
  env: Env,
  request: MetadataRequest
): Promise<MetadataResponse> => {
  return await fetchClusterMetadata(env, cleanTopics(request.topics));
};

const cleanTopics = (topics: KafkaArray<KafkaString>): string[] => {
  if (topics === null) {
    return [];
  }
  return topics.filter((topic): topic is string => topic !== null);
};
