import { Env } from "src/common";

export const partitionStubUrl = "https://partition.state";

export class Partition {
  private readonly state: DurableObjectState;
  private readonly env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  fetch(request: Request): Response {
    return new Response();
  }
}

export const generatePartitonId = (
  topicName: string,
  partitionIndex: number
): string => `${topicName}-${partitionIndex}`;
