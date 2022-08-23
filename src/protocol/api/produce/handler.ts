import { Env } from "src/common";
import {
  ProduceRequest,
  ProduceResponse,
} from "src/protocol/api/produce/types";

export const handleProduceRequest = async (
  env: Env,
  request: ProduceRequest
): Promise<ProduceResponse | null> => {
  return null;
};
