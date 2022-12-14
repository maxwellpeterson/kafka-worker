import { ValueOf } from "src/common";
import {
  ApiKey,
  Int32,
  generateEnumPredicate,
  int32Size,
} from "src/protocol/common";
import { Encoder } from "src/protocol/encoder";

// PartitionApiKey is an Int16
export const PartitionApiKey = {
  Produce: ApiKey.Produce,
  Fetch: ApiKey.Fetch,
  ListOffsets: ApiKey.ListOffsets,
} as const;
export type PartitionApiKey = ValueOf<typeof PartitionApiKey>;
export const validPartitionApiKey = generateEnumPredicate(PartitionApiKey);

export class InternalResponseEncoder extends Encoder {
  constructor(correlationId: Int32, initialBufferSize = 64) {
    super(initialBufferSize + int32Size);
    this.writeInt32(correlationId);
  }
}
