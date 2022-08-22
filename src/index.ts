/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `wrangler dev src/index.ts` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `wrangler publish src/index.ts --name my-worker` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

export interface Env {
  // Example binding to KV. Learn more at https://developers.cloudflare.com/workers/runtime-apis/kv/
  // MY_KV_NAMESPACE: KVNamespace;
  //
  // Example binding to Durable Object. Learn more at https://developers.cloudflare.com/workers/runtime-apis/durable-objects/
  // MY_DURABLE_OBJECT: DurableObjectNamespace;
  //
  // Example binding to R2. Learn more at https://developers.cloudflare.com/workers/runtime-apis/r2/
  // MY_BUCKET: R2Bucket;
}

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();
    server.addEventListener("message", (event) => {
      if (typeof event.data === "string") {
        console.log("Received string data, but we want binary data!");
        return;
      }
      try {
        const response = handleRequest(env, event.data);
        server.send(response);
      } catch (e) {
        console.log(`Error while processing request: ${e}`);
      }
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};

type KafkaString = string | null;
type KafkaArray<T> = T[] | null;

interface BaseKafkaRequest {
  apiVersion: Int16;
  correlationId: Int32;
  clientId: KafkaString;
}

// int16
enum ApiKey {
  MetadataRequest = 3,
}

// int16
enum ErrorCode {
  NoError = 0,
}

type MetadataRequest = BaseKafkaRequest & {
  apiKey: ApiKey.MetadataRequest;
  message: {
    topics: KafkaArray<KafkaString>;
  };
};

type KafkaRequest = MetadataRequest;
type Message<T extends KafkaRequest> = T["message"];

// These don't add type safety, but help with labeling
type Int16 = number;
type Int32 = number;

const handleRequest = (env: Env, buffer: ArrayBuffer): ArrayBuffer => {
  const decoder = new Decoder(buffer);

  const expectedSize = decoder.readInt32();
  const actualSize = buffer.byteLength - int32Size;
  if (expectedSize !== actualSize) {
    throw new Error(
      `Message length does not match size field: expected ${expectedSize} but got ${actualSize}`
    );
  }

  const apiKey = decoder.readInt16();
  const request = {
    apiVersion: decoder.readInt16(),
    correlationId: decoder.readInt32(),
    clientId: decoder.readString(),
  };

  if (request.apiVersion !== 0) {
    throw new Error(
      `Unsupported version of metadata api: expected 0 but got ${request.apiVersion}`
    );
  }

  switch (apiKey) {
    case ApiKey.MetadataRequest:
      const message = decodeMetadataRequest(decoder);
      return handleMetadataRequest(env, { apiKey, ...request, message });
    default:
      throw new Error(`Unknown api key: ${apiKey}`);
  }
};

const decodeMetadataRequest = (decoder: Decoder): Message<MetadataRequest> => {
  return { topics: decoder.readArray(() => decoder.readString()) };
};

const brokerNodeId = 100;
const workerHost = "kafka-worker.archmap.workers.dev";
const httpsPort = 443;
const stubTopicName = "test-topic";
const stubPartitionId = 1000;

const handleMetadataRequest = (
  env: Env,
  request: MetadataRequest
): ArrayBuffer => {
  console.log(`Received metadata request: ${JSON.stringify(request, null, 2)}`);
  const response = {
    brokers: [{ nodeId: brokerNodeId, host: workerHost, port: httpsPort }],
    topicMetadata: [
      {
        topicErrorCode: ErrorCode.NoError,
        topicName: stubTopicName,
        partitionMetadata: [
          {
            partitionErrorCode: ErrorCode.NoError,
            partitionId: stubPartitionId,
            leader: brokerNodeId,
            replicas: [brokerNodeId],
            isr: [brokerNodeId],
          },
        ],
      },
    ],
  };
  console.log(
    `Sending metadata response: ${JSON.stringify(response, null, 2)}`
  );
  return encodeMetadataResponse(request.correlationId, response);
};

const initialEncodeBufferSize = 64;

const encodeMetadataResponse = (
  correlationId: Int32,
  response: MetadataResponse
): ArrayBuffer => {
  const buffer = new ArrayBuffer(initialEncodeBufferSize);
  const encoder = new Encoder(buffer);

  // TODO: This doesn't belong in this function
  encoder.writeInt32(correlationId);

  encoder.writeArray(response.brokers, (broker) => {
    encoder.writeInt32(broker.nodeId);
    encoder.writeString(broker.host);
    encoder.writeInt32(broker.port);
  });

  encoder.writeArray(response.topicMetadata, (topicMetadata) => {
    encoder.writeInt16(topicMetadata.topicErrorCode);
    encoder.writeString(topicMetadata.topicName);
    encoder.writeArray(topicMetadata.partitionMetadata, (partitionMetadata) => {
      encoder.writeInt16(partitionMetadata.partitionErrorCode);
      encoder.writeInt32(partitionMetadata.partitionId);
      encoder.writeInt32(partitionMetadata.leader);
      encoder.writeArray(partitionMetadata.replicas, (replica) =>
        encoder.writeInt32(replica)
      );
      encoder.writeArray(partitionMetadata.isr, (isr) =>
        encoder.writeInt32(isr)
      );
    });
  });

  return encoder.sizedBuffer();
};

interface MetadataResponse {
  brokers: KafkaArray<Broker>;
  topicMetadata: KafkaArray<TopicMetadata>;
}

interface Broker {
  nodeId: Int32;
  host: KafkaString;
  port: Int32;
}

interface TopicMetadata {
  topicErrorCode: Int16;
  topicName: KafkaString;
  partitionMetadata: KafkaArray<PartitionMetadata>;
}

interface PartitionMetadata {
  partitionErrorCode: Int16;
  partitionId: Int32;
  leader: Int32;
  replicas: KafkaArray<Int32>;
  isr: KafkaArray<Int32>;
}

const int16Size = 2;
const int32Size = 4;

class Decoder {
  private readonly view: DataView;
  private offset: number;

  constructor(buffer: ArrayBuffer) {
    this.view = new DataView(buffer);
    this.offset = 0;
  }

  readInt16(): Int16 {
    const value = this.view.getInt16(this.offset);
    this.offset += int16Size;
    return value;
  }

  readInt32(): Int32 {
    const value = this.view.getInt32(this.offset);
    this.offset += int32Size;
    return value;
  }

  readString(): KafkaString {
    const size = this.readInt16();
    if (size === -1) {
      return null;
    }
    const stringBuffer = this.view.buffer.slice(
      this.offset,
      this.offset + size
    );
    const value = new TextDecoder().decode(stringBuffer);
    this.offset += size;
    return value;
  }

  readArray<T>(readElement: (index: number) => T): KafkaArray<T> {
    const size = this.readInt32();
    if (size === -1) {
      return null;
    }
    console.log(`Allocating Array of size ${size}`);
    const values = new Array<T>(size);
    for (let i = 0; i < size; i++) {
      values[i] = readElement(i);
    }
    return values;
  }
}

class Encoder {
  private view: DataView;
  private offset: number;

  constructor(buffer: ArrayBuffer) {
    this.view = new DataView(buffer);
    this.offset = int32Size;
  }

  private checkCapacity(size: number) {
    const nextLength = this.offset + size;
    if (nextLength > this.view.byteLength) {
      const newCapacity = 2 * nextLength;
      console.log(`Allocating ArrayBuffer of size ${newCapacity}`);
      const newBuffer = new ArrayBuffer(newCapacity);
      // Reserved header space is preserved after copy
      new Uint8Array(newBuffer).set(new Uint8Array(this.view.buffer));
      this.view = new DataView(newBuffer);
    }
  }

  writeInt16(value: Int16) {
    this.checkCapacity(int16Size);
    this.view.setInt16(this.offset, value);
    this.offset += int16Size;
  }

  writeInt32(value: Int32) {
    this.checkCapacity(int32Size);
    this.view.setInt32(this.offset, value);
    this.offset += int32Size;
  }

  writeString(value: KafkaString) {
    if (value === null) {
      this.writeInt16(-1);
      return;
    }
    const bytes = new TextEncoder().encode(value);
    this.writeInt16(bytes.length);
    this.checkCapacity(bytes.length);
    new Uint8Array(this.view.buffer).set(bytes, this.offset);
    this.offset += bytes.length;
  }

  writeArray<T>(values: KafkaArray<T>, writeElement: (value: T) => void) {
    if (values === null) {
      this.writeInt32(-1);
      return;
    }
    console.log(`Writing array of length ${values.length}`);
    this.writeInt32(values.length);
    values.forEach(writeElement);
  }

  sizedBuffer(): ArrayBuffer {
    // Write size header to reserved space at the front of the array
    this.view.setInt32(0, this.offset - int32Size);
    return this.view.buffer.slice(0, this.offset);
  }
}
