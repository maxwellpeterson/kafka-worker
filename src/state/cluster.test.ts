import { fetchClusterMetadata } from "src/state/cluster";

describe("fetchClusterMetadata", () => {
  const env = getMiniflareBindings();

  test("returns static metadata when fetching all topics", async () => {
    const response = await fetchClusterMetadata(env, []);
    expect(response).toMatchInlineSnapshot(`
      Object {
        "brokers": Array [
          Object {
            "host": "localhost",
            "nodeId": 333,
            "port": 8787,
          },
        ],
        "topics": Array [
          Object {
            "errorCode": 0,
            "name": "test-topic",
            "partitions": Array [
              Object {
                "errorCode": 0,
                "isrNodes": Array [],
                "leaderId": 333,
                "partitionIndex": 999,
                "replicaNodes": Array [],
              },
            ],
          },
        ],
      }
    `);
  });
});
