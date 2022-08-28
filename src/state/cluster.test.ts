import { Cluster, fetchClusterMetadata } from "src/state/cluster";
import { mockDOState, mockDOStub, mockEnv } from "src/state/test-utils";

describe("fetchClusterMetadata", () => {
  const cluster = new Cluster(mockDOState(), mockEnv());

  const clusterStub = mockDOStub();
  clusterStub.fetch.mockImplementation((request) =>
    cluster.fetch(new Request(request))
  );

  const fetchEnv = mockEnv();
  fetchEnv.CLUSTER.get.mockImplementation(() => clusterStub);

  test("returns static metadata when fetching all topics", async () => {
    const response = await fetchClusterMetadata(fetchEnv, []);
    expect(response).toMatchInlineSnapshot(`
      Object {
        "brokers": Array [
          Object {
            "host": "example.com",
            "nodeId": 333,
            "port": 443,
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
