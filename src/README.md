# Source

This folder contains all source files, including tests.

The `client` folder contains files related to managing client connections. This includes converting Kafka API requests into internal API requests, converting internal API responses into Kafka API responses, forwarding internal API requests to Durable Objects, and handling the fan out of internal requests and fan in of internal responses.

The `protocol` folder contains files related to the Kafka wire protocol and internal wire protocol. This includes API request and response type definitions and encoding and decoding functions. All this code does is convert JavaScript objects to ArrayBuffers, and vice-versa, so it's not super interesting.

The `state` folder contains files related to persistent state stored in Durable Objects. This includes records stored in partitions, as well as information about which topics and partitions exist. The code in the `client` folder interacts with the code in the `state` folder by making an HTTP request or sending a WebSocket message to one of these Durable Objects.

The `common.ts` file contains general-purpose types and functions that are used across the entire project.

The `index.ts` file contains the entrypoint to the gateway worker, which creates WebSocket connections with clients and sets up a new session for each connection. All messages exchanged with the client flow through the `fetch` handler in this file. The high-level code path for handling a Kafka API request is `index.ts` :arrow_right: `client` :arrow_right: `state` :arrow_right: `client` :arrow_right: `index.ts`, with code from the `protocol` folder used in both `client` and `state`.
