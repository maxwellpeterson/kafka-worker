# Internal Protocol

This folder contains files related to the internal wire protocol used for sending messages between the gateway worker and Durable Objects. This protocol uses the same primitives and message structure as the Kafka protocol.

Additionally, each internal message type corresponds one-to-one with a Kafka message type of the same name. In most cases, these internal messages contain a subset of the corresponding Kafka message that is scoped to a single partition. Since Kafka messages can address multiple partitions, each Kafka message can be split apart into many internal messages of the same type, with one internal message for each partition.

Each file contains message type definitions and encoding and decoding functions for one of the internal APIs, which again correspond one-to-one with Kafka APIs of the same name. The exception is `common.ts`, which contains code shared across multiple internal APIs.
