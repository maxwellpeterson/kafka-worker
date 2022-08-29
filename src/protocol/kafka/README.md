# Kafka Protocol

This folder contains files related to the Kafka wire protocol, as described in [A Guide To The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol) and the [Kafka Protocol Guide](https://kafka.apache.org/protocol.html). These two references contain much of the same information, but neither is a strict subset of the other. _A Guide To The Kafka Protocol_ is older, shorter, more readable, and contains almost all the information needed to implement message encoding and decoding for our (somewhat ancient) version of Kafka.

Each file contains message type definitions and encoding and decoding functions for one of the Kafka APIs, with the exception of `common.ts`, which contains code shared across multiple Kafka APIs.
