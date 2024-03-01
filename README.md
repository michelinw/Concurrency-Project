# Concurrency Project - Tributary

# Introduction
Welcome to Tributary 2.0, a Java API designed to facilitate the implementation of Event-Driven Architecture in modern software systems. In this project, I  have creating a powerful library that enables scalable asynchronous communication between software components. This library is inspired by the event streaming infrastructure Apache Kafka, offering a simplified yet robust alternative.

# Engineering Requirements
## Fundamental Premise
The core of Event-Driven Architecture lies in the asynchronous sharing of data between producer and consumer entities through stream-like channels. The Tributary library takes this a step further by introducing the concept of Tributary Clusters, which contain multiple topics, each logically grouping related events.

## Tributary Cluster Structure
1. Topics: A Tributary Cluster consists of topics, each representing a logical grouping of events. For instance, a cluster might have separate topics for images and videos.
2. Partitions: Within each topic, partitions exist as collections of sequenced messages. Partitions enable efficient handling of concurrent events. Think of them as queues where messages are appended at the end. Each message can have an optional key indicating the target partition.
3. Messages: Messages, also known as records or events, are the fundamental unit of data within a Tributary. For example, a user updating their profile generates a message appended to a specific partition in a topic.

## Handling Concurrent Requests
1. Consumer Handling: While multiple consumers can concurrently consume messages, each partition is managed by only one consumer. This design allows effective utilization of underlying hardware with multiple cores.
2. Parallelism: Multiple consumers enable parallel processing, enhancing the overall efficiency of the system.

## Generic Typing
In the library, topics are parameterized on a generic type. All event payloads within a specific topic must adhere to the specified type.

# Development Stages
## Stage 1: Framework Initialization
Start by initializing the Tributary framework, setting up the foundation for topic creation and management.

## Stage 2: Topic and Partition Management
Implement functionality for creating and managing topics and partitions within the Tributary Cluster.

## Stage 3: Asynchronous Communication
Establish mechanisms for asynchronous communication between producers and consumers, allowing seamless event streaming.

## Stage 4: Generic Typing Implementation
Enforce the parameterization of topics with a generic type, ensuring type safety within each topic.

# Key Features
- Tributary Clusters for logical event grouping
- Efficient partitioning for handling concurrent events
- Asynchronous communication between producers and consumers
- Support for multiple consumers to enhance parallelism
- Generic typing for type-safe event handling within topics

# Reflections
## Challenges and Solutions
Address challenges encountered during the development, such as managing concurrent consumers and ensuring type safety within topics.

## Soft Skills Demonstrated
Problem-solving in the context of asynchronous communication
Collaboration in handling parallel processing challenges

## Learning
Gain insights into the complexities of Event-Driven Architecture, particularly in handling concurrent events and ensuring generic typing.

## Techincal Knowledge
Enhance your understanding of building scalable and efficient event-driven systems in Java.

# Conclusion
Tributary 2.0 stands as a robust Java library, providing a foundation for the implementation of Event-Driven Architecture. By incorporating the principles discussed in lectures and offering a simplified version inspired by Apache Kafka, this library empowers engineers to build scalable and asynchronous systems with ease. The journey through framework initialization, topic management, and asynchronous communication reflects the dedication to creating a versatile and developer-friendly tool.
