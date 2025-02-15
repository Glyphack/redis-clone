My solution to ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).
This is a Redis clone that I built. It implements part of the Redis protocol (RESP 2) and supporting several commands.

## Features

- **Basic Commands**: `GET`, `SET`, `PING`
- **Concurrency**: Supports multiple concurrent clients
- **Persistence**: Loads RDB files and serves the content
- **Replication**:
  - Performs replication handshake
  - Propagates commands to replicas
  - Supports the `WAIT` command for synchronization of replicas
- Latency of `GET` and `SET` commands are similar to original Redis server! No libraries used.

## Architecture

- Uses an **event loop**
- Runs on a **single thread**

## Usage

```
make build
./build/redis
```

Send messages using `redis-cli ping`. Try other commands.

To run a replica run:

```
./build/redis --replicaof 6379
```

## Technical Deep Dives

- Implemented binary protocol parsing
- Built a non-blocking I/O system
- Managed concurrent client connections
- Used my own hash map: https://nullprogram.com/blog/2023/09/30/
- Used memory arena for easier memory management: https://nullprogram.com/blog/2023/09/27/
- Used custom string type to avoid limitations of C strings: https://nullprogram.com/blog/2023/10/08/
- Created a master-replica synchronization mechanism
- Handled data persistence with RDB files

## What I Learned

- Network programming and protocol implementation
- Async programming in C
- Distributed systems concepts through replication
- High-performance tcp server design
- Implemented a hash map in C
- Using my custom allocator
