[![progress-banner](https://backend.codecrafters.io/progress/redis/94d9788d-020a-4f54-b813-2fcd07efd8ab)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

My solution to ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).
This is a Redis clone that I built, implementing part of the Redis protocol (RESP2) and supporting several commands.  

## Features  

- **Basic Commands**: `GET`, `SET`, `PING`  
- **Concurrency**: Supports multiple concurrent clients  
- **Persistence**: Loads RDB files and serves the content  
- **Replication**:  
  - Performs replication handshake  
  - Propagates commands to replicas  
  - Supports the `WAIT` command  

## Architecture  

- Uses an **event loop**  
- Runs on a **single thread**  


## Usage
