[![progress-banner](https://backend.codecrafters.io/progress/redis/94d9788d-020a-4f54-b813-2fcd07efd8ab)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

My solution to ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

## Usage

```
make run ARGS="--port 8080"
```

In another window:

```
redis-cli SET key val px 10000
redis-cli GET key
```
