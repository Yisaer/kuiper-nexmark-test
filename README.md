# kuiper-nexmark-test
 
kuiper-nexmark-test will create the nexmark rule into eKuiper, then generate and send the data through emqx or other mqtt broker.

## params

1. `host`, the host of the ekuiper, default `127.0.0.1`
2. `port`, the port of the ekuiper, default `9081`
3. `broker`, the host of the mqtt broker, default `127.0.0.1`
4. `brokerPort`, the port of the mqtt broker, default `1883`
5. `duration`, the duration of the testcase running, default `5s`
6. `qps`, the qps for test tool sending data to the broker, default `10`
7. `queries`, the supported queries for the nexmark

## Build

```shell
make build
```

## Running Single query for Quick Start

```shell
./kuiper-nexmark-test --duration 10s --qps 10 --queries "q1"
```

## Running multiple queries

```shell
./kuiper-nexmark-test  --duration 10s --qps 10 --queries "q1,q2"
```

## Running multiple queries in parallel

```shell
./kuiper-nexmark-test  --duration 10s --qps 1000 --queries "q1,q2" --parallel true
```

## Supported Query

q1,q2,q3,q4,q6,q7,q8,q9,q10