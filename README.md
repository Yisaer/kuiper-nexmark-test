# kuiper-nexmark-test
 
kuiper-nexmark-test will create the nexmark rule into eKuiper, then generate and send the data through emqx or other mqtt broker.

## params

1. `host`, the host of the ekuiper, default `127.0.0.1`
2. `port`, the port of the ekuiper, default `9081`
3. `broker`, the host of the mqtt broker, default `127.0.0.1`
4. `brokerPort`, the port of the mqtt broker, default `1883`
5. `duration`, the duration of the testcase running, default `1m`
6. `qps`, the qps for test tool sending data to the broker, default `10`
7. `queries`, the supported queries for the nexmark


## Running Single test

```shell
./main --host <ekuiperHost> --port <ekuiperPort> --broker <brokerHost> --brokerPort <brokerPort> --duration 1m --qps 1000 --quest "q1"
```

## Running multiple quries
```shell
./main --host <ekuiperHost> --port <ekuiperPort> --broker <brokerHost> --brokerPort <brokerPort> --duration 1m --qps 1000 --quest "q1,q2"
```
