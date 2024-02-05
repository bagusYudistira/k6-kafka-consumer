# k6-kafka-consumer

K6 extension to test application consumer.

## requirement

- Go lang v1.17+
  ```bash
  brew install go
  ```
- xk6
  ```bash
  go install go.k6.io/xk6/cmd/xk6@latest
  ```

## build and test

- build the project
  ```bash
  xk6 build  --with github.com/telflow/xk6-kafka-consumer=.
  ```
- test the script
  ```bash
  ./k6 run example/test.js
  ```
