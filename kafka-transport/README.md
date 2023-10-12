# Go-kit Kafka Transport

This package provides support for Kafka as a `go-kit` transport layer.

## Usage

Install using one of the following:
- Using `go get`: `go get github.com/khakiem/go-kit-plugins/kafka-transport@v0.0.1`.
- In Go codes: `import "github.com/khakiem/go-kit-plugins/kafka-transport"`, then run `go mod tidy`.
- In the `go.mod` file:
```go
require (
	github.com/khakiem/go-kit-plugins/kafka-transport v0.0.1
)
```

Example of consumer (error handlings are omitted for brevity):
```go
package main

import (
	"context"

	"github.com/Shopify/sarama"
	kafka "github.com/khakiem/go-kit-plugins/kafka-transport"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, _ := sarama.NewClient([]string{"broker1.host"}, &sarama.Config{})
	consumerGroup, _ := sarama.NewConsumerGroupFromClient("dummy.group.id", client)
	handler := kafka.NewConsumer(
		func(ctx context.Context, request interface{}) (response interface{}, err error) {
			// business logic here
			return nil, nil
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage) (interface{}, error) {
			// decode message
			return nil, nil
		},
		nil,
	)

	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		err := consumerGroup.Consume(ctx, []string{"topic.name"}, handler)
		if err != nil {
			return
		}

		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return
		}
	}
}
```
