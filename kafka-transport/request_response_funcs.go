package kafka

import (
	"context"

	"github.com/Shopify/sarama"
)

// ConsumerRequestFunc may take information from a consumer request and put it into a
// request context. ConsumerRequestFuncs are executed prior to invoking the
// endpoint.
type ConsumerRequestFunc func(context.Context, *sarama.ConsumerMessage) context.Context

// ProducerRequestFunc may take information from a producer request and put it into a
// request context. ProducerRequestFuncs are executed prior to invoking the
// endpoint.
type ProducerRequestFunc func(context.Context, *sarama.ProducerMessage) context.Context

// ConsumerResponseFunc may take information from a request context and use it to
// manipulate a Producer. ConsumerResponseFuncs are only executed in
// consumers, after invoking the endpoint but prior to encoding a result.
type ConsumerResponseFunc func(context.Context, *sarama.ConsumerMessage, interface{}) context.Context

// ProducerResponseFunc may take information from an Kafka request and make the
// response available for consumption. ClientResponseFuncs are only executed in
// clients, after a request has been made, but prior to it being decoded.
type ProducerResponseFunc func(context.Context, *sarama.ProducerMessage, interface{}) context.Context
