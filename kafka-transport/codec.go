package kafka

import (
	"context"

	"github.com/Shopify/sarama"
)

// DecodeRequestFunc extracts a user-domain request object from a producer
// request object. It's designed to be used in Kafka consumers, for consumer-side
// endpoints. One straightforward DecodeRequestFunc could be something that
// JSON decodes from the request body to the concrete response type.
type DecodeRequestFunc func(context.Context, *sarama.ConsumerMessage) (interface{}, error)

// EncodeRequestFunc encodes the passed request object into the Kafka request
// object. It's designed to be used in Kafka producers, for producer-side
// endpoints. One straightforward EncodeRequestFunc could something that JSON
// encodes the object directly to the request payload.
type EncodeRequestFunc func(context.Context, *sarama.ProducerMessage, interface{}) error

// EncodeResponseFunc encodes the passed response object to the consumer result.
// It's designed to be used in Kafka consumers, for consumer-side
// endpoints. One straightforward EncodeResponseFunc could be something that
// JSON encodes the object directly to the response body.
type EncodeResponseFunc func(context.Context, *sarama.ConsumerMessage, interface{}) error

// DecodeResponseFunc extracts a user-domain response object from an Kafka
// response object. It's designed to be used in Kafka producer, for producer-side
// endpoints. One straightforward DecodeResponseFunc could be something that
// JSON decodes from the response payload to the concrete response type.
type DecodeResponseFunc func(context.Context, interface{}) (interface{}, error)
