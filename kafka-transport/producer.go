package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/transport"
	"github.com/go-kit/log"
)

// Producer wraps an endpoint and provides sarama.SyncProducer.
type Producer struct {
	producer     sarama.SyncProducer
	topic        string
	dec          DecodeResponseFunc
	enc          EncodeRequestFunc
	before       []ProducerRequestFunc
	after        []ProducerResponseFunc
	around       []ProducerMiddleware
	errorHandler transport.ErrorHandler
}

// NewProducer constructs a new producer, which provides sarama.SyncProducer and wraps
// the provided endpoint.
func NewProducer(
	syncProducer sarama.SyncProducer,
	topic string,
	enc EncodeRequestFunc,
	dec DecodeResponseFunc,
	options ...ProducerOption,
) *Producer {
	producer := &Producer{
		producer:     syncProducer,
		topic:        topic,
		enc:          enc,
		dec:          dec,
		errorHandler: transport.NewLogErrorHandler(log.NewNopLogger()),
	}

	for _, option := range options {
		option(producer)
	}

	return producer
}

// ProducerOption sets an optional parameter for producers.
type ProducerOption func(*Producer)

// ProducerBefore functions are executed after the request is encoded,
// but before the request is sent.
func ProducerBefore(before ...ProducerRequestFunc) ProducerOption {
	return func(p *Producer) { p.before = append(p.before, before...) }
}

// ProducerAfter functions are executed after the
// endpoint is invoked, but before response decoding.
func ProducerAfter(after ...ProducerResponseFunc) ProducerOption {
	return func(p *Producer) { p.after = append(p.after, after...) }
}

// ProducerAround functions are executed around the actual produce function.
// They act as middlewares that can be used to measure execution time for example.
func ProducerAround(around ...ProducerMiddleware) ProducerOption {
	return func(p *Producer) { p.around = append(p.around, around...) }
}

// ProducerErrorHandler is used to handle non-terminal errors. By default, non-terminal errors
// are ignored. This is intended as a diagnostic measure. Finer-grained control
// of error handling, including logging in more detail, should be performed in a
// custom ProducerErrorEncoder which has access to the context.
func ProducerErrorHandler(errorHandler transport.ErrorHandler) ProducerOption {
	return func(p *Producer) { p.errorHandler = errorHandler }
}

// Endpoint returns a usable endpoint that invokes the remote endpoint.
func (p Producer) Endpoint() endpoint.Endpoint {
	producerEndpoint := func(ctx context.Context, request interface{}) (interface{}, error) {
		msg := &sarama.ProducerMessage{Topic: p.topic}

		if err := p.enc(ctx, msg, request); err != nil {
			p.errorHandler.Handle(ctx, err)

			return nil, err
		}

		for _, f := range p.before {
			ctx = f(ctx, msg)
		}

		partition, offset, err := p.producer.SendMessage(msg)
		if err != nil {
			p.errorHandler.Handle(ctx, err)

			return nil, err //nolint:wrapcheck
		}

		response := ProducerResponse{
			Partition: partition,
			Offset:    offset,
		}

		for _, f := range p.after {
			ctx = f(ctx, msg, &response)
		}

		if p.dec == nil {
			return response, nil
		}

		decoded, err := p.dec(ctx, response)
		if err != nil {
			p.errorHandler.Handle(ctx, err)

			return nil, err
		}

		return decoded, nil
	}

	// reverse the order of middleware execution
	for i := len(p.around) - 1; i >= 0; i-- {
		producerEndpoint = p.around[i](producerEndpoint)
	}

	return producerEndpoint
}

// ProducerResponse represent minimalistic producer response that contains
// the partition and the offset of the produced message.
type ProducerResponse struct {
	Partition int32
	Offset    int64
}

// ProducerMiddleware can be used to wrap around the produce function.
// It has access to *sarama.ProducerMessage as well as response and error (if any).
// The principal intended use is for logging and other observability means.
type ProducerMiddleware func(endpoint.Endpoint) endpoint.Endpoint
