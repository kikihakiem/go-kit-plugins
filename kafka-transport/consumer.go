package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/transport"
	"github.com/go-kit/log"
)

// Consumer wraps an endpoint and provides sarama.ConsumerGroupHandler.
type Consumer struct {
	e            endpoint.Endpoint
	dec          DecodeRequestFunc
	enc          EncodeResponseFunc
	before       []ConsumerRequestFunc
	after        []ConsumerResponseFunc
	around       []ConsumerMiddleware
	errorEncoder ErrorEncoder
	errorHandler transport.ErrorHandler

	// Sarama specific
	consumerGroupHandlerSetup   func(sarama.ConsumerGroupSession) error
	consumerGroupHandlerCleanup func(sarama.ConsumerGroupSession) error
}

// NewConsumer constructs a new consumer, which provides sarama.ConsumerGroupHandler and wraps
// the provided endpoint.
func NewConsumer(
	endpt endpoint.Endpoint,
	dec DecodeRequestFunc,
	enc EncodeResponseFunc,
	options ...ConsumerOption,
) *Consumer {
	consumer := &Consumer{
		e:            endpt,
		dec:          dec,
		enc:          enc,
		errorEncoder: NopErrorHandler,
		errorHandler: transport.NewLogErrorHandler(log.NewNopLogger()),
	}

	for _, option := range options {
		option(consumer)
	}

	return consumer
}

// ConsumerOption sets an optional parameter for consumers.
type ConsumerOption func(*Consumer)

// ConsumerBefore functions are executed before the request is decoded.
func ConsumerBefore(before ...ConsumerRequestFunc) ConsumerOption {
	return func(c *Consumer) { c.before = append(c.before, before...) }
}

// ConsumerAfter functions are executed after the
// endpoint is invoked, but before response encoding.
func ConsumerAfter(after ...ConsumerResponseFunc) ConsumerOption {
	return func(c *Consumer) { c.after = append(c.after, after...) }
}

// ConsumerAround functions are executed around the actual consume function.
// They act as middlewares that can be used to measure execution time for example.
func ConsumerAround(around ...ConsumerMiddleware) ConsumerOption {
	return func(c *Consumer) { c.around = append(c.around, around...) }
}

// ConsumerErrorHandlerFunc is used to handle non-terminal errors. By default, non-terminal errors
// are ignored. This is intended as a diagnostic measure. Finer-grained control
// of error handling, including logging in more detail, should be performed in a
// custom ConsumerErrorHandlerFunc which has access to the context.
func ConsumerErrorHandlerFunc(ee ErrorEncoder) ConsumerOption {
	return func(c *Consumer) { c.errorEncoder = ee }
}

// ConsumerErrorHandler is used to handle non-terminal errors. By default, non-terminal errors
// are ignored. This is intended as a diagnostic measure. Finer-grained control
// of error handling, including logging in more detail, should be performed in a
// custom ConsumerErrorEncoder which has access to the context.
func ConsumerErrorHandler(errorHandler transport.ErrorHandler) ConsumerOption {
	return func(c *Consumer) { c.errorHandler = errorHandler }
}

// ConsumerGroupHandlerSetup is run at the beginning of a new session, before ConsumeClaim.
func ConsumerGroupHandlerSetup(fn func(sarama.ConsumerGroupSession) error) ConsumerOption {
	return func(c *Consumer) { c.consumerGroupHandlerSetup = fn }
}

// ConsumerGroupHandlerCleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func ConsumerGroupHandlerCleanup(fn func(sarama.ConsumerGroupSession) error) ConsumerOption {
	return func(c *Consumer) { c.consumerGroupHandlerCleanup = fn }
}

// Setup implements sarama.ConsumerGroupHandler.
func (c Consumer) Setup(session sarama.ConsumerGroupSession) error {
	if c.consumerGroupHandlerSetup == nil {
		return nil
	}

	return c.consumerGroupHandlerSetup(session)
}

// Cleanup implements sarama.ConsumerGroupHandler.
func (c Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	if c.consumerGroupHandlerCleanup == nil {
		return nil
	}

	return c.consumerGroupHandlerCleanup(session)
}

// ConsumeClaim implements sarama.ConsumerGroupHandler.
func (c Consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	consumeFn := c.consume

	// reverse the order of middleware execution
	for i := len(c.around) - 1; i >= 0; i-- {
		consumeFn = c.around[i](consumeFn)
	}

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case msg := <-claim.Messages():
			if _, err := consumeFn(msg); err == nil {
				session.MarkMessage(msg, "")
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

// ConsumeFunc process *sarama.ConsumerMessage.
type ConsumeFunc func(*sarama.ConsumerMessage) (interface{}, error)

// ConsumerMiddleware can be used to wrap around the consume function.
// It has access to *sarama.ConsumerMessage as well as response and error (if any).
// The principal intended use is for logging and other observability means.
type ConsumerMiddleware func(ConsumeFunc) ConsumeFunc

// consume process *sarama.ConsumerMessage.
func (c Consumer) consume(msg *sarama.ConsumerMessage) (interface{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		err               error
		request, response interface{} //nolint:typecheck,nolintlint
	)

	for _, f := range c.before {
		ctx = f(ctx, msg)
	}

	request, err = c.dec(ctx, msg)
	if err != nil {
		c.errorHandler.Handle(ctx, err)
		c.errorEncoder(ctx, err)

		return nil, err
	}

	response, err = c.e(ctx, request)
	if err != nil {
		c.errorHandler.Handle(ctx, err)
		c.errorEncoder(ctx, err)

		return nil, err
	}

	for _, f := range c.after {
		ctx = f(ctx, msg, &response)
	}

	if c.enc == nil {
		return response, nil
	}

	if err = c.enc(ctx, msg, &response); err != nil {
		c.errorHandler.Handle(ctx, err)
		c.errorEncoder(ctx, err)

		return response, err
	}

	return response, nil
}

// ErrorEncoder is responsible for encoding an error to the consumer result.
// Users are encouraged to use custom ErrorEncoders to encode errors to
// their replies, and will likely want to pass and check for their own error
// types.
type ErrorEncoder func(ctx context.Context, err error)

// NopRequestDecoder is a DecodeRequestFunc that can be used for requests that do not
// need to be decoded, and simply returns nil, nil.
func NopRequestDecoder(_ context.Context, _ *sarama.ConsumerMessage) (interface{}, error) {
	return nil, nil //nolint:nilnil
}

// NopErrorHandler is a ErrorEncoder that do nothing.
func NopErrorHandler(_ context.Context, err error) {
}
