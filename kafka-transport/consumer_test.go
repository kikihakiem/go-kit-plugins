//go:build unit

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/transport"
	"github.com/khakiem/go-kit-plugins/kafka-transport"
)

var errTypeAssertion = errors.New("type assertion error")

type consumerGroupSessionMock struct {
	processedMsgCount int
	ctx               context.Context
}

func (cs *consumerGroupSessionMock) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	cs.processedMsgCount += 1
}

func (cs *consumerGroupSessionMock) Context() context.Context {
	return cs.ctx
}

func (cs *consumerGroupSessionMock) Claims() map[string][]int32 {
	return nil
}

func (cs *consumerGroupSessionMock) MemberID() string {
	return ""
}

func (cs *consumerGroupSessionMock) GenerationID() int32 {
	return 0
}

func (cs *consumerGroupSessionMock) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}

func (cs *consumerGroupSessionMock) Commit() {}

func (cs *consumerGroupSessionMock) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}

type consumerGroupClaimMock struct {
	messageChan chan *sarama.ConsumerMessage
}

func (cc consumerGroupClaimMock) Messages() <-chan *sarama.ConsumerMessage {
	return cc.messageChan
}

func (cc consumerGroupClaimMock) Topic() string              { return "" }
func (cc consumerGroupClaimMock) Partition() int32           { return 0 }
func (cc consumerGroupClaimMock) InitialOffset() int64       { return 0 }
func (cc consumerGroupClaimMock) HighWaterMarkOffset() int64 { return 0 }

func setupMocks(ctx context.Context, t *testing.T) (*consumerGroupSessionMock, consumerGroupClaimMock) {
	t.Helper()

	cc := consumerGroupClaimMock{
		messageChan: make(chan *sarama.ConsumerMessage),
	}
	cs := &consumerGroupSessionMock{
		ctx:               ctx,
		processedMsgCount: 0,
	}

	return cs, cc
}

// TestConsumerBadDecode checks if decoder errors are handled properly.
func TestConsumerBadDecode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs, cc := setupMocks(ctx, t)

	consumer := kafka.NewConsumer(
		func(context.Context, interface{}) (interface{}, error) {
			return struct{}{}, nil
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage) (request interface{}, err error) {
			return nil, errors.New("decode error")
		},
		nil,
		kafka.ConsumerErrorHandler(transport.ErrorHandlerFunc(func(ctx context.Context, err error) {
			if want, have := "decode error", err.Error(); want != have {
				t.Errorf("want %s, have %s", want, have)
			}
		})),
	)

	go consumer.ConsumeClaim(cs, cc)
	cc.messageChan <- &sarama.ConsumerMessage{}
}

// TestConsumerBadEndpoint checks if endpoint errors are handled properly.
func TestConsumerBadEndpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs, cc := setupMocks(ctx, t)

	consumer := kafka.NewConsumer(
		func(context.Context, interface{}) (interface{}, error) {
			return nil, errors.New("endpoint error")
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage) (request interface{}, err error) {
			return struct{}{}, nil
		},
		nil,
		kafka.ConsumerErrorHandlerFunc(func(ctx context.Context, err error) {
			if want, have := "endpoint error", err.Error(); want != have {
				t.Errorf("want %s, have %s", want, have)
			}
		}),
	)

	go consumer.ConsumeClaim(cs, cc)
	cc.messageChan <- &sarama.ConsumerMessage{}
}

// TestConsumerBadEncoder checks if encoder errors are handled properly.
func TestConsumerBadEncoder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs, cc := setupMocks(ctx, t)

	consumer := kafka.NewConsumer(
		func(context.Context, interface{}) (interface{}, error) {
			return struct{}{}, nil
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage) (request interface{}, err error) {
			return struct{}{}, nil
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage, i interface{}) error {
			return errors.New("encode error")
		},
		kafka.ConsumerErrorHandler(transport.ErrorHandlerFunc(func(ctx context.Context, err error) {
			if want, have := "encode error", err.Error(); want != have {
				t.Errorf("want %s, have %s", want, have)
			}
		})),
	)

	go consumer.ConsumeClaim(cs, cc)
	cc.messageChan <- &sarama.ConsumerMessage{}
}

// TestConsumerSuccess checks if response payload encoded properly.
func TestConsumerSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs, cc := setupMocks(ctx, t)

	consumer := kafka.NewConsumer(
		func(context.Context, interface{}) (interface{}, error) {
			return "success", nil
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage) (request interface{}, err error) {
			return struct{}{}, nil
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage, i interface{}) error {
			return nil
		},
		kafka.ConsumerAround(func(next kafka.ConsumeFunc) kafka.ConsumeFunc {
			return func(cm *sarama.ConsumerMessage) (interface{}, error) {
				resp, err := next(cm)
				if err != nil {
					t.Errorf("want nil error, have %v", err)
				}

				if want, have := "success", resp; want != have {
					t.Errorf("want %s, have %s", want, have)
				}

				return resp, nil
			}
		}),
	)

	go consumer.ConsumeClaim(cs, cc)
	cc.messageChan <- &sarama.ConsumerMessage{}
}

// TestConsumerBefore checks if consumer before hook is called before decode.
func TestConsumerBefore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs, cc := setupMocks(ctx, t)

	consumer := kafka.NewConsumer(
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return struct{}{}, nil
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage) (request interface{}, err error) {
			if want, have := "an appended value", string(cm.Value); want != have {
				return nil, fmt.Errorf("want %s, have %s", want, have)
			}

			return struct{}{}, nil
		},
		nil,
		kafka.ConsumerBefore(func(ctx context.Context, cm *sarama.ConsumerMessage) context.Context {
			cm.Value = append(cm.Value, "n appended value"...) // append the message value
			return ctx
		}),
	)

	go consumer.ConsumeClaim(cs, cc)
	cc.messageChan <- &sarama.ConsumerMessage{Value: []byte(`a`)}
}

// TestConsumerAfter checks if after hook is called after endpoint and before encode.
func TestConsumerAfter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs, cc := setupMocks(ctx, t)

	consumer := kafka.NewConsumer(
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return req, nil // simply return the request as response
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage) (request interface{}, err error) {
			// at this point the message value should have not been appended yet
			if want, have := "a", string(cm.Value); want != have {
				return nil, fmt.Errorf("want %s, have %s", want, have)
			}

			return string(cm.Value), nil
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage, resp interface{}) error {
			val, ok := resp.(string)
			if !ok {
				return errTypeAssertion
			}

			// at this point the message value should have been appended
			if want, have := "an appended value", val; want != have {
				return fmt.Errorf("want %s, have %s", want, have)
			}

			return nil
		},
		kafka.ConsumerAfter(func(ctx context.Context, cm *sarama.ConsumerMessage, resp interface{}) context.Context {
			cm.Value = append(cm.Value, "n appended value"...)
			return ctx
		}),
	)

	go consumer.ConsumeClaim(cs, cc)
	cc.messageChan <- &sarama.ConsumerMessage{Value: []byte(`a`)}
}

func simulateKafkaConsumer(t *testing.T, c *kafka.Consumer, cs *consumerGroupSessionMock, cc consumerGroupClaimMock) {
	t.Helper()

	c.Setup(cs)
	c.ConsumeClaim(cs, cc)
	c.Cleanup(cs)
}

// TestConsumerGroupHandlerSetupAndCleanup checks if consumer group handler's
// Setup() and Cleanup() are called properly.
func TestConsumerGroupHandlerSetupAndCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs, cc := setupMocks(ctx, t)

	consumer := kafka.NewConsumer(
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return struct{}{}, nil
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage) (request interface{}, err error) {
			return struct{}{}, nil
		},
		nil,
		kafka.ConsumerGroupHandlerSetup(func(cgs sarama.ConsumerGroupSession) error {
			sessionMock, ok := cgs.(*consumerGroupSessionMock)
			if !ok {
				return errTypeAssertion
			}

			// processedMsgCount initial value
			if want, have := 0, sessionMock.processedMsgCount; want != have {
				t.Errorf("want %d, have %d", want, have)
			}

			return nil
		}),
		kafka.ConsumerGroupHandlerCleanup(func(cgs sarama.ConsumerGroupSession) error {
			sessionMock, ok := cgs.(*consumerGroupSessionMock)
			if !ok {
				return errTypeAssertion
			}

			// processedMsgCount should have been incremented
			if want, have := 1, sessionMock.processedMsgCount; want != have {
				t.Errorf("want %d, have %d", want, have)
			}

			return nil
		}),
	)

	go simulateKafkaConsumer(t, consumer, cs, cc)
	cc.messageChan <- &sarama.ConsumerMessage{}
}

// TestConsumerGroupHandlerNilSetupAndCleanup checks if a consumer group handler has
// nil Setup() and Cleanup() will not cause panic.
func TestConsumerGroupHandlerNilSetupAndCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs, cc := setupMocks(ctx, t)

	consumer := kafka.NewConsumer(
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return struct{}{}, nil
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage) (request interface{}, err error) {
			return struct{}{}, nil
		},
		nil,
	)

	go func(t *testing.T) {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("want no panic, have panic: %v", err)
			}
		}()

		simulateKafkaConsumer(t, consumer, cs, cc) // expect no panic
	}(t)

	cc.messageChan <- &sarama.ConsumerMessage{}
}

// TestConsumerNopRequestDecoder checks if nop request decoder works properly.
func TestConsumerNopRequestDecoder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs, cc := setupMocks(ctx, t)

	consumer := kafka.NewConsumer(
		func(ctx context.Context, req interface{}) (interface{}, error) {
			if req != nil {
				t.Errorf("want nil, have %v", req)
			}

			return struct{}{}, nil
		},
		kafka.NopRequestDecoder,
		nil,
	)

	go consumer.ConsumeClaim(cs, cc)
	cc.messageChan <- &sarama.ConsumerMessage{}
}

type consumerGroupSessionMock2 struct {
	consumerGroupSessionMock

	markMessageFn func(*sarama.ConsumerMessage, string)
}

func (cs *consumerGroupSessionMock2) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	cs.markMessageFn(msg, metadata)
}

// TestConsumerMiddleware checks if consumer middleware works properly.
func TestConsumerMiddleware(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cc := consumerGroupClaimMock{
		messageChan: make(chan *sarama.ConsumerMessage),
	}
	cs := &consumerGroupSessionMock2{
		consumerGroupSessionMock: consumerGroupSessionMock{ctx: ctx},
		markMessageFn: func(msg *sarama.ConsumerMessage, metadata string) {
			// at this point, ALL characters will appear
			if want, have := "abcyx", string(msg.Value); want != have {
				t.Errorf("want %s, have %s", want, have)
			}
		},
	}

	consumer := kafka.NewConsumer(
		func(ctx context.Context, req interface{}) (interface{}, error) {
			request, ok := req.([]byte)
			if !ok {
				return nil, errTypeAssertion
			}

			// at this point, only characters appended before consume will appear
			if want, have := "abc", string(request); want != have {
				t.Errorf("want %s, have %s", want, have)
			}

			return struct{}{}, nil
		},
		func(ctx context.Context, cm *sarama.ConsumerMessage) (interface{}, error) {
			return cm.Value, nil
		},
		nil,
		kafka.ConsumerAround(func(next kafka.ConsumeFunc) kafka.ConsumeFunc {
			return func(cm *sarama.ConsumerMessage) (interface{}, error) {
				cm.Value = append(cm.Value, "b"...)
				next(cm)
				cm.Value = append(cm.Value, "x"...)
				return nil, nil
			}
		}, func(next kafka.ConsumeFunc) kafka.ConsumeFunc {
			return func(cm *sarama.ConsumerMessage) (interface{}, error) {
				cm.Value = append(cm.Value, "c"...)
				next(cm)
				cm.Value = append(cm.Value, "y"...)
				return nil, nil
			}
		}),
	)

	go consumer.ConsumeClaim(cs, cc)
	cc.messageChan <- &sarama.ConsumerMessage{Value: []byte(`a`)}
}
