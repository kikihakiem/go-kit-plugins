//go:build unit

package kafka_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/transport"
	"github.com/khakiem/go-kit-plugins/kafka-transport"
)

type syncProducerMock struct {
	partition int32
	offset    int64
	err       error
}

func (sp *syncProducerMock) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	return sp.partition, sp.offset, sp.err
}
func (sp *syncProducerMock) SendMessages(msgs []*sarama.ProducerMessage) error { return nil }
func (sp *syncProducerMock) Close() error                                      { return nil }
func (sp *syncProducerMock) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnFlagCommittingTransaction
}
func (sp *syncProducerMock) IsTransactional() bool { return true }
func (sp *syncProducerMock) BeginTxn() error       { return nil }
func (sp *syncProducerMock) CommitTxn() error      { return nil }
func (sp *syncProducerMock) AbortTxn() error       { return nil }
func (sp *syncProducerMock) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	return nil
}

func (sp *syncProducerMock) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	return nil
}

// TestProducerBadEncode checks if encoder errors are handled properly.
func TestProducerBadEncode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncProducer := &syncProducerMock{}

	producer := kafka.NewProducer(
		syncProducer,
		"topic.test",
		func(ctx context.Context, msg *sarama.ProducerMessage, request interface{}) error {
			return errors.New("encode error")
		},
		nil,
	)

	_, err := producer.Endpoint()(ctx, struct{}{})
	if want, have := "encode error", err.Error(); want != have {
		t.Errorf("want %s, have %s", want, have)
	}
}

// TestProducerBadDecode checks if decoder errors are handled properly.
func TestProducerBadDecode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncProducer := &syncProducerMock{}

	producer := kafka.NewProducer(
		syncProducer,
		"topic.test",
		func(ctx context.Context, msg *sarama.ProducerMessage, req interface{}) error {
			return nil
		},
		func(ctx context.Context, resp interface{}) (interface{}, error) {
			return nil, errors.New("decode error")
		},
	)

	_, err := producer.Endpoint()(ctx, struct{}{})
	if want, have := "decode error", err.Error(); want != have {
		t.Errorf("want %s, have %s", want, have)
	}
}

// TestProducerSuccess checks if normal flow works properly.
func TestProducerSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncProducer := &syncProducerMock{partition: 1, offset: 99}

	producer := kafka.NewProducer(
		syncProducer,
		"topic.test",
		func(ctx context.Context, msg *sarama.ProducerMessage, req interface{}) error {
			return nil
		},
		nil,
	)

	resp, err := producer.Endpoint()(ctx, struct{}{})
	if err != nil {
		t.Errorf("want nil error, have %v", err)
	}

	result, ok := resp.(kafka.ProducerResponse)
	if !ok {
		t.Errorf("want kafka.ProducerResponse, have %v", resp)
	}

	if want, have := int32(1), result.Partition; want != have {
		t.Errorf("want %d, have %d", want, have)
	}

	if want, have := int64(99), result.Offset; want != have {
		t.Errorf("want %d, have %d", want, have)
	}
}

// TestProducerSendFailure checks if sender errors are handled properly.
func TestProducerSendFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncProducer := &syncProducerMock{err: errors.New("sender error")}

	producer := kafka.NewProducer(
		syncProducer,
		"topic.test",
		func(ctx context.Context, msg *sarama.ProducerMessage, req interface{}) error {
			return nil
		},
		nil,
		kafka.ProducerErrorHandler(transport.ErrorHandlerFunc(func(ctx context.Context, err error) {
			if want, have := "sender error", err.Error(); want != have {
				t.Errorf("want %s, have %s", want, have)
			}
		})),
	)

	producer.Endpoint()(ctx, struct{}{})
}

type syncProducerMock2 struct {
	syncProducerMock

	sendMessageFn func(*sarama.ProducerMessage) (int32, int64, error)
}

func (sp2 syncProducerMock2) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	return sp2.sendMessageFn(msg)
}

// TestProducerBefore checks if before hook is called after encoding and before sending the request.
func TestProducerBefore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncProducer := &syncProducerMock2{
		sendMessageFn: func(msg *sarama.ProducerMessage) (int32, int64, error) {
			encoded, err := msg.Value.Encode()
			if err != nil {
				t.Errorf("want nil error, have %v", err)
			}

			if want, have := "before value", string(encoded); want != have {
				t.Errorf("want %s, have %s", want, have)
			}

			return 0, 0, nil
		},
	}

	producer := kafka.NewProducer(
		syncProducer,
		"topic.test",
		func(ctx context.Context, msg *sarama.ProducerMessage, req interface{}) error {
			msg.Value = sarama.StringEncoder("encoded value")
			return nil
		},
		nil,
		kafka.ProducerBefore(func(ctx context.Context, msg *sarama.ProducerMessage) context.Context {
			msg.Value = sarama.StringEncoder("before value") // replace value
			return ctx
		}),
	)

	producer.Endpoint()(ctx, struct{}{})
}

// TestProducerAfter checks if after hook is called sending the request but before decoding the response.
func TestProducerAfter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncProducer := &syncProducerMock{partition: 1, offset: 2}

	producer := kafka.NewProducer(
		syncProducer,
		"topic.test",
		func(ctx context.Context, msg *sarama.ProducerMessage, req interface{}) error {
			msg.Value = sarama.StringEncoder("encoded value")
			return nil
		},
		func(ctx context.Context, resp interface{}) (interface{}, error) {
			return resp, nil
		},
		kafka.ProducerAfter(func(ctx context.Context, msg *sarama.ProducerMessage, resp interface{}) context.Context {
			result, ok := resp.(*kafka.ProducerResponse)
			if !ok {
				t.Errorf("want *kafka.ProducerResponse, have %v", resp)
			}

			result.Offset = 3 // replace original value

			return ctx
		}),
	)

	resp, err := producer.Endpoint()(ctx, struct{}{})
	if err != nil {
		t.Errorf("want nil error, have %v", err)
	}

	result, ok := resp.(kafka.ProducerResponse)
	if !ok {
		t.Errorf("want kafka.ProducerResponse, have %v", resp)
	}

	if want, have := int64(3), result.Offset; want != have {
		t.Errorf("want %d, have %d", want, have)
	}
}

type testRequest struct {
	str string
}

// TestProducerMiddleware checks if producer middleware works properly.
func TestProducerMiddleware(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncProducer := &syncProducerMock{err: errors.New("sender error")}

	producer := kafka.NewProducer(
		syncProducer,
		"topic.test",
		func(ctx context.Context, msg *sarama.ProducerMessage, req interface{}) error {
			request, ok := req.(*testRequest)
			if !ok {
				return errTypeAssertion
			}

			// at this point, only characters appended before sending will appear
			if want, have := "abc", request.str; want != have {
				t.Errorf("want %s, have %s", want, have)
			}

			return nil
		},
		nil,
		kafka.ProducerAround(func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, request interface{}) (interface{}, error) {
				req, ok := request.(*testRequest)
				if !ok {
					return nil, errTypeAssertion
				}

				req.str += "b"
				next(ctx, req)
				req.str += "x"

				return nil, nil
			}
		}, func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, request interface{}) (interface{}, error) {
				req, ok := request.(*testRequest)
				if !ok {
					return nil, errTypeAssertion
				}

				req.str += "c"
				next(ctx, req)
				req.str += "y"

				return nil, nil
			}
		}),
	)

	testReq := &testRequest{str: "a"}

	_, err := producer.Endpoint()(ctx, testReq)
	if err != nil {
		t.Errorf("want nil error, have %v", err)
	}

	// at this point, ALL characters will appear
	if want, have := "abcyx", testReq.str; want != have {
		t.Errorf("want %s, have %s", want, have)
	}
}
