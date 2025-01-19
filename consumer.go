package otelkafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

type Consumer struct {
	*kafka.Consumer
	cfg        config
	prev       trace.Span
	msgCounter metric.Int64Counter
}

func NewConsumer(conf *kafka.ConfigMap, opts ...Option) (*Consumer, error) {
	c, err := kafka.NewConsumer(conf)
	if err != nil {
		return nil, err
	}
	opts = append(opts, withConfig(conf))
	cfg := newConfig(opts...)
	meter := cfg.MeterProvider.Meter("kafka_consumer")

	// Stability: experimental
	msgCounter, err := meter.Int64Counter(
		"messaging.client.consumed.messages",
		metric.WithDescription("The number of messages pulled from the broker"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create produced message counter metric: %w", err)
	}

	return &Consumer{Consumer: c, cfg: cfg, msgCounter: msgCounter}, nil
}

// WrapConsumer wraps a kafka.Consumer so that any consumed events are traced.
func WrapConsumer(c *kafka.Consumer, opts ...Option) (*Consumer, error) {

	cfg := newConfig(opts...)
	meter := cfg.MeterProvider.Meter("kafka_consumer")

	// Stability: experimental, https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/#metric-messagingclientconsumedmessages
	msgCounter, err := meter.Int64Counter(
		"messaging.client.consumed.messages",
		metric.WithDescription("The number of messages pulled from the broker"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create produced message counter metric: %w", err)
	}

	wrapped := &Consumer{
		Consumer:   c,
		cfg:        cfg,
		msgCounter: msgCounter,
	}
	return wrapped, nil
}

func (c *Consumer) Poll(timeoutMs int) (event kafka.Event) {
	if c.prev != nil {
		c.prev.End()
	}
	e := c.Consumer.Poll(timeoutMs)
	switch e := e.(type) {
	case *kafka.Message:
		span := c.startSpanAndCount(e)
		// latest span is stored to be closed when the next message is polled or when the consumer is closed
		c.prev = span
	}

	return e
}

// ReadMessage polls the consumer for a message. Message will be traced.
func (c *Consumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	if c.prev != nil {
		if c.prev.IsRecording() {
			c.prev.End()
		}
		c.prev = nil
	}
	msg, err := c.Consumer.ReadMessage(timeout)
	if err != nil {
		return nil, err
	}
	// latest span is stored to be closed when the next message is polled or when the consumer is closed
	c.prev = c.startSpanAndCount(msg)
	return msg, nil
}

// Close calls the underlying Consumer.Close and if polling is enabled, finishes
// any remaining span.
func (c *Consumer) Close() error {
	err := c.Consumer.Close()
	// we only close the previous span if consuming via the events channel is
	// not enabled, because otherwise there would be a data race from the
	// consuming goroutine.
	if c.prev != nil {
		if c.prev.IsRecording() {
			c.prev.End()
		}
		c.prev = nil
	}
	return err
}

func (c *Consumer) startSpanAndCount(msg *kafka.Message) trace.Span {
	carrier := NewMessageCarrier(msg)
	parentSpanContext := c.cfg.Propagators.Extract(context.Background(), carrier)

	lowCardinalityAttrs := []attribute.KeyValue{
		semconv.MessagingOperationName("consume"),
		semconv.MessagingOperationTypeReceive,
		semconv.MessagingSystemKafka,
		semconv.ServerAddress(c.cfg.bootstrapServers),
		semconv.MessagingDestinationName(*msg.TopicPartition.Topic),
		semconv.MessagingConsumerGroupName(c.cfg.consumerGroupID),
	}

	// Create a span.
	highCardinalityAttrs := []attribute.KeyValue{
		semconv.MessagingKafkaOffset(int(msg.TopicPartition.Offset)),
		semconv.MessagingKafkaMessageKey(string(msg.Key)),
		semconv.MessagingMessageID(strconv.FormatInt(int64(msg.TopicPartition.Offset), 10)),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(int(msg.TopicPartition.Partition))),
		semconv.MessagingMessageBodySize(getMsgSize(msg)),
	}

	if c.cfg.attributeInjectFunc != nil {
		highCardinalityAttrs = append(highCardinalityAttrs, c.cfg.attributeInjectFunc(msg)...)
	}

	attrs := append(lowCardinalityAttrs, highCardinalityAttrs...)
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}
	newCtx, span := c.cfg.Tracer.Start(parentSpanContext, fmt.Sprintf("%v receive", msg.TopicPartition.Topic), opts...)

	// Inject current span context, so consumers can use it to propagate span.
	c.cfg.Propagators.Inject(newCtx, carrier)
	c.msgCounter.Add(newCtx, 1, metric.WithAttributes(lowCardinalityAttrs...))
	return span
}
