package otelkafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Producer supports only tracing mechanism for Produce method over deprecated ProduceChannel method
type Producer struct {
	*kafka.Producer
	cfg        config
	msgCounter metric.Int64Counter
}

func NewProducer(conf *kafka.ConfigMap, opts ...Option) (*Producer, error) {
	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, err
	}

	opts = append(opts, withConfig(conf))
	cfg := newConfig(opts...)

	// Create the counter metric
	meter := cfg.MeterProvider.Meter("kafka")
	msgCounter, err := meter.Int64Counter(
		"messaging.client.sent.messages",
		metric.WithDescription("Number of messages producer attempted to send to the broker"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create produced message counter metric: %w", err)
	}

	return &Producer{Producer: p, cfg: cfg, msgCounter: msgCounter}, nil
}

// Produce calls the underlying Producer.Produce and traces the request.
func (p *Producer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	// If there's a span context in the message, use that as the parent context.
	carrier := NewMessageCarrier(msg)
	ctx := p.cfg.Propagators.Extract(context.Background(), carrier)

	lowCardinalityAttrs := []attribute.KeyValue{
		semconv.MessagingOperationTypePublish,
		semconv.MessagingSystemKafka,
		semconv.ServerAddress(p.cfg.bootstrapServers),
		semconv.MessagingDestinationName(*msg.TopicPartition.Topic),
		semconv.MessagingOperationName("produce"),
	}

	highCardinalityAttrs := []attribute.KeyValue{
		semconv.MessagingKafkaMessageKey(string(msg.Key)),
		semconv.MessagingMessageBodySize(getMsgSize(msg)),
	}
	allAttrs := append(lowCardinalityAttrs, highCardinalityAttrs...)

	opts := []trace.SpanStartOption{
		trace.WithAttributes(allAttrs...),
		trace.WithSpanKind(trace.SpanKindProducer),
	}

	ctx, span := p.cfg.Tracer.Start(ctx, fmt.Sprintf("%s publish", *msg.TopicPartition.Topic), opts...)
	p.cfg.Propagators.Inject(ctx, carrier)

	// if the user has selected a delivery channel, we will wrap it and
	// wait for the delivery event to finish the span
	if deliveryChan != nil {
		oldDeliveryChan := deliveryChan
		deliveryChan = make(chan kafka.Event)
		go func() {
			evt := <-deliveryChan
			if resMsg, ok := evt.(*kafka.Message); ok {
				if err := resMsg.TopicPartition.Error; err != nil {
					span.RecordError(resMsg.TopicPartition.Error)
					span.SetStatus(codes.Error, err.Error())
				} else {
					if resMsg.TopicPartition.Partition >= 0 {
						partitionIDAttr := semconv.MessagingDestinationPartitionID(fmt.Sprintf("%d", resMsg.TopicPartition.Partition))
						lowCardinalityAttrs = append(lowCardinalityAttrs, partitionIDAttr)

						// update the span attribute with the partition ID and offset
						span.SetAttributes(partitionIDAttr)
						span.SetAttributes(semconv.MessagingKafkaMessageOffset(int(resMsg.TopicPartition.Offset)))
					}
				}
			}
			p.msgCounter.Add(ctx, 1, metric.WithAttributes(lowCardinalityAttrs...))
			span.End()
			oldDeliveryChan <- evt
		}()
	}

	err := p.Producer.Produce(msg, deliveryChan)
	// with no delivery channel or enqueue error, finish immediately
	if err != nil || deliveryChan == nil {
		span.RecordError(err)
		span.End()
	}

	return err
}

// Close calls the underlying Producer.Close and also closes the internal
// wrapping producer channel.
func (p *Producer) Close() {
	p.Producer.Close()
}
