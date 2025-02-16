# otelkafka

Open Telemetry instrumentation for [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go).


## Installation

```shell
go get -u github.com/jurabek/otelkafka
```

## Usage

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/confluentinc/confluent-kafka-go/kafka"
    "github.com/otelkafka/otelkafka"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/trace"
)

func main() {
    // Create a new Kafka producer
    producer, err := otelkafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
    })
    if err != nil {
        log.Fatalf("Failed to create producer: %v", err)
    }
    defer producer.Close()

    // Produce a message
    topic := "my-topic"
    message := &kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
        Value:          []byte("Hello, World!"),
    }

    // make sure the context is propagated before produce the message
	otel.GetTextMapPropagator().Inject(ctx, otelkafka.NewMessageCarrier(message))
    if err := producer.Produce(message, nil); err != nil {
        log.Fatalf("Failed to produce message: %v", err)
    }

    // Wait for the message to be delivered
    producer.Flush(15 * 1000)

    // Create a new Kafka consumer
    consumer, err := otelkafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
        "group.id":          "my-group",
        "auto.offset.reset": "earliest",
    })
    if err != nil {
        log.Fatalf("Failed to create consumer: %v", err)
    }
    defer consumer.Close()

    // Subscribe to the topic
    if err := consumer.Subscribe(topic, nil); err != nil {
        log.Fatalf("Failed to subscribe to topic: %v", err)
    }

    // Consume messages
    for {
        message, err := consumer.ReadMessage(-1)
        if err != nil {
            log.Fatalf("Failed to read message: %v", err)
        }
        fmt.Printf("Message: %s\n", message.Value)
    }
}
```

## Metrics list

Table below lists the metrics that are collected by the instrumentation. 

| Name | Description | Type | Attributes |
|------|-------------|------|------------|
| `messaging.client.sent.messages` | The total number of messages sent by the producer. | Counter | [more attributes](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/#metric-messagingclientsentmessages) |
| `messaging.client.consumed.messages` | The total number of messages received by the consumer. | Counter | [more attributes](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/#metric-messagingclientconsumedmessages) |
| `messaging.client.operation.duration` | The duration of the messaging operation initiated by a producer or consumer client. filtered **by `messaging.system.operation.name` | Histogram | [more attributes](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/#metric-messagingclientoperationduration) |
