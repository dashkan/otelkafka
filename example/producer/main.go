package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jurabek/otelkafka"
	"github.com/jurabek/otelkafka/example"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
)

func main() {
	tp, err := example.InitTracer("producer-app")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	mp, err := example.InitMeterProvider("producer-app")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down meter provider: %v", err)
		}
	}()

	bootstrapServers := os.Getenv("KAFKA_SERVER")
	topic := os.Getenv("KAFKA_TOPIC")

	p, err := otelkafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created Producer %v\n", p)

	go func() {
		tr := otel.Tracer("produce")
		ctx, span := tr.Start(context.Background(), "produce message")
		defer span.End()

		for i := 0; i < 500; i++ {
			deliveryChan := make(chan kafka.Event)
			message := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(fmt.Sprintf("Message %d", i)),
				Key:            []byte(fmt.Sprintf("key-%d", i)),
				Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
			}
			otel.GetTextMapPropagator().Inject(ctx, otelkafka.NewMessageCarrier(message))

			err = p.Produce(message, deliveryChan)
			if err != nil {
				fmt.Printf("Failed to produce message %d: %v\n", i, err)
				continue
			}

			e := <-deliveryChan
			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed for message %d: %v\n", i, m.TopicPartition.Error)
			} else {
				fmt.Printf("Delivered message %d to topic %s [%d] at offset %v\n",
					i, *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}

			close(deliveryChan)
		}
	}()
	serveMetrics()
}

func serveMetrics() {
	log.Printf("serving metrics at localhost:2224/metrics")
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":2224", nil) //nolint:gosec // Ignoring G114: Use of net/http serve function that has no support for setting timeouts.
	if err != nil {
		fmt.Printf("error serving http: %v", err)
		return
	}
}
