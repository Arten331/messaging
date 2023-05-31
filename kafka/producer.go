package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Arten331/observability/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type QueueableMessage interface {
	KafkaMessage() (kafka.Message, error)
}

type Producer struct {
	Writer           *kafka.Writer
	BeforeSendAction func(context.Context, []kafka.Message)
	metrics          *Metrics
}

type ProducerClientOptions struct {
	ServiceName      string
	Brokers          []string
	Topic            string
	BeforeSend       func(context.Context, []kafka.Message)
	AutoCreateTopics bool
}

func NewProducer(o ProducerClientOptions) (Producer, error) {
	if len(o.Brokers) == 0 || o.Brokers[0] == "" {
		return Producer{}, errors.New("kafka broker not specified")
	}

	if o.Topic == "" && !o.AutoCreateTopics {
		return Producer{}, errors.New("kafka producer necessary specified topic or enable auto-create")
	}

	if o.ServiceName == "" {
		return Producer{}, errors.New("specify a unique service name for the metrics")
	}

	producer := Producer{
		BeforeSendAction: o.BeforeSend,
		metrics:          newMetrics(o.ServiceName),
	}

	logInfo := kafka.LoggerFunc(func(s string, i ...interface{}) {
		logger.L().Debug(fmt.Sprintf(s, i...))
	})

	logError := kafka.LoggerFunc(func(s string, i ...interface{}) {
		logger.L().Error(fmt.Sprintf(s, i...))
	})

	producer.Writer = &kafka.Writer{
		ErrorLogger:            logError,
		Logger:                 logInfo,
		MaxAttempts:            3,
		RequiredAcks:           kafka.RequireAll,
		Addr:                   kafka.TCP(o.Brokers...),
		Topic:                  o.Topic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: o.AutoCreateTopics,
	}

	return producer, nil
}

func MustCreateProducer(o ProducerClientOptions) Producer {
	p, err := NewProducer(o)
	if err != nil {
		panic(err)
	}

	return p
}

func (c *Producer) SendMessages(ctx context.Context, msgs []QueueableMessage) error {
	queueMessages, err := MessagesFromMany(msgs)
	if err != nil {
		logger.L().Error(
			"queue producer unable create messages",
			zap.Array("messages", zapQueueMessages(queueMessages)),
			zap.String("topic", c.Writer.Topic),
			zap.Error(err),
		)

		return err
	}

	if c.BeforeSendAction != nil {
		c.BeforeSendAction(ctx, queueMessages)
	}

	start := time.Now()

	err = c.Writer.WriteMessages(ctx, queueMessages...)
	if err != nil {
		logger.L().Error(
			"Unable write messages",
			// zap.Array("messages", zapQueueMessages(queueMessages)),
			zap.Int("count", len(queueMessages)),
			zap.Error(err),
		)

		c.metrics.StoreProducerError()

		return err
	}

	c.metrics.StoreSentMessage(time.Since(start).Seconds())

	logger.L().Debug("Write queue messages",
		//zap.Array("messages", zapQueueMessages(queueMessages)),
		zap.String("topic", c.Writer.Topic),
		zap.Int("count", len(queueMessages)),
	)

	return nil
}

func MessagesFromMany(messages []QueueableMessage) ([]kafka.Message, error) {
	result := make([]kafka.Message, 0, len(messages))

	for _, message := range messages {
		kafkaMessage, err := message.KafkaMessage()
		if err != nil {
			return nil, err
		}

		result = append(result, kafkaMessage)
	}

	return result, nil
}

func (c *Producer) GetMetrics() []prometheus.Collector {
	return c.metrics.getProducerMetrics()
}
