package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/Arten331/observability/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Consumer struct {
	Reader  *kafka.Reader
	metrics *Metrics
}

type ConsumerClientOptions struct {
	ServiceName string
	Brokers     []string
	Topic       string
	Group       string
}

func NewConsumerClient(o ConsumerClientOptions) (Consumer, error) {
	if len(o.Brokers) == 0 || o.Brokers[0] == "" || o.Topic == "" || o.Group == "" {
		return Consumer{}, errors.New("kafka connection parameters not specified")
	}

	if o.ServiceName == "" {
		return Consumer{}, errors.New("specify a unique service name for the metrics")
	}

	c := Consumer{
		metrics: newMetrics(o.ServiceName),
	}

	logInfo := kafka.LoggerFunc(func(s string, i ...interface{}) {
		logger.L().Debug(fmt.Sprintf(s, i...))
	})

	logError := kafka.LoggerFunc(func(s string, i ...interface{}) {
		logger.L().Error(fmt.Sprintf(s, i...))
	})

	c.Reader = kafka.NewReader(kafka.ReaderConfig{
		ErrorLogger: logError,
		Logger:      logInfo,
		Brokers:     o.Brokers,
		Topic:       o.Topic,
		GroupID:     o.Group,
		MinBytes:    10e1,
		MaxBytes:    10e6,
	})

	return c, nil
}

func MustCreateConsumer(o ConsumerClientOptions) Consumer {
	c, err := NewConsumerClient(o)
	if err != nil {
		panic(err)
	}

	return c
}

func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := c.Reader.FetchMessage(ctx)
	if err != nil {
		logger.L().Error(
			"Unable fetch message",
			zap.Error(err),
			zap.String("topic", c.Reader.Config().Topic),
			zap.Strings("broker", c.Reader.Config().Brokers))

		c.metrics.StoreConsumerError()

		return msg, err
	}

	logger.L().Debug("Fetch queue message",
		zap.String("topic", c.Reader.Config().Topic),
		zap.ByteString("value", msg.Value),
		zap.ByteString("key", msg.Key),
	)

	c.metrics.StoreReceivedMessage()

	return msg, nil
}

func (c *Consumer) CommitMessage(ctx context.Context, msg kafka.Message) {
	err := c.Reader.CommitMessages(ctx, msg)
	if err != nil {
		logger.L().Error("failed to commit message",
			zap.Error(err),
			zap.ByteString("msg", msg.Value),
			zap.String("topic", c.Reader.Config().Topic),
			zap.Strings("broker", c.Reader.Config().Brokers),
		)
	}
}

func (c *Consumer) GetMetrics() []prometheus.Collector {
	return c.metrics.getConsumerMetrics()
}
