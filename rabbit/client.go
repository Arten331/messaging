package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Arten331/observability/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/streadway/amqp"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type EventKey int

const (
	KeyReconnect EventKey = iota + 1
	KeyClose
)

type Config struct {
	Schema              string
	Username            string
	Password            string
	Host                string
	Port                string
	Vhost               string
	ConnectionName      string
	ReconnectInterval   time.Duration
	ReconnectMaxAttempt int
}

type RabbitMQ struct {
	connMux    sync.Mutex // need for persistence connect: connection and checkChannel
	connected  atomic.Bool
	messagesWG sync.WaitGroup // waitgroup for shutdown client and wait sending all messages

	connection *amqp.Connection
	config     *Config
	handlers   map[EventKey][]chan struct{}
	dialConfig amqp.Config
	producers  []*Producer

	metrics *Metrics
}

func New(config *Config, serviceName string) *RabbitMQ {
	return &RabbitMQ{
		config:     config,
		dialConfig: amqp.Config{Properties: amqp.Table{"connection_name": config.ConnectionName}},
		handlers:   map[EventKey][]chan struct{}{},
		metrics:    newMetrics(serviceName),
	}
}

func (r *RabbitMQ) Run(ctx context.Context, cancel context.CancelFunc) {
	err := r.Connect()
	if err != nil {
		logger.L().Error("failed connect to rabbitMQ")
		cancel()
	}

	logger.L().Info("rabbitMQ connect successfuly",
		zap.String("name", r.config.ConnectionName),
		zap.String("vhost", r.connection.Config.Vhost),
	)

	for _, p := range r.producers {
		p.Run()
	}

	go r.reconnect(ctx, cancel)
}

func (r *RabbitMQ) Shutdown(ctx context.Context) error {
	var err error

	logger.L().Info("try shutdown rabbitMQ client")

	r.notifyClose()

	allMessagesSent := make(chan struct{})

	go func() {
		r.messagesWG.Wait()
		allMessagesSent <- struct{}{}
	}()

	ticker := time.NewTicker(time.Second)

LOOP:
	for {
		select {
		case <-ticker.C:
			logger.L().Info("waiting for all messages to be sent...")
		case <-ctx.Done():
			err = errors.New("timeout exceeded, force shutdown rabbitMQ client")
			_ = r.close()

			logger.L().Error(err.Error())

			break LOOP
		case <-allMessagesSent:
			logger.L().Info("all messages processed, close rabbit client")

			err = r.close()

			break LOOP
		}
	}

	return err
}

func (r *RabbitMQ) Connect() error {
	r.connMux.Lock()
	defer r.connMux.Unlock()

	conn, err := r.newConnection()
	if err != nil {
		return err
	}

	r.connection = conn

	r.setConnectionStatus(true)

	return nil
}

func (r *RabbitMQ) Channel() (*amqp.Channel, error) {
	r.connMux.Lock()
	defer r.connMux.Unlock()

	if r.connection == nil {
		return nil, errors.New("empty connection, run service early")
	}

	channel, err := r.connection.Channel()
	if err != nil {
		return nil, err
	}

	return channel, nil
}

func (r *RabbitMQ) Connection() *amqp.Connection {
	r.connMux.Lock()
	defer r.connMux.Unlock()

	return r.connection
}

func (r *RabbitMQ) close() error {
	r.setConnectionStatus(false)
	r.metrics.connectionStatus.WithLabelValues(r.config.ConnectionName).Sub(0)

	// close all events channels
	for i := range r.handlers {
		for _, ch := range r.handlers[i] {
			close(ch)
		}
	}

	return r.connection.Close()
}

func (r *RabbitMQ) newConnection() (*amqp.Connection, error) {
	return amqp.DialConfig(fmt.Sprintf(
		"%s://%s:%s@%s:%s/%s",
		r.config.Schema,
		r.config.Username,
		r.config.Password,
		r.config.Host,
		r.config.Port,
		r.config.Vhost,
	), r.dialConfig)
}

func (r *RabbitMQ) reconnect(ctx context.Context, cancelFunc context.CancelFunc) {
LOOP:
	for {
		select {
		case <-ctx.Done():
			return
		case conErr := <-r.connection.NotifyClose(make(chan *amqp.Error)):
			r.setConnectionStatus(false)

			if conErr != nil {
				logger.L().Error("rabbitMQ connection error, reconnect", zap.Error(conErr))

				var err error

				for i := 1; i <= r.config.ReconnectMaxAttempt; i++ {
					err = r.Connect()

					if err != nil {
						logger.L().Error(
							"unable reconnect",
							zap.Int("attempt", i),
							zap.Int("max attempts", r.config.ReconnectMaxAttempt),
						)

						<-time.After(r.config.ReconnectInterval)

						continue
					}

					if err == nil {
						logger.L().Info("rabbitMQ reconnected")

						for _, c := range r.handlers[KeyReconnect] {
							c <- struct{}{}
						}

						continue LOOP
					}

					time.Sleep(r.config.ReconnectInterval)
				}

				logger.L().Error("unable reconnect to rabbitMQ", zap.String("conn name", r.config.ConnectionName))
				cancelFunc()
			} else {
				logger.L().Info("rabbitMQ connection dropped normally, will not reconnect")
				cancelFunc()
			}
		}

		break
	}
}

func (r *RabbitMQ) Subscribe(key EventKey) <-chan struct{} {
	r.connMux.Lock()
	defer r.connMux.Unlock()

	ch := make(chan struct{})

	_, ok := r.handlers[key]
	if !ok {
		r.handlers[key] = make([]chan struct{}, 0, 1)
	}

	r.handlers[key] = append(r.handlers[key], ch)

	return ch
}

func (r *RabbitMQ) notifyClose() {
	r.connMux.Lock()
	defer r.connMux.Unlock()

	handlersClose := r.handlers[KeyClose]
	for _, chClose := range handlersClose {
		chClose <- struct{}{}
	}
}

func (r *RabbitMQ) RegisterProducer(p *Producer) {
	r.producers = append(r.producers, p)
}

func (r *RabbitMQ) GetMetrics() []prometheus.Collector {
	return r.metrics.GetMetrics()
}

func (r *RabbitMQ) setConnectionStatus(status bool) {
	var floatValue float64

	r.connected.Store(status)

	if status {
		r.metrics.StoreConnectionReconnect(r.config.ConnectionName)
		floatValue = 1
	} else {
		r.metrics.StoreConnectionClose(r.config.ConnectionName)
		floatValue = 0
	}

	r.metrics.connectionStatus.WithLabelValues(r.config.ConnectionName).Set(floatValue)
}
