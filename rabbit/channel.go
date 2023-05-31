package rabbitmq

import (
	"errors"
	"sync"
	"time"

	"github.com/Arten331/observability/logger"
	"github.com/streadway/amqp"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const ReconnectInterval = time.Second * 2

type ChannelManager struct {
	mu       sync.Mutex
	channel  *amqp.Channel
	client   *RabbitMQ
	exchange *Exchange

	notifyClosed      chan error
	notifyReconnected chan struct{}

	reconnectionCount atomic.Uint64
}

func NewChannel(client *RabbitMQ, exchange *Exchange) (*ChannelManager, error) {
	ch := &ChannelManager{
		mu:                sync.Mutex{},
		client:            client,
		notifyClosed:      make(chan error, 1),
		notifyReconnected: make(chan struct{}, 1),
		exchange:          exchange,
	}

	err := ch.tryConnect()
	if err != nil {
		return nil, err
	}

	return ch, nil
}

func (cm *ChannelManager) watch() {
	select {
	case err := <-cm.channel.NotifyClose(make(chan *amqp.Error, 1)):
		cm.setChannelStatus(false)

		if err != nil {
			logger.L().Error("channel closed with error", zap.Error(err))
			cm.notifyClosed <- err
			cm.reconnect()
		} else {
			logger.L().Info("channel closed without error")
		}
	case err := <-cm.channel.NotifyCancel(make(chan string, 1)):
		logger.L().Error("channel canceled with error", zap.Error(errors.New(err)))
		cm.notifyClosed <- errors.New(err)
		cm.reconnect()
	}
}

func (cm *ChannelManager) reconnect() {
	for {
		logger.L().Info("try reconnect channel")
		time.Sleep(ReconnectInterval)

		err := cm.tryConnect()
		if err != nil {
			logger.L().Error("failed creation channel", zap.Error(err))

			continue
		}

		cm.reconnectionCount.Add(1)

		cm.notifyReconnected <- struct{}{}

		return
	}
}

func (cm *ChannelManager) tryConnect() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	conn := cm.client.Connection()

	newChannel, err := conn.Channel()
	if err != nil {
		return err
	}

	// close old channelManager
	if cm.channel != nil {
		_ = cm.channel.Close()
	}

	cm.channel = newChannel

	cm.setChannelStatus(true)

	go cm.watch()

	return nil
}

func (cm *ChannelManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	_ = cm.channel.Close()
}

func (cm *ChannelManager) Channel() *amqp.Channel {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.channel
}

func (cm *ChannelManager) Confirm(noWait bool) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	err := cm.channel.Confirm(noWait)
	if err != nil {
		return err
	}

	return nil
}

func (cm *ChannelManager) setChannelStatus(status bool) {
	var floatValue float64

	if status {
		cm.client.metrics.StoreChannelReconnect(cm.client.config.ConnectionName, cm.exchange.name)
		floatValue = 1
	} else {
		cm.client.metrics.StoreChannelClose(cm.client.config.ConnectionName, cm.exchange.name)
		floatValue = 0
	}

	cm.client.metrics.StoreChannelStatus(cm.client.config.ConnectionName, cm.exchange.name, floatValue)
}
