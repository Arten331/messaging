package rabbitmq

import (
	"sync"
	"time"

	"github.com/Arten331/observability/logger"
	"github.com/streadway/amqp"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type ConfirmKey struct {
	reconnect   uint64
	deliveryTag uint64
}

type ConfirmMessage struct {
	key     ConfirmKey
	message *Message
}

type ConfirmList struct {
	c atomic.Uint64
	m sync.Map // m [ConfirmKey]*ConfirmMessage
}

func (p *Producer) watchMessages() {
	err := p.channelManager.Confirm(false)
	if err != nil {
		logger.L().Error("Channel could not be put into confirm mode", zap.Error(err))
	}

	returnCh := p.channelManager.Channel().NotifyReturn(make(chan amqp.Return, 1))
	publishedCh := p.channelManager.Channel().NotifyPublish(make(chan amqp.Confirmation, 1))

	go func() {
		for returnedMessage := range returnCh {
			p.returnChannel <- returnedMessage
		}
	}()

	go func() {
		for publishedMessage := range publishedCh {
			p.publishedChannel <- publishedMessage
		}
	}()
}

func (p *Producer) handlePublishedOrReturned() {
	ticker := time.NewTicker(time.Second * 2)

	for {
		select {
		case <-ticker.C:

			if p.waitConfirm {
				p.mqClient.metrics.StoreProducerMessageWaitConfirm(p.mqClient.config.ConnectionName, p.exchange.name, float64(p.waitingConfirmList.Count()))
			}

		case ret := <-p.returnChannel:
			logger.L().Error("message returned, try send again, wait 1 second",
				zap.String("messageID", ret.MessageId),
				zap.String("exchange", ret.Exchange),
				zap.ByteString("body", ret.Body),
			)
			p.mqClient.metrics.StoreProducerMessageReturned(p.mqClient.config.ConnectionName, p.exchange.name)

			time.Sleep(1 * time.Second)

			m := NewMessage(ret.Body, ret.DeliveryMode, ret.ContentType, ret.RoutingKey, ret.Exchange)

			p.mqClient.messagesWG.Done()

			err := p.SendMessage(&m)
			if err != nil {
				logger.L().Error("!IMPORTANT: error send returned message",
					zap.Object("message", m),
					zap.Error(err),
				)

				continue
			}
		case pub := <-p.publishedChannel:
			if p.waitConfirm {
				key := ConfirmKey{
					reconnect:   p.channelManager.reconnectionCount.Load(),
					deliveryTag: pub.DeliveryTag,
				}

				message := p.waitingConfirmList.LoadAndDelete(key)
				if message == nil {
					logger.L().Info("!IMPORTANT: message not found, unable to confirm delivery")

					continue
				}

				p.mqClient.metrics.StoreProducerMessageConfirm(p.mqClient.config.ConnectionName, p.exchange.name)
				logger.L().Debug("message delivery confirmed", zap.Object("msg", message.message), zap.Reflect("key", message.key))
			}

			p.mqClient.messagesWG.Done()
		}
	}
}

// channel reconnected, resend messages if waitConfirmMode
func (p *Producer) checkConfirmMessage() {
	if !p.waitConfirm {
		return
	}

	lastReconnectionKey := p.channelManager.reconnectionCount.Load() - 1

	needResendingMessages := make([]*Message, 0)

	p.waitingConfirmList.Range(func(_, val interface{}) bool {
		wm := val.(*ConfirmMessage)
		if wm.key.reconnect <= lastReconnectionKey {
			needResendingMessages = append(needResendingMessages, wm.message)

			p.waitingConfirmList.Delete(wm.key)
		}

		return true
	})

	if len(needResendingMessages) > 0 {
		p.mqClient.metrics.StoreProducerMessageReSent(p.mqClient.config.ConnectionName, p.exchange.name)
		logger.L().Info("unconfirmed messages resended", zap.Int("count", len(needResendingMessages)))

		for i := range needResendingMessages {
			err := p.SendMessage(needResendingMessages[i])
			if err != nil {
				logger.L().Error("Unable re-send message", zap.Error(err))
			}
		}
	}

	logger.L().Info("all messages confirmed")
}

func (l *ConfirmList) LoadAndDelete(k ConfirmKey) *ConfirmMessage {
	v, ok := l.m.Load(k)
	if ok {
		l.c.Add(1)
		cm := v.(*ConfirmMessage)
		return cm
	}

	return nil
}

func (l *ConfirmList) Delete(k ConfirmKey) {
	l.m.Delete(k)
}

func (l *ConfirmList) StoreMessage(k ConfirmKey, m *Message) *ConfirmMessage {
	cm := &ConfirmMessage{
		key:     k,
		message: m,
	}

	l.c.Add(1)
	l.m.Store(k, cm)

	return cm
}

func (l *ConfirmList) Range(f func(key, value interface{}) bool) {
	l.m.Range(f)
}

func (l *ConfirmList) Count() uint64 {
	return l.c.Load()
}
