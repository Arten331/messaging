package rabbitmq

import (
	"sync"

	"github.com/Arten331/observability/logger"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var ErrProducerClosed = errors.New("client closed, messages cannot be sent")

type QueueableMessage interface {
	AMQPMessage() (*Message, error)
}

type ProducerOptions struct {
	Exchange *Exchange
	// ConfirmMode Осторожно! Данный режим подверждает отправку каждого сообщения, но может продублировать их в очереди
	// ConfirmMode Carefully! This mode confirms the sending of each message, but may duplicate them in the queue
	ConfirmMode bool
}

type Producer struct {
	mqClient       *RabbitMQ
	channelManager *ChannelManager
	exchange       *Exchange
	mu             sync.RWMutex

	returnChannel    chan amqp.Return
	publishedChannel chan amqp.Confirmation
	disablePublish   bool

	// confirm sent block
	waitConfirm        bool
	waitingConfirmList ConfirmList
	channelSent        atomic.Uint64
}

func NewProducer(o ProducerOptions, mqClient *RabbitMQ) (*Producer, error) {
	p := Producer{
		exchange: o.Exchange,
		mqClient: mqClient,

		returnChannel:    make(chan amqp.Return, 1),
		publishedChannel: make(chan amqp.Confirmation, 1),

		waitConfirm: o.ConfirmMode,
		waitingConfirmList: ConfirmList{
			atomic.Uint64{},
			sync.Map{},
		},
	}

	return &p, nil
}

func (p *Producer) SendMessage(m *Message) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	err := p.publish(m)
	if err != nil {
		logger.L().Debug("failed send message", zap.Error(err), zap.Reflect("msg", m))

		return err
	}

	p.mqClient.messagesWG.Add(1)

	logger.L().Debug("rabbit message sent", zap.Object("msg", m))

	return nil
}

func (p *Producer) Run() {
	err := p.setup()
	if err != nil {
		return
	}
}

func (p *Producer) publish(m *Message) error {
	if p.disablePublish {
		logger.L().Debug(ErrProducerClosed.Error(), zap.Object("msg", m))

		return ErrProducerClosed
	}

	err := p.channelManager.Channel().Publish(m.exchange, m.route, false, false, amqp.Publishing{
		ContentType:  m.contentType,
		DeliveryMode: m.deliveryMode,
		Body:         m.body,
	})
	if err != nil {
		p.mqClient.metrics.StoreProducerMessageSentError(p.mqClient.config.ConnectionName, p.exchange.name, err.Error())

		return err
	}

	p.mqClient.metrics.StoreProducerMessageSent(p.mqClient.config.ConnectionName, p.exchange.name)

	if p.waitConfirm {
		chSent := p.channelSent.Add(1)

		key := ConfirmKey{
			reconnect:   p.channelManager.reconnectionCount.Load(),
			deliveryTag: chSent,
		}

		p.waitingConfirmList.StoreMessage(key, m)
	}

	return err
}

func (p *Producer) setup() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	channel, err := NewChannel(p.mqClient, p.exchange)
	if err != nil {
		return errors.Wrap(err, "filed to open new channel")
	}

	p.channelManager = channel

	// declare exchange if need
	err = p.exchange.Declare(channel.channel)
	if err != nil {
		return err
	}

	go p.handleChannelRestart()
	go p.handleStopService()

	return nil
}

func (p *Producer) handleStopService() {
	<-p.mqClient.Subscribe(KeyClose)

	p.mu.Lock()

	logger.L().Debug("client closed, stop producer", zap.String("exchange", p.exchange.name))

	p.disablePublish = true
}

func (p *Producer) handleChannelRestart() {
	go p.watchMessages()
	go p.handlePublishedOrReturned()

	for {
		select {
		case err := <-p.channelManager.notifyClosed:
			p.mu.Lock()

			logger.L().Info("channel try reconnect, stop sending messages", zap.Error(err))
		case <-p.channelManager.notifyReconnected:
			p.channelSent.Store(0)

			p.mu.Unlock()

			logger.L().Info("producer channel reconnected, run new notifiers")

			go p.checkConfirmMessage()
			go p.watchMessages()
			go p.handlePublishedOrReturned()
		}
	}
}
