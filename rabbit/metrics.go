package rabbitmq

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	connectionStatus            *prometheus.GaugeVec
	connectionClosed            *prometheus.CounterVec
	connectionReconnect         *prometheus.CounterVec
	channelStatus               *prometheus.GaugeVec
	channelClosed               *prometheus.CounterVec
	channelReconnect            *prometheus.CounterVec
	producerMessageSent         *prometheus.CounterVec
	producerMessageReSent       *prometheus.CounterVec
	producerMessageSentError    *prometheus.CounterVec
	producerMessagesConfirmed   *prometheus.CounterVec
	producerMessagesReturned    *prometheus.CounterVec
	producerMessagesWaitConfirm *prometheus.GaugeVec
}

func newMetrics(serviceName string) *Metrics {
	m := &Metrics{
		connectionStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Subsystem: serviceName,
				Name:      "rabbit_client_connection_status",
			},
			[]string{"connection_name"},
		),
		connectionClosed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: serviceName,
				Name:      "rabbit_client_connection_closed",
			},
			[]string{"connection_name"},
		),
		connectionReconnect: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: serviceName,
				Name:      "rabbit_client_connection_reconnect",
			},
			[]string{"connection_name"},
		),
		channelStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Subsystem: serviceName,
				Name:      "rabbit_channel_status",
			},
			[]string{"connection_name", "exchange_name"},
		),
		channelClosed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: serviceName,
				Name:      "rabbit_channel_closed",
			},
			[]string{"connection_name", "exchange_name"},
		),
		channelReconnect: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: serviceName,
				Name:      "rabbit_channel_reconnect",
			},
			[]string{"connection_name", "exchange_name"},
		),
		producerMessageSent: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: serviceName,
				Name:      "rabbit_producer_message_sent",
			},
			[]string{"connection_name", "exchange_name"},
		),
		producerMessageReSent: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: serviceName,
				Name:      "rabbit_producer_message_re_sent",
			},
			[]string{"connection_name", "exchange_name"},
		),
		producerMessageSentError: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: serviceName,
				Name:      "rabbit_producer_message_sent_error",
			},
			[]string{"connection_name", "exchange_name", "error_code"},
		),
		producerMessagesConfirmed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: serviceName,
				Name:      "rabbit_produce_message_confirmed",
			},
			[]string{"connection_name", "exchange_name"},
		),
		producerMessagesReturned: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: serviceName,
				Name:      "rabbit_produce_message_returned",
			},
			[]string{"connection_name", "exchange_name"},
		),
		producerMessagesWaitConfirm: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Subsystem: serviceName,
				Name:      "rabbit_producer_wait_confirm_messages",
			},
			[]string{"connection_name", "exchange_name"},
		),
	}

	return m
}

func (m *Metrics) StoreConnectionClose(connName string) {
	m.connectionClosed.WithLabelValues(connName).Add(1)
}

func (m *Metrics) StoreConnectionReconnect(connName string) {
	m.connectionReconnect.WithLabelValues(connName).Add(1)
}

func (m *Metrics) StoreChannelStatus(connName, exchangeName string, status float64) {
	m.channelStatus.WithLabelValues(connName, exchangeName).Set(status)
}

func (m *Metrics) StoreChannelReconnect(connName, exchangeName string) {
	m.channelReconnect.WithLabelValues(connName, exchangeName).Add(1)
}

func (m *Metrics) StoreChannelClose(connName, exchangeName string) {
	m.channelClosed.WithLabelValues(connName, exchangeName).Add(1)
}

func (m *Metrics) StoreProducerMessageSent(connName, exchangeName string) {
	m.producerMessageSent.WithLabelValues(connName, exchangeName).Add(1)
}

func (m *Metrics) StoreProducerMessageReSent(connName, exchangeName string) {
	m.producerMessageReSent.WithLabelValues(connName, exchangeName).Add(1)
}

func (m *Metrics) StoreProducerMessageSentError(connName, exchangeName, errorCode string) {
	m.producerMessageSentError.WithLabelValues(connName, exchangeName, errorCode).Add(1)
}

func (m *Metrics) StoreProducerMessageConfirm(connName, exchangeName string) {
	m.producerMessagesConfirmed.WithLabelValues(connName, exchangeName).Add(1)
}

func (m *Metrics) StoreProducerMessageReturned(connName, exchangeName string) {
	m.producerMessagesReturned.WithLabelValues(connName, exchangeName).Add(1)
}

func (m *Metrics) StoreProducerMessageWaitConfirm(connName, exchangeName string, count float64) {
	m.producerMessagesWaitConfirm.WithLabelValues(connName, exchangeName).Set(count)
}

func (m *Metrics) GetMetrics() []prometheus.Collector {
	collectors := []prometheus.Collector{
		m.connectionStatus,
		m.connectionClosed,
		m.connectionReconnect,
		m.channelStatus,
		m.channelClosed,
		m.channelReconnect,
		m.producerMessageSent,
		m.producerMessageReSent,
		m.producerMessageSentError,
		m.producerMessagesConfirmed,
		m.producerMessagesReturned,
		m.producerMessagesWaitConfirm,
	}

	return collectors
}
