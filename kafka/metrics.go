package kafka

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	messagesReceived *prometheus.CounterVec
	messagesSent     *prometheus.HistogramVec
	producerErrors   *prometheus.CounterVec
	consumerErrors   *prometheus.CounterVec
}

func newMetrics(service string) *Metrics {
	m := &Metrics{
		messagesReceived: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: service,
				Name:      "kafka_consumer_received",
			},
			[]string{},
		),
		messagesSent: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: service,
				Name:      "kafka_producer_sent",
			},
			[]string{},
		),
		producerErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: service,
				Name:      "kafka_producer_errors",
			},
			[]string{},
		),
		consumerErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: service,
				Name:      "kafka_consumer_errors",
			},
			[]string{},
		),
	}

	return m
}

func (m *Metrics) getProducerMetrics() []prometheus.Collector {
	collectors := []prometheus.Collector{
		m.messagesSent,
		m.producerErrors,
	}

	return collectors
}

func (m *Metrics) getConsumerMetrics() []prometheus.Collector {
	collectors := []prometheus.Collector{
		m.messagesReceived,
		m.consumerErrors,
	}

	return collectors
}

func (m *Metrics) StoreSentMessage(duration float64) {
	m.messagesSent.WithLabelValues().Observe(duration)
}

func (m *Metrics) StoreReceivedMessage() {
	m.messagesReceived.WithLabelValues().Inc()
}

func (m *Metrics) StoreProducerError() {
	m.producerErrors.WithLabelValues().Inc()
}

func (m *Metrics) StoreConsumerError() {
	m.consumerErrors.WithLabelValues().Inc()
}
