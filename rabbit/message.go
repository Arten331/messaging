package rabbitmq

import (
	"github.com/Arten331/observability/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Message struct {
	body         []byte
	deliveryMode uint8
	contentType  string
	exchange     string
	route        string
}

func NewMessage(body []byte, deliveryMode uint8, contentType, route string, exchange string) Message {
	if contentType == "" {
		contentType = "text/plain"
	}

	if deliveryMode > 2 {
		logger.L().Error("Wrong delivery mode, expected 0,1,2(Persistent)", zap.Uint8("delivery mode", deliveryMode))
		deliveryMode = 2
	}

	return Message{
		body:         body,
		deliveryMode: deliveryMode,
		contentType:  contentType,
		route:        route,
		exchange:     exchange,
	}
}

func (m Message) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddByteString("body", m.body)
	e.AddString("route", m.route)

	return nil
}
