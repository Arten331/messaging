package kafka

import (
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap/zapcore"
)

/* Only for zap logger and debug mode*/

type zapQueueMessages []kafka.Message

func (q zapQueueMessages) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for i := range q {
		msg := logMessage(q[i])
		_ = encoder.AppendObject(&msg)
	}

	return nil
}

type logMessage kafka.Message

func (l *logMessage) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddByteString("key", l.Key)
	encoder.AddByteString("msg", l.Value)

	return nil
}
