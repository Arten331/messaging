package rabbitmq

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Exchange struct {
	name       string
	kind       string // direct
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
}

func NewExchange(name, kind string, durable, autoDelete, internal, noWait bool) (Exchange, error) {
	exchange := Exchange{
		name:       name,
		kind:       kind,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		noWait:     noWait,
	}

	return exchange, nil // TODO: Check parameters
}

func (e *Exchange) Declare(channel *amqp.Channel) error {
	if e.name != "" {
		err := channel.ExchangeDeclare(
			e.name,
			e.kind,
			e.durable,
			e.autoDelete,
			e.internal,
			e.noWait,
			nil,
		)

		if err != nil {
			return errors.Wrap(err, "unable to declare exchange")
		}
	}

	return nil
}
