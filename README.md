# Messaging

These packages provide functionality for working with messaging systems, specifically Kafka and RabbitMQ, for personal use.

## Packages

### Kafka Package

The kafka package is a wrapper library built on top of the [github.com/segmentio/kafka-go](https://github.com/segmentio/kafka-go) library. It provides an easy-to-use interface for interacting with Kafka messaging system. The package is battle-tested and includes its own logging and metrics capabilities, allowing you to effectively monitor and analyze the performance of your Kafka integration.

### Rabbit Package

The rabbit package serves as a foundation for working with the RabbitMQ messaging system. It leverages the [github.com/streadway/amqp](https://github.com/streadway/amqp) API and extends its functionality with custom logic for connection handling, message delivery confirmation, and fault tolerance. The package ensures that messages are reliably delivered, even in challenging scenarios such as Kubernetes or Docker disruptions or network issues. While the approach may seem unconventional, it has proven to be effective in guaranteeing message delivery.

## Personal Use

Please note that this repository and its packages are intended for personal use only. While they provide functionality for Kafka and RabbitMQ integration, they may not be suitable for production environments or large-scale deployments. Use them at your own discretion.

## Contributions

Contributions to this repository are not accepted at the moment, as it is meant for personal use only.

## License

This repository and its packages are provided under the [MIT License](LICENSE). Feel free to modify and adapt the code for your personal use.
