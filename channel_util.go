package observer

import "github.com/streadway/amqp"

func channelPublish(channel *amqp.Channel, exchangeName string, cfg *PublishConfig, msg amqp.Publishing) error {
	return channel.Publish(
		exchangeName,
		cfg.Key,
		cfg.Mandatory,
		cfg.Immediate,
		msg,
	)
}

func channelConsume(channel *amqp.Channel, queueName string, consumeCfg *ConsumeConfig) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		queueName,
		consumeCfg.Consumer,
		consumeCfg.AutoAck,
		consumeCfg.Exclusive,
		consumeCfg.NoLocal,
		consumeCfg.NoWait,
		consumeCfg.Args,
	)
}

func channelExchangeDeclare(channel *amqp.Channel, exchangeName string, exchangeCfg *ExchangeConfig) error {
	return channel.ExchangeDeclare(
		exchangeName,
		exchangeCfg.Kind,
		exchangeCfg.Durable,
		exchangeCfg.AutoDelete,
		exchangeCfg.Internal,
		exchangeCfg.NoWait,
		exchangeCfg.Args,
	)
}

func channelQueueDeclare(channel *amqp.Channel, queueCfg *QueueConfig) (amqp.Queue, error) {
	return channel.QueueDeclare(
		queueCfg.Name,
		queueCfg.Durable,
		queueCfg.AutoDelete,
		queueCfg.Exclusive,
		queueCfg.NoWait,
		queueCfg.Args,
	)
}

func channelQueueBind(channel *amqp.Channel, queueName, exchangeName string, queueBindCfg *QueueBindConfig) error {
	return channel.QueueBind(
		queueName,
		queueBindCfg.Key,
		exchangeName,
		queueBindCfg.NoWait,
		queueBindCfg.Args,
	)
}
