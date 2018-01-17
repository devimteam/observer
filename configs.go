package observer

import "github.com/streadway/amqp"

type ExchangeConfig struct {
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

func DefaultExchangeConfig() *ExchangeConfig {
	return &ExchangeConfig{
		Kind:       "fanout",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	IfUnused   bool
	IfEmpty    bool
	Args       amqp.Table
}

func DefaultQueueConfig() *QueueConfig {
	return &QueueConfig{
		Name:       "queue" + genRandomString(15),
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		IfUnused:   false,
		IfEmpty:    false,
		Args:       nil,
	}
}

type QueueBindConfig struct {
	Key    string
	NoWait bool
	Args   amqp.Table
}

func DefaultQueueBindConfig() *QueueBindConfig {
	return &QueueBindConfig{
		Key:    "",
		NoWait: false,
		Args:   nil,
	}
}

type ConsumeConfig struct {
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

func DefaultConsumeConfig() *ConsumeConfig {
	return &ConsumeConfig{
		Consumer:  "",
		AutoAck:   false,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
		Args:      nil,
	}
}

type PublishConfig struct {
	Key       string
	Mandatory bool
	Immediate bool
}

func DefaultPublishConfig() *PublishConfig {
	return &PublishConfig{
		Key:       "",
		Immediate: false,
		Mandatory: false,
	}
}
