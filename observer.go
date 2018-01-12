package observer

import (
	"fmt"
	"reflect"

	"github.com/streadway/amqp"
)

// Codec creates a CodecRequest to process each request.
type Codec interface {
	NewRequest(req Request) CodecRequest
	NewResponse(data interface{}) CodecResponse
	ContentType() string
}

// CodecRequest decodes a request and encodes a response using a specific
// serialization scheme.
type CodecRequest interface {
	// Reads the request filling the RPC method args.
	ReadRequest(i interface{}) error
}

type CodecResponse interface {
	Body() ([]byte, error)
}

type Event struct {
	d    amqp.Delivery
	Data interface{}
}

func (e Event) Commit() {
	e.d.Ack(false)
}

type Observer interface {
	Sub(service string,
		reply interface{},
		config *ExchangeConfig,
		queueConfig *QueueConfig,
		bindConfig *QueueBindConfig,
		consumeConfig *ConsumeConfig,
	) (<-chan Event, <-chan error, error)
	Pub(service string,
		data interface{},
		config *ExchangeConfig,
		publishConfig *PublishConfig,
	) error
}

type observer struct {
	ch    *amqp.Channel
	codec Codec
}

func New(channel *amqp.Channel, codec Codec) Observer {
	return &observer{
		ch:    channel,
		codec: codec,
	}
}

func (o *observer) worker(exchangeName string,
	queueName string,
	deliveryCh <-chan amqp.Delivery,
	outCh chan<- Event,
	errorCh chan<- error,
	reply interface{},
	key string,
	ifUnused bool,
	ifEmpty bool,
	noWait bool,
	args amqp.Table,
) {
	defer func() {
		o.ch.QueueUnbind(queueName, key, exchangeName, args)
		o.ch.QueueDelete(queueName, ifUnused, ifEmpty, noWait)
	}()
	for d := range deliveryCh {
		if d.ContentType != o.codec.ContentType() {
			continue
		}

		data := reflect.New(reflect.TypeOf(reply))
		req := o.codec.NewRequest(
			&request{d: d},
		)
		err := req.ReadRequest(data.Interface())

		if err != nil {
			errorCh <- err
		}
		outCh <- Event{
			d:    d,
			Data: data.Interface(),
		}
	}
}

func (o *observer) Sub(exchangeName string,
	reply interface{},
	exchangeCfg *ExchangeConfig,
	queueCfg *QueueConfig,
	queueBindCfg *QueueBindConfig,
	consumeCfg *ConsumeConfig,
) (<-chan Event, <-chan error, error) {
	if exchangeCfg == nil {
		exchangeCfg = DefaultExchangeConfig()
	}
	if queueCfg == nil {
		queueCfg = DefaultQueueConfig()
	}
	if queueBindCfg == nil {
		queueBindCfg = DefaultQueueBindConfig()
	}
	if consumeCfg == nil {
		consumeCfg = DefaultConsumeConfig()
	}

	err := o.ch.ExchangeDeclare(exchangeName,
		exchangeCfg.Kind,
		exchangeCfg.Durable,
		exchangeCfg.AutoDelete,
		exchangeCfg.Internal,
		exchangeCfg.NoWait,
		exchangeCfg.Args,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("exchange declare err: %v", err)
	}
	q, err := o.ch.QueueDeclare(queueCfg.Name,
		queueCfg.Durable,
		queueCfg.AutoDelete,
		queueCfg.Exclusive,
		queueCfg.NoWait,
		queueCfg.Args,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("queue declare err: %v", err)
	}
	err = o.ch.QueueBind(q.Name,
		queueBindCfg.Key,
		exchangeName,
		queueBindCfg.NoWait,
		queueBindCfg.Args,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("queue bind err: %v", err)
	}
	deliveryCh, err := o.ch.Consume(q.Name,
		consumeCfg.Consumer,
		consumeCfg.AutoAck,
		consumeCfg.Exclusive,
		consumeCfg.NoLocal,
		consumeCfg.NoWait,
		consumeCfg.Args,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("consume err: %v", err)
	}
	outCh := make(chan Event)
	errCh := make(chan error)

	go o.worker(
		exchangeName,
		q.Name,
		deliveryCh,
		outCh,
		errCh,
		reflect.Indirect(reflect.ValueOf(reply)).Interface(),
		queueBindCfg.Key,
		queueCfg.IfUnused,
		queueCfg.IfEmpty,
		queueCfg.NoWait,
		queueBindCfg.Args,
	)

	return outCh, errCh, nil
}

func (o *observer) Pub(exchangeName string,
	data interface{},
	exchangeCfg *ExchangeConfig,
	publishCfg *PublishConfig,
) error {
	if exchangeCfg == nil {
		exchangeCfg = DefaultExchangeConfig()
	}
	if publishCfg == nil {
		publishCfg = DefaultPublishConfig()
	}

	err := o.ch.ExchangeDeclare(exchangeName,
		exchangeCfg.Kind,
		exchangeCfg.Durable,
		exchangeCfg.AutoDelete,
		exchangeCfg.Internal,
		exchangeCfg.NoWait,
		exchangeCfg.Args,
	)
	if err != nil {
		return fmt.Errorf("exchange declare err: %v", err)
	}

	resp := o.codec.NewResponse(data)
	dataBytes, err := resp.Body()
	if err != nil {
		return err
	}
	err = o.ch.Publish(exchangeName,
		publishCfg.Key,
		publishCfg.Mandatory,
		publishCfg.Immediate,
		amqp.Publishing{
			ContentType: o.codec.ContentType(),
			Body:        dataBytes,
		},
	)
	if err != nil {
		return fmt.Errorf("publish err: %v", err)
	}
	return nil
}
