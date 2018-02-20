package observer

import (
	"fmt"
	"reflect"

	"github.com/streadway/amqp"
)

type Event struct {
	d    amqp.Delivery
	Data interface{}
}

func (e Event) Commit() {
	e.Ack(false)
}

func (e Event) Ack(multiple bool) error {
	return e.d.Ack(multiple)
}

func (e Event) Reject(requeue bool) error {
	return e.d.Reject(requeue)
}

func (e Event) Nack(multiple, requeue bool) error {
	return e.d.Nack(multiple, requeue)
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
	done chan bool,
) {
	defer func() {
		o.ch.QueueUnbind(queueName, key, exchangeName, args)
		o.ch.QueueDelete(queueName, ifUnused, ifEmpty, noWait)
	}()
	for {
		select {
		case <-done:
			close(done)
			return
		case d := <-deliveryCh:
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
}

func (o *observer) Sub(exchangeName string,
	reply interface{},
	exchangeCfg *ExchangeConfig,
	queueCfg *QueueConfig,
	queueBindCfg *QueueBindConfig,
	consumeCfg *ConsumeConfig,
) (outCh <-chan Event, errCh <-chan error, doneCh chan<- bool) {
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
	outC := make(chan Event)
	outCh = outC
	errC := make(chan error)
	errCh = errC
	doneC := make(chan bool, 1)
	doneCh = doneC

	err := o.ch.ExchangeDeclare(exchangeName,
		exchangeCfg.Kind,
		exchangeCfg.Durable,
		exchangeCfg.AutoDelete,
		exchangeCfg.Internal,
		exchangeCfg.NoWait,
		exchangeCfg.Args,
	)
	if err != nil {
		errC <- fmt.Errorf("exchange declare err: %v", err)
		return
	}
	q, err := o.ch.QueueDeclare(queueCfg.Name,
		queueCfg.Durable,
		queueCfg.AutoDelete,
		queueCfg.Exclusive,
		queueCfg.NoWait,
		queueCfg.Args,
	)
	if err != nil {
		errC <- fmt.Errorf("queue declare err: %v", err)
		return
	}
	err = o.ch.QueueBind(q.Name,
		queueBindCfg.Key,
		exchangeName,
		queueBindCfg.NoWait,
		queueBindCfg.Args,
	)
	if err != nil {
		errC <- fmt.Errorf("queue bind err: %v", err)
		return
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
		errC <- fmt.Errorf("consume err: %v", err)
		return
	}

	go o.worker(
		exchangeName,
		q.Name,
		deliveryCh,
		outC,
		errC,
		reflect.Indirect(reflect.ValueOf(reply)).Interface(),
		queueBindCfg.Key,
		queueCfg.IfUnused,
		queueCfg.IfEmpty,
		queueCfg.NoWait,
		queueBindCfg.Args,
		doneC,
	)

	return
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
