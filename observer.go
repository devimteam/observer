package observer

import (
	"fmt"
	"log"
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

type OutCh chan Event

type Event struct {
	d    amqp.Delivery
	Data interface{}
}

func (e Event) Commit() {
	e.d.Ack(false)
}

type Observer interface {
	Sub(service string, reply interface{}) (OutCh, error)
	Pub(service string, data interface{}) error
}

type observer struct {
	ch      *amqp.Channel
	codec   Codec
	workers int
}

func New(channel *amqp.Channel, codec Codec, workers int) Observer {
	return &observer{
		ch:      channel,
		codec:   codec,
		workers: workers,
	}
}

func (o *observer) makeExchangeName(service, i interface{}) string {
	return fmt.Sprintf("event.%s.%s", service, reflect.Indirect(
		reflect.ValueOf(i)).Type().Name(),
	)
}

func (o *observer) worker(name string, queueName string, deliveryCh <-chan amqp.Delivery, outCh OutCh, reply interface{}) {
	defer func() {
		o.ch.QueueUnbind(queueName, "", name, nil)
		o.ch.QueueDelete(queueName, false, false, false)
		close(outCh)
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
			log.Println(err)
		}
		outCh <- Event{
			d:    d,
			Data: data.Interface(),
		}
	}
}

func (o *observer) Sub(service string, reply interface{}) (OutCh, error) {
	name := o.makeExchangeName(service, reply)

	err := o.ch.ExchangeDeclare(name, "fanout", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	q, err := o.ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		return nil, err
	}
	err = o.ch.QueueBind(q.Name, "", name, false, nil)
	if err != nil {
		return nil, err
	}
	deliveryCh, err := o.ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	outCh := make(OutCh)

	for w := 1; w <= o.workers; w++ {
		go o.worker(name, q.Name, deliveryCh, outCh, reflect.Indirect(reflect.ValueOf(reply)).Interface())
	}

	return outCh, nil
}

func (o *observer) Pub(service string, data interface{}) error {
	name := o.makeExchangeName(service, data)
	log.Println(name)
	err := o.ch.ExchangeDeclare(name, "fanout", true, false, false, false, nil)
	if err != nil {
		return err
	}

	resp := o.codec.NewResponse(data)
	dataBytes, err := resp.Body()
	if err != nil {
		return err
	}
	err = o.ch.Publish(name, "", false, false, amqp.Publishing{
		ContentType: o.codec.ContentType(),
		Body:        dataBytes,
	})
	if err != nil {
		return err
	}
	return nil
}
