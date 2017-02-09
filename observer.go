package observer

import (
	"log"
	"reflect"

	"github.com/streadway/amqp"
)

// Codec creates a CodecRequest to process each request.
type Codec interface {
	NewRequest(req Request) CodecRequest
	NewResponse(data interface{}) CodecResponse
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
	Sub(name string, chType interface{}) (OutCh, error)
	Pub(name string, data interface{}) error
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

func (o *observer) eachDelivery(deliveryCh <-chan amqp.Delivery, outCh OutCh, chType interface{}) {
	for d := range deliveryCh {
		data := reflect.New(reflect.TypeOf(chType))
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

func (o *observer) waitPub(name string, queueName string, deliveryCh <-chan amqp.Delivery, outCh OutCh, chType interface{}) {
	defer func() {
		o.ch.QueueUnbind(queueName, "", name, nil)
		o.ch.QueueDelete(queueName, false, false, false)
		close(outCh)
	}()
	o.eachDelivery(deliveryCh, outCh, chType)
}

func (o *observer) Sub(name string, chType interface{}) (OutCh, error) {
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
	go o.waitPub(name, q.Name, deliveryCh, outCh, chType)
	return outCh, nil
}

func (o *observer) Pub(name string, data interface{}) error {
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
		ContentType: "application/json",
		Body:        dataBytes,
	})
	if err != nil {
		return err
	}
	return nil
}
